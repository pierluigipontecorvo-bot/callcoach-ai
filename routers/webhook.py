"""
Acuity Scheduling webhook receiver + background analysis pipeline.

14-step semaphore pipeline:
  1  webhook     — received & validated
  2  firma       — HMAC signature
  3  acuity      — appointment fetched
  4  form        — form fields extracted
  5  etichetta   — label extracted
  6  data        — appointment date parsed
  7  campagna    — campaign identified
  8  operatore   — operator identified
  9  sidial      — lead(s) found on Sidial
  10 download    — recordings downloaded
  11 trascrizione — transcription
  12 analisi     — AI analysis
  13 salvataggio — saved to DB
  14 email       — report sent
"""

import asyncio
import json
import logging
from urllib.parse import parse_qs

from fastapi import APIRouter, BackgroundTasks, HTTPException, Request

from services.acuity import (
    check_webhook_signature,
    extract_form_fields,
    extract_label,
    extract_phone,
    extract_piva,
    extract_ragione_sociale,
    find_operator_email,
    get_appointment,
    get_operator_display,
    should_analyze,
)
from services.ai_analysis import analyze_call
from services.campaign_db import get_campaign_by_code
from services.campaign_parser import parse_campaign_code
from services.email_service import generate_html_report, send_analysis_report
from services.sidial import find_and_download_all_recordings
from services.transcription import transcribe_audio
from utils.helpers import parse_iso_datetime

router = APIRouter(prefix="/webhook", tags=["webhook"])
logger = logging.getLogger(__name__)


# ── DB helpers ─────────────────────────────────────────────────────────────────

async def _create_processing_record(appointment_id: str, acuity_account: int) -> int:
    """Create an Analysis record with status=processing and return its id."""
    from database import AsyncSessionLocal
    from models import Analysis

    async with AsyncSessionLocal() as session:
        obj = Analysis(
            appointment_id=str(appointment_id),
            acuity_account=acuity_account,
            processing_status="processing",
            progress=0,
            step_message="Avvio pipeline...",
            pipeline_steps={},
        )
        session.add(obj)
        await session.commit()
        await session.refresh(obj)
        return obj.id


async def _update_progress(analysis_id: int, progress: int, message: str):
    from database import AsyncSessionLocal
    from models import Analysis

    async with AsyncSessionLocal() as session:
        obj = await session.get(Analysis, analysis_id)
        if obj:
            obj.progress = progress
            obj.step_message = message
            await session.commit()


async def _update_initial_info(
    analysis_id: int,
    campaign_code: str,
    operator_name: str,
    appointment_dt,
):
    """Save identifying info early so UI can display it during processing."""
    from database import AsyncSessionLocal
    from models import Analysis

    async with AsyncSessionLocal() as session:
        obj = await session.get(Analysis, analysis_id)
        if obj:
            obj.campaign_code = campaign_code
            obj.operator_name = operator_name or None
            obj.appointment_datetime = appointment_dt
            await session.commit()


async def _save_analysis(
    *,
    analysis_id: int,
    appointment_id: str,
    campaign_code: str,
    transcript: str,
    report: dict,
    html_report: str,
    acuity_account: int,
    appointment_dt,
    phone: str,
    client_company: str,
    sidial_call_id: str,
    operator_name: str,
    operator_email: str = "",
    qualification_level: str,
    email_sent: bool = False,
):
    from database import AsyncSessionLocal
    from models import Analysis

    async with AsyncSessionLocal() as session:
        obj = await session.get(Analysis, analysis_id)
        if obj is None:
            obj = Analysis(appointment_id=appointment_id, acuity_account=acuity_account)
            session.add(obj)
        obj.campaign_code = campaign_code
        obj.appointment_datetime = appointment_dt
        obj.client_phone = phone
        obj.client_company = client_company or None
        obj.operator_name = operator_name
        obj.operator_email = operator_email or None
        obj.acuity_account = acuity_account
        obj.acuity_label = "PRESO"
        obj.sidial_call_id = sidial_call_id
        obj.transcript = transcript
        obj.qualification_level = qualification_level
        obj.report_json = report
        obj.report_html = html_report
        obj.email_sent = email_sent
        obj.processing_status = "completed"
        obj.progress = 100
        obj.step_message = "Completata"
        await session.commit()


async def _save_error(analysis_id: int | None, appointment_id: str, error_msg: str, acuity_account: int):
    from database import AsyncSessionLocal
    from models import Analysis

    async with AsyncSessionLocal() as session:
        obj = await session.get(Analysis, analysis_id) if analysis_id else None
        if obj is None:
            obj = Analysis(
                appointment_id=str(appointment_id),
                acuity_account=acuity_account,
            )
            session.add(obj)
        obj.processing_status = "error"
        obj.progress = 0
        obj.step_message = "Errore"
        obj.error_message = error_msg[:2000]
        await session.commit()


# ── Full pipeline ──────────────────────────────────────────────────────────────

async def run_analysis_pipeline(
    appointment_data: dict,
    acuity_account: int,
    engine_override: str | None = None,
):
    """
    14-step analysis pipeline with semaphore status tracking.

    engine_override: if provided, bypasses both global and campaign engine settings.
    """
    from services.pipeline import update_step, init_steps
    from services.operator_service import identify_operator
    from services.settings_service import get_setting
    from database import AsyncSessionLocal
    from models import Analysis
    from sqlalchemy import select

    appointment_id = str(appointment_data.get("id", "unknown"))
    logger.info("[%s] Pipeline started (account=%d)", appointment_id, acuity_account)

    # ── 0. Parse campaign code — pre-flight before creating any DB record ─────
    appointment_type = appointment_data.get("type", "")
    campaign_info = parse_campaign_code(appointment_type)

    if not campaign_info.get("valid"):
        logger.warning("[%s] Unparseable campaign code: %r — skip", appointment_id, appointment_type)
        return

    # ── 0b. Check campaign exists in DB — silent skip if not configured ───────
    _campaign_pre = None
    try:
        _campaign_pre = await get_campaign_by_code(campaign_info["raw"])
    except Exception as _exc:
        logger.warning("[%s] Could not check campaign in DB: %s — proceeding anyway", appointment_id, _exc)

    if _campaign_pre is None:
        logger.info(
            "[%s] Campagna '%s' non configurata in DB — analisi saltata.",
            appointment_id, campaign_info["raw"],
        )
        return

    # ── Create analysis record immediately (visible in UI) ────────────────────
    analysis_id = await _create_processing_record(appointment_id, acuity_account)
    await init_steps(analysis_id)

    # Mark steps 1-3 as ok (they happened before pipeline was invoked)
    await update_step(analysis_id, 1, "ok", "Webhook ricevuto")
    await update_step(analysis_id, 2, "ok", "Firma valida")
    await update_step(analysis_id, 3, "ok", f"Appuntamento {appointment_id} recuperato da Acuity")

    # ── STEP 4: Extract and save form fields ──────────────────────────────────
    await update_step(analysis_id, 4, "running", "Lettura form fields...")
    form_fields = extract_form_fields(appointment_data)
    logger.info("[%s] Form fields: %d campi: %s", appointment_id, len(form_fields), list(form_fields.keys()))

    # Extract P.IVA and Ragione Sociale from form fields
    piva = None
    ragione_sociale = None
    for fname, fval in form_fields.items():
        fl = fname.lower()
        if any(kw in fl for kw in ("partita iva", "p.iva", "piva", " pi ")):
            piva = str(fval).strip() if fval else None
        if any(kw in fl for kw in ("ragione", "ragione sociale")):
            ragione_sociale = str(fval).strip() if fval else None

    # Fallback to existing extract functions
    if not piva:
        piva = extract_piva(appointment_data) or None
    if not ragione_sociale:
        ragione_sociale = extract_ragione_sociale(appointment_data) or None

    # ── STEP 5: Extract label ─────────────────────────────────────────────────
    await update_step(analysis_id, 5, "running", "Lettura etichetta...")
    label_name, label_color = extract_label(appointment_data)

    # Save form fields + label to DB
    async with AsyncSessionLocal() as _sess:
        async with _sess.begin():
            _a = await _sess.get(Analysis, analysis_id)
            if _a:
                _a.acuity_form_fields = form_fields
                _a.label_name = label_name or None
                _a.label_color = label_color or None

    if label_name:
        await update_step(analysis_id, 5, "ok", f"Etichetta: {label_name} ({label_color})")
    else:
        await update_step(analysis_id, 5, "warning", "Nessuna etichetta sull'appuntamento")

    if piva and ragione_sociale:
        await update_step(analysis_id, 4, "ok", f"P.IVA: {piva} · Ragione Sociale: {ragione_sociale}")
    elif piva or ragione_sociale:
        await update_step(analysis_id, 4, "warning", f"Trovato solo: {'P.IVA' if piva else 'Ragione Sociale'}")
    else:
        await update_step(analysis_id, 4, "warning", "Nessun dato fiscale — uso fallback lastName")

    # ── STEP 6: Parse appointment date ────────────────────────────────────────
    await update_step(analysis_id, 6, "running", "Verifica data appuntamento...")
    appointment_dt_str = appointment_data.get("datetime", "")
    appointment_dt = parse_iso_datetime(appointment_dt_str) if appointment_dt_str else None
    if appointment_dt:
        await update_step(analysis_id, 6, "ok", f"Data: {appointment_dt.strftime('%d/%m/%Y %H:%M')}")
    else:
        await update_step(analysis_id, 6, "warning", "Data appuntamento non parseable")

    # ── STEP 7: Identify campaign ─────────────────────────────────────────────
    await update_step(analysis_id, 7, "running", "Verifica campagna...")
    # Reuse _campaign_pre already loaded above — refresh it
    try:
        _refreshed = await get_campaign_by_code(campaign_info["raw"])
        if _refreshed is not None:
            campaign_db = _refreshed
        else:
            campaign_db = _campaign_pre
    except Exception as exc:
        logger.warning("[%s] Could not refresh campaign: %s", appointment_id, exc)
        campaign_db = _campaign_pre

    await update_step(analysis_id, 7, "ok", f"Campagna '{campaign_db.code}' trovata")

    # ── Extract phone ─────────────────────────────────────────────────────────
    phone = extract_phone(appointment_data)
    last_name = appointment_data.get("lastName", "")

    # Save initial info for UI display
    _initial_op_name = get_operator_display(appointment_data)
    await _update_initial_info(
        analysis_id,
        campaign_code=campaign_info["raw"],
        operator_name=_initial_op_name,
        appointment_dt=appointment_dt,
    )

    # ── STEP 8: Identify operator ─────────────────────────────────────────────
    await update_step(analysis_id, 8, "running", "Identificazione operatore...")
    email_field = find_operator_email(appointment_data) or appointment_data.get("email", "")
    op_info = await identify_operator(email_field, form_fields)

    operator_email = op_info["email"] or ""
    operator_display = op_info["display_name"] or _initial_op_name

    if op_info["warning"]:
        await update_step(analysis_id, 8, "warning", op_info["warning"])
    else:
        await update_step(analysis_id, 8, "ok", f"Operatore #{op_info['number']} — {operator_display}")

    # Save operator info
    async with AsyncSessionLocal() as _sess:
        async with _sess.begin():
            _a = await _sess.get(Analysis, analysis_id)
            if _a:
                _a.operator_name = operator_display or None
                _a.operator_email = operator_email or None
                _a.client_phone = phone or None
                _a.client_company = ragione_sociale or None
                _a.appointment_datetime = appointment_dt
                _a.campaign_code = campaign_info["raw"]

    # ── Guard: phone required ─────────────────────────────────────────────────
    if not phone:
        msg = "Numero di telefono assente nell'appuntamento Acuity — impossibile cercare su Sidial"
        logger.error("[%s] %s", appointment_id, msg)
        await update_step(analysis_id, 9, "stop", msg)
        await _save_error(analysis_id, appointment_id, msg, acuity_account)
        return

    # ── STEP 9: Search Sidial ─────────────────────────────────────────────────
    await update_step(analysis_id, 9, "running", "Ricerca lead su Sidial...")

    try:
        lookback = int(await get_setting("sidial_lookback_days", "90"))
        min_secs = int(await get_setting("min_call_length_seconds", "20"))
        retry_count = int(await get_setting("sidial_retry_count", "5"))
        retry_wait = int(await get_setting("sidial_retry_wait_seconds", "180"))
    except Exception as exc:
        logger.warning("[%s] Could not read settings: %s — using defaults", appointment_id, exc)
        lookback, min_secs, retry_count, retry_wait = 90, 20, 5, 180

    try:
        recordings, sidial_stats = await find_and_download_all_recordings(
            phone=phone,
            campaign_code=campaign_info.get("raw"),
            lookback_days=lookback,
            piva=piva or "",
            ragione_sociale=ragione_sociale or "",
            last_name=last_name if not form_fields else "",
            min_call_seconds=min_secs,
            return_stats=True,
        )
    except Exception as exc:
        msg = f"Errore Sidial: {exc}"
        logger.error("[%s] %s", appointment_id, msg, exc_info=True)
        await update_step(analysis_id, 9, "stop", msg)
        await _save_error(analysis_id, appointment_id, msg, acuity_account)
        return

    stats_msg = (
        f"{sidial_stats['leads_found']} lead · "
        f"{sidial_stats['total_recs']} rec totali · "
        f"{sidial_stats['recent_recs']} recenti · "
        f"{sidial_stats['converting_recs']} in conversione"
    )

    if sidial_stats["leads_found"] == 0:
        await update_step(analysis_id, 9, "stop", f"Nessun lead trovato — {stats_msg}")
        await _save_error(analysis_id, appointment_id, "Nessun lead trovato su Sidial", acuity_account)
        return
    elif sidial_stats.get("search_params_used", 3) < 2:
        await update_step(analysis_id, 9, "warning", f"Lead trovati con parametri parziali — {stats_msg}")
    else:
        await update_step(analysis_id, 9, "ok", stats_msg)

    # ── STEP 10: Download recordings ──────────────────────────────────────────
    await update_step(analysis_id, 10, "running", "Download registrazioni...")

    # For old appointments (>1 day ago) recordings are already converted — no retry
    from datetime import datetime as _dt, timezone as _tz
    _is_old_appointment = (
        appointment_dt is not None and
        (_dt.now(_tz.utc) - appointment_dt.astimezone(_tz.utc)).days >= 1
    )

    if not recordings:
        # Try to wait for converting recordings — ONLY for today's appointments
        if sidial_stats["converting_recs"] > 0 and not _is_old_appointment:
            for attempt in range(1, retry_count + 1):
                await update_step(
                    analysis_id, 10, "running",
                    f"Registrazioni in conversione — attendo {retry_wait // 60} min "
                    f"(tentativo {attempt}/{retry_count})",
                )
                await asyncio.sleep(retry_wait)

                try:
                    recordings, sidial_stats = await find_and_download_all_recordings(
                        phone=phone,
                        campaign_code=campaign_info.get("raw"),
                        lookback_days=lookback,
                        piva=piva or "",
                        ragione_sociale=ragione_sociale or "",
                        last_name=last_name if not form_fields else "",
                        min_call_seconds=min_secs,
                        return_stats=True,
                    )
                except Exception as exc:
                    logger.warning("[%s] Retry %d/%d failed: %s", appointment_id, attempt, retry_count, exc)
                    continue

                if recordings:
                    break

        if not recordings:
            msg = f"0 registrazioni scaricabili dopo tutti i tentativi · {stats_msg}"
            await update_step(analysis_id, 10, "stop", msg,
                              detail={"can_upload_audio": True, "can_upload_transcript": True})
            await _save_error(analysis_id, appointment_id, msg, acuity_account)
            return

    n_recs = len(recordings)
    total_secs = sidial_stats.get("total_seconds", 0)
    mins = total_secs // 60
    secs = total_secs % 60
    await update_step(analysis_id, 10, "ok", f"{n_recs} registrazioni · {mins}m {secs:02d}s parlato")

    # Save recording stats
    async with AsyncSessionLocal() as _sess:
        async with _sess.begin():
            _a = await _sess.get(Analysis, analysis_id)
            if _a:
                _a.num_recordings = n_recs
                _a.total_talk_seconds = total_secs
                _a.sidial_call_id = ",".join(rec_id for rec_id, _ in recordings)

    # ── STEP 11: Transcription ────────────────────────────────────────────────
    await update_step(analysis_id, 11, "running", "Trascrizione in corso...")

    _campaign_engine = campaign_db.transcription_engine if campaign_db.transcription_engine else None
    global_engine = await get_setting("transcription_engine", "openai")
    _engine = engine_override or _campaign_engine or global_engine

    transcript_parts = []
    for idx, (call_id, audio_bytes) in enumerate(recordings, start=1):
        await update_step(
            analysis_id, 11, "running",
            f"Trascrizione chiamata {idx}/{n_recs} (motore: {_engine})...",
        )
        try:
            logger.info(
                "[%s] Trascrizione chiamata %d/%d (call_id=%s, %d bytes, engine=%s) …",
                appointment_id, idx, n_recs, call_id, len(audio_bytes), _engine,
            )
            part = await transcribe_audio(audio_bytes, engine=_engine)
            transcript_parts.append(f"--- CHIAMATA {idx} (id: {call_id}) ---\n{part}")
            logger.info("[%s] Chiamata %d: %d caratteri", appointment_id, idx, len(part))
        except Exception as exc:
            logger.error("[%s] Trascrizione fallita per call_id=%s: %s", appointment_id, call_id, exc)
            transcript_parts.append(f"--- CHIAMATA {idx} (id: {call_id}) ---\n[trascrizione non disponibile]")

    transcript = "\n\n".join(transcript_parts)
    logger.info("[%s] Trascrizione totale: %d caratteri", appointment_id, len(transcript))

    _all_unavailable = all("[trascrizione non disponibile]" in p for p in transcript_parts)
    _too_short = len(transcript.replace("\n", "").strip()) < 80

    if _all_unavailable or _too_short:
        msg = f"Trascrizione troppo breve ({len(transcript)} char) — errore tecnico"
        await update_step(analysis_id, 11, "stop", msg)
        # Save transcript and mark as errore tecnico
        async with AsyncSessionLocal() as _sess:
            async with _sess.begin():
                _obj = await _sess.get(Analysis, analysis_id)
                if _obj:
                    _obj.processing_status = "completed"
                    _obj.progress = 100
                    _obj.step_message = "Completata (errore tecnico)"
                    _obj.qualification_level = "errore_tecnico"
                    _obj.transcript = transcript
                    _obj.report_json = {
                        "errore_tecnico": True,
                        "qualificazione": {
                            "rating": 1, "label": "INSUFFICIENTE",
                            "fuori_parametro": False,
                            "spiegazione": "Analisi non valida — errore tecnico di trascrizione.",
                            "parametri_verificati": [], "parametri_mancanti": [],
                        },
                    }
        return

    await update_step(analysis_id, 11, "ok", f"{len(transcript)} caratteri trascritti · motore: {_engine}")

    # Save transcript
    async with AsyncSessionLocal() as _sess:
        async with _sess.begin():
            _a = await _sess.get(Analysis, analysis_id)
            if _a:
                _a.transcript = transcript

    # ── STEP 12: AI Analysis ──────────────────────────────────────────────────
    await update_step(analysis_id, 12, "running", "Analisi AI in corso...")

    # Load global documents
    global_docs = []
    try:
        from sqlalchemy import select as sa_select
        from models import GlobalDocument
        async with AsyncSessionLocal() as _sess:
            _res = await _sess.execute(
                sa_select(GlobalDocument)
                .where(GlobalDocument.is_active == True)
                .order_by(GlobalDocument.sort_order, GlobalDocument.id)
            )
            global_docs = [
                {"title": d.title, "content": d.content}
                for d in _res.scalars().all()
            ]
    except Exception as exc:
        logger.warning("[%s] Could not load global_documents: %s", appointment_id, exc)

    # Load prompt sections
    from services.prompt_db import get_prompt_sections
    prompt_sections = {}
    try:
        prompt_sections = await get_prompt_sections()
    except Exception as exc:
        logger.warning("[%s] Could not load prompt sections: %s", appointment_id, exc)

    report = None
    for attempt in range(1, 3):
        try:
            report = await analyze_call(
                transcript=transcript,
                campaign_info=campaign_info,
                script=campaign_db.script,
                qualification_params=campaign_db.qualification_params,
                client_info=campaign_db.client_info,
                operator_email=operator_email,
                prompt_sections=prompt_sections,
                prompt_extra=campaign_db.prompt_extra,
                global_docs=global_docs,
            )
            break
        except Exception as exc:
            if attempt == 2:
                msg = f"Analisi AI fallita dopo 2 tentativi: {exc}"
                await update_step(analysis_id, 12, "stop", msg)
                await _save_error(analysis_id, appointment_id, f"AI error: {exc}", acuity_account)
                return
            logger.warning("[%s] Analisi tentativo %d fallita: %s", appointment_id, attempt, exc)

    # Handle errore_tecnico detected by Claude
    if report.get("errore_tecnico"):
        logger.warning("[%s] Claude ha rilevato trascrizione illeggibile", appointment_id)
        qualification_level = "errore_tecnico"
        await update_step(analysis_id, 12, "ok", "Qualifica: errore_tecnico (rilevato da AI)")
        await _save_analysis(
            analysis_id=analysis_id,
            appointment_id=appointment_id,
            campaign_code=campaign_info["raw"],
            transcript=transcript,
            report=report,
            html_report=None,
            acuity_account=acuity_account,
            appointment_dt=appointment_dt,
            phone=phone,
            client_company=ragione_sociale,
            sidial_call_id=",".join(rec_id for rec_id, _ in recordings),
            operator_name=operator_display,
            operator_email=operator_email,
            qualification_level=qualification_level,
            email_sent=False,
        )
        await update_step(analysis_id, 13, "ok", "Salvato come errore tecnico")
        await update_step(analysis_id, 14, "ok", "Nessuna email — errore tecnico")
        return

    # Determine qualification level
    _rating_to_level = {
        1: "inaccurata", 2: "da_migliorare", 3: "sufficiente", 4: "buona", 5: "eccellente"
    }
    qual_obj = report.get("qualificazione", {})
    qual_rating = qual_obj.get("rating", 2)
    if qual_obj.get("fuori_parametro"):
        qualification_level = "non_in_target"
    else:
        qualification_level = _rating_to_level.get(qual_rating, "da_migliorare")

    await update_step(analysis_id, 12, "ok", f"Qualifica: {qualification_level}")

    # ── STEP 13: Save result ──────────────────────────────────────────────────
    await update_step(analysis_id, 13, "running", "Salvataggio...")

    # Generate HTML report
    appointment_info = {
        "datetime": appointment_data.get("datetime", ""),
        "phone": phone,
        "id": appointment_id,
    }
    html_report = generate_html_report(
        report, appointment_info, campaign_info,
        operator_name=operator_display,
        client_company=ragione_sociale,
        n_recordings=n_recs,
    )

    for attempt in range(1, 4):
        try:
            await _save_analysis(
                analysis_id=analysis_id,
                appointment_id=appointment_id,
                campaign_code=campaign_info["raw"],
                transcript=transcript,
                report=report,
                html_report=html_report,
                acuity_account=acuity_account,
                appointment_dt=appointment_dt,
                phone=phone,
                client_company=ragione_sociale,
                sidial_call_id=",".join(rec_id for rec_id, _ in recordings),
                operator_name=operator_display,
                operator_email=operator_email,
                qualification_level=qualification_level,
                email_sent=False,
            )
            await update_step(analysis_id, 13, "ok", "Risultato salvato")
            break
        except Exception as exc:
            if attempt == 3:
                logger.critical("[%s] Salvataggio fallito dopo 3 tentativi: %s", appointment_id, exc)
                await update_step(analysis_id, 13, "stop", f"Errore DB: {exc}")
                return
            logger.warning("[%s] Salvataggio tentativo %d fallito: %s", appointment_id, attempt, exc)

    # ── STEP 14: Send email ───────────────────────────────────────────────────
    await update_step(analysis_id, 14, "running", "Invio email...")

    from config import settings as cfg

    _email_disabled = bool(campaign_db and campaign_db.email_disabled)
    _email_no_operator = bool(campaign_db and campaign_db.email_no_operator)
    _INOLTRO = "inoltro@effoncall.com"

    if _email_disabled:
        await update_step(analysis_id, 14, "ok", "Email disabilitata per questa campagna")
        return

    if qualification_level in ("non_in_target", "errore_tecnico"):
        await update_step(analysis_id, 14, "ok", f"Nessuna email — qualifica: {qualification_level}")
        return

    recipients: list[str] = []
    if campaign_db and campaign_db.email_recipients:
        recipients = list(campaign_db.email_recipients)
    if not recipients:
        recipients = [cfg.fallback_email]
    if operator_email and not _email_no_operator and operator_email not in recipients:
        recipients.insert(0, operator_email)
    if _INOLTRO not in recipients:
        recipients.append(_INOLTRO)

    try:
        await send_analysis_report(
            recipients=recipients,
            html_content=html_report,
            operator_name=operator_display,
            qualification_level=qualification_level,
            appointment_datetime=appointment_data.get("datetime", ""),
        )
        # Mark email_sent in DB
        async with AsyncSessionLocal() as _sess:
            async with _sess.begin():
                _a = await _sess.get(Analysis, analysis_id)
                if _a:
                    _a.email_sent = True
        await update_step(analysis_id, 14, "ok", f"Email inviata a: {', '.join(recipients)}")
        logger.info("[%s] Email inviata a %s", appointment_id, recipients)
    except Exception as exc:
        logger.error("[%s] Email send failed: %s", appointment_id, exc, exc_info=True)
        await update_step(analysis_id, 14, "warning", f"Errore invio email: {exc}")

    logger.info("[%s] Pipeline completata — qualifica: %s", appointment_id, qualification_level)


# ── Webhook endpoint ──────────────────────────────────────────────────────────

@router.post("/acuity/{account_id}")
async def acuity_webhook(
    account_id: int,
    request: Request,
    background_tasks: BackgroundTasks,
):
    """
    Receive Acuity Scheduling webhook.
    account_id: 1 or 2 (two Effoncall Acuity accounts).
    """
    if account_id not in (1, 2):
        raise HTTPException(status_code=404, detail="Invalid account")

    payload_bytes = await request.body()

    # Signature verification (optional — controlled by ACUITY_VERIFY_WEBHOOK)
    signature = request.headers.get("X-Acuity-Signature", "")
    if not check_webhook_signature(payload_bytes, signature, account_id):
        raise HTTPException(status_code=401, detail="Invalid webhook signature")

    if not payload_bytes:
        return {"status": "ok"}

    content_type = request.headers.get("content-type", "")
    if "application/json" in content_type:
        try:
            data = json.loads(payload_bytes)
        except json.JSONDecodeError:
            return {"status": "ok"}
    else:
        # Acuity sends form-encoded: action=changed&id=123&...
        parsed = parse_qs(payload_bytes.decode("utf-8", errors="ignore"))
        data = {k: v[0] if len(v) == 1 else v for k, v in parsed.items()}
        if not data:
            return {"status": "ok"}

    appointment_id = data.get("id") or data.get("appointmentID")
    action = data.get("action")

    logger.info(
        "Acuity webhook account=%d action=%s appointment_id=%s",
        account_id, action, appointment_id,
    )

    if not appointment_id:
        return {"status": "skipped", "reason": "no appointment id"}

    # Fetch full appointment details (includes labels) from Acuity API
    full_appointment = await get_appointment(appointment_id, account_id)
    if not full_appointment:
        return {"status": "skipped", "reason": "could not fetch appointment"}

    if not should_analyze(full_appointment):
        return {"status": "skipped", "reason": "label not in trigger set"}

    # Analizza solo se l'appuntamento è stato CREATO oggi — evita ri-analisi
    # su modifiche/riprogrammazioni di appuntamenti vecchi.
    from datetime import date, timezone
    created_at_raw = full_appointment.get("createdAt") or full_appointment.get("created_at") or ""
    try:
        from utils.helpers import parse_iso_datetime
        created_date = parse_iso_datetime(created_at_raw).astimezone(timezone.utc).date()
    except Exception:
        created_date = None

    if created_date and created_date != date.today():
        logger.info(
            "[%s] Skipped: appuntamento creato il %s, non oggi",
            appointment_id, created_date,
        )
        return {"status": "skipped", "reason": "appointment not created today"}

    # Respond immediately with 200 — pipeline runs in background
    background_tasks.add_task(
        run_analysis_pipeline,
        appointment_data=full_appointment,
        acuity_account=account_id,
    )

    return {"status": "accepted", "appointment_id": data.get("id")}

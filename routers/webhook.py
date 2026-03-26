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
import time as _time_mod
from urllib.parse import parse_qs

# Versione pipeline — visibile nello step 1 per verificare deploy
_PIPELINE_VERSION = "v2024-03-26a"

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
    """Safe wrapper — catches any unhandled exception and saves error to DB."""
    appointment_id = str(appointment_data.get("id", "unknown"))
    try:
        await _run_pipeline_inner(appointment_data, acuity_account, engine_override)
    except Exception as exc:
        logger.exception("[%s] PIPELINE CRASH: %s", appointment_id, exc)
        try:
            from database import AsyncSessionLocal
            from sqlalchemy import text
            async with AsyncSessionLocal() as _sess:
                async with _sess.begin():
                    await _sess.execute(
                        text("""
                            UPDATE analyses
                            SET processing_status = 'error',
                                qualification_level = 'errore_tecnico',
                                step_message = 'Pipeline crash: ' || LEFT(:err, 200),
                                error_message = :err
                            WHERE appointment_id = :appt_id
                              AND processing_status = 'processing'
                        """),
                        {"appt_id": appointment_id, "err": str(exc)[:2000]},
                    )
        except Exception:
            logger.error("[%s] Could not even save pipeline crash error", appointment_id)


async def _run_pipeline_inner(
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
    analysis_id: int | None = None
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
    await update_step(analysis_id, 1, "ok", f"Webhook ricevuto ({_PIPELINE_VERSION})")
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

    # Save form fields + label to DB (non-fatal)
    try:
        async with AsyncSessionLocal() as _sess:
            async with _sess.begin():
                _a = await _sess.get(Analysis, analysis_id)
                if _a:
                    _a.acuity_form_fields = form_fields
                    _a.label_name = label_name or None
                    _a.label_color = label_color or None
    except Exception as _e:
        logger.warning("[%s] Save form fields fallito (non-fatale): %s", appointment_id, _e)

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

    # Save initial info for UI display (non-fatal)
    _initial_op_name = get_operator_display(appointment_data)
    try:
        await _update_initial_info(
            analysis_id,
            campaign_code=campaign_info["raw"],
            operator_name=_initial_op_name,
            appointment_dt=appointment_dt,
        )
    except Exception as _e:
        logger.warning("[%s] _update_initial_info fallito (non-fatale): %s", appointment_id, _e)

    # ── STEP 8: Identify operator ─────────────────────────────────────────────
    await update_step(analysis_id, 8, "running", "Identificazione operatore...")
    email_field = find_operator_email(appointment_data) or appointment_data.get("email", "")
    try:
        op_info = await identify_operator(email_field, form_fields)
    except Exception as _op_exc:
        logger.warning("[%s] identify_operator error (non-fatal): %s", appointment_id, _op_exc)
        op_info = {
            "number": None,
            "email": email_field,
            "display_name": None,
            "source": None,
            "warning": f"Errore lookup operatore: {_op_exc}",
        }

    operator_email = op_info["email"] or ""
    operator_display = op_info["display_name"] or _initial_op_name

    if op_info["warning"]:
        await update_step(analysis_id, 8, "warning", op_info["warning"])
    else:
        await update_step(analysis_id, 8, "ok", f"Operatore #{op_info['number']} — {operator_display}")

    # Save operator info (non-fatal if fails)
    try:
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
    except Exception as _save8_exc:
        logger.warning("[%s] Save operator info fallito (non-fatale): %s", appointment_id, _save8_exc)

    # ── Guard: phone required ─────────────────────────────────────────────────
    if not phone:
        msg = "Numero di telefono assente nell'appuntamento Acuity — impossibile cercare su Sidial"
        logger.error("[%s] %s", appointment_id, msg)
        await update_step(analysis_id, 9, "stop", msg)
        await _save_error(analysis_id, appointment_id, msg, acuity_account)
        return

    # ── STEP 9: Search Sidial ─────────────────────────────────────────────────
    await update_step(analysis_id, 9, "running", f"Ricerca lead su Sidial (tel: {phone})...")

    try:
        lookback = int(await get_setting("sidial_lookback_days", "90"))
        min_secs = int(await get_setting("min_call_length_seconds", "20"))
        retry_count = int(await get_setting("sidial_retry_count", "5"))
        retry_wait = int(await get_setting("sidial_retry_wait_seconds", "180"))
    except Exception as exc:
        logger.warning("[%s] Could not read settings: %s — using defaults", appointment_id, exc)
        lookback, min_secs, retry_count, retry_wait = 90, 20, 5, 180

    async def _step9_progress(msg):
        """Progress callback — non bloccante, ignora errori DB."""
        try:
            await update_step(analysis_id, 9, "running", f"Sidial: {msg}")
        except Exception:
            pass  # Se il DB è lento, ignoriamo — la pipeline continua

    _sidial_start = _time_mod.monotonic()
    try:
        # NO asyncio.wait_for: sidial.py gestisce internamente il deadline (180s)
        # con timeout httpx nativi. asyncio.wait_for causa hang su aclose() httpx.
        recordings, sidial_stats = await find_and_download_all_recordings(
            phone=phone,
            campaign_code=campaign_info.get("raw"),
            lookback_days=lookback,
            piva=piva or "",
            ragione_sociale=ragione_sociale or "",
            last_name=last_name if not form_fields else "",
            min_call_seconds=min_secs,
            return_stats=True,
            progress_cb=_step9_progress,
        )
    except Exception as exc:
        elapsed = int(_time_mod.monotonic() - _sidial_start)
        msg = f"Errore Sidial dopo {elapsed}s: {type(exc).__name__}: {exc}"
        logger.error("[%s] %s", appointment_id, msg, exc_info=True)
        await update_step(analysis_id, 9, "stop", msg)
        await _save_error(analysis_id, appointment_id, msg, acuity_account)
        return

    _method = sidial_stats.get("search_method", "?")
    stats_msg = (
        f"{sidial_stats['leads_found']} lead · "
        f"{sidial_stats['total_recs']} rec totali · "
        f"{sidial_stats['recent_recs']} recenti · "
        f"{sidial_stats['converting_recs']} in conversione · "
        f"metodo: {_method}"
    )

    if sidial_stats["leads_found"] == 0:
        _variants = sidial_stats.get("phone_variants", [])
        msg_no_lead = (
            f"Nessun lead trovato su Sidial — "
            f"tel: {phone} · varianti provate: {_variants} · "
            f"piva: {piva or '—'} · rs: {ragione_sociale or '—'}"
        )
        await update_step(analysis_id, 9, "stop", msg_no_lead)
        await _save_error(analysis_id, appointment_id, msg_no_lead, acuity_account)
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
        if sidial_stats["converting_recs"] > 0 and not _is_old_appointment:
            # Registrazioni in conversione wav→mp3: salva come pending_conversion
            # Il retry automatico avviene ogni 10 minuti (fino a 6 volte = ~60 min)
            _conv_msg = (
                f"Registrazioni in conversione — retry automatico ogni 10 min "
                f"(max 6 tentativi) · {stats_msg}"
            )
            await update_step(analysis_id, 10, "running", _conv_msg)
            try:
                async with AsyncSessionLocal() as _cs:
                    async with _cs.begin():
                        _ca = await _cs.get(Analysis, analysis_id)
                        if _ca:
                            _ca.processing_status = "pending_conversion"
                            _ca.step_message = _conv_msg
            except Exception as _ce:
                logger.warning("[%s] Set pending_conversion failed: %s", appointment_id, _ce)
            return

        if not recordings:
            msg = f"0 registrazioni trovate · {stats_msg}"
            await update_step(analysis_id, 10, "stop", msg,
                              detail={"can_upload_audio": True, "can_upload_transcript": True})
            await _save_error(analysis_id, appointment_id, msg, acuity_account)
            return

    n_recs = len(recordings)
    total_secs = sidial_stats.get("total_seconds", 0)
    mins = total_secs // 60
    secs = total_secs % 60
    await update_step(analysis_id, 10, "ok", f"{n_recs} registrazioni · {mins}m {secs:02d}s parlato")

    # Save recording stats (non-fatal)
    try:
        async with AsyncSessionLocal() as _sess:
            async with _sess.begin():
                _a = await _sess.get(Analysis, analysis_id)
                if _a:
                    _a.num_recordings = n_recs
                _a.total_talk_seconds = total_secs
                _a.sidial_call_id = ",".join(rec_id for rec_id, _ in recordings)
    except Exception as _e:
        logger.warning("[%s] Save recording stats fallito (non-fatale): %s", appointment_id, _e)

    # ── STEP 11: Transcription (CON FALLBACK AUTOMATICO) ──────────────────
    await update_step(analysis_id, 11, "running", "Trascrizione in corso...")

    _campaign_engine = campaign_db.transcription_engine if campaign_db.transcription_engine else None
    global_engine = await get_setting("transcription_engine", "openai")
    _engine = engine_override or _campaign_engine or global_engine

    # Motore di fallback: se il primario fallisce, prova l'altro
    _fallback_engine = "openai" if _engine == "assemblyai" else "assemblyai"

    async def _transcribe_one(call_id: str, audio_bytes: bytes, idx: int, engine: str) -> tuple[str, bool]:
        """Trascrive una chiamata. Restituisce (testo, successo)."""
        try:
            part = await transcribe_audio(audio_bytes, engine=engine)
            if part and len(part.strip()) > 20:
                return f"--- CHIAMATA {idx} (id: {call_id}) ---\n{part}", True
            return f"--- CHIAMATA {idx} (id: {call_id}) ---\n[trascrizione vuota]", False
        except Exception as exc:
            logger.error("[%s] Trascrizione fallita call_id=%s engine=%s: %s",
                        appointment_id, call_id, engine, exc)
            return f"--- CHIAMATA {idx} (id: {call_id}) ---\n[trascrizione non disponibile]", False

    transcript_parts = []
    used_engine = _engine
    primary_failures = 0

    for idx, (call_id, audio_bytes) in enumerate(recordings, start=1):
        await update_step(
            analysis_id, 11, "running",
            f"Trascrizione chiamata {idx}/{n_recs} (motore: {used_engine})...",
        )
        logger.info("[%s] Trascrizione %d/%d (call_id=%s, %d bytes, engine=%s)",
                    appointment_id, idx, n_recs, call_id, len(audio_bytes), used_engine)

        text, ok = await _transcribe_one(call_id, audio_bytes, idx, used_engine)

        # Se il motore primario fallisce, PROVA IL FALLBACK
        if not ok and used_engine != _fallback_engine:
            primary_failures += 1
            logger.warning("[%s] Motore %s fallito — fallback a %s per chiamata %d",
                          appointment_id, used_engine, _fallback_engine, idx)
            await update_step(
                analysis_id, 11, "running",
                f"Fallback → {_fallback_engine} per chiamata {idx}/{n_recs}...",
            )
            text, ok = await _transcribe_one(call_id, audio_bytes, idx, _fallback_engine)

            # Se il fallback funziona, CAMBIA MOTORE per le prossime chiamate
            if ok and primary_failures >= 1:
                logger.warning("[%s] Motore %s fallisce — switch permanente a %s",
                              appointment_id, _engine, _fallback_engine)
                used_engine = _fallback_engine

        transcript_parts.append(text)
        if ok:
            logger.info("[%s] Chiamata %d: %d caratteri (engine=%s)",
                        appointment_id, idx, len(text), used_engine)

    transcript = "\n\n".join(transcript_parts)
    logger.info("[%s] Trascrizione totale: %d caratteri (engine finale: %s)",
                appointment_id, len(transcript), used_engine)

    _all_unavailable = all("[trascrizione non disponibile]" in p or "[trascrizione vuota]" in p
                           for p in transcript_parts)
    _clean_len = len(transcript.replace("\n", "").strip())
    _too_short = _clean_len < 80

    if _all_unavailable or _too_short:
        if _all_unavailable:
            reason = (f"Trascrizione fallita con ENTRAMBI i motori ({_engine} + {_fallback_engine}) "
                     f"per tutte le {n_recs} registrazioni")
        else:
            reason = f"Trascrizione troppo breve: {_clean_len} caratteri da {n_recs} registrazioni"

        logger.warning("[%s] %s — testo: %r", appointment_id, reason, transcript[:300])
        await update_step(analysis_id, 11, "stop", reason)

        # Save transcript and mark as errore tecnico
        try:
            async with AsyncSessionLocal() as _sess:
                async with _sess.begin():
                    _obj = await _sess.get(Analysis, analysis_id)
                    if _obj:
                        _obj.processing_status = "completed"
                        _obj.progress = 100
                        _obj.step_message = reason
                        _obj.qualification_level = "errore_tecnico"
                        _obj.error_message = reason
                        _obj.transcript = transcript
                        _obj.num_recordings = n_recs
                        _obj.total_talk_seconds = total_secs
                        _obj.report_json = {
                            "errore_tecnico": True,
                            "motivo": reason,
                            "registrazioni_scaricate": n_recs,
                            "secondi_totali": total_secs,
                            "motore_trascrizione": _engine,
                            "qualificazione": {
                                "rating": 1, "label": "INSUFFICIENTE",
                                "fuori_parametro": False,
                                "spiegazione": f"Analisi non valida — {reason}",
                                "parametri_verificati": [], "parametri_mancanti": [],
                            },
                        }
        except Exception as _e:
            logger.error("[%s] Salvataggio errore_tecnico fallito: %s", appointment_id, _e)
        return

    _engine_note = f" (fallback da {_engine})" if used_engine != _engine else ""
    await update_step(analysis_id, 11, "ok", f"{len(transcript)} caratteri · motore: {used_engine}{_engine_note}")

    # Save transcript (non-fatal — it will be saved again in _save_analysis)
    try:
        async with AsyncSessionLocal() as _sess:
            async with _sess.begin():
                _a = await _sess.get(Analysis, analysis_id)
                if _a:
                    _a.transcript = transcript
    except Exception as _e:
        logger.warning("[%s] Save transcript fallito (non-fatale): %s", appointment_id, _e)

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


# ── Retry automatico registrazioni in conversione ─────────────────────────────

async def retry_conversion_analysis(analysis_id: int) -> bool:
    """
    Riprova download+trascrizione+analisi per un'analisi in pending_conversion.
    Chiamata ogni 10 minuti dal loop in main.py.
    Ritorna True se completata (qualsiasi stato), False se ancora in conversione.
    """
    from sqlalchemy.orm.attributes import flag_modified as _fm
    from sqlalchemy import select as _sa_select
    from models import GlobalDocument

    # Carica analisi
    try:
        async with AsyncSessionLocal() as _s:
            _a = await _s.get(Analysis, analysis_id)
            if not _a or _a.processing_status != "pending_conversion":
                return True
            _steps     = dict(_a.pipeline_steps or {})
            _phone     = _a.client_phone or ""
            _appt_id   = _a.appointment_id or "?"
            _account   = _a.acuity_account or 1
            _rs        = _a.client_company or ""
            _ff        = _a.acuity_form_fields or {}
            _piva      = _ff.get("piva") or _ff.get("P.IVA") or ""
            _op        = _a.operator_name or ""
            _op_email  = _a.operator_email or ""
            _appt_dt   = _a.appointment_datetime
            _camp_code = _a.campaign_code or ""
    except Exception as exc:
        logger.error("[retry_conv] Load analysis %d failed: %s", analysis_id, exc)
        return True

    _retry_n = _steps.get("_conv_retry", 0) + 1
    _MAX     = 6

    if _retry_n > _MAX:
        _msg = f"Registrazioni non disponibili dopo {_MAX} tentativi automatici (~60 min)"
        await update_step(analysis_id, 10, "stop", _msg,
                          detail={"can_upload_audio": True, "can_upload_transcript": True})
        await _save_error(analysis_id, _appt_id, _msg, _account)
        return True

    # Aggiorna contatore e imposta processing
    try:
        async with AsyncSessionLocal() as _s:
            async with _s.begin():
                _a2 = await _s.get(Analysis, analysis_id)
                if _a2:
                    _ns = dict(_a2.pipeline_steps or {})
                    _ns["_conv_retry"] = _retry_n
                    _a2.pipeline_steps = _ns
                    _fm(_a2, "pipeline_steps")
                    _a2.processing_status = "processing"
    except Exception as exc:
        logger.warning("[retry_conv] Update counter failed: %s", exc)

    await update_step(analysis_id, 10, "running",
                      f"Auto-retry #{_retry_n}/{_MAX} — cerco registrazioni...")

    # Helper: torna a pending_conversion
    async def _back_to_pending(msg: str):
        await update_step(analysis_id, 10, "running", msg)
        try:
            async with AsyncSessionLocal() as _s:
                async with _s.begin():
                    _a3 = await _s.get(Analysis, analysis_id)
                    if _a3:
                        _a3.processing_status = "pending_conversion"
        except Exception:
            pass

    # Download
    try:
        _lookback = int(await get_setting("sidial_lookback_days", "90"))
        _min_s    = int(await get_setting("min_call_length_seconds", "20"))
        _recs, _stats = await asyncio.wait_for(
            find_and_download_all_recordings(
                phone=_phone, campaign_code=_camp_code,
                lookback_days=_lookback, piva=_piva,
                ragione_sociale=_rs, min_call_seconds=_min_s,
                return_stats=True,
            ),
            timeout=200,
        )
    except Exception as exc:
        logger.warning("[retry_conv %d] Download failed analysis %d: %s", _retry_n, analysis_id, exc)
        await _back_to_pending(f"Errore download (retry {_retry_n}/{_MAX}) — riprovo tra 10 min")
        return False

    if not _recs:
        if _stats.get("converting_recs", 0) > 0:
            await _back_to_pending(
                f"Ancora in conversione (tentativo {_retry_n}/{_MAX}) — prossimo tra 10 min"
            )
            return False
        _sm = f"{_stats.get('leads_found',0)} lead · {_stats.get('total_recs',0)} rec"
        _msg = f"0 registrazioni trovate dopo {_retry_n} tentativi · {_sm}"
        await update_step(analysis_id, 10, "stop", _msg,
                          detail={"can_upload_audio": True, "can_upload_transcript": True})
        await _save_error(analysis_id, _appt_id, _msg, _account)
        return True

    # Trovate! Step 10 ok
    _n    = len(_recs)
    _ts   = _stats.get("total_seconds", 0)
    await update_step(analysis_id, 10, "ok", f"{_n} registrazioni · {_ts//60}m {_ts%60:02d}s")

    # Carica campagna
    from services.campaign_db import get_campaign_by_code as _gcbc
    _cdb  = await _gcbc(_camp_code)
    _cinfo = {"raw": _camp_code, "code": _camp_code}

    # Step 11: Trascrizione
    await update_step(analysis_id, 11, "running", "Trascrizione in corso...")
    _geng = await get_setting("transcription_engine", "openai")
    _eng  = (_cdb.transcription_engine if _cdb and _cdb.transcription_engine else None) or _geng
    _fb   = "openai" if _eng == "assemblyai" else "assemblyai"
    _parts, _used = [], _eng

    for _i, (_cid, _audio) in enumerate(_recs, 1):
        _ok = False
        for _e in [_used, _fb]:
            try:
                _t = await transcribe_audio(_audio, engine=_e)
                if _t and len(_t.strip()) > 20:
                    _parts.append(f"--- CHIAMATA {_i} (id:{_cid}) ---\n{_t}")
                    if _e != _used:
                        _used = _e
                    _ok = True
                    break
            except Exception:
                continue
        if not _ok:
            _parts.append(f"--- CHIAMATA {_i} (id:{_cid}) ---\n[trascrizione non disponibile]")

    _transcript = "\n\n".join(_parts)
    _clen = len(_transcript.replace("\n","").strip())

    if _clen < 80:
        _reason = f"Trascrizione troppo breve: {_clen} caratteri"
        await update_step(analysis_id, 11, "stop", _reason)
        try:
            async with AsyncSessionLocal() as _s:
                async with _s.begin():
                    _a4 = await _s.get(Analysis, analysis_id)
                    if _a4:
                        _a4.processing_status = "completed"
                        _a4.qualification_level = "errore_tecnico"
                        _a4.transcript = _transcript
                        _a4.error_message = _reason
                        _a4.report_json = {"errore_tecnico": True, "motivo": _reason}
        except Exception as exc:
            logger.warning("[retry_conv] Save errore_tecnico failed: %s", exc)
        return True

    await update_step(analysis_id, 11, "ok", f"{len(_transcript)} caratteri · {_used}")

    # Step 12: AI Analysis
    await update_step(analysis_id, 12, "running", "Analisi AI in corso...")
    _gdocs = []
    try:
        async with AsyncSessionLocal() as _s:
            _res = await _s.execute(
                _sa_select(GlobalDocument)
                .where(GlobalDocument.is_active == True)
                .order_by(GlobalDocument.sort_order)
            )
            _gdocs = [{"title": d.title, "content": d.content} for d in _res.scalars().all()]
    except Exception:
        pass

    from services.prompt_db import get_prompt_sections as _gps
    _psec = {}
    try:
        _psec = await _gps()
    except Exception:
        pass

    _report = None
    for _att in range(1, 3):
        try:
            _report = await analyze_call(
                transcript=_transcript, campaign_info=_cinfo,
                script=_cdb.script if _cdb else "",
                qualification_params=_cdb.qualification_params if _cdb else "",
                client_info=_cdb.client_info if _cdb else "",
                operator_email=_op_email, prompt_sections=_psec,
                prompt_extra=_cdb.prompt_extra if _cdb else None,
                global_docs=_gdocs,
            )
            break
        except Exception as exc:
            if _att == 2:
                _msg2 = f"Analisi AI fallita: {exc}"
                await update_step(analysis_id, 12, "stop", _msg2)
                await _save_error(analysis_id, _appt_id, _msg2, _account)
                return True

    _r2l   = {1:"inaccurata",2:"da_migliorare",3:"sufficiente",4:"buona",5:"eccellente"}
    _qobj  = _report.get("qualificazione", {})
    if _report.get("errore_tecnico"):
        _qlev = "errore_tecnico"
    elif _qobj.get("fuori_parametro"):
        _qlev = "non_in_target"
    else:
        _qlev = _r2l.get(_qobj.get("rating", 2), "da_migliorare")

    await update_step(analysis_id, 12, "ok", f"Qualifica: {_qlev}")

    # Step 13: Salvataggio
    await update_step(analysis_id, 13, "running", "Salvataggio...")
    try:
        _html = generate_html_report(
            _report, {"datetime": str(_appt_dt or ""), "phone": _phone, "id": _appt_id},
            _cinfo, operator_name=_op, client_company=_rs,
        )
    except Exception:
        _html = None

    try:
        await _save_analysis(
            analysis_id=analysis_id, appointment_id=_appt_id,
            campaign_code=_camp_code, transcript=_transcript,
            report=_report, html_report=_html, acuity_account=_account,
            appointment_dt=_appt_dt, phone=_phone, client_company=_rs,
            sidial_call_id=",".join(c for c,_ in _recs),
            operator_name=_op, operator_email=_op_email,
            qualification_level=_qlev, email_sent=False,
        )
        await update_step(analysis_id, 13, "ok", "Salvato")
    except Exception as exc:
        logger.error("[retry_conv] Save analysis %d failed: %s", analysis_id, exc)
        await update_step(analysis_id, 13, "stop", f"Salvataggio fallito: {exc}")
        return True

    # Step 14: Email
    await update_step(analysis_id, 14, "running", "Invio email...")
    if _qlev not in ("non_in_target", "errore_tecnico") and _cdb and not _cdb.email_disabled:
        try:
            _recps = list(_cdb.email_recipients or [])
            if _op_email and not _cdb.email_no_operator and _op_email not in _recps:
                _recps.insert(0, _op_email)
            if _INOLTRO not in _recps:
                _recps.append(_INOLTRO)
            await send_analysis_report(
                recipients=_recps, html_content=_html or "",
                operator_name=_op, qualification_level=_qlev,
                appointment_datetime=str(_appt_dt or ""),
            )
            await update_step(analysis_id, 14, "ok", f"Email inviata a: {', '.join(_recps)}")
        except Exception as exc:
            await update_step(analysis_id, 14, "warning", f"Errore email: {exc}")
    else:
        await update_step(analysis_id, 14, "ok", f"Nessuna email — {_qlev}")

    logger.info("[retry_conv] Analysis %d completata — qualifica: %s", analysis_id, _qlev)
    return True


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

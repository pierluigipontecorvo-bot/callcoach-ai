"""
Acuity Scheduling webhook receiver + background analysis pipeline.
"""

import asyncio
import json
import logging
from urllib.parse import parse_qs

from fastapi import APIRouter, BackgroundTasks, HTTPException, Request

from services.acuity import (
    check_webhook_signature,
    extract_ragione_sociale,
    find_operator_email,
    get_appointment,
    get_operator_display,
    should_analyze,
)
from services.ai_analysis import _extract_operator_name, analyze_call
from services.campaign_db import get_campaign_by_code
from services.campaign_parser import parse_campaign_code
from services.email_service import generate_html_report, send_analysis_report
from services.sidial import find_and_download_all_recordings, _normalize_phone
from services.transcription import transcribe_audio
from utils.helpers import parse_iso_datetime

router = APIRouter(prefix="/webhook", tags=["webhook"])
logger = logging.getLogger(__name__)


# ── DB helpers (imported lazily to avoid circular imports at module load) ──────

async def _create_processing_record(appointment_id: str, acuity_account: int) -> int:
    """Crea subito un record Analysis con status=processing e ritorna l'id."""
    from database import AsyncSessionLocal
    from models import Analysis

    async with AsyncSessionLocal() as session:
        obj = Analysis(
            appointment_id=str(appointment_id),
            acuity_account=acuity_account,
            processing_status="processing",
            progress=0,
            step_message="Avvio pipeline...",
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
    """Save identifying info as soon as it is known, so the UI can display it during processing."""
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


# _get_campaign_config replaced by services.campaign_db.get_campaign_by_code
# (longest-prefix matching: "INTER" covers all INTER-* campaigns)


# ── Full pipeline ──────────────────────────────────────────────────────────────

async def run_analysis_pipeline(appointment_data: dict, acuity_account: int):
    """
    1. Parse campaign code
    2. Find & download recording from Sidial
    3. Transcribe with Whisper
    4. Load campaign config from DB
    5. Analyse with Claude
    6. Generate HTML report
    7. Send email
    8. Save to DB
    """
    appointment_id = str(appointment_data.get("id", "unknown"))
    logger.info("[%s] Pipeline started (account=%d)", appointment_id, acuity_account)

    # ── 0. Parse campaign code (pre-flight, before creating any DB record) ────
    appointment_type = appointment_data.get("type", "")
    campaign_info = parse_campaign_code(appointment_type)

    if not campaign_info.get("valid"):
        logger.warning("[%s] Unparseable campaign code: %r — skip (no DB record)", appointment_id, appointment_type)
        return

    # ── 0b. Check campaign exists in DB — if not configured, skip silently ───
    # (user requested: "se la campagna non è stata ancora creata non deve fare analisi")
    _campaign_pre = None
    try:
        _campaign_pre = await get_campaign_by_code(campaign_info["raw"])
    except Exception as _exc:
        logger.warning("[%s] Could not check campaign in DB: %s — proceeding anyway", appointment_id, _exc)

    if _campaign_pre is None:
        logger.info(
            "[%s] Campagna '%s' non configurata in DB — analisi saltata. "
            "Configurare la campagna nell'admin prima di ri-analizzare.",
            appointment_id, campaign_info["raw"],
        )
        return  # Silent skip — no processing record created

    # ── Crea subito il record con status=processing (visibile nella UI) ───────
    analysis_id = await _create_processing_record(appointment_id, acuity_account)

    # ── 1. Campaign code already parsed above — update progress ──────────────
    await _update_progress(analysis_id, 5, "Verifica codice campagna...")

    logger.info(
        "[%s] Campaign: %s | operator: %s",
        appointment_id,
        campaign_info.get("raw"),
        campaign_info.get("agente"),
    )

    phone = appointment_data.get("phone", "")
    appointment_dt_str = appointment_data.get("datetime", "")
    appointment_dt = parse_iso_datetime(appointment_dt_str) if appointment_dt_str else None
    client_company = extract_ragione_sociale(appointment_data)

    # ── Operator: OPR. form field (primary) or op.XX.nome@effoncall.com (fallback) ──
    operator_email = find_operator_email(appointment_data)
    operator_display = get_operator_display(appointment_data)
    logger.info("[%s] Operator: %s (email=%s)", appointment_id, operator_display, operator_email)

    # Save identifying info immediately so the UI shows it during processing
    _initial_op_name = operator_display
    await _update_initial_info(
        analysis_id,
        campaign_code=campaign_info["raw"],
        operator_name=_initial_op_name,
        appointment_dt=appointment_dt,
    )

    # ── 2. Find & download ALL recordings for this contact ────────────────────
    await _update_progress(analysis_id, 10, "Ricerca registrazione su Sidial...")
    try:
        recordings = await find_and_download_all_recordings(
            phone=phone,
            campaign_code=campaign_info.get("raw"),
            lookback_days=90,   # cattura tutte le telefonate degli ultimi 3 mesi
        )
    except Exception as exc:
        msg = f"Sidial error: {exc}"
        logger.error("[%s] %s", appointment_id, msg, exc_info=True)
        await _save_error(analysis_id, appointment_id, msg, acuity_account)
        return

    if not phone:
        msg = "Numero di telefono assente nell'appuntamento Acuity — impossibile cercare su Sidial"
        logger.error("[%s] %s", appointment_id, msg)
        await _save_error(analysis_id, appointment_id, msg, acuity_account)
        return

    if not recordings:
        msg = f"Nessuna registrazione trovata su Sidial per phone='{phone}' (norm='{_normalize_phone(phone)}') lookback=90gg"
        logger.error("[%s] %s", appointment_id, msg)
        await _save_error(analysis_id, appointment_id, msg, acuity_account)
        return

    logger.info("[%s] %d registrazioni trovate", appointment_id, len(recordings))
    await _update_progress(analysis_id, 20, f"{len(recordings)} registrazione/i trovata/e, trascrizione in corso...")

    # ── 3. Transcribe all recordings and concatenate ───────────────────────────
    transcript_parts = []
    for idx, (call_id, audio_bytes) in enumerate(recordings, start=1):
        pct = 20 + int(40 * (idx - 1) / len(recordings))
        await _update_progress(analysis_id, pct, f"Trascrizione chiamata {idx}/{len(recordings)}...")
        try:
            logger.info(
                "[%s] Trascrizione chiamata %d/%d (call_id=%s, %d bytes) …",
                appointment_id, idx, len(recordings), call_id, len(audio_bytes),
            )
            # Audio cappato a 1 min → Whisper small su CPU ~10-20s → nessun timeout necessario
            part = await transcribe_audio(audio_bytes)
            transcript_parts.append(f"--- CHIAMATA {idx} (id: {call_id}) ---\n{part}")
            logger.info("[%s] Chiamata %d: %d caratteri", appointment_id, idx, len(part))
        except Exception as exc:
            logger.error("[%s] Trascrizione fallita per call_id=%s: %s", appointment_id, call_id, exc)
            transcript_parts.append(f"--- CHIAMATA {idx} (id: {call_id}) ---\n[trascrizione non disponibile]")

    transcript = "\n\n".join(transcript_parts)
    logger.info("[%s] Trascrizione totale: %d caratteri", appointment_id, len(transcript))

    # ── Pre-flight: trascrizione completamente non disponibile ────────────────
    _all_unavailable = all("[trascrizione non disponibile]" in p for p in transcript_parts)
    _too_short = len(transcript.replace("\n", "").strip()) < 80
    if _all_unavailable or _too_short:
        logger.warning("[%s] Trascrizione illeggibile o assente — errore tecnico automatico", appointment_id)
        from database import AsyncSessionLocal
        from models import Analysis
        from sqlalchemy.orm.attributes import flag_modified as _flag_mod
        async with AsyncSessionLocal() as _sess:
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
                        "spiegazione": "Analisi non valida — errore tecnico di trascrizione. La registrazione non è stata trascritta correttamente.",
                        "parametri_verificati": [], "parametri_mancanti": [],
                    },
                }
                _flag_mod(_obj, "report_json")
                await _sess.commit()
        return

    # ── 4. Load campaign config + global documents ────────────────────────────
    await _update_progress(analysis_id, 60, "Caricamento configurazione campagna...")
    # Reuse _campaign_pre fetched in pre-flight (already confirmed not None)
    campaign_db = _campaign_pre
    try:
        # Refresh in case the config changed since the pre-flight check
        _refreshed = await get_campaign_by_code(campaign_info["raw"])
        if _refreshed is not None:
            campaign_db = _refreshed
    except Exception as exc:
        logger.warning("[%s] Could not refresh campaign config: %s — using pre-flight data", appointment_id, exc)

    campaign_script = campaign_db.script             if campaign_db else None
    campaign_client = campaign_db.client_info        if campaign_db else None
    # Qualificazione: SOLO dalla campagna specifica — mai generica o globale
    campaign_qual   = campaign_db.qualification_params if campaign_db else None

    # Documenti globali dalla nuova tabella global_documents
    global_docs = []
    try:
        from sqlalchemy import select as sa_select
        from models import GlobalDocument
        from database import AsyncSessionLocal
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

    # ── 5. Claude analysis ────────────────────────────────────────────────────
    await _update_progress(analysis_id, 65, "Analisi AI in corso...")

    from services.prompt_db import get_prompt_sections
    prompt_sections = {}
    try:
        prompt_sections = await get_prompt_sections()
    except Exception as exc:
        logger.warning("[%s] Could not load prompt sections: %s", appointment_id, exc)

    try:
        report = await analyze_call(
            transcript=transcript,
            campaign_info=campaign_info,
            script=campaign_script,
            qualification_params=campaign_qual,
            client_info=campaign_client,
            operator_email=operator_email,
            prompt_sections=prompt_sections,
            prompt_extra=campaign_db.prompt_extra if campaign_db else None,
            global_docs=global_docs,
        )
    except Exception as exc:
        msg = f"Claude analysis failed: {exc}"
        logger.error("[%s] %s", appointment_id, msg, exc_info=True)
        await _save_error(analysis_id, appointment_id, msg, acuity_account)
        return

    # ── 6. Controllo errore tecnico rilevato da Claude ────────────────────────
    if report.get("errore_tecnico"):
        logger.warning("[%s] Claude ha rilevato trascrizione illeggibile — errore tecnico automatico", appointment_id)
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
            client_company=client_company,
            sidial_call_id=",".join(rec_id for rec_id, _ in recordings),
            operator_name=operator_display,
            qualification_level="errore_tecnico",
            email_sent=False,
        )
        return

    # ── 7. HTML report ────────────────────────────────────────────────────────
    await _update_progress(analysis_id, 85, "Generazione report HTML...")
    appointment_info = {
        "datetime": appointment_data.get("datetime", ""),
        "phone": phone,
        "id": appointment_id,
    }
    html_report = generate_html_report(report, appointment_info, campaign_info, operator_name=operator_display, client_company=client_company, n_recordings=len(recordings))

    # ── 8. Email ──────────────────────────────────────────────────────────────
    from config import settings as cfg

    _rating_to_level = {
        1: "inaccurata", 2: "da_migliorare", 3: "sufficiente", 4: "buona", 5: "eccellente"
    }
    qual_obj = report.get("qualificazione", {})
    qual_rating = qual_obj.get("rating", 2)
    if qual_obj.get("fuori_parametro"):
        qualification_level = "non_in_target"
    else:
        qualification_level = _rating_to_level.get(qual_rating, "da_migliorare")

    _INOLTRO = "inoltra@effoncall.com"
    _email_disabled    = bool(campaign_db and campaign_db.email_disabled)
    _email_no_operator = bool(campaign_db and campaign_db.email_no_operator)

    recipients: list[str] = []
    if not _email_disabled:
        if campaign_db and campaign_db.email_recipients:
            recipients = list(campaign_db.email_recipients)
        if not recipients:
            recipients = [cfg.fallback_email]
        if operator_email and not _email_no_operator and operator_email not in recipients:
            recipients.insert(0, operator_email)
        if _INOLTRO not in recipients:
            recipients.append(_INOLTRO)

    # ── 8b. Invia email report ────────────────────────────────────────────────
    email_sent = False
    # Non inviare se: campagna disabilitata, NON IN TARGET, errore tecnico
    if not _email_disabled and qualification_level not in ("non_in_target", "errore_tecnico"):
        try:
            await _update_progress(analysis_id, 90, "Invio email report...")
            await send_analysis_report(
                recipients=recipients,
                html_content=html_report,
                operator_name=operator_display,
                qualification_level=qualification_level,
                appointment_datetime=appointment_data.get("datetime", ""),
            )
            email_sent = True
            logger.info("[%s] Email inviata a %s", appointment_id, recipients)
        except Exception as exc:
            logger.error("[%s] Email send failed: %s", appointment_id, exc, exc_info=True)

    # ── 9. Save to DB ─────────────────────────────────────────────────────────
    await _update_progress(analysis_id, 95, "Salvataggio...")
    try:
        operator_name_db = operator_display

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
            client_company=client_company,
            sidial_call_id=",".join(rec_id for rec_id, _ in recordings),
            operator_name=operator_name_db,
            qualification_level=qualification_level,
            email_sent=email_sent,
        )
    except Exception as exc:
        logger.error("[%s] DB save failed: %s", appointment_id, exc, exc_info=True)

    logger.info("[%s] Pipeline completed successfully.", appointment_id)


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
        account_id,
        action,
        appointment_id,
    )

    if not appointment_id:
        return {"status": "skipped", "reason": "no appointment id"}

    # Fetch full appointment details (includes labels) from Acuity API
    full_appointment = await get_appointment(appointment_id, account_id)
    if not full_appointment:
        return {"status": "skipped", "reason": "could not fetch appointment"}

    if not should_analyze(full_appointment):
        return {"status": "skipped", "reason": "label not in trigger set"}

    # Respond immediately with 200 — pipeline runs in background
    background_tasks.add_task(
        run_analysis_pipeline,
        appointment_data=full_appointment,
        acuity_account=account_id,
    )

    return {"status": "accepted", "appointment_id": data.get("id")}

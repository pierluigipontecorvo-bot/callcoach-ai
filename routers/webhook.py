"""
Acuity Scheduling webhook receiver + background analysis pipeline.
"""

import json
import logging
from urllib.parse import parse_qs

from fastapi import APIRouter, BackgroundTasks, HTTPException, Request

from services.acuity import (
    check_webhook_signature,
    find_operator_email,
    get_appointment,
    should_analyze,
)
from services.ai_analysis import _extract_operator_name, analyze_call
from services.campaign_db import get_campaign_by_code, get_global_campaign
from services.campaign_parser import parse_campaign_code
from services.email_service import generate_html_report, send_analysis_report
from services.sidial import find_and_download_all_recordings
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

    # Crea subito il record con status=processing (visibile nella UI)
    analysis_id = await _create_processing_record(appointment_id, acuity_account)

    # ── 1. Parse campaign code ────────────────────────────────────────────────
    await _update_progress(analysis_id, 5, "Verifica codice campagna...")
    appointment_type = appointment_data.get("type", "")
    campaign_info = parse_campaign_code(appointment_type)

    if not campaign_info.get("valid"):
        msg = f"Unparseable campaign code: {appointment_type!r}"
        logger.warning("[%s] %s", appointment_id, msg)
        await _save_error(analysis_id, appointment_id, msg, acuity_account)
        return

    logger.info(
        "[%s] Campaign: %s | operator: %s",
        appointment_id,
        campaign_info.get("raw"),
        campaign_info.get("agente"),
    )

    phone = appointment_data.get("phone", "")
    appointment_dt_str = appointment_data.get("datetime", "")
    appointment_dt = parse_iso_datetime(appointment_dt_str) if appointment_dt_str else None

    # ── Operator email (format: op.XX.nome@effoncall.com) ─────────────────────
    operator_email = find_operator_email(appointment_data)
    if operator_email:
        logger.info("[%s] Operator email: %s", appointment_id, operator_email)
    else:
        logger.info("[%s] No operator email found in appointment data", appointment_id)

    # ── 2. Find & download ALL recordings for this contact ────────────────────
    await _update_progress(analysis_id, 10, "Ricerca registrazione su Sidial...")
    try:
        recordings = await find_and_download_all_recordings(
            phone=phone,
            campaign_code=campaign_info.get("raw"),
        )
    except Exception as exc:
        msg = f"Sidial error: {exc}"
        logger.error("[%s] %s", appointment_id, msg, exc_info=True)
        await _save_error(analysis_id, appointment_id, msg, acuity_account)
        return

    if not recordings:
        msg = "Nessuna registrazione trovata su Sidial"
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
            part = await transcribe_audio(audio_bytes)
            transcript_parts.append(f"--- CHIAMATA {idx} (id: {call_id}) ---\n{part}")
            logger.info("[%s] Chiamata %d: %d caratteri", appointment_id, idx, len(part))
        except Exception as exc:
            logger.error("[%s] Trascrizione fallita per call_id=%s: %s", appointment_id, call_id, exc)
            transcript_parts.append(f"--- CHIAMATA {idx} (id: {call_id}) ---\n[trascrizione non disponibile]")

    transcript = "\n\n".join(transcript_parts)
    logger.info("[%s] Trascrizione totale: %d caratteri", appointment_id, len(transcript))

    # ── 4. Load campaign config + global documents ────────────────────────────
    await _update_progress(analysis_id, 60, "Caricamento configurazione campagna...")
    campaign_db = None
    global_doc = None
    try:
        campaign_db = await get_campaign_by_code(campaign_info["raw"])
        global_doc  = await get_global_campaign()
    except Exception as exc:
        logger.warning("[%s] Could not fetch campaign config: %s", appointment_id, exc)

    def _merge_text(global_val: str | None, campaign_val: str | None) -> str | None:
        parts = [p.strip() for p in [global_val, campaign_val] if p and p.strip()]
        return "\n\n---\n\n".join(parts) if parts else None

    merged_script = _merge_text(
        global_doc.script if global_doc else None,
        campaign_db.script if campaign_db else None,
    )
    merged_client_info = _merge_text(
        global_doc.client_info if global_doc else None,
        campaign_db.client_info if campaign_db else None,
    )
    merged_qual = (
        (campaign_db.qualification_params if campaign_db else None)
        or (global_doc.qualification_params if global_doc else None)
    )

    # ── 5. Claude analysis ────────────────────────────────────────────────────
    await _update_progress(analysis_id, 65, "Analisi AI in corso...")
    try:
        report = await analyze_call(
            transcript=transcript,
            campaign_info=campaign_info,
            script=merged_script,
            qualification_params=merged_qual,
            client_info=merged_client_info,
            operator_email=operator_email,
        )
    except Exception as exc:
        msg = f"Claude analysis failed: {exc}"
        logger.error("[%s] %s", appointment_id, msg, exc_info=True)
        await _save_error(analysis_id, appointment_id, msg, acuity_account)
        return

    # ── 6. HTML report ────────────────────────────────────────────────────────
    await _update_progress(analysis_id, 85, "Generazione report HTML...")
    appointment_info = {
        "datetime": appointment_data.get("datetime", ""),
        "phone": phone,
        "id": appointment_id,
    }
    html_report = generate_html_report(report, appointment_info, campaign_info)

    # ── 7. Email ──────────────────────────────────────────────────────────────
    from config import settings as cfg

    _rating_to_level = {1: "inaccurata", 2: "da_migliorare", 3: "buona"}
    qual_rating = report.get("qualificazione", {}).get("rating", 2)
    qualification_level = _rating_to_level.get(qual_rating, "da_migliorare")

    _INOLTRO = "inoltra@effoncall.com"
    recipients: list[str] = []
    if campaign_db and campaign_db.email_recipients:
        recipients = list(campaign_db.email_recipients)
    if not recipients:
        recipients = [cfg.fallback_email]
    if operator_email and operator_email not in recipients:
        recipients.insert(0, operator_email)
    if _INOLTRO not in recipients:
        recipients.append(_INOLTRO)

    # EMAIL TEMPORANEAMENTE DISABILITATA — bug in corso di risoluzione
    email_sent = False
    # try:
    #     await send_analysis_report(...)
    #     email_sent = True
    # except Exception as exc:
    #     logger.error("[%s] Email send failed: %s", appointment_id, exc, exc_info=True)

    # ── 8. Save to DB ─────────────────────────────────────────────────────────
    await _update_progress(analysis_id, 95, "Salvataggio...")
    try:
        operator_name_db = _extract_operator_name(operator_email) or campaign_info.get("agente", "")

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

"""
Acuity Scheduling webhook receiver + background analysis pipeline.
"""

import json
import logging

from fastapi import APIRouter, BackgroundTasks, HTTPException, Request

from services.acuity import check_webhook_signature, should_analyze
from services.ai_analysis import analyze_call
from services.campaign_parser import parse_campaign_code
from services.email_service import generate_html_report, send_analysis_report
from services.sidial import find_and_download_recording
from services.transcription import transcribe_audio
from utils.helpers import parse_iso_datetime

router = APIRouter(prefix="/webhook", tags=["webhook"])
logger = logging.getLogger(__name__)


# ── DB helpers (imported lazily to avoid circular imports at module load) ──────

async def _save_analysis(
    *,
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
):
    from database import AsyncSessionLocal
    from models import Analysis

    async with AsyncSessionLocal() as session:
        obj = Analysis(
            appointment_id=appointment_id,
            campaign_code=campaign_code,
            appointment_datetime=appointment_dt,
            client_phone=phone,
            operator_name=operator_name,
            acuity_account=acuity_account,
            acuity_label="PRESO",
            sidial_call_id=sidial_call_id,
            transcript=transcript,
            qualification_level=qualification_level,
            report_json=report,
            report_html=html_report,
            email_sent=True,
            processing_status="completed",
        )
        session.add(obj)
        await session.commit()


async def _save_error(appointment_id: str, error_msg: str, acuity_account: int):
    from database import AsyncSessionLocal
    from models import Analysis

    async with AsyncSessionLocal() as session:
        obj = Analysis(
            appointment_id=str(appointment_id),
            acuity_account=acuity_account,
            processing_status="error",
            error_message=error_msg[:2000],
        )
        session.add(obj)
        await session.commit()


async def _get_campaign_config(campaign_code: str):
    from database import AsyncSessionLocal
    from models import Campaign
    from sqlalchemy import select

    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(Campaign).where(Campaign.code == campaign_code)
        )
        return result.scalar_one_or_none()


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

    # ── 1. Parse campaign code ────────────────────────────────────────────────
    appointment_type = appointment_data.get("type", "")
    campaign_info = parse_campaign_code(appointment_type)

    if not campaign_info.get("valid"):
        msg = f"Unparseable campaign code: {appointment_type!r}"
        logger.warning("[%s] %s", appointment_id, msg)
        await _save_error(appointment_id, msg, acuity_account)
        return

    logger.info(
        "[%s] Campaign: %s | operator: %s",
        appointment_id,
        campaign_info.get("raw"),
        campaign_info.get("agente"),
    )

    phone = appointment_data.get("phone", "")
    appointment_dt_str = appointment_data.get("datetime", "")
    appointment_dt = parse_iso_datetime(appointment_dt_str)

    # ── 2. Find & download recording ─────────────────────────────────────────
    try:
        sidial_call_id, audio_bytes = await find_and_download_recording(
            phone=phone,
            appointment_datetime=appointment_dt_str,
            campaign_code=campaign_info.get("raw"),
        )
    except Exception as exc:
        msg = f"Sidial error: {exc}"
        logger.error("[%s] %s", appointment_id, msg, exc_info=True)
        await _save_error(appointment_id, msg, acuity_account)
        return

    if not audio_bytes:
        msg = "Recording not found on Sidial"
        logger.error("[%s] %s", appointment_id, msg)
        await _save_error(appointment_id, msg, acuity_account)
        return

    # ── 3. Transcribe ─────────────────────────────────────────────────────────
    try:
        logger.info("[%s] Transcribing %d bytes …", appointment_id, len(audio_bytes))
        transcript = await transcribe_audio(audio_bytes)
        logger.info("[%s] Transcript: %d chars", appointment_id, len(transcript))
    except Exception as exc:
        msg = f"Whisper transcription failed: {exc}"
        logger.error("[%s] %s", appointment_id, msg, exc_info=True)
        await _save_error(appointment_id, msg, acuity_account)
        return

    # ── 4. Load campaign config ───────────────────────────────────────────────
    campaign_db = None
    try:
        campaign_db = await _get_campaign_config(campaign_info["raw"])
    except Exception as exc:
        logger.warning("[%s] Could not fetch campaign config: %s", appointment_id, exc)

    # ── 5. Claude analysis ────────────────────────────────────────────────────
    try:
        report = await analyze_call(
            transcript=transcript,
            campaign_info=campaign_info,
            script=campaign_db.script if campaign_db else None,
            qualification_params=campaign_db.qualification_params if campaign_db else None,
            client_info=campaign_db.client_info if campaign_db else None,
        )
    except Exception as exc:
        msg = f"Claude analysis failed: {exc}"
        logger.error("[%s] %s", appointment_id, msg, exc_info=True)
        await _save_error(appointment_id, msg, acuity_account)
        return

    # ── 6. HTML report ────────────────────────────────────────────────────────
    appointment_info = {
        "datetime": appointment_dt_str,
        "phone": phone,
        "id": appointment_id,
    }
    html_report = generate_html_report(report, appointment_info, campaign_info)

    # ── 7. Email ──────────────────────────────────────────────────────────────
    from config import settings as cfg

    recipients: list[str] = []
    if campaign_db and campaign_db.email_recipients:
        recipients = list(campaign_db.email_recipients)
    if not recipients:
        recipients = [cfg.fallback_email]

    try:
        await send_analysis_report(
            recipients=recipients,
            html_content=html_report,
            operator_name=campaign_info.get("agente", "Operatore"),
            qualification_level=report.get("livello_qualificazione", "corretta"),
            appointment_datetime=appointment_dt_str,
        )
    except Exception as exc:
        # Non-fatal: log the error but still save the analysis
        logger.error("[%s] Email send failed: %s", appointment_id, exc, exc_info=True)

    # ── 8. Save to DB ─────────────────────────────────────────────────────────
    try:
        await _save_analysis(
            appointment_id=appointment_id,
            campaign_code=campaign_info["raw"],
            transcript=transcript,
            report=report,
            html_report=html_report,
            acuity_account=acuity_account,
            appointment_dt=appointment_dt,
            phone=phone,
            sidial_call_id=sidial_call_id or "",
            operator_name=campaign_info.get("agente", ""),
            qualification_level=report.get("livello_qualificazione", "corretta"),
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

    data = json.loads(payload_bytes)

    logger.info(
        "Acuity webhook account=%d action=%s appointment_id=%s labels=%s",
        account_id,
        data.get("action"),
        data.get("id"),
        data.get("labels"),
    )

    if not should_analyze(data):
        return {"status": "skipped", "reason": "label not in trigger set"}

    # Respond immediately with 200 — pipeline runs in background
    background_tasks.add_task(
        run_analysis_pipeline,
        appointment_data=data,
        acuity_account=account_id,
    )

    return {"status": "accepted", "appointment_id": data.get("id")}

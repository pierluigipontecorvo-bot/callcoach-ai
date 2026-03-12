"""
On-demand analysis endpoints.

POST /analysis/trigger   — manually trigger pipeline for a known appointment
POST /analysis/upload    — upload an audio file and run the full pipeline
"""

import logging

from fastapi import APIRouter, Depends, HTTPException, UploadFile, File, Form
from typing import Optional

from schemas import ManualTriggerRequest
from utils.auth import require_admin

router = APIRouter(tags=["analysis"])
logger = logging.getLogger(__name__)


@router.post("/trigger")
async def trigger_analysis(
    body: ManualTriggerRequest,
    _=Depends(require_admin),
):
    """
    Manually re-run the analysis pipeline for an existing appointment.
    Useful for re-processing after a previous failure.
    """
    from routers.webhook import run_analysis_pipeline

    # Build a synthetic appointment_data dict that matches what Acuity sends
    appointment_data = {
        "id": body.appointment_id,
        "phone": body.phone,
        "datetime": body.appointment_datetime,
        "type": body.campaign_code,
        "labels": [{"id": 1, "name": "PRESO"}],
        "action": "manual_trigger",
    }

    import asyncio
    asyncio.create_task(
        run_analysis_pipeline(
            appointment_data=appointment_data,
            acuity_account=body.acuity_account,
        )
    )

    return {
        "status": "accepted",
        "message": "Pipeline started in background",
        "appointment_id": body.appointment_id,
    }


@router.post("/upload")
async def upload_and_analyze(
    campaign_code: str = Form(...),
    appointment_id: str = Form(...),
    appointment_datetime: str = Form(...),
    phone: str = Form(default=""),
    acuity_account: int = Form(default=1),
    audio_file: UploadFile = File(...),
    _=Depends(require_admin),
):
    """
    Upload an audio file directly and run transcription + AI analysis.
    Skips the Sidial recording lookup step.
    """
    from services.campaign_parser import parse_campaign_code
    from services.transcription import transcribe_audio
    from services.ai_analysis import analyze_call
    from services.email_service import generate_html_report, send_analysis_report
    from config import settings as cfg

    if not audio_file.content_type or not audio_file.content_type.startswith("audio"):
        raise HTTPException(
            status_code=400,
            detail=f"Expected an audio file, got {audio_file.content_type!r}",
        )

    campaign_info = parse_campaign_code(campaign_code)
    if not campaign_info.get("valid"):
        raise HTTPException(status_code=400, detail=f"Invalid campaign code: {campaign_code!r}")

    audio_bytes = await audio_file.read()

    try:
        transcript = await transcribe_audio(audio_bytes, filename=audio_file.filename or "upload.mp3")
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Transcription failed: {exc}")

    try:
        report = await analyze_call(transcript=transcript, campaign_info=campaign_info)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Analysis failed: {exc}")

    appointment_info = {
        "datetime": appointment_datetime,
        "phone": phone,
        "id": appointment_id,
    }
    html_report = generate_html_report(report, appointment_info, campaign_info)

    # Save to DB
    from database import AsyncSessionLocal
    from models import Analysis
    from utils.helpers import parse_iso_datetime

    async with AsyncSessionLocal() as session:
        obj = Analysis(
            appointment_id=appointment_id,
            campaign_code=campaign_info["raw"],
            appointment_datetime=parse_iso_datetime(appointment_datetime),
            client_phone=phone,
            operator_name=campaign_info.get("agente", ""),
            acuity_account=acuity_account,
            acuity_label="UPLOAD",
            transcript=transcript,
            qualification_level=report.get("livello_qualificazione", "corretta"),
            report_json=report,
            report_html=html_report,
            processing_status="completed",
        )
        session.add(obj)
        await session.commit()
        await session.refresh(obj)

    return {
        "status": "completed",
        "analysis_id": obj.id,
        "qualification_level": report.get("livello_qualificazione"),
        "riepilogo": report.get("riepilogo_appuntamento"),
    }

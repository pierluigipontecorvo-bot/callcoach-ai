"""
Pipeline step tracking — salva lo stato di ogni fase nel DB.
Status: pending | running | ok | warning | stop
"""
import json
import logging
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

STEPS = {
    1: "webhook",
    2: "firma",
    3: "acuity",
    4: "form",
    5: "etichetta",
    6: "data",
    7: "campagna",
    8: "operatore",
    9: "sidial",
    10: "download",
    11: "trascrizione",
    12: "analisi",
    13: "salvataggio",
    14: "email",
}


def step_key(n: int) -> str:
    return f"{n}_{STEPS[n]}"


async def update_step(
    analysis_id: int,
    step: int,
    status: str,
    message: str = "",
    detail: dict = None,
):
    """Update a single pipeline step status in DB."""
    from database import AsyncSessionLocal
    from sqlalchemy import text

    key = step_key(step)
    data = {
        "status": status,
        "message": message,
        "ts": datetime.now(timezone.utc).isoformat(),
    }
    if detail:
        data["detail"] = detail

    patch_json = json.dumps({key: data})

    async with AsyncSessionLocal() as sess:
        async with sess.begin():
            await sess.execute(
                text("""
                    UPDATE analyses
                    SET pipeline_steps = COALESCE(pipeline_steps, '{}'::jsonb) || CAST(:patch AS jsonb)
                    WHERE id = :aid
                """),
                {"patch": patch_json, "aid": analysis_id},
            )

    logger.debug("pipeline step %d=%s aid=%d msg=%s", step, status, analysis_id, message)


async def init_steps(analysis_id: int):
    """Initialize all steps as pending."""
    from database import AsyncSessionLocal
    from sqlalchemy import text

    steps = {step_key(n): {"status": "pending", "message": ""} for n in range(1, 15)}
    steps_json = json.dumps(steps)

    async with AsyncSessionLocal() as sess:
        async with sess.begin():
            await sess.execute(
                text("UPDATE analyses SET pipeline_steps = :steps WHERE id = :aid"),
                {"steps": steps_json, "aid": analysis_id},
            )

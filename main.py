import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # ── Startup ────────────────────────────────────────────────────────────
    logger.info("CallCoach AI starting — loading Whisper model …")
    try:
        from services.transcription import get_whisper_model
        get_whisper_model()
        logger.info("Whisper model loaded. Server ready.")
    except Exception as exc:
        # Don't crash on startup if Whisper isn't installed yet (e.g. local dev)
        logger.warning("Whisper model not loaded at startup: %s", exc)

    # ── Fix campaigns with active=NULL → set to TRUE (schema DEFAULT TRUE) ────
    try:
        from sqlalchemy import text
        from database import AsyncSessionLocal
        async with AsyncSessionLocal() as session:
            result = await session.execute(
                text("UPDATE campaigns SET active = TRUE WHERE active IS NULL")
            )
            await session.commit()
            if result.rowcount:
                logger.info(
                    "campaign active migration: fixed %d NULL→TRUE record(s)", result.rowcount
                )
    except Exception as exc:
        logger.warning("campaign active migration failed (non-fatal): %s", exc)

    # ── Migrate operator_name separator: ' · ' → '-' ───────────────────────
    # Old records were stored as 'XX · NOME'; new format is 'XX-NOME'.
    try:
        from sqlalchemy import text
        from database import AsyncSessionLocal
        async with AsyncSessionLocal() as session:
            result = await session.execute(
                text(
                    "UPDATE analyses "
                    "SET operator_name = REPLACE(operator_name, ' · ', '-') "
                    "WHERE operator_name LIKE '% · %'"
                )
            )
            await session.commit()
            if result.rowcount:
                logger.info(
                    "operator_name migration: updated %d record(s) (' · ' → '-')",
                    result.rowcount,
                )
    except Exception as exc:
        logger.warning("operator_name migration failed (non-fatal): %s", exc)

    yield

    # ── Shutdown ───────────────────────────────────────────────────────────
    logger.info("CallCoach AI shutting down.")


app = FastAPI(
    title="CallCoach AI",
    description="Automatic call analysis for Effoncall telemarketing operations.",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Routers ────────────────────────────────────────────────────────────────────
from routers import webhook, admin, admin_ui, analysis  # noqa: E402

app.mount("/static", StaticFiles(directory="static"), name="static")

app.include_router(webhook.router)
app.include_router(admin.router, prefix="/admin")
app.include_router(admin_ui.router)          # prefix="/admin/ui" defined in router
app.include_router(analysis.router, prefix="/analysis")


@app.get("/", tags=["root"])
async def root():
    from fastapi.responses import RedirectResponse
    return RedirectResponse(url="/admin/ui/login", status_code=302)


@app.get("/health", tags=["root"])
async def health():
    return {"status": "healthy"}

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

    # ── Create prompt_sections table if it doesn't exist ─────────────────────
    try:
        from sqlalchemy import text
        from database import AsyncSessionLocal
        async with AsyncSessionLocal() as session:
            await session.execute(text("""
                CREATE TABLE IF NOT EXISTS prompt_sections (
                    id SERIAL PRIMARY KEY,
                    section_key VARCHAR(50) UNIQUE NOT NULL,
                    title VARCHAR(100) NOT NULL,
                    content TEXT NOT NULL DEFAULT '',
                    sort_order INT NOT NULL DEFAULT 0,
                    updated_at TIMESTAMPTZ DEFAULT NOW()
                )
            """))
            await session.commit()
    except Exception as exc:
        logger.warning("prompt_sections table migration failed (non-fatal): %s", exc)

    # ── Add prompt_extra column to campaigns if not present ───────────────────
    try:
        from sqlalchemy import text
        from database import AsyncSessionLocal
        async with AsyncSessionLocal() as session:
            await session.execute(
                text("ALTER TABLE campaigns ADD COLUMN IF NOT EXISTS prompt_extra TEXT")
            )
            await session.commit()
    except Exception as exc:
        logger.warning("campaigns.prompt_extra migration failed (non-fatal): %s", exc)

    # ── Create global_documents table if not present ──────────────────────────
    try:
        from sqlalchemy import text
        from database import AsyncSessionLocal
        async with AsyncSessionLocal() as session:
            await session.execute(text("""
                CREATE TABLE IF NOT EXISTS global_documents (
                    id SERIAL PRIMARY KEY,
                    title VARCHAR(200) NOT NULL,
                    content TEXT NOT NULL DEFAULT '',
                    sort_order INTEGER NOT NULL DEFAULT 0,
                    is_active BOOLEAN NOT NULL DEFAULT true,
                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    updated_at TIMESTAMPTZ DEFAULT NOW()
                )
            """))
            await session.commit()
    except Exception as exc:
        logger.warning("global_documents table migration failed (non-fatal): %s", exc)

    # ── Reset analisi bloccate in 'processing'/'pending' al riavvio ──────────
    # Se il container si riavvia mentre un'analisi è in corso, il record DB
    # rimane bloccato su 'processing' per sempre. Lo resettiamo a 'error' con
    # qualification_level='errore_tecnico' in modo che appaia come tale nella UI.
    try:
        from sqlalchemy import text
        from database import AsyncSessionLocal
        async with AsyncSessionLocal() as session:
            result = await session.execute(text("""
                UPDATE analyses
                SET processing_status = 'error',
                    qualification_level = 'errore_tecnico',
                    step_message = 'Analisi interrotta per riavvio del server.',
                    report_json = COALESCE(report_json, '{}'::jsonb) || '{"errore_tecnico": true}'::jsonb
                WHERE processing_status IN ('processing', 'pending')
            """))
            await session.commit()
            if result.rowcount:
                logger.warning(
                    "Startup cleanup: reset %d analisi bloccate → errore_tecnico",
                    result.rowcount,
                )
    except Exception as exc:
        logger.warning("Startup cleanup analisi bloccate fallito (non-fatal): %s", exc)

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

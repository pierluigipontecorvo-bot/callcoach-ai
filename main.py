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
    logger.info("CallCoach AI starting — trascrizione via OpenAI Whisper API.")

    # ── Startup DB migrations — each with hard timeout ──────────────────────
    import asyncio as _asyncio
    from sqlalchemy import text as _text
    from database import AsyncSessionLocal as _ASL

    async def _run_sql(sql: str, label: str):
        """Run a single SQL statement with a 6s hard timeout."""
        try:
            async def _exec():
                async with _ASL() as s:
                    await s.execute(_text("SET LOCAL statement_timeout = '5000'"))
                    r = await s.execute(_text(sql))
                    await s.commit()
                    return getattr(r, "rowcount", 0)
            rows = await _asyncio.wait_for(_exec(), timeout=6.0)
            if rows:
                logger.info("Migration '%s': %d row(s) affected", label, rows)
        except Exception as exc:
            logger.warning("Migration '%s' failed (non-fatal): %s", label, exc)

    await _run_sql(
        "UPDATE campaigns SET active = TRUE WHERE active IS NULL",
        "campaigns.active NULL→TRUE",
    )
    await _run_sql(
        "UPDATE analyses SET operator_name = REPLACE(operator_name, ' · ', '-') "
        "WHERE operator_name LIKE '% · %'",
        "analyses.operator_name separator",
    )
    await _run_sql("""
        CREATE TABLE IF NOT EXISTS prompt_sections (
            id SERIAL PRIMARY KEY, section_key VARCHAR(50) UNIQUE NOT NULL,
            title VARCHAR(100) NOT NULL, content TEXT NOT NULL DEFAULT '',
            sort_order INT NOT NULL DEFAULT 0, updated_at TIMESTAMPTZ DEFAULT NOW()
        )""", "create prompt_sections")
    await _run_sql(
        "ALTER TABLE campaigns ADD COLUMN IF NOT EXISTS prompt_extra TEXT",
        "campaigns.prompt_extra",
    )
    await _run_sql(
        "ALTER TABLE campaigns ADD COLUMN IF NOT EXISTS email_no_operator BOOLEAN DEFAULT FALSE",
        "campaigns.email_no_operator",
    )
    await _run_sql(
        "ALTER TABLE campaigns ADD COLUMN IF NOT EXISTS email_disabled BOOLEAN DEFAULT FALSE",
        "campaigns.email_disabled",
    )
    await _run_sql(
        "ALTER TABLE campaigns ADD COLUMN IF NOT EXISTS transcription_engine VARCHAR",
        "campaigns.transcription_engine",
    )
    await _run_sql("""
        CREATE TABLE IF NOT EXISTS global_documents (
            id SERIAL PRIMARY KEY, title VARCHAR(200) NOT NULL,
            content TEXT NOT NULL DEFAULT '', sort_order INTEGER NOT NULL DEFAULT 0,
            is_active BOOLEAN NOT NULL DEFAULT true,
            created_at TIMESTAMPTZ DEFAULT NOW(), updated_at TIMESTAMPTZ DEFAULT NOW()
        )""", "create global_documents")
    await _run_sql(
        "ALTER TABLE analyses ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NOW()",
        "analyses.updated_at",
    )
    await _run_sql(
        "ALTER TABLE analyses ADD COLUMN IF NOT EXISTS operator_email VARCHAR(200)",
        "analyses.operator_email",
    )
    await _run_sql(
        "ALTER TABLE analyses ADD COLUMN IF NOT EXISTS acuity_form_fields JSONB",
        "analyses.acuity_form_fields",
    )
    await _run_sql("""
        UPDATE analyses
        SET processing_status = 'error', qualification_level = 'errore_tecnico',
            step_message = 'Analisi interrotta per riavvio del server.',
            report_json = COALESCE(report_json, '{}'::jsonb) || '{"errore_tecnico": true}'::jsonb
        WHERE processing_status IN ('processing', 'pending')
        """, "reset stuck analyses")

    logger.info("Startup migrations complete. Server accepting requests.")
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

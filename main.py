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
        """Run a single SQL statement with a 20s hard timeout. Non-fatal."""
        try:
            async def _exec():
                async with _ASL() as s:
                    r = await s.execute(_text(sql))
                    await s.commit()
                    return getattr(r, "rowcount", 0)
            rows = await _asyncio.wait_for(_exec(), timeout=20.0)
            logger.info("Migration '%s': OK (%d rows)", label, rows or 0)
        except Exception as exc:
            logger.warning("Migration '%s' FAILED (non-fatal): %s", label, exc)

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
    await _run_sql(
        "ALTER TABLE analyses ADD COLUMN IF NOT EXISTS pipeline_steps JSONB DEFAULT '{}'",
        "analyses.pipeline_steps",
    )
    await _run_sql(
        "ALTER TABLE analyses ADD COLUMN IF NOT EXISTS label_name VARCHAR(100)",
        "analyses.label_name",
    )
    await _run_sql(
        "ALTER TABLE analyses ADD COLUMN IF NOT EXISTS label_color VARCHAR(50)",
        "analyses.label_color",
    )
    await _run_sql(
        "ALTER TABLE analyses ADD COLUMN IF NOT EXISTS num_recordings INTEGER DEFAULT 0",
        "analyses.num_recordings",
    )
    await _run_sql(
        "ALTER TABLE analyses ADD COLUMN IF NOT EXISTS total_talk_seconds INTEGER DEFAULT 0",
        "analyses.total_talk_seconds",
    )
    await _run_sql("""CREATE TABLE IF NOT EXISTS operators (
        id SERIAL PRIMARY KEY,
        number VARCHAR(10) UNIQUE NOT NULL,
        display_name VARCHAR(100),
        email VARCHAR(200),
        active BOOLEAN DEFAULT TRUE,
        created_at TIMESTAMPTZ DEFAULT NOW()
    )""", "operators table")
    # operators table was created with 'name' in old schema.sql — add missing cols
    await _run_sql(
        "ALTER TABLE operators ADD COLUMN IF NOT EXISTS number VARCHAR(10) UNIQUE",
        "operators.number",
    )
    await _run_sql(
        "ALTER TABLE operators ADD COLUMN IF NOT EXISTS display_name VARCHAR(100)",
        "operators.display_name",
    )
    await _run_sql(
        "ALTER TABLE analyses ADD COLUMN IF NOT EXISTS progress INTEGER DEFAULT 0",
        "analyses.progress",
    )
    await _run_sql(
        "ALTER TABLE analyses ADD COLUMN IF NOT EXISTS step_message TEXT",
        "analyses.step_message",
    )
    await _run_sql("""CREATE TABLE IF NOT EXISTS settings (
        key VARCHAR(100) PRIMARY KEY,
        value TEXT,
        description TEXT,
        updated_at TIMESTAMPTZ DEFAULT NOW()
    )""", "settings table")
    await _run_sql("""INSERT INTO settings (key, value, description) VALUES
        ('transcription_engine', 'openai', 'Motore trascrizione default: openai o assemblyai'),
        ('min_call_length_seconds', '20', 'Durata minima registrazione in secondi'),
        ('sidial_lookback_days', '90', 'Giorni lookback registrazioni Sidial'),
        ('sidial_retry_count', '5', 'Numero massimo retry download'),
        ('sidial_retry_wait_seconds', '180', 'Attesa secondi tra retry')
    ON CONFLICT (key) DO NOTHING""", "settings defaults")
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
    """Health check + DB connectivity check."""
    from database import AsyncSessionLocal
    from sqlalchemy import text
    db_ok = False
    db_err = ""
    try:
        async with AsyncSessionLocal() as s:
            await s.execute(text("SELECT 1"))
        db_ok = True
    except Exception as exc:
        db_err = str(exc)
    return {"status": "healthy" if db_ok else "degraded", "db": db_ok, "db_error": db_err}


@app.exception_handler(Exception)
async def global_exception_handler(request, exc):
    """Log full traceback for every unhandled 500 — visible in Railway logs."""
    import traceback
    from fastapi.responses import HTMLResponse
    tb = traceback.format_exc()
    logger.error("UNHANDLED 500 | %s %s\n%s", request.method, request.url, tb)
    return HTMLResponse(
        content=f"<h2>Internal Server Error</h2><pre>{tb}</pre>",
        status_code=500,
    )

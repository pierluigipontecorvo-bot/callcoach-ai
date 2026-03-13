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
    return {"status": "ok", "service": "CallCoach AI", "version": "1.0.0"}


@app.get("/health", tags=["root"])
async def health():
    return {"status": "healthy"}

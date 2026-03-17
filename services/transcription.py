"""
Whisper transcription service.

Design choices:
  - Model is loaded once at startup (via get_whisper_model()) and reused.
  - An asyncio.Lock serialises concurrent transcription requests because
    Whisper is not thread-safe.
  - Audio is written to a temp file, transcribed, then immediately deleted.
  - If the configured model causes OOM, the service falls back to "tiny".
"""

import asyncio
import logging
import os
import tempfile
from typing import Optional

from config import settings

logger = logging.getLogger(__name__)

_whisper_model = None
_transcription_lock = asyncio.Lock()


def get_whisper_model():
    """
    Load and cache the Whisper model.
    Falls back from 'small' to 'tiny' if an out-of-memory error occurs.
    """
    global _whisper_model
    if _whisper_model is not None:
        return _whisper_model

    import whisper  # lazy import – not available until requirements are installed

    model_size = settings.whisper_model_size
    logger.info("Loading Whisper model '%s' …", model_size)
    try:
        _whisper_model = whisper.load_model(model_size)
        logger.info("Whisper model '%s' loaded.", model_size)
    except (RuntimeError, MemoryError) as exc:
        if model_size != "tiny":
            logger.warning(
                "Failed to load model '%s' (%s). Falling back to 'tiny'.", model_size, exc
            )
            _whisper_model = whisper.load_model("tiny")
            logger.info("Whisper model 'tiny' loaded (fallback).")
        else:
            raise

    return _whisper_model


async def transcribe_audio(
    audio_bytes: bytes, filename: str = "recording.mp3"
) -> str:
    """
    Transcribe *audio_bytes* (Italian) using the locally loaded Whisper model.

    The audio is saved to a temporary file, processed, and the file is deleted
    before returning.  Concurrent calls are serialised by ``_transcription_lock``.
    """
    ext = filename.rsplit(".", 1)[-1] if "." in filename else "mp3"
    suffix = f".{ext}"

    async with _transcription_lock:
        tmp_path: Optional[str] = None
        try:
            # Write bytes to a temp file
            with tempfile.NamedTemporaryFile(suffix=suffix, delete=False) as tmp:
                tmp.write(audio_bytes)
                tmp_path = tmp.name

            logger.info(
                "Transcribing %d bytes (file: %s) …", len(audio_bytes), tmp_path
            )

            # Run synchronous Whisper in a thread pool to avoid blocking the event loop
            loop = asyncio.get_event_loop()
            try:
                transcript = await asyncio.wait_for(
                    loop.run_in_executor(None, _transcribe_sync, tmp_path),
                    timeout=600,  # 10 minuti massimo per file
                )
            except asyncio.TimeoutError:
                raise RuntimeError("Trascrizione interrotta: timeout di 10 minuti superato.")

            logger.info("Transcription complete (%d chars).", len(transcript))
            return transcript
        finally:
            if tmp_path and os.path.exists(tmp_path):
                try:
                    os.unlink(tmp_path)
                except OSError:
                    pass


def _transcribe_sync(file_path: str) -> str:
    """Blocking Whisper transcription (runs in a thread executor)."""
    model = get_whisper_model()
    result = model.transcribe(
        file_path,
        language="it",
        task="transcribe",
        fp16=False,           # CPU-only on Railway
        verbose=False,
        # Qualità migliorata: riduce allucinazioni e testo ripetitivo
        condition_on_previous_text=False,
        no_speech_threshold=0.6,
        compression_ratio_threshold=2.4,
        initial_prompt=(
            "Trascrizione di una telefonata commerciale B2B in italiano. "
            "L'operatore propone un servizio di logistica/spedizioni a un'azienda."
        ),
    )
    return result["text"].strip()

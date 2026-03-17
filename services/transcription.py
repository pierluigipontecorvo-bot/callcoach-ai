"""
Whisper transcription service.

Design choices:
  - Model is loaded once at startup (via get_whisper_model()) and reused.
  - An asyncio.Lock serialises concurrent transcription requests because
    Whisper is not thread-safe.
  - Audio is written to a temp file, trimmed to MAX_AUDIO_MINUTES via ffmpeg,
    transcribed, then immediately deleted.
  - If the configured model causes OOM, the service falls back to "tiny".
"""

import asyncio
import logging
import os
import subprocess
import tempfile
from typing import Optional

from config import settings

logger = logging.getLogger(__name__)

_whisper_model = None
_transcription_lock = asyncio.Lock()

# Cap at 1 min per recording — Whisper small on Railway CPU takes ~10x realtime,
# so 1 min audio ≈ 10-20s transcription. 3 min caused timeout + zombie threads.
MAX_AUDIO_MINUTES = 1


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


def _trim_audio(src_path: str, max_seconds: int = MAX_AUDIO_MINUTES * 60) -> str:
    """
    Use ffmpeg to trim audio to at most max_seconds.
    Returns the path of the (possibly trimmed) file.
    If ffmpeg is not available or trimming fails, returns src_path unchanged.
    """
    trimmed_path = src_path + "_trimmed.mp3"
    try:
        result = subprocess.run(
            [
                "ffmpeg", "-y",
                "-i", src_path,
                "-t", str(max_seconds),
                "-acodec", "libmp3lame", "-q:a", "4",
                trimmed_path,
            ],
            capture_output=True,
            timeout=60,
        )
        if result.returncode == 0 and os.path.exists(trimmed_path):
            orig_mb = os.path.getsize(src_path) / 1_048_576
            trim_mb = os.path.getsize(trimmed_path) / 1_048_576
            logger.info(
                "Audio trimmed to %d min: %.1f MB → %.1f MB",
                MAX_AUDIO_MINUTES, orig_mb, trim_mb,
            )
            return trimmed_path
        else:
            logger.warning("ffmpeg trim failed (rc=%d), using original.", result.returncode)
    except Exception as exc:
        logger.warning("ffmpeg trim error: %s — using original.", exc)

    # Clean up failed trimmed file
    if os.path.exists(trimmed_path):
        try:
            os.unlink(trimmed_path)
        except OSError:
            pass
    return src_path


async def transcribe_audio(
    audio_bytes: bytes, filename: str = "recording.mp3"
) -> str:
    """
    Transcribe *audio_bytes* (Italian) using the locally loaded Whisper model.

    Audio is trimmed to MAX_AUDIO_MINUTES before transcription to prevent
    indefinite hangs on very long or corrupt recordings.
    Concurrent calls are serialised by ``_transcription_lock``.
    """
    ext = filename.rsplit(".", 1)[-1] if "." in filename else "mp3"
    suffix = f".{ext}"

    async with _transcription_lock:
        tmp_path: Optional[str] = None
        trimmed_path: Optional[str] = None
        try:
            # Write bytes to a temp file
            with tempfile.NamedTemporaryFile(suffix=suffix, delete=False) as tmp:
                tmp.write(audio_bytes)
                tmp_path = tmp.name

            logger.info(
                "Transcribing %d bytes (file: %s) …", len(audio_bytes), tmp_path
            )

            # Trim to MAX_AUDIO_MINUTES via ffmpeg (runs fast, before Whisper)
            loop = asyncio.get_event_loop()
            transcribe_path = await loop.run_in_executor(
                None, _trim_audio, tmp_path
            )
            if transcribe_path != tmp_path:
                trimmed_path = transcribe_path

            # Run synchronous Whisper in a thread pool
            transcript = await loop.run_in_executor(
                None, _transcribe_sync, transcribe_path
            )

            logger.info("Transcription complete (%d chars).", len(transcript))
            return transcript
        finally:
            for path in (tmp_path, trimmed_path):
                if path and os.path.exists(path):
                    try:
                        os.unlink(path)
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
        condition_on_previous_text=False,
        no_speech_threshold=0.6,
        compression_ratio_threshold=2.4,
        initial_prompt="Trascrizione di una telefonata commerciale B2B in italiano.",
    )
    return result["text"].strip()

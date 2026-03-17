"""
Trascrizione audio via OpenAI Whisper API.

- Nessun modello locale, nessun blocco CPU, nessun lock.
- Ogni chiamata è una semplice richiesta HTTP ad OpenAI (~5-15 secondi).
- Limite OpenAI: 25 MB per file. Le registrazioni Sidial sono tipicamente < 5 MB.
"""

import io
import logging

from config import settings

logger = logging.getLogger(__name__)


async def transcribe_audio(
    audio_bytes: bytes, filename: str = "recording.mp3"
) -> str:
    """
    Trascrive audio_bytes in italiano usando OpenAI Whisper API.
    Restituisce il testo trascritto.
    """
    from openai import AsyncOpenAI

    if not settings.openai_api_key:
        raise RuntimeError(
            "OPENAI_API_KEY non configurata. "
            "Aggiungila nelle variabili Railway."
        )

    client = AsyncOpenAI(api_key=settings.openai_api_key)

    mb = len(audio_bytes) / 1_048_576
    logger.info("Trascrizione OpenAI Whisper: %.1f MB (file: %s)", mb, filename)

    # OpenAI richiede un file-like object con attributo .name
    audio_file = io.BytesIO(audio_bytes)
    audio_file.name = filename

    response = await client.audio.transcriptions.create(
        model="whisper-1",
        file=audio_file,
        language="it",
        response_format="text",
    )

    text = (response or "").strip()
    logger.info("Trascrizione completata: %d caratteri.", len(text))
    return text

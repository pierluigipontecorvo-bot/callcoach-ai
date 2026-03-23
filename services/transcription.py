"""
Trascrizione audio: supporta OpenAI Whisper API e AssemblyAI.

Selezione motore (3 livelli, priorità crescente):
  1. Globale: variabile Railway TRANSCRIPTION_ENGINE (default "openai")
  2. Per campagna: Campaign.transcription_engine (sovrascrive il globale)
  3. Per analisi: parametro engine passato direttamente a transcribe_audio()

- OpenAI Whisper: richiesta HTTP ad OpenAI (~5-15s, limite 25 MB).
- AssemblyAI: upload + trascrizione asincrona con speaker diarization.
"""

import io
import logging

from config import settings

logger = logging.getLogger(__name__)


async def transcribe_with_openai(
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
    logger.info("Trascrizione OpenAI completata: %d caratteri.", len(text))
    return text


async def transcribe_with_assemblyai(
    audio_bytes: bytes, filename: str = "recording.mp3"
) -> str:
    """Transcribe using AssemblyAI with speaker diarization."""
    import asyncio
    import httpx

    if not settings.assemblyai_api_key:
        raise RuntimeError(
            "ASSEMBLYAI_API_KEY non configurata. "
            "Aggiungila nelle variabili Railway."
        )

    headers = {"authorization": settings.assemblyai_api_key}
    mb = len(audio_bytes) / 1_048_576
    logger.info("Trascrizione AssemblyAI: %.1f MB (file: %s)", mb, filename)

    # Step 1: Upload audio
    async with httpx.AsyncClient(timeout=120.0) as client:
        upload_resp = await client.post(
            "https://api.assemblyai.com/v2/upload",
            headers=headers,
            content=audio_bytes,
        )
        upload_resp.raise_for_status()
        upload_url = upload_resp.json()["upload_url"]

    # Step 2: Request transcription with speaker diarization
    async with httpx.AsyncClient(timeout=30.0) as client:
        transcript_resp = await client.post(
            "https://api.assemblyai.com/v2/transcript",
            headers=headers,
            json={
                "audio_url": upload_url,
                "speaker_labels": True,
                "language_code": "it",
                # speech_model "best" (slam-1) only supports English — omit to use
                # Universal-2 which supports Italian + speaker diarization
            },
        )
        transcript_resp.raise_for_status()
        transcript_id = transcript_resp.json()["id"]

    # Step 3: Poll until complete
    polling_url = f"https://api.assemblyai.com/v2/transcript/{transcript_id}"
    for _ in range(120):  # max 2 min polling
        await asyncio.sleep(1)
        async with httpx.AsyncClient(timeout=30.0) as client:
            poll_resp = await client.get(polling_url, headers=headers)
            poll_resp.raise_for_status()
            data = poll_resp.json()

        if data["status"] == "completed":
            # Format with speaker labels
            utterances = data.get("utterances") or []
            if utterances:
                lines = [f"Speaker {u['speaker']}: {u['text']}" for u in utterances]
                result = "\n".join(lines)
            else:
                result = data.get("text") or ""
            logger.info("AssemblyAI trascrizione completata: %d caratteri", len(result))
            return result
        elif data["status"] == "error":
            raise RuntimeError(f"AssemblyAI error: {data.get('error')}")

    raise TimeoutError("AssemblyAI polling timeout")


async def transcribe_audio(
    audio_bytes: bytes, filename: str = "recording.mp3", engine: str | None = None
) -> str:
    """
    Trascrive audio_bytes usando il motore selezionato.

    engine=None usa il default globale (settings.transcription_engine).
    """
    _engine = engine or settings.transcription_engine or "openai"
    logger.info("🎙 Motore trascrizione selezionato: %s (engine param=%s, global=%s)",
                _engine, engine, settings.transcription_engine)

    if _engine == "assemblyai":
        return await transcribe_with_assemblyai(audio_bytes, filename)
    else:
        return await transcribe_with_openai(audio_bytes, filename)

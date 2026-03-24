"""
Trascrizione audio: supporta OpenAI Whisper API e AssemblyAI.

Selezione motore (3 livelli, priorita crescente):
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
    """Transcribe using AssemblyAI with speaker diarization.

    Usa UN SINGOLO httpx client per tutta la sessione (upload + request + polling).
    Polling max 5 minuti con intervallo progressivo (2s → 5s).
    """
    import asyncio
    import httpx

    if not settings.assemblyai_api_key:
        raise RuntimeError(
            "ASSEMBLYAI_API_KEY non configurata. "
            "Aggiungila nelle variabili Railway."
        )

    headers = {"authorization": settings.assemblyai_api_key}
    mb = len(audio_bytes) / 1_048_576
    logger.info("AssemblyAI: inizio trascrizione %.1f MB (file: %s)", mb, filename)

    async with httpx.AsyncClient(timeout=180.0, headers=headers) as client:

        # ── Step 1: Upload audio ─────────────────────────────────────────────
        logger.info("AssemblyAI: upload audio...")
        upload_resp = await client.post(
            "https://api.assemblyai.com/v2/upload",
            content=audio_bytes,
        )
        if upload_resp.status_code != 200:
            body = upload_resp.text[:500]
            raise RuntimeError(
                f"AssemblyAI upload fallito HTTP {upload_resp.status_code}: {body}"
            )
        upload_url = upload_resp.json().get("upload_url")
        if not upload_url:
            raise RuntimeError(f"AssemblyAI upload: nessun upload_url nella risposta: {upload_resp.text[:300]}")
        logger.info("AssemblyAI: upload completato → %s", upload_url[:80])

        # ── Step 2: Request transcription ────────────────────────────────────
        logger.info("AssemblyAI: richiesta trascrizione con speaker_labels + language_code=it...")
        transcript_resp = await client.post(
            "https://api.assemblyai.com/v2/transcript",
            json={
                "audio_url": upload_url,
                "speaker_labels": True,
                "language_code": "it",
                # speech_model "best" (slam-1) supporta SOLO inglese — non specificare
                # per usare Universal-2 che supporta italiano + speaker diarization
            },
        )
        if transcript_resp.status_code != 200:
            body = transcript_resp.text[:500]
            raise RuntimeError(
                f"AssemblyAI transcript request fallito HTTP {transcript_resp.status_code}: {body}"
            )
        resp_data = transcript_resp.json()
        transcript_id = resp_data.get("id")
        if not transcript_id:
            raise RuntimeError(f"AssemblyAI: nessun transcript id nella risposta: {resp_data}")
        logger.info("AssemblyAI: trascrizione avviata id=%s status=%s", transcript_id, resp_data.get("status"))

        # ── Step 3: Poll until complete (max 5 min) ─────────────────────────
        polling_url = f"https://api.assemblyai.com/v2/transcript/{transcript_id}"
        max_polls = 60       # 60 iterazioni
        poll_interval = 5    # 5 secondi tra i poll → max ~5 minuti

        for attempt in range(1, max_polls + 1):
            await asyncio.sleep(poll_interval)

            try:
                poll_resp = await client.get(polling_url)
                poll_resp.raise_for_status()
                data = poll_resp.json()
            except Exception as poll_exc:
                logger.warning("AssemblyAI: poll %d/%d fallito: %s", attempt, max_polls, poll_exc)
                continue

            status = data.get("status", "unknown")

            if attempt % 6 == 0:  # log ogni 30 secondi
                logger.info("AssemblyAI: poll %d/%d — status=%s", attempt, max_polls, status)

            if status == "completed":
                # Format with speaker labels
                utterances = data.get("utterances") or []
                if utterances:
                    lines = [f"Speaker {u['speaker']}: {u['text']}" for u in utterances]
                    result = "\n".join(lines)
                else:
                    result = data.get("text") or ""
                logger.info("AssemblyAI: trascrizione completata in ~%ds — %d caratteri",
                            attempt * poll_interval, len(result))
                return result

            elif status == "error":
                error_msg = data.get("error", "errore sconosciuto")
                raise RuntimeError(f"AssemblyAI errore trascrizione: {error_msg}")

            elif status not in ("queued", "processing"):
                logger.warning("AssemblyAI: status inatteso '%s' — continuo polling", status)

    raise TimeoutError(
        f"AssemblyAI: polling timeout dopo {max_polls * poll_interval}s "
        f"per transcript_id={transcript_id}"
    )


async def transcribe_audio(
    audio_bytes: bytes, filename: str = "recording.mp3", engine: str | None = None
) -> str:
    """
    Trascrive audio_bytes usando il motore selezionato.

    engine=None usa il default globale (settings.transcription_engine).
    """
    _engine = engine or settings.transcription_engine or "openai"
    logger.info("Motore trascrizione selezionato: %s (engine param=%s, global=%s)",
                _engine, engine, settings.transcription_engine)

    if _engine == "assemblyai":
        return await transcribe_with_assemblyai(audio_bytes, filename)
    else:
        return await transcribe_with_openai(audio_bytes, filename)

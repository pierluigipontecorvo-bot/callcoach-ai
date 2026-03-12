"""
Sidial CRM client.

Responsibilities:
  1. Discover available API actions (get_api_docs)
  2. Search recent calls for a given phone number & date
  3. Find the call closest in time to the Acuity appointment (±60 min)
  4. Retrieve the recording URL for that call
  5. Download the audio bytes
"""

import logging
import re
from datetime import datetime, timedelta, timezone
from typing import Optional, Tuple
from urllib.parse import quote

import httpx

from config import settings

logger = logging.getLogger(__name__)

# Base URL and auth
_BASE = settings.sidial_api_url.rstrip("/")
_HEADERS = {
    "Authorization": f"Bearer {settings.sidial_api_token}",
    "Content-Type": "application/json",
    "Accept": "application/json",
}

# Maximum time delta to match a call to an appointment (seconds)
_MATCH_WINDOW_SECONDS = 60 * 60  # ±60 minutes


# ── Phone normalisation ───────────────────────────────────────────────────────

def _normalize_phone(raw: str) -> str:
    """
    Strip all non-digit characters, remove leading country codes (+39 / 0039),
    and return the significant part of an Italian mobile/landline number.
    """
    digits = re.sub(r"\D", "", raw)
    for prefix in ("0039", "39"):
        if digits.startswith(prefix) and len(digits) > len(prefix) + 6:
            digits = digits[len(prefix):]
            break
    return digits


# ── API discovery ─────────────────────────────────────────────────────────────

async def get_api_docs() -> dict:
    """
    Hit the Sidial API discovery endpoint to learn available actions.
    Tries several common patterns; returns the first successful response.
    """
    probes = [
        f"{_BASE}?action=help",
        f"{_BASE}?action=list_actions",
        f"{_BASE}?action=docs",
        f"{_BASE}",
    ]
    async with httpx.AsyncClient(timeout=15.0) as client:
        for url in probes:
            try:
                resp = await client.get(url, headers=_HEADERS)
                if resp.status_code == 200:
                    logger.info("Sidial API docs retrieved from %s", url)
                    try:
                        return resp.json()
                    except Exception:
                        return {"raw": resp.text}
            except Exception as exc:
                logger.debug("Sidial probe %s failed: %s", url, exc)
    return {}


# ── Call search ───────────────────────────────────────────────────────────────

async def _search_calls(phone: str, date_str: Optional[str] = None) -> list[dict]:
    """
    Query Sidial for all calls matching *phone*.
    If *date_str* (YYYY-MM-DD) is provided, filters by that date.
    Returns the raw list of call objects.
    """
    params: dict = {
        "action": "calls",
        "phone": phone,
        "limit": "500",
    }
    if date_str:
        params["date"] = date_str

    url = _BASE
    async with httpx.AsyncClient(timeout=60.0) as client:
        try:
            resp = await client.get(url, params=params, headers=_HEADERS)
            resp.raise_for_status()
            data = resp.json()
            if isinstance(data, list):
                return data
            return data.get("calls", data.get("data", data.get("results", [])))
        except Exception as exc:
            logger.error("Sidial call search failed: %s", exc)
            return []


def _parse_call_datetime(call: dict) -> Optional[datetime]:
    """
    Extract a datetime from a Sidial call object.
    Tries common field names and formats.
    """
    for field in ("start_time", "datetime", "date", "created_at", "timestamp"):
        val = call.get(field)
        if not val:
            continue
        if isinstance(val, (int, float)):
            return datetime.fromtimestamp(val, tz=timezone.utc)
        for fmt in (
            "%Y-%m-%dT%H:%M:%S%z",
            "%Y-%m-%dT%H:%M:%SZ",
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%dT%H:%M:%S",
        ):
            try:
                dt = datetime.strptime(str(val), fmt)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt
            except ValueError:
                pass
    return None


def _find_closest_call(
    calls: list[dict], appointment_dt: datetime
) -> Optional[dict]:
    """
    Return the call whose start_time is closest to *appointment_dt*
    and within ±60 minutes.  Returns None if no match.
    """
    best: Optional[dict] = None
    best_delta = timedelta(seconds=_MATCH_WINDOW_SECONDS + 1)

    for call in calls:
        call_dt = _parse_call_datetime(call)
        if call_dt is None:
            continue
        # Ensure both are timezone-aware
        if appointment_dt.tzinfo is None:
            appointment_dt = appointment_dt.replace(tzinfo=timezone.utc)
        delta = abs(call_dt - appointment_dt)
        if delta < best_delta:
            best_delta = delta
            best = call

    return best


# ── Recording URL ─────────────────────────────────────────────────────────────

async def _get_recording_url(call_id: str) -> Optional[str]:
    """
    Given a Sidial call_id, return the direct recording URL (mp3/wav).
    """
    params = {"action": "recording", "call_id": call_id}
    async with httpx.AsyncClient(timeout=15.0) as client:
        try:
            resp = await client.get(_BASE, params=params, headers=_HEADERS)
            resp.raise_for_status()
            data = resp.json()
            # Support multiple field name conventions
            for key in ("recording_url", "url", "audio_url", "file_url"):
                url = data.get(key)
                if url:
                    return url
            logger.warning("Sidial recording response has no URL field: %s", data)
            return None
        except Exception as exc:
            logger.error("Sidial get_recording_url failed for call %s: %s", call_id, exc)
            return None


# ── Audio download ────────────────────────────────────────────────────────────

async def download_recording(recording_url: str) -> bytes:
    """Download raw audio bytes from *recording_url*."""
    async with httpx.AsyncClient(timeout=120.0) as client:
        resp = await client.get(
            recording_url,
            headers={"Authorization": f"Bearer {settings.sidial_api_token}"},
            follow_redirects=True,
        )
        resp.raise_for_status()
        return resp.content


# ── Public façade ─────────────────────────────────────────────────────────────

async def find_and_download_recording(
    phone: str,
    appointment_datetime: str,
    campaign_code: Optional[str] = None,
) -> Tuple[Optional[str], Optional[bytes]]:
    """
    Full flow: search for the call → find closest match → download audio.

    Returns ``(call_id, audio_bytes)`` or ``(None, None)`` on failure.
    """
    norm_phone = _normalize_phone(phone)
    logger.info(
        "Sidial: searching recording for phone=%s (normalised=%s) appointment=%s",
        phone,
        norm_phone,
        appointment_datetime,
    )

    # Parse appointment datetime
    appt_dt: Optional[datetime] = None
    for fmt in (
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S",
    ):
        try:
            appt_dt = datetime.strptime(appointment_datetime, fmt)
            if appt_dt.tzinfo is None:
                appt_dt = appt_dt.replace(tzinfo=timezone.utc)
            break
        except ValueError:
            pass

    if appt_dt is None:
        logger.error("Cannot parse appointment_datetime: %s", appointment_datetime)
        return None, None

    date_str = appt_dt.strftime("%Y-%m-%d")
    calls = await _search_calls(norm_phone, date_str)

    if not calls:
        logger.warning(
            "Sidial: no calls found for phone=%s date=%s (campaign=%s)",
            norm_phone,
            date_str,
            campaign_code,
        )
        return None, None

    call = _find_closest_call(calls, appt_dt)
    if call is None:
        logger.warning(
            "Sidial: no call within ±60 min for phone=%s appointment=%s",
            norm_phone,
            appointment_datetime,
        )
        return None, None

    call_id = str(call.get("id") or call.get("call_id") or "")
    logger.info("Sidial: matched call_id=%s for appointment=%s", call_id, appointment_datetime)

    recording_url = await _get_recording_url(call_id)
    if not recording_url:
        logger.warning("Sidial: no recording URL for call_id=%s", call_id)
        return call_id, None

    logger.info("Sidial: downloading recording from %s", recording_url)
    try:
        audio_bytes = await download_recording(recording_url)
        logger.info(
            "Sidial: downloaded %d bytes for call_id=%s", len(audio_bytes), call_id
        )
        return call_id, audio_bytes
    except Exception as exc:
        logger.error("Sidial: download failed for call_id=%s: %s", call_id, exc)
        return call_id, None


async def find_and_download_all_recordings(
    phone: str,
    campaign_code: Optional[str] = None,
) -> list[Tuple[str, bytes]]:
    """
    Trova e scarica TUTTE le registrazioni per un numero di telefono,
    senza filtro di data. Restituisce lista di (call_id, audio_bytes)
    ordinata cronologicamente (dalla più vecchia alla più recente).
    """
    norm_phone = _normalize_phone(phone)
    logger.info(
        "Sidial: searching ALL recordings for phone=%s (normalised=%s) campaign=%s",
        phone,
        norm_phone,
        campaign_code,
    )

    calls = await _search_calls(norm_phone)

    if not calls:
        logger.warning("Sidial: no calls found for phone=%s", norm_phone)
        return []

    # Ordina dalla più vecchia alla più recente
    def _sort_key(c: dict):
        dt = _parse_call_datetime(c)
        return dt if dt else datetime.min.replace(tzinfo=timezone.utc)

    calls_sorted = sorted(calls, key=_sort_key)
    logger.info("Sidial: found %d calls for phone=%s", len(calls_sorted), norm_phone)

    results: list[Tuple[str, bytes]] = []
    for call in calls_sorted:
        call_id = str(call.get("id") or call.get("call_id") or "")
        if not call_id:
            continue
        recording_url = await _get_recording_url(call_id)
        if not recording_url:
            logger.warning("Sidial: no recording URL for call_id=%s — skipping", call_id)
            continue
        try:
            audio_bytes = await download_recording(recording_url)
            logger.info("Sidial: downloaded %d bytes for call_id=%s", len(audio_bytes), call_id)
            results.append((call_id, audio_bytes))
        except Exception as exc:
            logger.error("Sidial: download failed for call_id=%s: %s", call_id, exc)

    logger.info("Sidial: %d recordings downloaded for phone=%s", len(results), norm_phone)
    return results

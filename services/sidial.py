"""
Sidial CRM client — implementazione corretta basata su DOC-010.

Flusso:
  1. POST a=searchLeads  — trova leadId dal numero di telefono (filtro JSON params)
  2. POST a=searchRecs   — lista registrazioni del lead (tabella leadsRecs)
  3. GET  a=getLeadRec   — scarica singola registrazione per id numerico

Parametri sempre in GET salvo azioni che richiedono POST (searchLeads, searchRecs).
Auth: apiToken come query param in GET, oppure nel body in POST.
"""

import json
import logging
import re
from datetime import datetime, timezone
from typing import Optional, Tuple

import httpx

from config import settings

logger = logging.getLogger(__name__)

_BASE  = settings.sidial_api_url.rstrip("/")
_TOKEN = settings.sidial_api_token


def _log_config() -> None:
    logger.info("Sidial config: BASE=%s token_len=%d", _BASE, len(_TOKEN))



# ── Normalizzazione telefono ──────────────────────────────────────────────────

def _normalize_phone(raw: str) -> str:
    """
    Rimuove tutti i caratteri non numerici, elimina i prefissi paese
    +39 / 0039 e restituisce la parte significativa del numero italiano.
    """
    digits = re.sub(r"\D", "", raw)
    for prefix in ("0039", "39"):
        if digits.startswith(prefix) and len(digits) > len(prefix) + 6:
            digits = digits[len(prefix):]
            break
    return digits


# ── Ricerca lead per telefono ─────────────────────────────────────────────────

async def _search_leads_by_phone(phone: str) -> list[dict]:
    """
    POST a=searchLeads con filtro JSON esatto su phone1/phone2/phone3/phone4.
    Usa operator="=" per match esatto (non LIKE) → evita OOM.
    Restituisce lista di lead (di solito 0 o 1 elemento).
    """
    # Cerca in tutti i campi telefono
    filters = [
        {"table": "leads", "field": field, "operator": "=", "value": phone}
        for field in ("phone1", "phone2", "phone3", "phone4")
    ]

    results: list[dict] = []
    async with httpx.AsyncClient(timeout=30.0) as client:
        # searchLeads con OR implicito su più chiamate separate (AND non supporta OR)
        for flt in filters:
            params_json = json.dumps([flt])
            form_body = {"a": "searchLeads", "apiToken": _TOKEN, "params": params_json}
            logger.info(
                "searchLeads POST url=%s body_keys=%s", _BASE, list(form_body.keys())
            )
            try:
                resp = await client.post(_BASE, data=form_body)
                if resp.status_code != 200:
                    logger.error(
                        "searchLeads HTTP %s per field=%s body=%s",
                        resp.status_code, flt["field"], resp.text[:500],
                    )
                    continue
                data = resp.json()
                # Risposta: {"response": {"error": false, "totLeads": N}, "results": [...]}
                if isinstance(data, dict):
                    if data.get("response", {}).get("error"):
                        logger.debug(
                            "searchLeads field=%s: %s", flt["field"],
                            data.get("response", {}).get("message", "errore sconosciuto"),
                        )
                    elif "results" in data:
                        results.extend(data["results"])
                    elif isinstance(data, list):
                        results.extend(data)
                elif isinstance(data, list):
                    results.extend(data)
            except Exception as exc:
                logger.error("searchLeads fallito per %s=%s: %s", flt["field"], phone, exc)

    # Deduplica per leadId
    seen: set = set()
    unique: list[dict] = []
    for lead in results:
        lead_id = str(lead.get("id") or "")
        if lead_id and lead_id not in seen:
            seen.add(lead_id)
            unique.append(lead)

    logger.info("searchLeads: trovati %d lead per phone=%s", len(unique), phone)
    return unique


# ── Ricerca registrazioni per leadId ─────────────────────────────────────────

async def _search_recs_by_lead(lead_id: str) -> list[dict]:
    """
    POST a=searchRecs con filtro JSON su leadsRecs.lead = {leadId}.
    Restituisce lista di record con campi: id, createdWhen, callLength, fileName, ecc.
    """
    filters = [{"table": "leadsRecs", "field": "lead", "operator": "=", "value": int(lead_id)}]
    params_json = json.dumps(filters)

    async with httpx.AsyncClient(timeout=30.0) as client:
        try:
            resp = await client.post(
                _BASE,
                data={"a": "searchRecs", "apiToken": _TOKEN, "params": params_json},
            )
            # 404 = nessuna registrazione per questo lead → risposta normale, non errore
            if resp.status_code == 404:
                logger.info("searchRecs: nessuna registrazione per leadId=%s", lead_id)
                return []
            if resp.status_code != 200:
                logger.error(
                    "searchRecs HTTP %s per leadId=%s body=%s",
                    resp.status_code, lead_id, resp.text[:500],
                )
                return []
            data = resp.json()
            logger.info("searchRecs raw response leadId=%s: %s", lead_id, str(data)[:300])
            # Risposta può essere lista diretta o {"response":{...},"results":[...]}
            if isinstance(data, list):
                logger.info("searchRecs: %d registrazioni per leadId=%s", len(data), lead_id)
                return data
            if isinstance(data, dict):
                if data.get("response", {}).get("error"):
                    logger.info(
                        "searchRecs nessuna rec per leadId=%s: %s",
                        lead_id, data.get("response", {}).get("message", "?"),
                    )
                    return []
                if "results" in data:
                    recs = data["results"]
                    logger.info("searchRecs: %d registrazioni per leadId=%s", len(recs), lead_id)
                    return recs
            logger.warning("searchRecs risposta inattesa per leadId=%s: %s", lead_id, str(data)[:300])
            return []
        except Exception as exc:
            logger.error("searchRecs fallito per leadId=%s: %s", lead_id, exc)
            return []


def _parse_rec_datetime(rec: dict) -> Optional[datetime]:
    """Estrae datetime timezone-aware da un record leadsRecs."""
    val = rec.get("createdWhen")
    if not val:
        return None
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


# ── Download singola registrazione ────────────────────────────────────────────

async def _download_rec(rec_id: str) -> Optional[bytes]:
    """
    GET a=getLeadRec&id={rec_id} → bytes audio (mp3/wav).
    """
    url = f"{_BASE}?a=getLeadRec&id={rec_id}&apiToken={_TOKEN}"
    async with httpx.AsyncClient(timeout=120.0, follow_redirects=True) as client:
        try:
            resp = await client.get(url)
            if resp.status_code != 200:
                logger.warning("getLeadRec id=%s HTTP %s", rec_id, resp.status_code)
                return None

            content_type = resp.headers.get("content-type", "")

            # Se la risposta è JSON → errore o URL indiretto
            if "application/json" in content_type or resp.content[:1] in (b"{", b"["):
                try:
                    data = resp.json()
                    # Errore esplicito dall'API
                    if data.get("error") or data.get("response", {}).get("error"):
                        msg = (
                            data.get("message")
                            or data.get("response", {}).get("message", "?")
                        )
                        logger.warning("getLeadRec id=%s errore: %s", rec_id, msg)
                        return None
                    # Potrebbe essere un URL indiretto
                    for key in ("url", "recording_url", "audio_url", "file_url"):
                        rec_url = data.get(key)
                        if rec_url:
                            audio_resp = await client.get(rec_url)
                            audio_resp.raise_for_status()
                            if len(audio_resp.content) > 1000:
                                return audio_resp.content
                except Exception:
                    pass  # non è JSON, potrebbe essere audio diretto

            # Audio diretto (mp3/wav/ogg/…)
            if len(resp.content) > 1000:
                logger.info("getLeadRec: %d bytes per id=%s", len(resp.content), rec_id)
                return resp.content

            logger.warning("getLeadRec id=%s: risposta troppo corta (%d B)", rec_id, len(resp.content))
            return None

        except Exception as exc:
            logger.error("getLeadRec fallito per id=%s: %s", rec_id, exc)
            return None


# ── Facciata pubblica ─────────────────────────────────────────────────────────

async def find_and_download_all_recordings(
    phone: str,
    campaign_code: Optional[str] = None,
    appointment_dt: Optional[datetime] = None,   # ignorato, mantenuto per compatibilità
    lookback_days: int = 30,
    max_recs: int = 20,
) -> list[Tuple[str, bytes]]:
    """
    Trova e scarica le registrazioni degli ultimi lookback_days giorni per un
    numero di telefono.  L'obiettivo è analizzare le TELEFONATE fatte al cliente,
    non la data dell'appuntamento Acuity.

    Flusso:
      1. searchLeads → leadId(s) per il numero
      2. searchRecs  → tutte le registrazioni del lead
      3. filtra quelle negli ultimi lookback_days giorni (default 30)
         → se nessuna cade nella finestra, usa solo la più recente disponibile
         → al massimo max_recs registrazioni (le più recenti)
      4. getLeadRec  → audio bytes per ognuna

    Restituisce lista di (rec_id, audio_bytes), ordine cronologico (più vecchia prima).
    """
    from datetime import timedelta

    _log_config()
    norm_phone = _normalize_phone(phone)

    now_utc = datetime.now(tz=timezone.utc)
    cutoff  = now_utc - timedelta(days=lookback_days)

    logger.info(
        "Sidial: ricerca phone=%s (norm=%s) campaign=%s ultimi %d giorni (dal %s) max=%d",
        phone, norm_phone, campaign_code, lookback_days,
        cutoff.strftime("%Y-%m-%d"), max_recs,
    )

    # 1. Trova lead(s)
    leads = await _search_leads_by_phone(norm_phone)

    # Se numero normalizzato non trova nulla, prova con originale
    if not leads and norm_phone != phone:
        logger.info("Sidial: riprovo searchLeads con phone originale=%s", phone)
        leads = await _search_leads_by_phone(phone)

    if not leads:
        logger.warning("Sidial: nessun lead per phone=%s", norm_phone)
        return []

    # 2. Raccoglie tutte le registrazioni per ogni lead trovato
    all_recs: list[dict] = []
    for lead in leads:
        lead_id = str(lead.get("id") or "")
        if not lead_id:
            continue
        recs = await _search_recs_by_lead(lead_id)
        for r in recs:
            r["_lead_id"] = lead_id
        all_recs.extend(recs)

    if not all_recs:
        logger.warning(
            "Sidial: nessuna registrazione per phone=%s (lead IDs: %s)",
            norm_phone, [str(l.get("id")) for l in leads],
        )
        return []

    # 3. Ordina cronologicamente (dalla più vecchia alla più recente)
    def _sort_key(r: dict) -> datetime:
        dt = _parse_rec_datetime(r)
        return dt if dt else datetime.min.replace(tzinfo=timezone.utc)

    all_recs_sorted = sorted(all_recs, key=_sort_key)
    logger.info(
        "Sidial: %d registrazioni totali per phone=%s",
        len(all_recs_sorted), norm_phone,
    )

    # 4. Filtra: tieni solo quelle negli ultimi lookback_days giorni
    recent = [
        r for r in all_recs_sorted
        if (_parse_rec_datetime(r) or datetime.min.replace(tzinfo=timezone.utc)) >= cutoff
    ]

    if recent:
        # Tutte le registrazioni recenti in ordine cronologico (più vecchia → più recente)
        # Cap a max_recs come valvola di sicurezza (default 20)
        recs_to_download = recent[:max_recs]
        logger.info(
            "Sidial: %d registrazioni negli ultimi %d giorni (su %d totali) — scarico tutte (%d)",
            len(recent), lookback_days, len(all_recs_sorted), len(recs_to_download),
        )
    else:
        # Fallback: nessuna recente → usa solo la più recente in assoluto
        recs_to_download = all_recs_sorted[-1:]
        logger.warning(
            "Sidial: nessuna registrazione negli ultimi %d giorni per phone=%s — uso l'ultima disponibile",
            lookback_days, norm_phone,
        )

    # 5. Scarica le registrazioni selezionate
    results: list[Tuple[str, bytes]] = []
    for rec in recs_to_download:
        rec_id = str(rec.get("id") or "")
        if not rec_id:
            logger.warning("Sidial: record senza id: %s", rec)
            continue
        audio_bytes = await _download_rec(rec_id)
        if audio_bytes:
            results.append((rec_id, audio_bytes))
        else:
            logger.warning("Sidial: nessun audio per rec_id=%s — skipping", rec_id)

    logger.info(
        "Sidial: %d/%d registrazioni scaricate per phone=%s",
        len(results), len(recs_to_download), norm_phone,
    )
    return results


async def find_and_download_recording(
    phone: str,
    appointment_datetime: str,
    campaign_code: Optional[str] = None,
) -> Tuple[Optional[str], Optional[bytes]]:
    """
    Compatibilità retroattiva: restituisce (rec_id, audio_bytes) della prima registrazione.
    """
    results = await find_and_download_all_recordings(phone, campaign_code)
    if not results:
        return None, None
    rec_id, audio_bytes = results[0]
    return rec_id, audio_bytes

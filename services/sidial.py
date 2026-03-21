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


# ── Ricerca lead per telefono / P.IVA ────────────────────────────────────────

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


async def _search_leads_by_ragione_sociale(ragione_sociale: str) -> list[dict]:
    """
    POST a=searchLeads cercando per Ragione Sociale nei campi comuni Sidial.
    Usa LIKE (contains) perché il nome potrebbe essere parziale.
    """
    rs_fields = ("companyName", "company", "ragioneSociale", "businessName", "name", "ragione_sociale")
    results: list[dict] = []
    async with httpx.AsyncClient(timeout=30.0) as client:
        for field in rs_fields:
            params_json = json.dumps([{"table": "leads", "field": field, "operator": "like", "value": ragione_sociale}])
            try:
                resp = await client.post(_BASE, data={"a": "searchLeads", "apiToken": _TOKEN, "params": params_json})
                if resp.status_code == 200:
                    data = resp.json()
                    if isinstance(data, dict) and not data.get("response", {}).get("error") and data.get("results"):
                        logger.info("searchLeads RS: trovati %d lead in campo '%s' per rs=%s",
                                    len(data["results"]), field, ragione_sociale)
                        results.extend(data["results"])
                        break
            except Exception as exc:
                logger.warning("searchLeads RS fallito campo=%s: %s", field, exc)

    seen: set = set()
    unique: list[dict] = []
    for lead in results:
        lead_id = str(lead.get("id") or "")
        if lead_id and lead_id not in seen:
            seen.add(lead_id)
            unique.append(lead)
    return unique


async def _search_leads_by_piva(piva: str) -> list[dict]:
    """
    POST a=searchLeads cercando la P.IVA in campi comuni Sidial.
    Usato come fallback quando la ricerca per telefono non trova nulla.
    """
    # Prova i campi più probabili dove Sidial potrebbe salvare la P.IVA
    piva_fields = ("vat", "piva", "fiscal_code", "codfis", "partitaiva", "taxid", "cf")
    results: list[dict] = []
    async with httpx.AsyncClient(timeout=30.0) as client:
        for field in piva_fields:
            params_json = json.dumps([{"table": "leads", "field": field, "operator": "=", "value": piva}])
            try:
                resp = await client.post(_BASE, data={"a": "searchLeads", "apiToken": _TOKEN, "params": params_json})
                if resp.status_code == 200:
                    data = resp.json()
                    if isinstance(data, dict) and not data.get("response", {}).get("error") and "results" in data:
                        if data["results"]:
                            logger.info("searchLeads P.IVA: trovati %d lead in campo '%s' per piva=%s",
                                        len(data["results"]), field, piva)
                            results.extend(data["results"])
                            break  # trovato, non serve continuare
                    logger.debug("searchLeads P.IVA: campo '%s' non ha trovato nulla per piva=%s", field, piva)
            except Exception as exc:
                logger.warning("searchLeads P.IVA fallito campo=%s: %s", field, exc)

    seen: set = set()
    unique: list[dict] = []
    for lead in results:
        lead_id = str(lead.get("id") or "")
        if lead_id and lead_id not in seen:
            seen.add(lead_id)
            unique.append(lead)
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

async def _try_fetch_audio(client: httpx.AsyncClient, url: str, label: str) -> Optional[bytes]:
    """Tenta un GET sull'URL e restituisce i bytes se sembra audio valido."""
    try:
        resp = await client.get(url)
        logger.info("%s → HTTP %s content-type=%s len=%d",
                    label, resp.status_code,
                    resp.headers.get("content-type", "?"), len(resp.content))
        if resp.status_code != 200:
            return None
        content_type = resp.headers.get("content-type", "")
        # Se JSON → controlla se c'è un URL indiretto
        if "application/json" in content_type or resp.content[:1] in (b"{", b"["):
            try:
                data = resp.json()
                for key in ("url", "recording_url", "audio_url", "file_url", "path"):
                    rec_url = data.get(key)
                    if rec_url:
                        audio_resp = await client.get(rec_url)
                        if audio_resp.status_code == 200 and len(audio_resp.content) > 1000:
                            return audio_resp.content
            except Exception:
                pass
            return None
        if len(resp.content) > 1000:
            return resp.content
        return None
    except Exception as exc:
        logger.warning("%s fallito: %s", label, exc)
        return None


async def _download_rec(rec_id: str, file_name: str = "") -> Optional[bytes]:
    """
    Tenta di scaricare l'audio di una registrazione Sidial.
    Prova più strategie in sequenza fino a trovare un file audio valido.
    """
    base_host = _BASE.split("/api.php")[0]  # es. https://effoncall.sidial.cloud

    async with httpx.AsyncClient(timeout=120.0, follow_redirects=True) as client:

        # 1. GET standard
        url_std = f"{_BASE}?a=getLeadRec&id={rec_id}&apiToken={_TOKEN}"
        audio = await _try_fetch_audio(client, url_std, f"getLeadRec GET id={rec_id}")
        if audio:
            return audio

        # 2. GET con raw=1
        url_raw = f"{_BASE}?a=getLeadRec&id={rec_id}&apiToken={_TOKEN}&raw=1"
        audio = await _try_fetch_audio(client, url_raw, f"getLeadRec GET raw=1 id={rec_id}")
        if audio:
            logger.info("Audio scaricato via raw=1 per rec_id=%s", rec_id)
            return audio

        # 3. POST a=getLeadRec
        try:
            resp = await client.post(_BASE, data={"a": "getLeadRec", "id": rec_id, "apiToken": _TOKEN})
            logger.info("getLeadRec POST id=%s → HTTP %s content-type=%s len=%d",
                        rec_id, resp.status_code,
                        resp.headers.get("content-type", "?"), len(resp.content))
            if resp.status_code == 200 and len(resp.content) > 1000:
                ct = resp.headers.get("content-type", "")
                if "application/json" not in ct and resp.content[:1] not in (b"{", b"["):
                    logger.info("Audio scaricato via POST getLeadRec per rec_id=%s", rec_id)
                    return resp.content
        except Exception as exc:
            logger.warning("getLeadRec POST fallito rec_id=%s: %s", rec_id, exc)

        # 4. URL diretti basati sul fileName (percorsi comuni Sidial)
        if file_name:
            name = file_name.strip()
            # Estrai data dal nome file (YYYYMMDD_...)
            date_prefix = name[:8] if len(name) >= 8 and name[:8].isdigit() else ""
            candidate_paths = [
                f"/recordings/{name}",
                f"/recordings/{name}.mp3",
                f"/recordings/{name}.wav",
                f"/audio/{name}",
                f"/audio/{name}.mp3",
                f"/rec/{name}",
                f"/rec/{name}.mp3",
                f"/media/{name}",
                f"/media/{name}.mp3",
            ]
            if date_prefix and len(date_prefix) == 8:
                yr, mo, dy = date_prefix[:4], date_prefix[4:6], date_prefix[6:8]
                candidate_paths += [
                    f"/recordings/{yr}/{mo}/{dy}/{name}.mp3",
                    f"/recordings/{yr}/{mo}/{dy}/{name}.wav",
                    f"/recordings/{yr}-{mo}-{dy}/{name}.mp3",
                ]
            for path in candidate_paths:
                url = f"{base_host}{path}"
                try:
                    resp = await client.get(url, headers={"Authorization": f"Bearer {_TOKEN}"})
                    if resp.status_code == 200 and len(resp.content) > 1000:
                        ct = resp.headers.get("content-type", "")
                        if "application/json" not in ct and resp.content[:1] not in (b"{", b"["):
                            logger.info("Audio scaricato via percorso diretto %s per rec_id=%s", path, rec_id)
                            return resp.content
                except Exception:
                    pass

        logger.warning("Sidial: nessun audio scaricabile per rec_id=%s fileName=%s", rec_id, file_name)
        return None


# ── Facciata pubblica ─────────────────────────────────────────────────────────

async def _collect_all_leads(
    phone: str,
    piva: str = "",
    ragione_sociale: str = "",
    last_name: str = "",
) -> list[dict]:
    """
    Cerca lead su Sidial con tutti i parametri disponibili nell'ordine:
    1. phone (normalizzato)
    2. phone (originale se diverso)
    3. P.IVA
    4. Ragione Sociale
    5. lastName (fallback se form assente)
    Restituisce lista di lead unici (deduplicati per ID).
    """
    seen_ids: set = set()
    all_leads: list[dict] = []

    def _add_leads(new_leads: list[dict], source: str) -> int:
        added = 0
        for lead in new_leads:
            lid = str(lead.get("id") or "")
            if lid and lid not in seen_ids:
                seen_ids.add(lid)
                all_leads.append(lead)
                added += 1
        if added:
            logger.info("Sidial: +%d lead nuovi da ricerca per %s", added, source)
        return added

    norm_phone = _normalize_phone(phone)

    # 1. Telefono normalizzato
    leads = await _search_leads_by_phone(norm_phone)
    _add_leads(leads, f"phone={norm_phone}")

    # 2. Telefono originale (se diverso)
    if norm_phone != phone and not leads:
        leads2 = await _search_leads_by_phone(phone)
        _add_leads(leads2, f"phone_orig={phone}")

    # Form presente: cerca per P.IVA e Ragione Sociale
    form_present = bool(piva or ragione_sociale)

    if piva:
        leads_piva = await _search_leads_by_piva(piva)
        _add_leads(leads_piva, f"piva={piva}")

    if ragione_sociale:
        leads_rs = await _search_leads_by_ragione_sociale(ragione_sociale)
        _add_leads(leads_rs, f"ragione_sociale={ragione_sociale}")

    # Form assente: cerca per lastName come ricerca indipendente (non fallback)
    if not form_present and last_name:
        leads_ln = await _search_leads_by_ragione_sociale(last_name)
        _add_leads(leads_ln, f"lastName={last_name}")

    logger.info("Sidial: totale %d lead unici trovati", len(all_leads))
    return all_leads


async def find_and_download_all_recordings(
    phone: str,
    campaign_code: Optional[str] = None,
    appointment_dt: Optional[datetime] = None,
    lookback_days: int = 90,
    max_recs: int = 20,
    piva: str = "",
    ragione_sociale: str = "",
    last_name: str = "",
    min_call_seconds: int = 20,
    return_stats: bool = False,
) -> "list[Tuple[str, bytes]] | tuple[list[Tuple[str, bytes]], dict]":
    """
    Trova e scarica le registrazioni degli ultimi lookback_days giorni.

    Ordine di ricerca lead (come da istruzione):
      1. phone_top_level (normalizzato + originale)
      2. P.IVA (dal form field "PARTITA IVA*")
      3. Ragione Sociale (dal form field "RAGIONE SOCIALE*")
      4. lastName (fallback se form assente)

    Per ogni parametro raccoglie i lead, deduplica per ID, poi raccoglie
    tutte le registrazioni, deduplica per rec_id, conta e scarica.
    """
    from datetime import timedelta

    _log_config()
    norm_phone = _normalize_phone(phone)
    now_utc = datetime.now(tz=timezone.utc)
    cutoff = now_utc - timedelta(days=lookback_days)

    logger.info(
        "Sidial: ricerca multi-parametro — phone=%s piva=%s rs=%s ultimi %d giorni",
        norm_phone, piva or "—", ragione_sociale or "—", lookback_days,
    )

    # ── FASE A: raccogli tutti i lead unici da tutti i parametri ─────────────
    all_leads = await _collect_all_leads(
        phone=phone, piva=piva,
        ragione_sociale=ragione_sociale, last_name=last_name,
    )

    if not all_leads:
        logger.warning("Sidial: nessun lead trovato con nessun parametro")
        return []

    # ── FASE B: raccogli tutte le registrazioni, deduplica per rec_id ────────
    seen_rec_ids: set = set()
    all_recs: list[dict] = []
    for lead in all_leads:
        lead_id = str(lead.get("id") or "")
        if not lead_id:
            continue
        recs = await _search_recs_by_lead(lead_id)
        for r in recs:
            rid = str(r.get("id") or "")
            if rid and rid not in seen_rec_ids:
                seen_rec_ids.add(rid)
                r["_lead_id"] = lead_id
                all_recs.append(r)

    if not all_recs:
        logger.warning("Sidial: nessuna registrazione per %d lead trovati", len(all_leads))
        return []

    # ── FASE C: filtra per lookback_days, ordina cronologicamente ────────────
    def _sort_key(r: dict) -> datetime:
        dt = _parse_rec_datetime(r)
        return dt if dt else datetime.min.replace(tzinfo=timezone.utc)

    all_recs_sorted = sorted(all_recs, key=_sort_key)
    logger.info("Sidial: %d registrazioni uniche totali", len(all_recs_sorted))

    recent = [
        r for r in all_recs_sorted
        if (_parse_rec_datetime(r) or datetime.min.replace(tzinfo=timezone.utc)) >= cutoff
    ]

    if recent:
        recs_to_download = recent[:max_recs]
        logger.info(
            "Sidial: %d registrazioni negli ultimi %d giorni → scarico %d",
            len(recent), lookback_days, len(recs_to_download),
        )
    else:
        recs_to_download = all_recs_sorted[-1:]
        logger.warning("Sidial: nessuna registrazione recente — uso l'ultima disponibile")

    # ── FASE D: filtra ringback puri, verifica converted ─────────────────────
    _MIN_SEC = max(10, min_call_seconds)  # never filter below 10s absolute floor
    useful = [r for r in recs_to_download if int(r.get("callLength") or 0) >= _MIN_SEC]
    if not useful:
        logger.warning("Sidial: filtro %ds ha eliminato tutto — uso tutte", _MIN_SEC)
        useful = recs_to_download

    # Track recordings still being converted (converted != 'y') with length >= threshold
    pending_long = [
        r for r in useful
        if (r.get("converted") or "n").lower() != "y"
        and int(r.get("callLength") or 0) >= _MIN_SEC
    ]
    if pending_long:
        logger.warning(
            "Sidial: %d registrazioni utili ancora in conversione: %s",
            len(pending_long), [r.get("id") for r in pending_long],
        )

    logger.info(
        "Sidial: RIEPILOGO — %d lead | %d rec totali | %d recenti | %d da scaricare | %d in conversione",
        len(all_leads), len(all_recs_sorted), len(recent), len(useful), len(pending_long),
    )

    # Count how many search params found leads
    _phone_leads = len(await _search_leads_by_phone(_normalize_phone(phone))) if phone else 0
    _piva_leads = len(await _search_leads_by_piva(piva)) if piva else 0
    _rs_leads = len(await _search_leads_by_ragione_sociale(ragione_sociale)) if ragione_sociale else 0
    search_params_used = sum([bool(_phone_leads), bool(_piva_leads), bool(_rs_leads)])

    # Build stats dict
    stats: dict = {
        "leads_found": len(all_leads),
        "total_recs": len(all_recs_sorted),
        "recent_recs": len(recent),
        "converting_recs": len(pending_long),
        "total_seconds": 0,
        "search_params_used": search_params_used,
    }

    # ── FASE E: scarica ───────────────────────────────────────────────────────
    results: list[Tuple[str, bytes]] = []
    total_secs = 0
    for rec in useful:
        rec_id = str(rec.get("id") or "")
        if not rec_id:
            continue
        call_len = int(rec.get("callLength") or 0)
        if (rec.get("converted") or "n").lower() != "y":
            logger.info("Sidial: rec_id=%s callLength=%ss — in conversione, skip", rec_id, rec.get("callLength"))
            continue
        audio_bytes = await _download_rec(rec_id, file_name=rec.get("fileName") or "")
        if audio_bytes:
            # Coherence check: bytes should be >= callLength_seconds * 500 (rough floor)
            coherence_ok = len(audio_bytes) >= call_len * 500 if call_len > 0 else True
            if not coherence_ok:
                logger.warning(
                    "Sidial: rec_id=%s INCOERENTE — bytes=%d callLength=%ds (atteso>=%d)",
                    rec_id, len(audio_bytes), call_len, call_len * 500,
                )
            results.append((rec_id, audio_bytes))
            total_secs += call_len
        else:
            logger.warning("Sidial: nessun audio per rec_id=%s — skipping", rec_id)

    stats["total_seconds"] = total_secs
    logger.info("Sidial: %d/%d registrazioni scaricate · %ds totale", len(results), len(useful), total_secs)

    if return_stats:
        return results, stats
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

"""
Sidial CRM client — implementazione corretta basata su DOC-010.

Flusso:
  1. POST a=searchLeads  — trova leadId dal numero di telefono (filtro JSON params)
  2. POST a=searchRecs   — lista registrazioni del lead (tabella leadsRecs)
  3. GET  a=getLeadRec   — scarica singola registrazione per id numerico

Parametri sempre in GET salvo azioni che richiedono POST (searchLeads, searchRecs).
Auth: apiToken come query param in GET, oppure nel body in POST.

STRATEGIA RICERCA (fail-fast):
  1. Telefono esatto (4 campi in parallelo) — 3 varianti formato
  2. Se 0 lead → LIKE con ultimi 9 digit (4 campi in parallelo)
  3. Solo se 0 lead → P.IVA / Ragione Sociale (in parallelo tra loro)
  SHORT-CIRCUIT: appena troviamo lead, ci fermiamo.
"""

import asyncio
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

# Timeout aggressivi: connect 5s, read 10s — meglio fallire veloce che bloccare
_SEARCH_TIMEOUT = httpx.Timeout(connect=5.0, read=10.0, write=5.0, pool=5.0)
_DOWNLOAD_TIMEOUT = httpx.Timeout(connect=10.0, read=60.0, write=5.0, pool=10.0)

_PHONE_FIELDS = ("phone1", "phone2", "phone3", "phone4")


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


def _phone_variants(raw: str) -> list[str]:
    """
    Genera varianti del numero di telefono da cercare su Sidial.
    Sidial potrebbe aver salvato il numero in qualsiasi formato.
    Es. input "+39 333 1234567" → ["3331234567", "393331234567", "00393331234567"]
    """
    norm = _normalize_phone(raw)
    if not norm:
        return []
    variants = [norm]
    # Con prefisso +39
    with_39 = f"39{norm}"
    if with_39 not in variants:
        variants.append(with_39)
    # Con prefisso 0039
    with_0039 = f"0039{norm}"
    if with_0039 not in variants:
        variants.append(with_0039)
    # Originale solo-digit (potrebbe essere diverso dal normalizzato)
    raw_digits = re.sub(r"\D", "", raw)
    if raw_digits and raw_digits not in variants:
        variants.append(raw_digits)
    return variants


# ── Singola ricerca API ──────────────────────────────────────────────────────

async def _search_leads_single(
    client: httpx.AsyncClient,
    field: str,
    value: str,
    operator: str = "=",
) -> list[dict]:
    """Una singola chiamata searchLeads. Restituisce lista lead o []."""
    params_json = json.dumps([{
        "table": "leads", "field": field, "operator": operator, "value": value,
    }])
    try:
        resp = await client.post(
            _BASE, data={"a": "searchLeads", "apiToken": _TOKEN, "params": params_json},
        )
        if resp.status_code != 200:
            logger.warning("searchLeads HTTP %s field=%s op=%s val=%s",
                           resp.status_code, field, operator, value[:30])
            return []
        data = resp.json()
        if isinstance(data, dict):
            if data.get("response", {}).get("error"):
                return []
            if "results" in data:
                return data["results"]
        if isinstance(data, list):
            return data
        return []
    except httpx.TimeoutException:
        logger.warning("searchLeads TIMEOUT field=%s op=%s val=%s", field, operator, value[:30])
        return []
    except Exception as exc:
        logger.warning("searchLeads ERRORE field=%s val=%s: %s", field, value[:30], exc)
        return []


def _dedup_leads(leads: list[dict]) -> list[dict]:
    """Deduplica lead per ID."""
    seen: set = set()
    unique: list[dict] = []
    for lead in leads:
        lid = str(lead.get("id") or "")
        if lid and lid not in seen:
            seen.add(lid)
            unique.append(lead)
    return unique


# ── Ricerche composite ───────────────────────────────────────────────────────

async def _search_phone_exact(client: httpx.AsyncClient, phone_variant: str) -> list[dict]:
    """Cerca UN formato telefono su tutti i phone1-4 in parallelo."""
    tasks = [_search_leads_single(client, f, phone_variant) for f in _PHONE_FIELDS]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    all_leads: list[dict] = []
    for r in results:
        if isinstance(r, list):
            all_leads.extend(r)
    return _dedup_leads(all_leads)


async def _search_phone_like(client: httpx.AsyncClient, last_digits: str) -> list[dict]:
    """Cerca con LIKE gli ultimi digit su phone1-4 in parallelo."""
    tasks = [_search_leads_single(client, f, last_digits, operator="like") for f in _PHONE_FIELDS]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    all_leads: list[dict] = []
    for r in results:
        if isinstance(r, list):
            all_leads.extend(r)
    return _dedup_leads(all_leads)


async def _search_piva(client: httpx.AsyncClient, piva: str) -> list[dict]:
    """Cerca P.IVA — prova i 3 campi più probabili IN PARALLELO."""
    piva_fields = ("vat", "piva", "partitaiva")
    tasks = [_search_leads_single(client, f, piva) for f in piva_fields]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    all_leads: list[dict] = []
    for r in results:
        if isinstance(r, list):
            all_leads.extend(r)
    return _dedup_leads(all_leads)


async def _search_ragione_sociale(client: httpx.AsyncClient, rs: str) -> list[dict]:
    """Cerca Ragione Sociale — prova i 3 campi più probabili IN PARALLELO con LIKE."""
    rs_fields = ("companyName", "company", "ragioneSociale")
    tasks = [_search_leads_single(client, f, rs, operator="like") for f in rs_fields]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    all_leads: list[dict] = []
    for r in results:
        if isinstance(r, list):
            all_leads.extend(r)
    return _dedup_leads(all_leads)


# ── Raccolta lead con SHORT-CIRCUIT ──────────────────────────────────────────

async def _collect_all_leads(
    phone: str,
    piva: str = "",
    ragione_sociale: str = "",
    last_name: str = "",
    progress_cb=None,
) -> tuple[list[dict], int, str]:
    """
    Cerca lead su Sidial con strategia fail-fast:
      1. Telefono esatto (3+ varianti formato, tutte in parallelo)
      2. Telefono LIKE (ultimi 9 digit)
      3. P.IVA + Ragione Sociale (solo se telefono non ha trovato nulla)

    SHORT-CIRCUIT: appena troviamo lead, ci fermiamo.
    progress_cb: async callback(msg) per aggiornare lo step nella UI.

    Returns: (leads, search_params_used, search_method)
    """
    variants = _phone_variants(phone)
    logger.info("Sidial: varianti telefono da cercare: %s", variants)

    async def _progress(msg):
        if progress_cb:
            try:
                await progress_cb(msg)
            except Exception:
                pass

    async with httpx.AsyncClient(timeout=_SEARCH_TIMEOUT) as client:

        # ── FASE 1: match esatto su tutte le varianti in parallelo ────────
        if variants:
            await _progress(f"FASE 1: ricerca telefono esatto ({len(variants)} varianti × 4 campi)...")
            all_phone_tasks = []
            for v in variants:
                for f in _PHONE_FIELDS:
                    all_phone_tasks.append(_search_leads_single(client, f, v))

            logger.info("Sidial FASE 1: %d ricerche telefono esatte in parallelo", len(all_phone_tasks))
            all_results = await asyncio.gather(*all_phone_tasks, return_exceptions=True)

            all_leads: list[dict] = []
            for r in all_results:
                if isinstance(r, list):
                    all_leads.extend(r)
            leads = _dedup_leads(all_leads)

            if leads:
                logger.info("Sidial FASE 1 OK: %d lead trovati con telefono esatto", len(leads))
                await _progress(f"FASE 1 OK: {len(leads)} lead trovati!")
                return leads, 1, "telefono_esatto"

            await _progress("FASE 1: nessun lead con match esatto")

        # ── FASE 2: LIKE con ultimi 9 digit ──────────────────────────────
        norm = _normalize_phone(phone)
        if len(norm) >= 9:
            last9 = norm[-9:]
            await _progress(f"FASE 2: ricerca LIKE con ultimi 9 cifre ({last9})...")
            logger.info("Sidial FASE 2: ricerca LIKE con ultimi 9 digit=%s", last9)
            leads = await _search_phone_like(client, last9)
            if leads:
                logger.info("Sidial FASE 2 OK: %d lead trovati con LIKE %s", len(leads), last9)
                await _progress(f"FASE 2 OK: {len(leads)} lead trovati con LIKE!")
                return leads, 1, f"telefono_like_{last9}"

            await _progress("FASE 2: nessun lead con LIKE")

        # ── FASE 3: P.IVA e Ragione Sociale in parallelo (fallback) ──────
        fallback_tasks = []
        fallback_labels = []

        if piva:
            fallback_tasks.append(_search_piva(client, piva))
            fallback_labels.append(f"piva={piva}")
        if ragione_sociale:
            fallback_tasks.append(_search_ragione_sociale(client, ragione_sociale))
            fallback_labels.append(f"rs={ragione_sociale}")
        if not fallback_tasks and last_name:
            fallback_tasks.append(_search_ragione_sociale(client, last_name))
            fallback_labels.append(f"lastName={last_name}")

        if fallback_tasks:
            await _progress(f"FASE 3: fallback con {', '.join(fallback_labels)}...")
            logger.info("Sidial FASE 3: %d ricerche fallback in parallelo (%s)",
                        len(fallback_tasks), ", ".join(fallback_labels))
            fallback_results = await asyncio.gather(*fallback_tasks, return_exceptions=True)
            all_leads = []
            for r in fallback_results:
                if isinstance(r, list):
                    all_leads.extend(r)
            leads = _dedup_leads(all_leads)
            if leads:
                logger.info("Sidial FASE 3 OK: %d lead trovati con fallback", len(leads))
                await _progress(f"FASE 3 OK: {len(leads)} lead trovati!")
                return leads, len([r for r in fallback_results if isinstance(r, list) and r]), "fallback_piva_rs"

    logger.warning("Sidial: NESSUN lead trovato con nessuna strategia (phone=%s piva=%s rs=%s)",
                   phone, piva or "—", ragione_sociale or "—")
    await _progress("Nessun lead trovato con nessuna strategia")
    return [], 0, "nessuno"


# ── Ricerca registrazioni per leadId ─────────────────────────────────────────

async def _search_recs_by_lead(client: httpx.AsyncClient, lead_id: str) -> list[dict]:
    """
    POST a=searchRecs con filtro JSON su leadsRecs.lead = {leadId}.
    Restituisce lista di record con campi: id, createdWhen, callLength, fileName, ecc.
    """
    filters = [{"table": "leadsRecs", "field": "lead", "operator": "=", "value": int(lead_id)}]
    params_json = json.dumps(filters)

    try:
        resp = await client.post(
            _BASE,
            data={"a": "searchRecs", "apiToken": _TOKEN, "params": params_json},
        )
        if resp.status_code == 404:
            logger.info("searchRecs: nessuna registrazione per leadId=%s", lead_id)
            return []
        if resp.status_code != 200:
            logger.error("searchRecs HTTP %s per leadId=%s", resp.status_code, lead_id)
            return []
        data = resp.json()
        if isinstance(data, list):
            logger.info("searchRecs: %d registrazioni per leadId=%s", len(data), lead_id)
            return data
        if isinstance(data, dict):
            if data.get("response", {}).get("error"):
                return []
            if "results" in data:
                recs = data["results"]
                logger.info("searchRecs: %d registrazioni per leadId=%s", len(recs), lead_id)
                return recs
        logger.warning("searchRecs risposta inattesa per leadId=%s: %s", lead_id, str(data)[:300])
        return []
    except httpx.TimeoutException:
        logger.warning("searchRecs TIMEOUT per leadId=%s", lead_id)
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
        # Rifiuta risposte HTML/XML (pagine di errore Sidial)
        if any(t in content_type for t in ("text/html", "text/xml", "text/plain")):
            logger.warning("%s: risposta non-audio content-type=%s — scartata", label, content_type)
            return None
        # Verifica che inizi con header audio valido (non testo/HTML)
        if resp.content[:1] in (b"<", b"\n", b"\r"):
            logger.warning("%s: contenuto sembra HTML/testo (inizia con %r) — scartato", label, resp.content[:20])
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
    base_host = _BASE.split("/api.php")[0]

    async with httpx.AsyncClient(timeout=_DOWNLOAD_TIMEOUT, follow_redirects=True) as client:

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
            if resp.status_code == 200 and len(resp.content) > 1000:
                ct = resp.headers.get("content-type", "")
                if "application/json" not in ct and resp.content[:1] not in (b"{", b"["):
                    logger.info("Audio scaricato via POST getLeadRec per rec_id=%s", rec_id)
                    return resp.content
        except Exception as exc:
            logger.warning("getLeadRec POST fallito rec_id=%s: %s", rec_id, exc)

        # 4. URL diretti basati sul fileName
        if file_name:
            name = file_name.strip()
            date_prefix = name[:8] if len(name) >= 8 and name[:8].isdigit() else ""
            candidate_paths = [
                f"/recordings/{name}",
                f"/recordings/{name}.mp3",
                f"/recordings/{name}.wav",
            ]
            if date_prefix and len(date_prefix) == 8:
                yr, mo, dy = date_prefix[:4], date_prefix[4:6], date_prefix[6:8]
                candidate_paths += [
                    f"/recordings/{yr}/{mo}/{dy}/{name}.mp3",
                    f"/recordings/{yr}/{mo}/{dy}/{name}.wav",
                ]
            for path in candidate_paths:
                url = f"{base_host}{path}"
                try:
                    resp = await client.get(url, headers={"Authorization": f"Bearer {_TOKEN}"})
                    if resp.status_code == 200 and len(resp.content) > 1000:
                        ct = resp.headers.get("content-type", "")
                        if "application/json" not in ct and resp.content[:1] not in (b"{", b"["):
                            logger.info("Audio scaricato via percorso %s per rec_id=%s", path, rec_id)
                            return resp.content
                except Exception:
                    pass

        logger.warning("Sidial: nessun audio scaricabile per rec_id=%s fileName=%s", rec_id, file_name)
        return None


# ── Facciata pubblica ─────────────────────────────────────────────────────────

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
    progress_cb=None,
) -> "list[Tuple[str, bytes]] | tuple[list[Tuple[str, bytes]], dict]":
    """
    Trova e scarica le registrazioni degli ultimi lookback_days giorni.
    Usa strategia fail-fast con short-circuit.
    """
    from datetime import timedelta

    _log_config()
    norm_phone = _normalize_phone(phone)
    now_utc = datetime.now(tz=timezone.utc)
    cutoff = now_utc - timedelta(days=lookback_days)

    logger.info(
        "Sidial: ricerca — phone=%s (norm=%s) piva=%s rs=%s ultimi %d giorni",
        phone, norm_phone, piva or "—", ragione_sociale or "—", lookback_days,
    )

    # ── FASE A: raccogli lead con short-circuit ───────────────────────────
    all_leads, search_params_used, search_method = await _collect_all_leads(
        phone=phone, piva=piva,
        ragione_sociale=ragione_sociale, last_name=last_name,
        progress_cb=progress_cb,
    )

    _empty_stats = {
        "leads_found": 0, "total_recs": 0, "recent_recs": 0,
        "converting_recs": 0, "total_seconds": 0, "search_params_used": 0,
        "search_method": "nessuno",
        "phone_variants": _phone_variants(phone),
    }

    if not all_leads:
        logger.warning("Sidial: nessun lead trovato con nessun parametro")
        return ([], _empty_stats) if return_stats else []

    # ── FASE B: raccogli registrazioni (riusa un singolo client) ──────────
    async def _progress(msg):
        if progress_cb:
            try:
                await progress_cb(msg)
            except Exception:
                pass

    await _progress(f"{len(all_leads)} lead trovati — cerco registrazioni...")
    seen_rec_ids: set = set()
    all_recs: list[dict] = []

    async with httpx.AsyncClient(timeout=_SEARCH_TIMEOUT) as client:
        for lead in all_leads:
            lead_id = str(lead.get("id") or "")
            if not lead_id:
                continue
            recs = await _search_recs_by_lead(client, lead_id)
            for r in recs:
                rid = str(r.get("id") or "")
                if rid and rid not in seen_rec_ids:
                    seen_rec_ids.add(rid)
                    r["_lead_id"] = lead_id
                    all_recs.append(r)

    if not all_recs:
        logger.warning("Sidial: nessuna registrazione per %d lead trovati", len(all_leads))
        _no_recs_stats = {**_empty_stats, "leads_found": len(all_leads), "search_method": search_method}
        return ([], _no_recs_stats) if return_stats else []

    # ── FASE C: filtra per lookback_days, ordina cronologicamente ─────────
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

    # ── FASE D: filtra ringback puri, verifica converted ──────────────────
    _MIN_SEC = max(10, min_call_seconds)
    useful = [r for r in recs_to_download if int(r.get("callLength") or 0) >= _MIN_SEC]
    if not useful:
        logger.warning("Sidial: filtro %ds ha eliminato tutto — uso tutte", _MIN_SEC)
        useful = recs_to_download

    pending_long = [
        r for r in useful
        if (r.get("converted") or "n").lower() != "y"
        and int(r.get("callLength") or 0) >= _MIN_SEC
    ]

    logger.info(
        "Sidial: RIEPILOGO — %d lead | %d rec totali | %d recenti | %d da scaricare | %d in conversione | metodo=%s",
        len(all_leads), len(all_recs_sorted), len(recent), len(useful), len(pending_long), search_method,
    )

    # Build stats dict
    stats: dict = {
        "leads_found": len(all_leads),
        "total_recs": len(all_recs_sorted),
        "recent_recs": len(recent),
        "converting_recs": len(pending_long),
        "total_seconds": 0,
        "search_params_used": search_params_used,
        "search_method": search_method,
    }

    # ── FASE E: scarica ──────────────────────────────────────────────────
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
            coherence_ok = len(audio_bytes) >= call_len * 500 if call_len > 0 else True
            if not coherence_ok:
                logger.warning(
                    "Sidial: rec_id=%s INCOERENTE — bytes=%d callLength=%ds",
                    rec_id, len(audio_bytes), call_len,
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

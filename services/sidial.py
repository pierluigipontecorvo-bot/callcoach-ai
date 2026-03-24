"""
Sidial CRM client — implementazione corretta basata su DOC-010.

Flusso:
  1. POST a=searchLeads  — trova leadId dal numero di telefono (filtro JSON params)
  2. POST a=searchRecs   — lista registrazioni del lead (tabella leadsRecs)
  3. GET  a=getLeadRec   — scarica singola registrazione per id numerico

STRATEGIA RICERCA (fail-fast + deadline):
  - Ogni singola HTTP call wrappata in asyncio.wait_for(timeout=12)
  - Deadline globale di 90s: se superata, STOP immediato
  - SHORT-CIRCUIT: appena troviamo lead, ci fermiamo.
"""

import asyncio
import json
import logging
import re
import time
from datetime import datetime, timezone
from typing import Optional, Tuple

import httpx

from config import settings

logger = logging.getLogger(__name__)

_BASE  = settings.sidial_api_url.rstrip("/")
_TOKEN = settings.sidial_api_token

# Timeout per singola HTTP call — httpx-level (backup)
_HTTP_TIMEOUT = httpx.Timeout(connect=5.0, read=8.0, write=5.0, pool=5.0)
_DOWNLOAD_TIMEOUT = httpx.Timeout(connect=5.0, read=15.0, write=5.0, pool=5.0)

# Timeout DURO per singola HTTP call — asyncio.wait_for (primario, 100% affidabile)
_CALL_TIMEOUT = 12  # secondi

# Deadline globale per tutta la ricerca Sidial
_GLOBAL_DEADLINE = 120  # secondi

_PHONE_FIELDS = ("phone1", "phone2", "phone3", "phone4")


class SidialDeadlineError(TimeoutError):
    """Deadline globale Sidial superata."""
    pass


def _log_config() -> None:
    logger.info("Sidial config: BASE=%s token_len=%d", _BASE, len(_TOKEN))


# ── Normalizzazione telefono ──────────────────────────────────────────────────

def _normalize_phone(raw: str) -> str:
    digits = re.sub(r"\D", "", raw)
    for prefix in ("0039", "39"):
        if digits.startswith(prefix) and len(digits) > len(prefix) + 6:
            digits = digits[len(prefix):]
            break
    return digits


def _phone_variants(raw: str) -> list[str]:
    norm = _normalize_phone(raw)
    if not norm:
        return []
    variants = [norm]
    with_39 = f"39{norm}"
    if with_39 not in variants:
        variants.append(with_39)
    with_0039 = f"0039{norm}"
    if with_0039 not in variants:
        variants.append(with_0039)
    raw_digits = re.sub(r"\D", "", raw)
    if raw_digits and raw_digits not in variants:
        variants.append(raw_digits)
    return variants


# ── Singola ricerca API con timeout DURO ─────────────────────────────────────

async def _safe_post(client: httpx.AsyncClient, **kwargs) -> Optional[httpx.Response]:
    """POST con asyncio.wait_for per garantire timeout anche se httpx non risponde."""
    try:
        return await asyncio.wait_for(
            client.post(_BASE, **kwargs),
            timeout=_CALL_TIMEOUT,
        )
    except asyncio.TimeoutError:
        logger.warning("Sidial: POST hard-timeout dopo %ds", _CALL_TIMEOUT)
        return None
    except httpx.TimeoutException as exc:
        logger.warning("Sidial: httpx timeout: %s", exc)
        return None
    except Exception as exc:
        logger.warning("Sidial: POST errore: %s", exc)
        return None


async def _search_leads_single(
    client: httpx.AsyncClient,
    field: str,
    value: str,
    operator: str = "=",
    deadline: float = 0,
) -> list[dict]:
    """Una singola chiamata searchLeads con timeout duro."""
    if deadline and time.monotonic() > deadline:
        return []

    params_json = json.dumps([{
        "table": "leads", "field": field, "operator": operator, "value": value,
    }])
    resp = await _safe_post(client, data={
        "a": "searchLeads", "apiToken": _TOKEN, "params": params_json,
    })
    if resp is None:
        return []
    if resp.status_code != 200:
        logger.warning("searchLeads HTTP %s field=%s val=%s", resp.status_code, field, value[:30])
        return []
    try:
        data = resp.json()
    except Exception:
        return []
    if isinstance(data, dict):
        if data.get("response", {}).get("error"):
            return []
        if "results" in data:
            return data["results"]
    if isinstance(data, list):
        return data
    return []


def _dedup_leads(leads: list[dict]) -> list[dict]:
    seen: set = set()
    unique: list[dict] = []
    for lead in leads:
        lid = str(lead.get("id") or "")
        if lid and lid not in seen:
            seen.add(lid)
            unique.append(lead)
    return unique


# ── Ricerche composite ───────────────────────────────────────────────────────

async def _search_phone_exact(client: httpx.AsyncClient, variants: list[str], deadline: float) -> list[dict]:
    """Cerca TUTTE le varianti telefono su tutti i phone1-4 in parallelo."""
    tasks = []
    for v in variants:
        for f in _PHONE_FIELDS:
            tasks.append(_search_leads_single(client, f, v, deadline=deadline))

    results = await asyncio.gather(*tasks, return_exceptions=True)
    all_leads: list[dict] = []
    for r in results:
        if isinstance(r, list):
            all_leads.extend(r)
    return _dedup_leads(all_leads)


async def _search_phone_like(client: httpx.AsyncClient, last_digits: str, deadline: float) -> list[dict]:
    """Cerca con LIKE gli ultimi digit su phone1-4 in parallelo."""
    tasks = [
        _search_leads_single(client, f, last_digits, operator="like", deadline=deadline)
        for f in _PHONE_FIELDS
    ]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    all_leads: list[dict] = []
    for r in results:
        if isinstance(r, list):
            all_leads.extend(r)
    return _dedup_leads(all_leads)


async def _search_fallback(client: httpx.AsyncClient, piva: str, ragione_sociale: str, last_name: str, deadline: float) -> list[dict]:
    """Cerca P.IVA + Ragione Sociale in parallelo."""
    tasks = []
    if piva:
        for f in ("vat", "piva", "partitaiva"):
            tasks.append(_search_leads_single(client, f, piva, deadline=deadline))
    rs = ragione_sociale or last_name
    if rs:
        for f in ("companyName", "company", "ragioneSociale"):
            tasks.append(_search_leads_single(client, f, rs, operator="like", deadline=deadline))
    if not tasks:
        return []
    results = await asyncio.gather(*tasks, return_exceptions=True)
    all_leads: list[dict] = []
    for r in results:
        if isinstance(r, list):
            all_leads.extend(r)
    return _dedup_leads(all_leads)


# ── Raccolta lead con SHORT-CIRCUIT + DEADLINE ───────────────────────────────

async def _collect_all_leads(
    phone: str,
    piva: str = "",
    ragione_sociale: str = "",
    last_name: str = "",
    progress_cb=None,
    deadline: float = 0,
) -> tuple[list[dict], int, str]:
    """
    SHORT-CIRCUIT + DEADLINE: veloce e sicuro.
    """
    variants = _phone_variants(phone)
    logger.info("Sidial: varianti telefono: %s", variants)

    async def _progress(msg):
        if progress_cb:
            try:
                await progress_cb(msg)
            except Exception:
                pass

    def _check_deadline(phase):
        if deadline and time.monotonic() > deadline:
            elapsed = int(time.monotonic() - (deadline - _GLOBAL_DEADLINE))
            raise SidialDeadlineError(f"Deadline {_GLOBAL_DEADLINE}s superata alla {phase} (elapsed: {elapsed}s)")

    async with httpx.AsyncClient(timeout=_HTTP_TIMEOUT) as client:

        # ── FASE 1: match esatto tutte le varianti in parallelo ───────
        if variants:
            _check_deadline("FASE 1")
            await _progress(f"FASE 1: ricerca telefono esatto ({len(variants)} varianti × 4 campi)...")
            logger.info("Sidial FASE 1: %d varianti × 4 campi = %d ricerche parallele",
                        len(variants), len(variants) * 4)

            leads = await _search_phone_exact(client, variants, deadline)

            if leads:
                logger.info("Sidial FASE 1 OK: %d lead", len(leads))
                await _progress(f"FASE 1 OK: {len(leads)} lead trovati!")
                return leads, 1, "telefono_esatto"

            elapsed = int(time.monotonic() - (deadline - _GLOBAL_DEADLINE))
            await _progress(f"FASE 1: 0 lead in {elapsed}s")
            logger.info("Sidial FASE 1: 0 lead (elapsed: %ds)", elapsed)

        # ── FASE 2: LIKE con ultimi 9 digit ──────────────────────────
        norm = _normalize_phone(phone)
        if len(norm) >= 9:
            _check_deadline("FASE 2")
            last9 = norm[-9:]
            await _progress(f"FASE 2: ricerca LIKE ultimi 9 cifre ({last9})...")
            leads = await _search_phone_like(client, last9, deadline)
            if leads:
                logger.info("Sidial FASE 2 OK: %d lead con LIKE %s", len(leads), last9)
                await _progress(f"FASE 2 OK: {len(leads)} lead con LIKE!")
                return leads, 1, f"telefono_like_{last9}"

            elapsed = int(time.monotonic() - (deadline - _GLOBAL_DEADLINE))
            await _progress(f"FASE 2: 0 lead in {elapsed}s")

        # ── FASE 3: P.IVA + Ragione Sociale (parallelo) ──────────────
        if piva or ragione_sociale or last_name:
            _check_deadline("FASE 3")
            await _progress("FASE 3: fallback P.IVA / Ragione Sociale...")
            leads = await _search_fallback(client, piva, ragione_sociale, last_name, deadline)
            if leads:
                logger.info("Sidial FASE 3 OK: %d lead", len(leads))
                await _progress(f"FASE 3 OK: {len(leads)} lead!")
                return leads, 1, "fallback_piva_rs"

    logger.warning("Sidial: 0 lead con qualsiasi strategia (phone=%s piva=%s rs=%s)",
                   phone, piva or "—", ragione_sociale or "—")
    await _progress("Nessun lead trovato con nessuna strategia")
    return [], 0, "nessuno"


# ── Ricerca registrazioni per leadId ─────────────────────────────────────────

async def _search_recs_by_lead(client: httpx.AsyncClient, lead_id: str, deadline: float = 0) -> list[dict]:
    """POST a=searchRecs con timeout duro."""
    if deadline and time.monotonic() > deadline:
        logger.warning("searchRecs: deadline superata, skip leadId=%s", lead_id)
        return []

    filters = [{"table": "leadsRecs", "field": "lead", "operator": "=", "value": int(lead_id)}]
    resp = await _safe_post(client, data={
        "a": "searchRecs", "apiToken": _TOKEN, "params": json.dumps(filters),
    })
    if resp is None:
        return []
    if resp.status_code == 404:
        return []
    if resp.status_code != 200:
        logger.error("searchRecs HTTP %s per leadId=%s", resp.status_code, lead_id)
        return []
    try:
        data = resp.json()
    except Exception:
        return []
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
    return []


def _parse_rec_datetime(rec: dict) -> Optional[datetime]:
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

def _time_left(deadline: float, max_sec: float = 15.0) -> float:
    """Calcola secondi rimasti fino alla deadline, capped a max_sec. 0 se scaduta."""
    if not deadline:
        return max_sec
    left = deadline - time.monotonic()
    return max(0, min(left, max_sec))


async def _try_fetch_audio(client: httpx.AsyncClient, url: str, label: str, deadline: float = 0) -> Optional[bytes]:
    t = _time_left(deadline, 15.0)
    if t <= 0:
        return None
    try:
        resp = await asyncio.wait_for(client.get(url), timeout=t)
        logger.info("%s → HTTP %s ct=%s len=%d",
                    label, resp.status_code,
                    resp.headers.get("content-type", "?"), len(resp.content))
        if resp.status_code != 200:
            return None
        content_type = resp.headers.get("content-type", "")
        if "application/json" in content_type or resp.content[:1] in (b"{", b"["):
            try:
                data = resp.json()
                for key in ("url", "recording_url", "audio_url", "file_url", "path"):
                    rec_url = data.get(key)
                    if rec_url:
                        t2 = _time_left(deadline, 15.0)
                        if t2 <= 0:
                            return None
                        audio_resp = await asyncio.wait_for(client.get(rec_url), timeout=t2)
                        if audio_resp.status_code == 200 and len(audio_resp.content) > 1000:
                            return audio_resp.content
            except Exception:
                pass
            return None
        if any(t in content_type for t in ("text/html", "text/xml", "text/plain")):
            return None
        if resp.content[:1] in (b"<", b"\n", b"\r"):
            return None
        if len(resp.content) > 1000:
            return resp.content
        return None
    except asyncio.TimeoutError:
        logger.warning("%s: timeout %.0fs", label, t)
        return None
    except Exception as exc:
        logger.warning("%s fallito: %s", label, exc)
        return None


async def _download_rec(rec_id: str, file_name: str = "", deadline: float = 0) -> Optional[bytes]:
    """Scarica audio con UNA sola strategia e timeout stretto."""
    if _time_left(deadline) <= 0:
        logger.warning("download_rec: deadline superata, skip rec_id=%s", rec_id)
        return None

    url = f"{_BASE}?a=getLeadRec&id={rec_id}&apiToken={_TOKEN}"
    t = min(_time_left(deadline, 20.0), 20.0)  # max 20s per download

    try:
        async with httpx.AsyncClient(timeout=_DOWNLOAD_TIMEOUT, follow_redirects=True) as client:
            resp = await asyncio.wait_for(client.get(url), timeout=t)

            if resp.status_code != 200:
                logger.warning("download rec_id=%s: HTTP %s", rec_id, resp.status_code)
                return None

            ct = resp.headers.get("content-type", "")
            body = resp.content

            # Rifiuta HTML/JSON/testo
            if any(x in ct for x in ("text/html", "text/xml", "text/plain", "application/json")):
                logger.warning("download rec_id=%s: content-type non audio: %s", rec_id, ct)
                return None
            if body[:1] in (b"<", b"{", b"["):
                logger.warning("download rec_id=%s: contenuto non audio", rec_id)
                return None
            if len(body) < 1000:
                logger.warning("download rec_id=%s: file troppo piccolo (%d bytes)", rec_id, len(body))
                return None

            logger.info("download rec_id=%s OK: %d bytes, ct=%s", rec_id, len(body), ct)
            return body

    except asyncio.TimeoutError:
        logger.warning("download rec_id=%s: TIMEOUT dopo %.0fs", rec_id, t)
        return None
    except Exception as exc:
        logger.warning("download rec_id=%s: errore: %s", rec_id, exc)
        return None

        logger.warning("Sidial: nessun audio per rec_id=%s (deadline_left=%.0fs)", rec_id, _time_left(deadline))
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
    Trova e scarica le registrazioni con DEADLINE globale.
    Se supera _GLOBAL_DEADLINE secondi, interrompe tutto e restituisce quello che ha.
    """
    from datetime import timedelta

    _log_config()
    norm_phone = _normalize_phone(phone)
    now_utc = datetime.now(tz=timezone.utc)
    cutoff = now_utc - timedelta(days=lookback_days)
    deadline = time.monotonic() + _GLOBAL_DEADLINE
    t0 = time.monotonic()

    logger.info(
        "Sidial: ricerca — phone=%s (norm=%s) piva=%s rs=%s deadline=%ds",
        phone, norm_phone, piva or "—", ragione_sociale or "—", _GLOBAL_DEADLINE,
    )

    async def _progress(msg):
        if progress_cb:
            try:
                await progress_cb(msg)
            except Exception:
                pass

    # ── FASE A: raccogli lead con short-circuit ───────────────────────────
    all_leads, search_params_used, search_method = await _collect_all_leads(
        phone=phone, piva=piva,
        ragione_sociale=ragione_sociale, last_name=last_name,
        progress_cb=progress_cb, deadline=deadline,
    )

    elapsed_a = int(time.monotonic() - t0)
    logger.info("Sidial: FASE A completata in %ds — %d lead, metodo=%s", elapsed_a, len(all_leads), search_method)

    _empty_stats = {
        "leads_found": 0, "total_recs": 0, "recent_recs": 0,
        "converting_recs": 0, "total_seconds": 0, "search_params_used": 0,
        "search_method": "nessuno",
        "phone_variants": _phone_variants(phone),
        "elapsed_seconds": elapsed_a,
    }

    if not all_leads:
        logger.warning("Sidial: 0 lead trovati (elapsed: %ds)", elapsed_a)
        return ([], _empty_stats) if return_stats else []

    # ── FASE B: raccogli registrazioni ────────────────────────────────────
    await _progress(f"{len(all_leads)} lead trovati in {elapsed_a}s — cerco registrazioni...")
    seen_rec_ids: set = set()
    all_recs: list[dict] = []

    async with httpx.AsyncClient(timeout=_HTTP_TIMEOUT) as client:
        for i, lead in enumerate(all_leads):
            if time.monotonic() > deadline:
                await _progress(f"Deadline! Interrotto dopo {i}/{len(all_leads)} lead")
                break
            lead_id = str(lead.get("id") or "")
            if not lead_id:
                continue
            await _progress(f"searchRecs lead {i+1}/{len(all_leads)} (id={lead_id})...")
            recs = await _search_recs_by_lead(client, lead_id, deadline=deadline)
            for r in recs:
                rid = str(r.get("id") or "")
                if rid and rid not in seen_rec_ids:
                    seen_rec_ids.add(rid)
                    r["_lead_id"] = lead_id
                    all_recs.append(r)
            elapsed_bi = int(time.monotonic() - t0)
            await _progress(f"Lead {i+1}/{len(all_leads)}: {len(all_recs)} rec trovate ({elapsed_bi}s)")

    elapsed_b = int(time.monotonic() - t0)

    if not all_recs:
        logger.warning("Sidial: 0 registrazioni per %d lead (elapsed: %ds)", len(all_leads), elapsed_b)
        await _progress(f"0 registrazioni trovate per {len(all_leads)} lead ({elapsed_b}s)")
        _no_recs_stats = {**_empty_stats, "leads_found": len(all_leads), "search_method": search_method, "elapsed_seconds": elapsed_b}
        return ([], _no_recs_stats) if return_stats else []

    # ── FASE C: filtra per lookback_days + ordina per durata DESC ────────
    _MIN_SEC = max(10, min_call_seconds)

    all_recs_sorted = sorted(
        all_recs,
        key=lambda r: int(r.get("callLength") or 0),
        reverse=True,  # le più lunghe PRIMA — la chiamata di vendita è la più lunga
    )
    logger.info("Sidial: %d registrazioni totali, top callLength=%ss",
                len(all_recs_sorted),
                all_recs_sorted[0].get("callLength") if all_recs_sorted else "?")

    recent = [
        r for r in all_recs_sorted
        if (_parse_rec_datetime(r) or datetime.min.replace(tzinfo=timezone.utc)) >= cutoff
    ]

    if recent:
        candidates = recent
    else:
        candidates = all_recs_sorted[:3]
        logger.warning("Sidial: nessuna rec recente — uso le 3 più lunghe")

    # Filtra per durata minima e converted=y
    useful = [
        r for r in candidates
        if int(r.get("callLength") or 0) >= _MIN_SEC
        and (r.get("converted") or "n").lower() == "y"
    ]

    # Se nessuna converted, tieni le non-converted come "pending"
    pending_long = [
        r for r in candidates
        if int(r.get("callLength") or 0) >= _MIN_SEC
        and (r.get("converted") or "n").lower() != "y"
    ]

    if not useful and not pending_long:
        # Nessuna con durata sufficiente — usa la più lunga disponibile
        useful = [r for r in candidates if (r.get("converted") or "n").lower() == "y"][:1]

    # LIMITE: scarica max 10 registrazioni (ordinate per durata)
    _MAX_DOWNLOAD = 10
    useful = useful[:_MAX_DOWNLOAD]

    await _progress(f"{len(useful)} registrazioni da scaricare (max {_MAX_DOWNLOAD}, ordinate per durata)...")
    logger.info(
        "Sidial: %d lead | %d rec tot | %d recenti | %d utili | %d pending | metodo=%s | elapsed=%ds",
        len(all_leads), len(all_recs_sorted), len(recent), len(useful), len(pending_long), search_method, elapsed_b,
    )
    for j, r in enumerate(useful[:5]):
        logger.info("  → [%d] rec_id=%s callLength=%ss converted=%s",
                     j+1, r.get("id"), r.get("callLength"), r.get("converted"))

    stats: dict = {
        "leads_found": len(all_leads),
        "total_recs": len(all_recs_sorted),
        "recent_recs": len(recent),
        "converting_recs": len(pending_long),
        "total_seconds": 0,
        "search_params_used": search_params_used,
        "search_method": search_method,
    }

    # ── FASE E: scarica (max 5, stop appena ne abbiamo 3 OK) ────────────
    _MIN_GOOD = 99  # scarica tutto quello che riesce entro la deadline
    results: list[Tuple[str, bytes]] = []
    total_secs = 0
    failed = 0
    for i, rec in enumerate(useful):
        if time.monotonic() > deadline:
            await _progress(f"Deadline! Scaricate {len(results)}/{len(useful)}")
            break
        if len(results) >= _MIN_GOOD:
            logger.info("Sidial: già %d registrazioni OK, stop download", len(results))
            await _progress(f"{len(results)} registrazioni scaricate — sufficienti!")
            break

        rec_id = str(rec.get("id") or "")
        if not rec_id:
            continue
        call_len = int(rec.get("callLength") or 0)

        await _progress(f"Download {i+1}/{len(useful)} (rec_id={rec_id}, {call_len}s)...")
        audio_bytes = await _download_rec(rec_id, file_name=rec.get("fileName") or "", deadline=deadline)
        if audio_bytes:
            results.append((rec_id, audio_bytes))
            total_secs += call_len
            await _progress(f"✓ Scaricata {len(results)}/{len(useful)} ({call_len}s)")
        else:
            failed += 1
            logger.warning("Sidial: no audio per rec_id=%s (failed=%d)", rec_id, failed)

    stats["total_seconds"] = total_secs
    elapsed_tot = int(time.monotonic() - t0)
    stats["elapsed_seconds"] = elapsed_tot
    logger.info("Sidial: %d/%d scaricate · %ds audio · %ds elapsed", len(results), len(useful), total_secs, elapsed_tot)

    if return_stats:
        return results, stats
    return results


async def find_and_download_recording(
    phone: str,
    appointment_datetime: str,
    campaign_code: Optional[str] = None,
) -> Tuple[Optional[str], Optional[bytes]]:
    results = await find_and_download_all_recordings(phone, campaign_code)
    if not results:
        return None, None
    return results[0]

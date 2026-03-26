"""
Sidial CRM client — COMPLETAMENTE SINCRONO in thread dedicato.

Architettura:
  - find_and_download_all_recordings() è l'unica funzione pubblica async.
  - Internamente chiama _find_all_sync() via asyncio.to_thread().
  - _find_all_sync() fa TUTTO in un singolo thread con httpx.Client sync.
  - Nessun asyncio all'interno del thread → nessun problema di cancellazione.
  - ThreadPoolExecutor dedicato (max 4 worker) → nessuna contesa con il resto.

Flusso Sidial API:
  1. POST a=searchLeads  — trova lead per telefono / P.IVA / RS
  2. POST a=searchRecs   — lista registrazioni del lead
  3. GET  a=getLeadRec   — scarica audio registrazione
"""

import asyncio
import functools
import json
import logging
import re
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone, timedelta
from typing import Optional, Tuple

import httpx

from config import settings

logger = logging.getLogger(__name__)

_BASE  = settings.sidial_api_url.rstrip("/")
_TOKEN = settings.sidial_api_token

# Executor DEDICATO — max 4 thread contemporanei (4 analisi parallele)
# Separato dal ThreadPoolExecutor di sistema → nessuna contesa
_EXECUTOR = ThreadPoolExecutor(max_workers=4, thread_name_prefix="sidial")

# Deadline interna del thread (secondi) — INDIPENDENTE da asyncio
_THREAD_DEADLINE = 120   # 2 minuti max per l'intero thread

# Campi telefono Sidial
_PHONE_FIELDS = ("phone1", "phone2", "phone3", "phone4")


class SidialDeadlineError(TimeoutError):
    """Deadline thread Sidial superata."""


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


# ── Nucleo SINCRONO — tutto gira in un singolo thread ────────────────────────

def _find_all_sync(
    phone: str,
    piva: str,
    ragione_sociale: str,
    last_name: str,
    lookback_days: int,
    min_call_seconds: int,
    max_recs: int,
) -> tuple[list, dict]:
    """
    COMPLETAMENTE SINCRONO.
    Eseguito in thread dedicato via asyncio.to_thread — zero asyncio qui dentro.
    Timeout httpx nativi (connect=5s, read=10s, total=15s).
    Deadline interna: _THREAD_DEADLINE secondi dall'inizio.
    """
    t0 = time.monotonic()
    deadline = t0 + _THREAD_DEADLINE

    # ── HTTP helper ──────────────────────────────────────────────────────
    _TO = httpx.Timeout(connect=5.0, read=10.0, write=5.0, pool=2.0)
    _DL = httpx.Timeout(connect=5.0, read=30.0, write=5.0, pool=2.0)

    def _post(data: dict) -> Optional[httpx.Response]:
        if time.monotonic() > deadline:
            return None
        try:
            with httpx.Client(timeout=_TO) as c:
                return c.post(_BASE, data=data)
        except httpx.TimeoutException:
            logger.warning("Sidial POST timeout: a=%s", data.get("a"))
            return None
        except Exception as exc:
            logger.warning("Sidial POST error: %s", exc)
            return None

    def _get_url(url: str) -> Optional[httpx.Response]:
        if time.monotonic() > deadline:
            return None
        try:
            with httpx.Client(timeout=_DL, follow_redirects=True) as c:
                return c.get(url)
        except httpx.TimeoutException:
            logger.warning("Sidial GET timeout: %s", url[:80])
            return None
        except Exception as exc:
            logger.warning("Sidial GET error: %s", exc)
            return None

    # ── Ricerca lead ─────────────────────────────────────────────────────
    def _search(field: str, value: str, operator: str = "=") -> list[dict]:
        pj = json.dumps([{"table": "leads", "field": field, "operator": operator, "value": value}])
        r = _post({"a": "searchLeads", "apiToken": _TOKEN, "params": pj})
        if r is None or r.status_code not in (200, 404):
            return []
        try:
            d = r.json()
        except Exception:
            return []
        if isinstance(d, list):
            return d
        if isinstance(d, dict):
            if d.get("response", {}).get("error"):
                return []
            return d.get("results", [])
        return []

    def _dedup(leads: list[dict]) -> list[dict]:
        seen: set = set()
        out: list = []
        for lead in leads:
            lid = str(lead.get("id") or "")
            if lid and lid not in seen:
                seen.add(lid)
                out.append(lead)
        return out

    # ── Registrazioni per lead ───────────────────────────────────────────
    def _get_recs(lead_id: str) -> list[dict]:
        if time.monotonic() > deadline:
            return []
        f = json.dumps([{"table": "leadsRecs", "field": "lead", "operator": "=", "value": int(lead_id)}])
        r = _post({"a": "searchRecs", "apiToken": _TOKEN, "params": f})
        if r is None or r.status_code == 404:
            return []
        if r.status_code != 200:
            return []
        try:
            d = r.json()
        except Exception:
            return []
        if isinstance(d, list):
            return d
        if isinstance(d, dict):
            return d.get("results", [])
        return []

    # ── Download registrazione ───────────────────────────────────────────
    def _download(rec_id: str) -> tuple[Optional[bytes], str]:
        if time.monotonic() > deadline:
            return None, "deadline"
        url = f"{_BASE}?a=getLeadRec&id={rec_id}&apiToken={_TOKEN}"
        r = _get_url(url)
        if r is None:
            return None, "timeout/error"
        ct = r.headers.get("content-type", "")
        body = r.content
        if r.status_code == 200 and len(body) > 1000:
            if "text/html" not in ct and body[:1] not in (b"<", b"{"):
                return body, ""
            if "application/json" in ct or body[:1] in (b"{", b"["):
                try:
                    data = json.loads(body)
                    for key in ("url", "recording_url", "audio_url", "file_url", "path"):
                        rec_url = data.get(key)
                        if rec_url:
                            r2 = _get_url(rec_url)
                            if r2 and r2.status_code == 200 and len(r2.content) > 1000:
                                return r2.content, ""
                except Exception:
                    pass
        # Strategia 2: POST
        if time.monotonic() < deadline:
            try:
                with httpx.Client(timeout=_DL) as c:
                    r2 = c.post(_BASE, data={"a": "getLeadRec", "id": rec_id, "apiToken": _TOKEN})
                if r2.status_code == 200 and len(r2.content) > 1000:
                    ct2 = r2.headers.get("content-type", "")
                    if "text/html" not in ct2 and r2.content[:1] not in (b"<", b"{"):
                        return r2.content, ""
            except Exception:
                pass
        return None, f"HTTP {r.status_code}, ct={ct[:30]}"

    # ── FASE A: ricerca lead ─────────────────────────────────────────────
    all_leads: list[dict] = []
    variants = _phone_variants(phone)
    logger.info("Sidial thread: phone=%s variants=%s piva=%s rs=%s",
                phone, variants, piva or "—", ragione_sociale or "—")

    # Telefono — TUTTE le varianti × tutti i campi (no short-circuit)
    for v in variants:
        for f in _PHONE_FIELDS:
            if time.monotonic() > deadline:
                break
            leads = _search(f, v)
            if leads:
                logger.info("Sidial: %d lead con %s=%s", len(leads), f, v)
            all_leads.extend(leads)

    # P.IVA
    if piva:
        for f in ("vat", "piva", "partitaiva", "fiscal_code", "codfis", "taxid", "cf"):
            if time.monotonic() > deadline:
                break
            leads = _search(f, piva)
            if leads:
                logger.info("Sidial: %d lead con piva:%s", len(leads), f)
            all_leads.extend(leads)

    # Ragione Sociale / last_name
    rs = ragione_sociale or last_name
    if rs:
        for f in ("companyName", "company", "ragioneSociale", "businessName",
                  "name", "ragione_sociale", "surname"):
            if time.monotonic() > deadline:
                break
            leads = _search(f, rs, "like")
            if leads:
                logger.info("Sidial: %d lead con rs:%s", len(leads), f)
            all_leads.extend(leads)

    all_leads = _dedup(all_leads)
    elapsed_a = time.monotonic() - t0
    logger.info("Sidial: %d lead unici in %.1fs", len(all_leads), elapsed_a)

    _phone_vars = variants
    _empty = {
        "leads_found": 0, "total_recs": 0, "recent_recs": 0,
        "converting_recs": 0, "total_seconds": 0,
        "search_method": "nessuno", "phone_variants": _phone_vars,
        "elapsed_seconds": int(elapsed_a),
    }

    if not all_leads:
        return [], _empty

    # ── FASE B: registrazioni per ogni lead ──────────────────────────────
    seen_ids: set = set()
    all_recs: list[dict] = []

    for lead in all_leads:
        if time.monotonic() > deadline:
            break
        lid = str(lead.get("id") or "")
        if not lid:
            continue
        recs = _get_recs(lid)
        for r in recs:
            rid = str(r.get("id") or "")
            if rid and rid not in seen_ids:
                seen_ids.add(rid)
                r["_lead_id"] = lid
                all_recs.append(r)

    elapsed_b = time.monotonic() - t0

    # ── FASE C: filtra e ordina ───────────────────────────────────────────
    now_utc = datetime.now(tz=timezone.utc)
    cutoff = now_utc - timedelta(days=lookback_days)

    def _rec_dt(rec: dict) -> Optional[datetime]:
        val = rec.get("createdWhen")
        if not val:
            return None
        for fmt in ("%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%SZ",
                    "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
            try:
                dt = datetime.strptime(str(val)[:25], fmt)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt
            except ValueError:
                pass
        return None

    def _dur(rec: dict) -> int:
        try:
            return int(rec.get("callLength") or 0)
        except Exception:
            return 0

    def _converted(rec: dict) -> bool:
        return str(rec.get("converted") or "n").lower() == "y"

    # Conta statistiche PRIMA del filtraggio
    total_recs_count = len(all_recs)
    converting_count = sum(1 for r in all_recs if not _converted(r))

    # Recenti
    recent = [r for r in all_recs
              if (_rec_dt(r) or datetime.min.replace(tzinfo=timezone.utc)) >= cutoff]

    candidates = recent if recent else all_recs[:5]

    # Converted e durata sufficiente
    useful = sorted(
        [r for r in candidates if _converted(r) and _dur(r) >= min_call_seconds],
        key=_dur, reverse=True,
    )[:max_recs]

    methods = []
    if any(str(l.get("id")) in {str(r.get("_lead_id")) for r in useful}
           for l in all_leads[:len(variants) * 4]):
        methods.append(f"tel:{len(all_leads)}")
    else:
        methods.append(f"tel+piva_rs:{len(all_leads)}")

    logger.info("Sidial: leads=%d | recs=%d | recent=%d | useful=%d | converting=%d | %.1fs",
                len(all_leads), total_recs_count, len(recent), len(useful),
                converting_count, elapsed_b)

    stats: dict = {
        "leads_found": len(all_leads),
        "total_recs": total_recs_count,
        "recent_recs": len(recent),
        "converting_recs": converting_count,
        "total_seconds": 0,
        "search_method": "+".join(methods) or "nessuno",
        "phone_variants": _phone_vars,
        "elapsed_seconds": int(elapsed_b),
    }

    if not useful:
        return [], stats

    # ── FASE D: download ──────────────────────────────────────────────────
    downloaded: list[Tuple[str, bytes]] = []
    total_secs = 0

    for rec in useful:
        if time.monotonic() > deadline:
            logger.warning("Sidial: deadline durante download, fermato a %d/%d",
                           len(downloaded), len(useful))
            break
        rid = str(rec.get("id") or "")
        if not rid:
            continue
        call_len = _dur(rec)
        audio, err = _download(rid)
        if audio:
            downloaded.append((rid, audio))
            total_secs += call_len
            logger.info("Sidial: ✓ rec %s (%ds, %d bytes)", rid, call_len, len(audio))
        else:
            logger.warning("Sidial: ✗ rec %s: %s", rid, err)

    elapsed_tot = time.monotonic() - t0
    stats["total_seconds"] = total_secs
    stats["elapsed_seconds"] = int(elapsed_tot)
    logger.info("Sidial: %d/%d scaricate | %ds audio | %.1fs totali",
                len(downloaded), len(useful), total_secs, elapsed_tot)
    return downloaded, stats


# ── Wrapper async pubblico ────────────────────────────────────────────────────

async def find_and_download_all_recordings(
    phone: str,
    campaign_code: Optional[str] = None,
    appointment_dt: Optional[datetime] = None,
    lookback_days: int = 90,
    max_recs: int = 10,
    piva: str = "",
    ragione_sociale: str = "",
    last_name: str = "",
    min_call_seconds: int = 20,
    return_stats: bool = False,
    progress_cb=None,
) -> "list[Tuple[str, bytes]] | tuple[list[Tuple[str, bytes]], dict]":
    """
    Async wrapper: esegue TUTTO in un thread dedicato (executor separato).
    Un singolo asyncio.to_thread per tutta l'operazione — nessuna contesa.
    """
    # Aggiorna progress prima di entrare nel thread (è async, non può farlo il thread)
    if progress_cb:
        try:
            await progress_cb(
                f"Ricerca lead (tel: {_normalize_phone(phone)}, "
                f"piva: {piva or '—'}, rs: {ragione_sociale or '—'})..."
            )
        except Exception:
            pass

    logger.info("Sidial: avvio thread — phone=%s piva=%s rs=%s",
                phone, piva or "—", ragione_sociale or "—")

    fn = functools.partial(
        _find_all_sync,
        phone=phone,
        piva=piva,
        ragione_sociale=ragione_sociale,
        last_name=last_name,
        lookback_days=lookback_days,
        min_call_seconds=min_call_seconds,
        max_recs=max_recs,
    )

    try:
        loop = asyncio.get_running_loop()
        recordings, stats = await loop.run_in_executor(_EXECUTOR, fn)
    except Exception as exc:
        logger.error("Sidial: thread error: %s", exc, exc_info=True)
        _empty = {
            "leads_found": 0, "total_recs": 0, "recent_recs": 0,
            "converting_recs": 0, "total_seconds": 0,
            "search_method": f"errore:{type(exc).__name__}",
            "phone_variants": _phone_variants(phone),
        }
        return ([], _empty) if return_stats else []

    if progress_cb:
        try:
            n_recs = stats.get("total_recs", 0)
            n_leads = stats.get("leads_found", 0)
            n_dl = len(recordings)
            await progress_cb(
                f"{n_leads} lead · {n_recs} rec totali · {n_dl} scaricate"
            )
        except Exception:
            pass

    return (recordings, stats) if return_stats else recordings


# Alias per compatibilità con vecchio codice
async def find_and_download_recording(
    phone: str,
    appointment_datetime: str,
    campaign_code: Optional[str] = None,
) -> Tuple[Optional[str], Optional[bytes]]:
    results = await find_and_download_all_recordings(phone, campaign_code)
    if not results:
        return None, None
    return results[0]

"""
Acuity Scheduling client.

  - verify_acuity_webhook: HMAC-SHA256 signature check
  - should_analyze:        decides if a webhook payload should trigger a pipeline
  - get_appointment:       fetch full appointment details from Acuity REST API
"""

import base64
import hashlib
import hmac
import logging
import re
import time as _time
from typing import Optional

import httpx

from config import settings

logger = logging.getLogger(__name__)

# Labels that trigger analysis (lowercase, stripped)
TRIGGER_LABELS: set[str] = {"preso"}

_ACUITY_API_BASE = "https://acuityscheduling.com/api/v1"


# ── Credentials per account ───────────────────────────────────────────────────

def _get_credentials(account_id: int) -> tuple[str, str, Optional[str]]:
    """Return (user_id, api_key, webhook_secret) for the given account."""
    if account_id == 1:
        return (
            settings.acuity_account1_user_id,
            settings.acuity_account1_api_key,
            settings.acuity_account1_webhook_secret,
        )
    elif account_id == 2:
        return (
            settings.acuity_account2_user_id,
            settings.acuity_account2_api_key,
            settings.acuity_account2_webhook_secret,
        )
    raise ValueError(f"Unknown Acuity account_id: {account_id}")


def _basic_auth_header(user_id: str, api_key: str) -> str:
    token = base64.b64encode(f"{user_id}:{api_key}".encode()).decode()
    return f"Basic {token}"


# ── Webhook verification ──────────────────────────────────────────────────────

def verify_acuity_webhook(payload: bytes, signature: str, secret: str) -> bool:
    """
    Verify the X-Acuity-Signature HMAC-SHA256 header.
    Returns True if valid; False otherwise.
    """
    expected = hmac.new(
        secret.encode(),
        payload,
        hashlib.sha256,
    ).hexdigest()
    return hmac.compare_digest(expected, signature)


def check_webhook_signature(
    payload: bytes,
    signature: str,
    account_id: int,
) -> bool:
    """
    Validate the webhook signature for the given Acuity account.
    If ACUITY_VERIFY_WEBHOOK is False or no secret is configured, always returns True.
    """
    if not settings.acuity_verify_webhook:
        return True

    _, _, secret = _get_credentials(account_id)
    if not secret:
        logger.warning(
            "ACUITY_VERIFY_WEBHOOK=true but no secret configured for account %d — skipping",
            account_id,
        )
        return True

    result = verify_acuity_webhook(payload, signature, secret)
    if not result:
        logger.warning(
            "Invalid Acuity webhook signature for account %d", account_id
        )
    return result


# ── Trigger check ─────────────────────────────────────────────────────────────

def should_analyze(payload: dict) -> bool:
    """
    Return True if the appointment has the 'PRESO' label (case-insensitive).
    """
    labels = payload.get("labels") or []
    for label in labels:
        if label.get("name", "").lower().strip() in TRIGGER_LABELS:
            return True
    return False


# ── REST API – get appointment ────────────────────────────────────────────────

# ── Operator detection ────────────────────────────────────────────────────────

_OPERATOR_EMAIL_RE = re.compile(r"op\.\d+\.[^@]+@effoncall\.com", re.IGNORECASE)
# Matches op.XX.*@* (any domain — e.g. Gmail) for extracting the operator number
_ANY_OP_EMAIL_RE  = re.compile(r"op\.(\d{2,})\.[^@]+@", re.IGNORECASE)

# Formato valore operatore: "71-STEFANIA M." o "71 - STEFANIA M." o "91-STEFANO C."
# Numero univoco XX + nome + iniziale cognome puntata (opzionale)
_OPR_VALUE_RE = re.compile(r"^(\d+)\s*-\s*([A-Z][A-Za-zÀ-ÿ]+(?:\s+[A-Z]\.?)?)$")

# Form field names che identificano l'operatore (case-insensitive, accent-stripped)
# Usati come hint prioritario ma NON esclusivi: il formato del valore è determinante
_OPR_FIELD_KEYWORDS_U = ("OPR", "OPER", "OPERATRICE", "OPERATORE", "NOME OP")


def find_operator_email(appointment_data: dict) -> str:
    """
    Recursively scan all string values in the Acuity appointment dict for an
    address matching op.XX.nome@effoncall.com.  Returns empty string if not found.
    """
    def _search(v: object) -> str:
        if isinstance(v, str):
            m = _OPERATOR_EMAIL_RE.search(v)
            return m.group(0) if m else ""
        if isinstance(v, dict):
            for val in v.values():
                found = _search(val)
                if found:
                    return found
        if isinstance(v, list):
            for item in v:
                found = _search(item)
                if found:
                    return found
        return ""

    return _search(appointment_data)


def _norm_fieldname(s: str) -> str:
    """Uppercase + strip common Latin accents for keyword matching."""
    s = s.strip().upper()
    for src, dst in [
        ("À","A"),("Á","A"),("Â","A"),("Ã","A"),("Ä","A"),("Å","A"),
        ("È","E"),("É","E"),("Ê","E"),("Ë","E"),
        ("Ì","I"),("Í","I"),("Î","I"),("Ï","I"),
        ("Ò","O"),("Ó","O"),("Ô","O"),("Õ","O"),("Ö","O"),
        ("Ù","U"),("Ú","U"),("Û","U"),("Ü","U"),
    ]:
        s = s.replace(src, dst)
    return s


def _search_opr_in_list(items: list) -> str:
    """
    Cerca il campo operatore in una lista di {name, value}.
    Strategia doppia:
    1. Prima passa: cerca per nome campo (keywords OPR/OPER/OPERATORE/ecc.)
    2. Seconda passa: cerca per formato valore XX-NOME C. (es. "71-STEFANIA M.")
       indipendentemente dal nome del campo — il formato è determinante.
    L'operatore è identificato univocamente dal numero XX.
    """
    # Passa 1: match per nome campo
    for item in items:
        if not isinstance(item, dict):
            continue
        raw_name = item.get("name") or item.get("label") or ""
        fn = _norm_fieldname(raw_name)
        if any(kw in fn for kw in _OPR_FIELD_KEYWORDS_U):
            v = (item.get("value") or "").strip()
            if v and _OPR_VALUE_RE.match(v):
                return v

    # Passa 2: match per formato valore (XX-NOME C.) — il nome del campo è irrilevante
    for item in items:
        if not isinstance(item, dict):
            continue
        v = (item.get("value") or "").strip()
        if v and _OPR_VALUE_RE.match(v):
            return v

    return ""


def find_opr_field(appointment_data: dict) -> str:
    """
    Search Acuity form fields for an operator-name field and return its value
    (e.g. '91-STEFANO C.').

    Searches both forms[].values[] and forms[].fields[], and also checks
    top-level 'fields' / 'forms' at any nesting level.
    Returns empty string if not found.
    """
    # Primary: forms[].values[] (standard Acuity structure)
    for form in (appointment_data.get("forms") or []):
        # Acuity may use "values" or "fields" as the key
        items = form.get("values") or form.get("fields") or []
        found = _search_opr_in_list(items)
        if found:
            return found

    # Fallback: top-level "fields" array (some Acuity response shapes)
    top_fields = appointment_data.get("fields") or []
    if top_fields:
        found = _search_opr_in_list(top_fields)
        if found:
            return found

    return ""


def get_operator_display(appointment_data: dict) -> str:
    """
    Return the best available operator display string for an appointment.

    Priority:
      1. OPR. / NOME OPR / NOME OPERATRICE / NOME OPERATORE form field
         Value already in '91-STEFANO C.' format → rendered as '91-STEFANO C.'
      2. op.XX.nome@effoncall.com email  →  '91-STEFANO'
      3. Any op.XX.*@* email (e.g. Gmail)  →  '91-—'
         (number known but name not available separately)
      4. '—'
    """
    # 1. OPR./NOME OPERATRICE/etc. form field
    opr = find_opr_field(appointment_data)
    if opr:
        m = _OPR_VALUE_RE.match(opr)
        if m:
            return f"{m.group(1)}-{m.group(2).strip().upper()}"
        return opr.upper()

    # 2. op.XX.nome@effoncall.com email
    email = find_operator_email(appointment_data)
    if email:
        return format_operator_display(email)

    # 3. Any op.XX.*@* email (Gmail etc.) — extract number only
    def _find_op_num(v: object) -> str:
        if isinstance(v, str):
            m = _ANY_OP_EMAIL_RE.search(v)
            return m.group(1) if m else ""
        if isinstance(v, dict):
            for x in v.values():
                r = _find_op_num(x)
                if r:
                    return r
        if isinstance(v, list):
            for x in v:
                r = _find_op_num(x)
                if r:
                    return r
        return ""

    op_num = _find_op_num(appointment_data)
    if op_num:
        return f"{op_num}-—"

    return "—"


def format_operator_display(op_email: str) -> str:
    """
    op.12.mario@effoncall.com  →  '12-MARIO'
    Returns the raw email (or '—') if the pattern doesn't match.
    """
    m = re.match(r"op\.(\d+)\.([^@]+)@effoncall\.com", op_email.strip(), re.IGNORECASE)
    if m:
        return f"{m.group(1)}-{m.group(2).upper()}"
    return op_email or "—"


# ── Phone extraction ─────────────────────────────────────────────────────────

_PHONE_FIELD_KEYWORDS = ("tel", "phone", "numero", "cellulare", "mobile", "cel")
_PIVA_FIELD_KEYWORDS  = ("partita iva", "piva", "p.iva", "p. iva", " pi ", "codice fiscale", "vat")
_PHONE_VALUE_RE = re.compile(r"[\d\s\+\-\(\)]{7,}")

def extract_phone(appointment_data: dict) -> str:
    """
    Extract phone number from Acuity appointment data.
    Priority:
      1. Top-level 'phone' field
      2. Form fields whose name contains phone-related keywords
      3. Any field value that looks like a phone number
    """
    # 1. Standard top-level field
    phone = (appointment_data.get("phone") or "").strip()
    if phone:
        return phone

    # 2. Search form fields by keyword
    for form in (appointment_data.get("forms") or []):
        for val in (form.get("values") or form.get("fields") or []):
            field_name = (val.get("name") or val.get("label") or "").lower()
            if any(kw in field_name for kw in _PHONE_FIELD_KEYWORDS):
                v = (val.get("value") or "").strip()
                if v:
                    logger.info("Phone trovato in form field '%s': %s", field_name, v)
                    return v

    # 3. Top-level fields array
    for val in (appointment_data.get("fields") or []):
        field_name = (val.get("name") or val.get("label") or "").lower()
        if any(kw in field_name for kw in _PHONE_FIELD_KEYWORDS):
            v = (val.get("value") or "").strip()
            if v:
                logger.info("Phone trovato in top-level field '%s': %s", field_name, v)
                return v

    return ""


def extract_piva(appointment_data: dict) -> str:
    """
    Estrae la Partita IVA dall'appuntamento Acuity.
    Cerca nei form fields per keyword: 'partita iva', 'piva', 'p.iva', ecc.
    """
    for form in (appointment_data.get("forms") or []):
        for val in (form.get("values") or form.get("fields") or []):
            field_name = (val.get("name") or val.get("label") or "").lower()
            if any(kw in field_name for kw in _PIVA_FIELD_KEYWORDS):
                v = (val.get("value") or "").strip()
                if v:
                    logger.info("P.IVA trovata in form field '%s': %s", field_name, v)
                    return v
    for val in (appointment_data.get("fields") or []):
        field_name = (val.get("name") or val.get("label") or "").lower()
        if any(kw in field_name for kw in _PIVA_FIELD_KEYWORDS):
            v = (val.get("value") or "").strip()
            if v:
                return v
    return ""


# ── Ragione Sociale extraction ────────────────────────────────────────────────

_RS_KEYWORDS = ("ragione", "azienda", "cliente", "societa", "company")

def _norm(s: str) -> str:
    """Lowercase + strip accents for keyword matching."""
    return re.sub(r"[àáâãäåèéêëìíîïòóôõöùúûü]", lambda m: "aeiou"["aeiouaeiouaeiouaeiou".index(m.group())//4] if m.group() in "àáâãäåèéêëìíîïòóôõöùúûü" else m.group(), s.lower())

def extract_ragione_sociale(appointment_data: dict) -> str:
    """
    Extract company/client name from Acuity appointment data.
    Searches form fields whose name contains keywords like 'ragione',
    'azienda', 'cliente', 'societa', 'company'.
    Falls back to firstName + lastName.
    """
    for form in (appointment_data.get("forms") or []):
        for val in (form.get("values") or []):
            field_name = re.sub(r"[àèéìòù]", "a", (val.get("name") or "").lower())
            if any(kw in field_name for kw in _RS_KEYWORDS):
                v = (val.get("value") or "").strip()
                if v:
                    return v
    parts = [appointment_data.get("firstName", ""), appointment_data.get("lastName", "")]
    return " ".join(p for p in parts if p).strip() or ""


# ── Form fields extraction ────────────────────────────────────────────────────

def extract_all_form_fields(appointment_data: dict) -> dict:
    """
    Estrae TUTTI i form fields dall'appuntamento Acuity come dizionario piatto
    {field_name: value}. Usato per salvare nel DB e per future ricerche
    (es. telefono diretto, P.IVA alternativa, note operatore).
    Campi vuoti vengono inclusi ugualmente per tracciabilità.
    """
    fields: dict = {}
    for form in (appointment_data.get("forms") or []):
        for val in (form.get("values") or form.get("fields") or []):
            name = (val.get("name") or val.get("label") or "").strip()
            value = (val.get("value") or "")
            if name:
                fields[name] = value
    for val in (appointment_data.get("fields") or []):
        name = (val.get("name") or val.get("label") or "").strip()
        value = (val.get("value") or "")
        if name:
            fields[name] = value
    return fields


# ── In-memory cache for list_appointments (avoids re-fetching on every page nav) ─

_APPTS_CACHE: dict = {}
_APPTS_CACHE_TTL = 300  # seconds — 5 minutes


def clear_appointments_cache() -> None:
    """Invalidate all cached appointment lists (forces next call to hit Acuity API)."""
    _APPTS_CACHE.clear()


# ── REST API – list appointments ───────────────────────────────────────────────

async def list_appointments(
    account_id: int,
    min_date: Optional[str] = None,
    max_date: Optional[str] = None,
    max_results: int = 200,
) -> list[dict]:
    """
    Fetch a paginated list of appointments (newest first).
    Results are cached for _APPTS_CACHE_TTL seconds to avoid hitting the
    Acuity API on every page navigation.
    Returns an empty list on failure or if credentials are not configured.
    """
    user_id, api_key, _ = _get_credentials(account_id)
    if not user_id or not api_key:
        return []

    cache_key = (account_id, min_date, max_date, max_results)
    now = _time.monotonic()
    if cache_key in _APPTS_CACHE:
        ts, cached_data = _APPTS_CACHE[cache_key]
        if now - ts < _APPTS_CACHE_TTL:
            logger.debug("list_appointments cache HIT account=%d", account_id)
            return cached_data

    url = f"{_ACUITY_API_BASE}/appointments"
    params: dict = {"max": max_results, "direction": "DESC"}
    if min_date:
        params["minDate"] = min_date
    if max_date:
        params["maxDate"] = max_date

    async with httpx.AsyncClient(timeout=20.0) as client:
        try:
            resp = await client.get(
                url,
                headers={"Authorization": _basic_auth_header(user_id, api_key)},
                params=params,
            )
            resp.raise_for_status()
            data = resp.json()
            result = data if isinstance(data, list) else []
        except Exception as exc:
            logger.error("list_appointments failed account=%d: %s", account_id, exc)
            return []

    _APPTS_CACHE[cache_key] = (_time.monotonic(), result)
    return result


# ── REST API – get appointment ────────────────────────────────────────────────

async def get_appointment(appointment_id: str | int, account_id: int) -> dict:
    """
    Fetch full appointment details from Acuity REST API.
    Returns the JSON dict or an empty dict on failure.
    """
    user_id, api_key, _ = _get_credentials(account_id)
    url = f"{_ACUITY_API_BASE}/appointments/{appointment_id}"

    async with httpx.AsyncClient(timeout=15.0) as client:
        try:
            resp = await client.get(
                url,
                headers={"Authorization": _basic_auth_header(user_id, api_key)},
            )
            resp.raise_for_status()
            return resp.json()
        except Exception as exc:
            logger.error(
                "Acuity get_appointment failed for id=%s account=%d: %s",
                appointment_id,
                account_id,
                exc,
            )
            return {}

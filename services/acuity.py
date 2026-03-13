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

# OPR. field: value like "91-STEFANO C." or "12-MARIO R."
_OPR_VALUE_RE = re.compile(r"^(\d+)-(.+)$")


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


def find_opr_field(appointment_data: dict) -> str:
    """
    Search Acuity form fields for a field named 'OPR.' (or similar) and
    return its value (e.g. '91-STEFANO C.').
    Returns empty string if not found.
    """
    for form in (appointment_data.get("forms") or []):
        for val in (form.get("values") or []):
            field_name = (val.get("name") or "").strip().upper()
            if field_name.startswith("OPR"):
                v = (val.get("value") or "").strip()
                if v:
                    return v
    return ""


def get_operator_display(appointment_data: dict) -> str:
    """
    Return the best available operator display string for an appointment.

    Priority:
      1. OPR. form field  →  '91-STEFANO C.'  →  '91 · STEFANO C.'
      2. op.XX.nome@effoncall.com email  →  '91 · STEFANO'
      3. '—' (should never occur if Acuity data is complete)
    """
    # 1. OPR. field
    opr = find_opr_field(appointment_data)
    if opr:
        m = _OPR_VALUE_RE.match(opr)
        if m:
            return f"{m.group(1)} · {m.group(2).strip().upper()}"
        return opr.upper()

    # 2. Email fallback
    email = find_operator_email(appointment_data)
    if email:
        return format_operator_display(email)

    return "—"


def format_operator_display(op_email: str) -> str:
    """
    op.12.mario@effoncall.com  →  '12 · MARIO'
    Returns the raw email (or '—') if the pattern doesn't match.
    """
    m = re.match(r"op\.(\d+)\.([^@]+)@effoncall\.com", op_email.strip(), re.IGNORECASE)
    if m:
        return f"{m.group(1)} · {m.group(2).upper()}"
    return op_email or "—"


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


# ── REST API – list appointments ───────────────────────────────────────────────

async def list_appointments(
    account_id: int,
    min_date: Optional[str] = None,
    max_date: Optional[str] = None,
    max_results: int = 200,
) -> list[dict]:
    """
    Fetch a paginated list of appointments (newest first).
    Returns an empty list on failure or if credentials are not configured.
    """
    user_id, api_key, _ = _get_credentials(account_id)
    if not user_id or not api_key:
        return []

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
            return data if isinstance(data, list) else []
        except Exception as exc:
            logger.error("list_appointments failed account=%d: %s", account_id, exc)
            return []


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

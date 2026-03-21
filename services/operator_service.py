"""
Operator identification service.

Extracts the operator number (XX) from email or form field values,
then looks up the operator record in the DB.
"""
import logging
import re

logger = logging.getLogger(__name__)

# op.71.stefania@effoncall.com → group(1) = "71"
_OP_NUM_FROM_EMAIL = re.compile(r"op\.(\d+)\.", re.IGNORECASE)

# "71-STEFANIA M." or "71 - STEFANIA M." → group(1) = "71"
_OP_NUM_FROM_FIELD = re.compile(r"^(\d+)\s*-\s*[A-Z]", re.IGNORECASE)


async def identify_operator(email: str, form_fields: dict) -> dict:
    """
    Identify operator by extracting the XX number.

    Priority:
      1. email regex  (op.XX.nome@*)
      2. scan form field values for XX-NOME C. pattern
      3. look up in operators table

    Returns dict with keys: number, email, display_name, source, warning
    """
    number = None
    source = None

    # Step 1: try email
    if email:
        m = _OP_NUM_FROM_EMAIL.search(email)
        if m:
            number = m.group(1)
            source = "email"

    # Step 2: fallback — scan form fields for XX-NOME C. pattern
    if not number and form_fields:
        for field_name, value in form_fields.items():
            if isinstance(value, str):
                m = _OP_NUM_FROM_FIELD.match(value.strip())
                if m:
                    number = m.group(1)
                    source = f"form field '{field_name}'"
                    break

    if not number:
        return {
            "number": None,
            "email": email,
            "display_name": None,
            "source": None,
            "warning": "Numero operatore non estraibile da nessun campo",
        }

    # Step 3: lookup in operators table
    from database import AsyncSessionLocal
    from models import Operator
    from sqlalchemy import select

    async with AsyncSessionLocal() as sess:
        op = await sess.scalar(
            select(Operator).where(Operator.number == number, Operator.active == True)
        )

    if op:
        return {
            "number": number,
            "email": op.email or email,
            "display_name": op.display_name,
            "source": source,
            "warning": None,
        }
    else:
        return {
            "number": number,
            "email": email,  # fallback to form email
            "display_name": None,
            "source": source,
            "warning": (
                f"Operatore #{number} non trovato in tabella operators "
                f"— usata email form come fallback"
            ),
        }

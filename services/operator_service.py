"""
Operator identification service.

Extracts the operator number (XX) from email or form field values,
then looks up the operator record in the DB.
If the operator doesn't exist yet, AUTO-CREATES it so subsequent
analyses don't repeat the warning.
"""
import logging
import re

logger = logging.getLogger(__name__)

# op.71.stefania@effoncall.com → group(1) = "71"
_OP_NUM_FROM_EMAIL = re.compile(r"op\.(\d+)\.", re.IGNORECASE)

# "71-STEFANIA M." or "71 - STEFANIA M." → group(1) = "71", group(2) = "STEFANIA M."
_OP_NUM_FROM_FIELD = re.compile(r"^(\d+)\s*-\s*([A-Za-z].*)", re.IGNORECASE)

# Extract name from email: op.71.stefania@... → "STEFANIA"
_OP_NAME_FROM_EMAIL = re.compile(r"op\.\d+\.([a-z]+)", re.IGNORECASE)


async def identify_operator(email: str, form_fields: dict) -> dict:
    """
    Identify operator by extracting the XX number.

    Priority:
      1. email regex  (op.XX.nome@*)
      2. scan form field values for XX-NOME C. pattern
      3. look up in operators table — if not found, AUTO-CREATE

    Returns dict with keys: number, email, display_name, source, warning
    """
    number = None
    source = None
    extracted_name = None

    # Step 1: try email
    if email:
        m = _OP_NUM_FROM_EMAIL.search(email)
        if m:
            number = m.group(1)
            source = "email"
            # Try to extract name from email too
            nm = _OP_NAME_FROM_EMAIL.search(email)
            if nm:
                extracted_name = nm.group(1).upper()

    # Step 2: fallback — scan form fields for XX-NOME C. pattern
    if not number and form_fields:
        for field_name, value in form_fields.items():
            if isinstance(value, str):
                m = _OP_NUM_FROM_FIELD.match(value.strip())
                if m:
                    number = m.group(1)
                    extracted_name = m.group(2).strip().upper()
                    source = f"form field '{field_name}'"
                    break

    # Also try form fields even if we got number from email — to get a better name
    if number and not extracted_name and form_fields:
        for field_name, value in form_fields.items():
            if isinstance(value, str):
                m = _OP_NUM_FROM_FIELD.match(value.strip())
                if m and m.group(1) == number:
                    extracted_name = m.group(2).strip().upper()
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
        # Update display_name/email if we have better info now
        needs_update = False
        if extracted_name and not op.display_name:
            op.display_name = extracted_name
            needs_update = True
        if email and not op.email:
            op.email = email
            needs_update = True
        if needs_update:
            try:
                async with AsyncSessionLocal() as sess:
                    async with sess.begin():
                        db_op = await sess.get(Operator, op.id)
                        if db_op:
                            if extracted_name and not db_op.display_name:
                                db_op.display_name = extracted_name
                            if email and not db_op.email:
                                db_op.email = email
                logger.info("Operatore #%s aggiornato con info mancanti", number)
            except Exception as exc:
                logger.warning("Aggiornamento operatore #%s fallito (non-fatale): %s", number, exc)

        return {
            "number": number,
            "email": op.email or email,
            "display_name": op.display_name or extracted_name,
            "source": source,
            "warning": None,
        }

    # ── AUTO-CREATE operator ─────────────────────────────────────────────────
    display_name = extracted_name or f"OP-{number}"
    try:
        async with AsyncSessionLocal() as sess:
            async with sess.begin():
                new_op = Operator(
                    number=number,
                    display_name=display_name,
                    email=email or None,
                    active=True,
                )
                sess.add(new_op)
        logger.info(
            "Operatore #%s AUTO-CREATO: display_name=%s email=%s",
            number, display_name, email,
        )
        return {
            "number": number,
            "email": email,
            "display_name": display_name,
            "source": source,
            "warning": None,  # nessun warning — operatore creato automaticamente
        }
    except Exception as exc:
        # Potrebbe fallire per unique constraint se creato da un'altra pipeline concorrente
        logger.warning("Auto-creazione operatore #%s fallita: %s — uso fallback", number, exc)
        return {
            "number": number,
            "email": email,
            "display_name": display_name,
            "source": source,
            "warning": None,  # non è un problema reale, l'operatore è comunque identificato
        }

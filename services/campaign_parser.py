import re


def parse_campaign_code(appointment_type_name: str) -> dict:
    """
    Parses the Acuity appointment type name to extract campaign components.

    Examples:
        "INTER-J&A-0000-0091-STEFANO-(SEGRATE)" -> multisede
        "AVANZ-COL-3314-GIOVANNI-(NA)"           -> standard
    """
    name = appointment_type_name.strip()

    # Extract province (inside parentheses at the end)
    province_match = re.search(r'\(([^)]+)\)$', name)
    province = province_match.group(1) if province_match else None
    name_without_province = re.sub(r'-?\([^)]+\)$', '', name).strip()

    parts = name_without_province.split('-')

    # Minimum: TIPO-CLIENTE-CODICE (3 segments); agente is optional
    if len(parts) < 3:
        return {"raw": appointment_type_name, "valid": False}

    tipo = parts[0]

    # Multisede: parts[2] and parts[3] are both 4-digit numeric strings
    # e.g.  INTER - J&A - 0000 - 0091 - STEFANO
    #        [0]    [1]    [2]    [3]    [4]
    is_multisede = (
        len(parts) >= 5
        and re.match(r'^\d{4}$', parts[2]) is not None
        and re.match(r'^\d{4}$', parts[3]) is not None
    )

    if is_multisede:
        cliente = parts[1]
        codice = f"{parts[2]}-{parts[3]}"
        agente = '-'.join(parts[4:])
    else:
        cliente = parts[1]
        codice = parts[2]
        agente = '-'.join(parts[3:])  # empty string if only 3 segments

    return {
        "raw": appointment_type_name,
        "valid": True,
        "tipo": tipo,
        "cliente": cliente,
        "codice": codice,
        "agente": agente,
        "provincia": province,
        "is_multisede": is_multisede,
    }


# ── Unit tests ────────────────────────────────────────────────────────────────

def _run_tests() -> None:
    cases = [
        (
            "INTER-J&A-0000-0091-STEFANO-(SEGRATE)",
            {
                "valid": True,
                "tipo": "INTER",
                "cliente": "J&A",
                "codice": "0000-0091",
                "agente": "STEFANO",
                "provincia": "SEGRATE",
                "is_multisede": True,
            },
        ),
        (
            "AVANZ-COL-3314-GIOVANNI-(NA)",
            {
                "valid": True,
                "tipo": "AVANZ",
                "cliente": "COL",
                "codice": "3314",
                "agente": "GIOVANNI",
                "provincia": "NA",
                "is_multisede": False,
            },
        ),
        (
            "REFER-XYZ-1234-MARIO-ROSSI-(MI)",
            {
                "valid": True,
                "tipo": "REFER",
                "cliente": "XYZ",
                "codice": "1234",
                "agente": "MARIO-ROSSI",
                "provincia": "MI",
                "is_multisede": False,
            },
        ),
        # 3-segment code — agente assente (es. AVANZ-AVI-0000)
        (
            "AVANZ-AVI-0000",
            {
                "valid": True,
                "tipo": "AVANZ",
                "cliente": "AVI",
                "codice": "0000",
                "agente": "",
                "is_multisede": False,
            },
        ),
        ("BADCODE", {"valid": False}),
        ("BAD-CODE", {"valid": False}),
    ]

    for raw, expected in cases:
        result = parse_campaign_code(raw)
        for key, val in expected.items():
            assert result.get(key) == val, (
                f"FAIL [{raw}] — key={key!r}: expected {val!r}, got {result.get(key)!r}"
            )
        print(f"PASS: {raw}")

    print("All campaign_parser tests passed.")


if __name__ == "__main__":
    _run_tests()

"""
Claude AI analysis service.

Builds the coaching prompt, calls the Anthropic API, and parses
the structured JSON response.
"""

import json
import logging
from typing import Optional

import anthropic

from config import settings

logger = logging.getLogger(__name__)

_client: Optional[anthropic.Anthropic] = None


def get_client() -> anthropic.Anthropic:
    global _client
    if _client is None:
        _client = anthropic.Anthropic(api_key=settings.anthropic_api_key)
    return _client


# ── Prompt builder ─────────────────────────────────────────────────────────────

def build_analysis_prompt(
    transcript: str,
    campaign_info: dict,
    script: Optional[str] = None,
    qualification_params: Optional[str] = None,
    client_info: Optional[str] = None,
) -> str:
    context_parts: list[str] = []

    if client_info:
        context_parts.append(f"## INFORMAZIONI SUL CLIENTE\n{client_info}")
    if script:
        context_parts.append(f"## SCRIPT DI RIFERIMENTO\n{script}")
    if qualification_params:
        context_parts.append(f"## PARAMETRI DI QUALIFICAZIONE\n{qualification_params}")

    context = (
        "\n\n".join(context_parts)
        if context_parts
        else "Nessun contesto specifico disponibile."
    )

    return f"""Sei un coach esperto di telemarketing B2B italiano. Analizza la seguente chiamata telefonica e fornisci un feedback strutturato e professionale.

## CAMPAGNA
- Tipo: {campaign_info.get('tipo', 'N/A')}
- Cliente: {campaign_info.get('cliente', 'N/A')}
- Operatore: {campaign_info.get('agente', 'N/A')}
- Provincia: {campaign_info.get('provincia', 'N/A')}

{context}

## TRASCRIZIONE CHIAMATA
{transcript}

---

## ISTRUZIONI PER L'ANALISI

Analizza la chiamata e rispondi ESCLUSIVAMENTE in formato JSON con questa struttura esatta:

{{
  "riepilogo_appuntamento": "Breve riepilogo di 2-3 frasi: chi ha chiamato, chi è il prospect, qual è l'esito e i punti chiave emersi.",

  "livello_qualificazione": "eccellente" | "corretta" | "da_migliorare" | "insufficiente",

  "motivazione_livello": "Spiegazione in 1-2 frasi del perché hai assegnato questo livello.",

  "punti_di_forza": [
    "Punto di forza 1 — sii specifico e cita momenti della chiamata",
    "Punto di forza 2",
    "Punto di forza 3"
  ],

  "aree_di_miglioramento": [
    "Area 1 — descrivi cosa non ha funzionato e perché",
    "Area 2",
    "Area 3"
  ],

  "suggerimenti_pratici": [
    {{
      "problema": "Descrizione del problema specifico",
      "suggerimento": "Cosa fare invece",
      "esempio": "Esempio concreto di frase/approccio da usare nella prossima chiamata"
    }}
  ],

  "frase_motivazionale": "Una frase breve di incoraggiamento personalizzata per l'operatore."
}}

Criteri di qualificazione:
- "eccellente": Chiamata esemplare, ottimo approccio, appuntamento preso in modo naturale e professionale
- "corretta": Buona chiamata con piccoli margini di miglioramento
- "da_migliorare": Appuntamento preso ma con lacune significative nella tecnica
- "insufficiente": Problemi seri nell'approccio che compromettono i risultati

Rispondi SOLO con il JSON, senza testo aggiuntivo prima o dopo."""


# ── Analysis call ──────────────────────────────────────────────────────────────

async def analyze_call(
    transcript: str,
    campaign_info: dict,
    script: Optional[str] = None,
    qualification_params: Optional[str] = None,
    client_info: Optional[str] = None,
) -> dict:
    """
    Send the transcript to Claude and return the structured report dict.
    Raises on API errors or JSON parse failure.
    """
    prompt = build_analysis_prompt(
        transcript=transcript,
        campaign_info=campaign_info,
        script=script,
        qualification_params=qualification_params,
        client_info=client_info,
    )

    logger.info(
        "Calling Claude (model=claude-3-haiku-20240307) for campaign=%s operator=%s",
        campaign_info.get("cliente"),
        campaign_info.get("agente"),
    )

    client = get_client()
    message = client.messages.create(
        model="claude-3-haiku-20240307",
        max_tokens=2000,
        messages=[{"role": "user", "content": prompt}],
    )

    response_text = message.content[0].text.strip()

    # Strip possible ```json … ``` fences
    if response_text.startswith("```"):
        parts = response_text.split("```")
        # parts[1] is the content between first pair of fences
        response_text = parts[1]
        if response_text.startswith("json"):
            response_text = response_text[4:]
        response_text = response_text.strip()

    try:
        report = json.loads(response_text)
    except json.JSONDecodeError as exc:
        logger.error("Claude returned invalid JSON: %s\nRaw: %s", exc, response_text[:500])
        raise ValueError(f"Claude returned invalid JSON: {exc}") from exc

    # Ensure required keys are present with fallbacks
    report.setdefault("livello_qualificazione", "corretta")
    report.setdefault("punti_di_forza", [])
    report.setdefault("aree_di_miglioramento", [])
    report.setdefault("suggerimenti_pratici", [])

    return report

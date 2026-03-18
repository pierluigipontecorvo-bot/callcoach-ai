"""
Claude AI analysis service.

Builds the coaching prompt, calls the Anthropic API, and parses
the structured JSON response.
"""

import json
import logging
import re
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


# ── Operator name extractor ─────────────────────────────────────────────────────

def _extract_operator_name(operator_email: Optional[str] = None) -> str:
    """
    Return name from op.xx.nome@effoncall.com email.
    Returns 'N/A' if email is absent or doesn't match the pattern.
    NOTE: never falls back to the campaign 'agente' field — that is the
    client-side commercial, not the Effoncall operator who made the call.
    """
    if operator_email:
        match = re.match(r"op\.\d+\.(.+?)@effoncall\.com", operator_email.lower())
        if match:
            return match.group(1).upper()
    return "N/A"


# ── Field ordering & default disclaimer ────────────────────────────────────────

_REPORT_FIELD_ORDER = [
    "errore_tecnico",
    "ragione_sociale",
    "data_appuntamento",
    "ora_appuntamento",
    "qualificazione",
    "analisi_telefonata",
    "punti_di_forza",
    "aree_di_miglioramento",
    "frase_motivazionale",
    "disclaimer",
]

_DEFAULT_DISCLAIMER = (
    "Questo report è generato automaticamente da un sistema di intelligenza artificiale "
    "sulla base della trascrizione audio della chiamata. I giudizi espressi sono indicativi "
    "e a scopo formativo. La qualità dell'analisi dipende dalla fedeltà della trascrizione, "
    "che potrebbe contenere imprecisioni legate a rumori di fondo, sovrapposizione di voci "
    "o accenti. Utilizzare come strumento di supporto al coaching, non come valutazione definitiva."
)


# ── Prompt builder ─────────────────────────────────────────────────────────────

def build_analysis_prompt(
    transcript: str,
    campaign_info: dict,
    script: Optional[str] = None,
    qualification_params: Optional[str] = None,
    client_info: Optional[str] = None,
    operator_email: Optional[str] = None,
    prompt_sections: Optional[dict] = None,
    prompt_extra: Optional[str] = None,
    global_docs: Optional[list] = None,
) -> str:
    """
    global_docs: list of {"title": str, "content": str} — framework/technique docs
                 loaded from the global_documents table (order respected).
    """

    from services.prompt_db import _DEFAULT_SECTIONS
    secs = {**_DEFAULT_SECTIONS, **(prompt_sections or {})}

    operator_name = _extract_operator_name(operator_email)
    agente = campaign_info.get("agente", "N/A")

    # ── Build documents section ───────────────────────────────────────────────
    docs_parts = []

    # Documenti globali: framework e tecniche di comunicazione sempre validi
    # Ognuno è una sezione separata con il proprio titolo
    for gdoc in (global_docs or []):
        t = (gdoc.get("title") or "").strip()
        c = (gdoc.get("content") or "").strip()
        if t and c:
            docs_parts.append(f"### {t}\n{c}")

    # Documenti specifici della campagna
    if client_info:
        docs_parts.append(f"### INFORMAZIONI SUL CLIENTE E SUL SERVIZIO\n{client_info}")
    if script:
        docs_parts.append(f"### SCRIPT DI CAMPAGNA\n{script}")
    if qualification_params:
        docs_parts.append(f"### PARAMETRI DI QUALIFICAZIONE\n{qualification_params}")

    if docs_parts:
        docs_section = "\n\n".join(docs_parts)
        docs_note = (
            "I seguenti documenti sono stati caricati nel sistema e devono essere "
            "utilizzati come riferimento primario per l'analisi:"
        )
    else:
        docs_section = "Nessun documento caricato per questa campagna."
        docs_note = (
            "⚠️ Non sono disponibili documenti di riferimento (script, parametri di "
            "qualificazione, info cliente). Analizza la chiamata basandoti sulla "
            "trascrizione e sulle best practice del telemarketing B2B outbound italiano."
        )

    # ── Qualification instruction varies by whether params are defined ────────
    if qualification_params:
        qual_instruction = (
            '1. **PARAMETRI DI QUALIFICAZIONE** → Sezione "### PARAMETRI DI QUALIFICAZIONE" nei documenti sopra.\n'
            '   Verifica ESCLUSIVAMENTE i parametri elencati in quella sezione. NON aggiungere parametri non indicati.\n\n'
            '   REGOLE DI CLASSIFICAZIONE — rispettale in ordine:\n\n'
            '   a) CONDIZIONI ALTERNATIVE ("oppure" / "o" / "OR"):\n'
            '      Se un parametro è scritto come "CONDIZIONE A  oppure  CONDIZIONE B", basta che UNA SOLA\n'
            '      delle due sia vera. NON sommare, NON combinare, NON richiedere entrambe.\n'
            '      ✅ Esempio corretto — soglia budget:\n'
            '         Parametro: "spesa internazionale ≥ 3.000€/anno  OPPURE  spesa nazionale ≥ 1.800€/anno"\n'
            '         Prospect dichiara: internazionale 0€, nazionale 1.000€/mese (= 12.000€/anno)\n'
            '         → 12.000€/anno ≥ 1.800€/anno → condizione nazionale SODDISFATTA → parametro OK, NON fuori target.\n'
            '         NON verificare anche la condizione internazionale: basta che una sia soddisfatta.\n'
            '      ❌ Ragionamento VIETATO: "la combinazione non raggiunge il requisito combinato" — questo\n'
            '         tipo di conclusione è SBAGLIATA quando le condizioni sono alternative (oppure).\n'
            '      NON inserirlo in "parametri_mancanti" e NON impostare "fuori_parametro": true per questo motivo.\n\n'
            '   b) PARAMETRI NON RICHIESTI: se l\'operatore non ha chiesto un parametro obbligatorio durante la chiamata,\n'
            '      inseriscilo in "parametri_mancanti". L\'appuntamento è NON IN TARGET → imposta "fuori_parametro": true.\n\n'
            '   c) PARAMETRI RICHIESTI MA FUORI SOGLIA: se il prospect ha dichiarato un valore che non rispetta\n'
            '      la soglia richiesta, l\'appuntamento è NON IN TARGET → imposta "fuori_parametro": true.\n'
            '      - ⚠️ TOLLERANZA 10% — si applica SOLO a parametri monetari (budget, spesa, fatturato, corrispettivo):\n'
            '        Es. soglia minima 12.000€/anno → accettabile ≥ 10.800€/anno (≈ 900€/mese).\n'
            '        Se il valore dichiarato è < 90% della soglia → fuori target.\n'
            '      - ❌ La tolleranza NON si applica a parametri numerici non monetari\n'
            '        (es. numero massimo dipendenti, peso massimo, dimensioni, ecc.) — la soglia è esatta.\n\n'
            '   d) CALCOLO RATING — basato sulla percentuale di parametri correttamente qualificati:\n'
            '      - 100% dei parametri richiesti e conformi → rating 5\n'
            '      - 75-99% → rating 4\n'
            '      - 50-74% → rating 3\n'
            '      - 25-49% → rating 2\n'
            '      - < 25% o "fuori_parametro": true → rating 1\n\n'
            '   e) APPUNTAMENTO NON IN TARGET: imposta "fuori_parametro": true e assegna rating 1 quando:\n'
            '      - Uno o più parametri obbligatori NON sono stati richiesti dall\'operatore, OPPURE\n'
            '      - Uno o più parametri sono stati richiesti ma il valore dichiarato è fuori soglia\n'
            '        (tenendo conto della tolleranza 10% solo per i parametri monetari).\n'
            '      ✅ NON è fuori target se tutti i parametri obbligatori sono stati verificati\n'
            '         e almeno una condizione alternativa ("oppure") è soddisfatta per ciascuno.\n'
            '      Nella "spiegazione" indica esplicitamente: quale parametro è mancante o fuori soglia,\n'
            '      il valore dichiarato vs la soglia richiesta, e la conclusione "Appuntamento NON IN TARGET."'
        )
    else:
        qual_instruction = (
            '1. **PARAMETRI DI QUALIFICAZIONE** → Non sono stati definiti parametri per questa campagna.\n'
            '   In "parametri_verificati": inserisci [] (array vuoto).\n'
            '   In "parametri_mancanti": inserisci [] (array vuoto).\n'
            '   In "spiegazione": scrivi "Parametri di qualificazione non definiti per questa campagna."\n'
            '   Imposta "fuori_parametro": false. Assegna "rating": 3 (neutro). NON inventare parametri.'
        )

    # Build optional extra sections
    _altre = secs.get("altre_istruzioni", "").strip()
    _altre_block = f"\n\n## ALTRE ISTRUZIONI\n\n{_altre}" if _altre else ""
    _extra = (prompt_extra or "").strip()
    _extra_block = f"\n\n## ISTRUZIONI SPECIFICHE DI CAMPAGNA\n\n{_extra}" if _extra else ""

    return f"""## SITUAZIONE

{secs['contesto']}


## RUOLO

{secs['ruolo']}


## COMPITI

{secs['compiti']}


## OBIETTIVO DEL COMPITO

{secs['obiettivo']}


## TONO

{secs['tono']}


## INFORMAZIONI DI CAMPAGNA

- Codice campagna: {campaign_info.get('raw', 'N/A')}
- Tipo di campagna: {campaign_info.get('tipo', 'N/A')}
- Azienda rappresentata dall'operatore: {campaign_info.get('cliente', 'N/A')}
- Agente/Commerciale del cliente (NON è l'operatore che ha chiamato): {agente}
- Operatore Effoncall che ha effettuato la chiamata: {operator_name}
- Provincia: {campaign_info.get('provincia', 'N/A')}

⚠️ ATTENZIONE NOMENCLATURA: Il campo "Agente/Commerciale del cliente" indica il referente commerciale dell'azienda cliente — NON la persona che ha effettuato la telefonata. L'operatore che ha effettuato la chiamata è quello indicato nel campo "Operatore Effoncall". Usa SEMPRE il nome dell'Operatore Effoncall quando nel report ti riferisci a chi ha effettuato la chiamata.


## DOCUMENTI DI RIFERIMENTO

{docs_note}

{docs_section}


## TRASCRIZIONE CHIAMATA

⚠️ REGOLA FONDAMENTALE — PIÙ CHIAMATE:
Se la trascrizione contiene più sezioni "--- CHIAMATA N ---", leggile TUTTE come un percorso
conversazionale unico e progressivo. Le risposte del prospect possono evolvere nel corso delle
chiamate: una risposta inizialmente incerta ("non lo so", "devo controllare") può essere
precisata o confermata in una chiamata successiva.

CONTA SOLO LA RISPOSTA FINALE E CONFERMATA — non la prima risposta vaga o incompleta.
Esempio: se nella CHIAMATA 1 il prospect dice "non so quanto spendiamo" e nella CHIAMATA 3
conferma "controlliamo… spendiamo più di 1.000 euro al mese" → il valore da usare per la
qualificazione è "più di 1.000 euro al mese", NON "non lo so".

MAI classificare "non in target" basandosi solo sulla prima chiamata se le successive
forniscono informazioni più precise e aggiornate.

{transcript}


---

## ISTRUZIONI TECNICHE PER IL RECUPERO DELLE INFORMAZIONI

Per completare l'analisi, utilizza le informazioni secondo questa priorità:

{qual_instruction}

{secs['istruzioni_tecniche']}{_altre_block}{_extra_block}

---

## FORMATO RISPOSTA

⚠️ REGOLE CRITICHE — RISPETTALE TUTTE SENZA NESSUNA ECCEZIONE:

0. PRIMA DI TUTTO — valuta la qualità della trascrizione:
   - Se la trascrizione è incomprensibile, illeggibile, piena di caratteri strani, o ha meno di 10 parole di senso compiuto in italiano → imposta "errore_tecnico": true
   - Se la trascrizione è leggibile (anche parzialmente) → imposta "errore_tecnico": false
   - Se "errore_tecnico" è true: puoi compilare i campi rimanenti con valori di default neutri — il report NON verrà inviato all'operatore.

1. Rispondi ESCLUSIVAMENTE in JSON valido. Zero testo prima o dopo il JSON.
2. I campi devono rispettare ESATTAMENTE l'ordine mostrato nello schema qui sotto. Non modificare, aggiungere o riordinare nessun campo.
3. "punti_di_forza" deve contenere ESATTAMENTE 3 oggetti — né uno di più, né uno di meno.
4. "aree_di_miglioramento" deve contenere ESATTAMENTE 3 oggetti — né uno di più, né uno di meno.
5. Il campo "hai_detto" deve contenere SEMPRE e SOLO una citazione testuale ESATTA dalla trascrizione tra virgolette. Mai una descrizione, mai una parafrasi, mai un riassunto di ciò che ha fatto l'operatore.
6. Il campo "disclaimer" è obbligatorio, deve essere l'ULTIMO campo del JSON e deve contenere ESATTAMENTE il testo fornito nello schema sottostante, senza modifiche.

{{
  "errore_tecnico": false,

  "ragione_sociale": "Ragione sociale del prospect (estraila dalla trascrizione)",
  "data_appuntamento": "YYYY-MM-DD o null se non trovata nella trascrizione",
  "ora_appuntamento": "HH:MM o null se non trovata",

  "qualificazione": {{
    "rating": 3,
    "label": "SUFFICIENTE",
    "fuori_parametro": false,
    "spiegazione": "Spiegazione sintetica in circa 30 parole del perché di questo rating. Se fuori_parametro=true, indica esplicitamente quale soglia minima non è stata raggiunta.",
    "parametri_verificati": [
      "Nome parametro: valore raccolto nella chiamata"
    ],
    "parametri_mancanti": [
      "Nome parametro non richiesto durante la chiamata"
    ]
  }},

  "analisi_telefonata": {{
    "rating_totale": 3,
    "spiegazione_totale": "Spiegazione sintetica in circa 30 parole del rating complessivo della telefonata",
    "fasi": {{
      "apertura": {{
        "rating": 3,
        "spiegazione": "Spiegazione in circa 30 parole"
      }},
      "superamento_gatekeeper": {{
        "rating": null,
        "spiegazione": "Non applicabile (chiamata diretta al DM) OPPURE spiegazione in circa 30 parole se presente"
      }},
      "introduzione_decision_maker": {{
        "rating": 3,
        "spiegazione": "Spiegazione in circa 30 parole"
      }},
      "trasmissione_valore": {{
        "rating": 3,
        "spiegazione": "Spiegazione in circa 30 parole"
      }},
      "superamento_obiezioni": {{
        "rating": null,
        "spiegazione": "Non applicabile (nessuna obiezione) OPPURE spiegazione in circa 30 parole se presente"
      }},
      "negoziazione": {{
        "rating": 3,
        "spiegazione": "Spiegazione in circa 30 parole"
      }},
      "chiusura": {{
        "rating": 3,
        "spiegazione": "Spiegazione in circa 30 parole"
      }}
    }}
  }},

  "punti_di_forza": [
    {{
      "titolo": "Titolo breve del punto di forza",
      "hai_detto": "Citazione TESTUALE ed ESATTA dalla trascrizione tra virgolette — mai una descrizione o parafrasi",
      "perche_efficace": "Spiegazione del perché questa frase o approccio è efficace"
    }},
    {{
      "titolo": "Secondo punto di forza",
      "hai_detto": "Citazione TESTUALE ed ESATTA",
      "perche_efficace": "Spiegazione"
    }},
    {{
      "titolo": "Terzo punto di forza",
      "hai_detto": "Citazione TESTUALE ed ESATTA",
      "perche_efficace": "Spiegazione"
    }}
  ],

  "aree_di_miglioramento": [
    {{
      "titolo": "Titolo breve dell'area di miglioramento",
      "hai_detto": "Citazione TESTUALE ed ESATTA dalla trascrizione tra virgolette — mai una descrizione o parafrasi",
      "avresti_potuto_dire": "Esempio concreto alternativo da usare nella prossima chiamata",
      "perche": "Spiegazione del perché questa versione alternativa è più efficace"
    }},
    {{
      "titolo": "Seconda area",
      "hai_detto": "Citazione TESTUALE ed ESATTA",
      "avresti_potuto_dire": "Esempio alternativo",
      "perche": "Spiegazione"
    }},
    {{
      "titolo": "Terza area",
      "hai_detto": "Citazione TESTUALE ed ESATTA",
      "avresti_potuto_dire": "Esempio alternativo",
      "perche": "Spiegazione"
    }}
  ],

  "frase_motivazionale": "Una frase breve di incoraggiamento personalizzata per l'operatore, basata su ciò che ha fatto bene.",

  "disclaimer": "Questo report è generato automaticamente da un sistema di intelligenza artificiale sulla base della trascrizione audio della chiamata. I giudizi espressi sono indicativi e a scopo formativo. La qualità dell'analisi dipende dalla fedeltà della trascrizione, che potrebbe contenere imprecisioni legate a rumori di fondo, sovrapposizione di voci o accenti. Utilizzare come strumento di supporto al coaching, non come valutazione definitiva."
}}

Scala di rating da 1 a 5 (valida per qualificazione E per ogni singola fase):
- 1 = INSUFFICIENTE: gravemente carente, molto lontano dagli standard richiesti
- 2 = DA MIGLIORARE: sotto le aspettative, lacune significative da correggere
- 3 = SUFFICIENTE: nella norma, eseguito ma con margini importanti di miglioramento
- 4 = BUONA: eseguita bene, in linea con le aspettative e i framework di riferimento
- 5 = ECCELLENTE: esecuzione esemplare, supera le aspettative
- null = NON APPLICABILE (fase non presente nella chiamata — usa null senza virgolette, non "N/A" né 0)

Label corrispondenti ai rating (usa SEMPRE la label corretta nel campo "label"):
- 1 → "INSUFFICIENTE"
- 2 → "DA MIGLIORARE"
- 3 → "SUFFICIENTE"
- 4 → "BUONA"
- 5 → "ECCELLENTE"

⚠️ QUALIFICAZIONE — REGOLA CRITICA:
Il rating della qualificazione NON segue la stessa scala delle fasi della telefonata.
Per la qualificazione il rating dipende dalla % di parametri correttamente verificati (vedi istruzione 1 sopra).
"fuori_parametro": true → rating SEMPRE 1, anche se la percentuale di parametri raccolti è alta.
Esempio spiegazione per fuori_parametro=true: "Spesa dichiarata 5.000€/anno — sotto la soglia minima di 12.000€/anno (tolleranza 10%: min accettabile 10.800€). Appuntamento NON IN TARGET."

Il rating delle FASI DELLA TELEFONATA segue invece la scala qualitativa 1-5 basata sull'esecuzione."""


# ── Analysis call ──────────────────────────────────────────────────────────────

async def analyze_call(
    transcript: str,
    campaign_info: dict,
    script: Optional[str] = None,
    qualification_params: Optional[str] = None,
    client_info: Optional[str] = None,
    operator_email: Optional[str] = None,
    prompt_sections: Optional[dict] = None,
    prompt_extra: Optional[str] = None,
    global_docs: Optional[list] = None,
) -> dict:
    """
    Send the transcript to Claude and return the structured report dict.
    Raises on API errors or JSON parse failure.

    global_docs: list of {"title": str, "content": str} from global_documents table.
    """
    prompt = build_analysis_prompt(
        transcript=transcript,
        campaign_info=campaign_info,
        script=script,
        qualification_params=qualification_params,
        client_info=client_info,
        operator_email=operator_email,
        prompt_sections=prompt_sections,
        prompt_extra=prompt_extra,
        global_docs=global_docs,
    )

    logger.info(
        "Calling Claude (model=%s) for campaign=%s operator=%s",
        settings.anthropic_model,
        campaign_info.get("cliente"),
        _extract_operator_name(operator_email),
    )

    client = get_client()
    message = client.messages.create(
        model=settings.anthropic_model,
        max_tokens=5000,
        messages=[{"role": "user", "content": prompt}],
    )

    response_text = message.content[0].text.strip()

    # Strip possible ```json … ``` fences
    if response_text.startswith("```"):
        parts = response_text.split("```")
        response_text = parts[1]
        if response_text.startswith("json"):
            response_text = response_text[4:]
        response_text = response_text.strip()

    try:
        report = json.loads(response_text)
    except json.JSONDecodeError as exc:
        logger.error("Claude returned invalid JSON: %s\nRaw: %s", exc, response_text[:500])
        raise ValueError(f"Claude returned invalid JSON: {exc}") from exc

    # Ensure required top-level keys with safe fallbacks
    report.setdefault("errore_tecnico", False)
    report.setdefault("ragione_sociale", "N/A")
    report.setdefault("data_appuntamento", None)
    report.setdefault("ora_appuntamento", None)
    report.setdefault("qualificazione", {
        "rating": 3, "label": "SUFFICIENTE", "fuori_parametro": False,
        "spiegazione": "", "parametri_verificati": [], "parametri_mancanti": [],
    })
    report.setdefault("analisi_telefonata", {
        "rating_totale": 2, "spiegazione_totale": "", "fasi": {},
    })
    report.setdefault("punti_di_forza", [])
    report.setdefault("aree_di_miglioramento", [])
    report.setdefault("frase_motivazionale", "")
    report.setdefault("disclaimer", _DEFAULT_DISCLAIMER)

    # Enforce canonical field order (JSON dicts preserve insertion order in Python 3.7+)
    ordered: dict = {}
    for key in _REPORT_FIELD_ORDER:
        if key in report:
            ordered[key] = report[key]
    # Append any unexpected extra keys at the end (shouldn't happen, but safe)
    for key in report:
        if key not in ordered:
            ordered[key] = report[key]

    return ordered

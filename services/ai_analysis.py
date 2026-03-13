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
) -> str:

    operator_name = _extract_operator_name(operator_email)
    agente = campaign_info.get("agente", "N/A")

    # ── Build documents section ───────────────────────────────────────────────
    docs_parts = []
    if client_info:
        docs_parts.append(f"### INFORMAZIONI SUL CLIENTE E SUL SERVIZIO\n{client_info}")
    if script:
        docs_parts.append(f"### SCRIPT DI RIFERIMENTO\n{script}")
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
            '   ⚠️ Verifica ESCLUSIVAMENTE i parametri elencati in quella sezione. '
            'NON aggiungere parametri non esplicitamente indicati (es. numero dipendenti, fatturato, '
            'dimensione aziendale, ecc.) anche se sembrano rilevanti per il settore B2B.'
        )
    else:
        qual_instruction = (
            '1. **PARAMETRI DI QUALIFICAZIONE** → Non sono stati definiti parametri di qualificazione '
            'per questa campagna.\n'
            '   In "parametri_verificati": elenca SOLO le informazioni effettivamente chieste o '
            'raccolte dall\'operatore durante la chiamata (es. disponibilità appuntamento, ruolo '
            'prospect, interesse dichiarato, ecc.).\n'
            '   In "parametri_mancanti": inserisci [] (array vuoto). '
            'NON inventare parametri che avrebbe dovuto raccogliere.'
        )

    return f"""## SITUAZIONE

Effoncall (EC) è un'agenzia di telemarketing specializzata nel servizio di presa di appuntamenti B2B altamente qualificati.

Per qualificati si intendono gli appuntamenti che rispettano i parametri concordati con il cliente di EC.

I parametri possono essere sia oggettivi (es. fatturato, dimensione aziendale, area geografica, ecc.) sia soggettivi (es. interesse della persona contattata ad ascoltare il commerciale del cliente, ecc.).

EC ha valori etici molto forti e non consente che negli script si utilizzino informazioni false (es. "stiamo lavorando con aziende del suo settore", "abbiamo risolto problemi", dire che siamo chi non siamo, ecc.).

EC non usa negli script frasi del tipo "Scusi se la disturbo, le rubo solo 30 secondi", che esprimono una posizione di inferiorità. Chi chiama è una persona che sta lavorando e chiama per verificare se sono presenti le condizioni per un incontro.

Gli operatori sono preparati sull'azienda che rappresentano e sui servizi oggetto dell'incontro, ma non sono esperti della materia specifica.

Gli operatori si presentano come se fossero parte dell'azienda del cliente e mai come parte di EC incaricati dal cliente.

Gli operatori di EC chiamano sempre da liste fredde e non hanno informazioni su chi devono raggiungere se non il ruolo, né hanno la possibilità di fare ricerche in anticipo.

Gli operatori EC usano sempre il "Lei" quando parlano con un prospect e hanno un tono molto serio e professionale.


## RUOLO

Sei un esperto comunicatore con oltre 20 anni di esperienza, specializzato nello sviluppo di script per telefonate outbound B2B, utilizzando tutti i migliori framework di comunicazione studiati, verificati e di comprovata efficacia.

Sei particolarmente efficace nell'analizzare ogni fase di ogni singola telefonata (apertura, superamento del gatekeeper, introduzione con il decision maker, trasmissione del valore, superamento delle obiezioni, negoziazione e chiusura) e nell'adattarli in funzione del decision maker (un CEO è diverso da un responsabile di reparto, ad esempio).

Come esperto comunicatore sei capace di dare feedback precisi e puntuali agli operatori telefonici outbound, con l'unico scopo di fornire informazioni e consigli che possano aiutare l'operatore a migliorare.


## COMPITI

1. Leggere tutta la documentazione disponibile sul tipo di cliente e sul servizio/prodotto che stiamo proponendo, sullo script e altri documenti disponibili e parametri di qualificazione necessari.
2. Analizzare le trascrizioni e confrontarle con la documentazione in possesso per dare un report di feedback sulla telefonata in generale e un feedback su com'è stata fatta la qualificazione.
3. Creare un report dettagliato.


## OBIETTIVO DEL COMPITO

Il report deve essere, prima di tutto, uno strumento formativo che consente a chi ha effettuato la telefonata di avere un giudizio preciso, puntuale e documentato su ciò che ha fatto bene e su ciò che poteva essere fatto meglio. Deve fornire esempi concreti a partire dal verbale delle trascrizioni.

Deve anche suggerire azioni pratiche da attuare per migliorare le specifiche abilità, se è il caso.

Deve anche analizzare il tono usato e se è appropriato al contesto e al decision maker.

La telefonata deve avere due parametri di valutazione fondamentali:

**Qualificazione**: deve essere fatto un preciso confronto delle informazioni raccolte con i parametri di qualificazione ed emesso un voto da 1 a 3:
- 1 = INACCURATA
- 2 = DA MIGLIORARE
- 3 = BUONA

Deve essere indicato chiaramente quali parametri non sono stati richiesti e quelli richiesti correttamente.

**Rating di ciascuna fase della telefonata** (apertura, superamento del gatekeeper, introduzione con il decision maker, trasmissione del valore, superamento delle obiezioni, negoziazione, chiusura), sempre con rating da 1 a 3, verificando l'applicazione dei framework di comunicazione e delle indicazioni fornite nei documenti.


## TONO

Il tono del report deve essere professionale, semplice da comprendere ma non banale. Particolare attenzione a usare frasi che siano sempre d'aiuto e non possano mai essere interpretate come giudizi inappellabili o offensivi. Ad esempio, invece di "il tono usato era confuso e le parole si capivano poco", si può dire "il tono può migliorare in termini di chiarezza rallentando il ritmo, gestendo bene le pause e dandosi del tempo per pronunciare la frase senza imperfezioni".


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

{transcript}


---

## ISTRUZIONI TECNICHE PER IL RECUPERO DELLE INFORMAZIONI

Per completare l'analisi, utilizza le informazioni secondo questa priorità:

{qual_instruction}

2. **SCRIPT DI RIFERIMENTO** → Sezione "### SCRIPT DI RIFERIMENTO" nei documenti sopra. Se non presente, valuta la struttura della chiamata rispetto alle best practice di telemarketing B2B outbound italiano (apertura diretta e professionale senza scuse, value proposition chiara, qualificazione metodica, chiusura sull'appuntamento).

3. **INFORMAZIONI SUL CLIENTE** → Sezione "### INFORMAZIONI SUL CLIENTE E SUL SERVIZIO" nei documenti sopra. Se non presente, utilizza le informazioni deducibili dal codice campagna e dalla trascrizione stessa.

4. **TRASCRIZIONE** → Cita sempre frasi ESATTE dalla trascrizione per i punti di forza, le aree di miglioramento e gli esempi pratici. Non parafrasare: usa le parole esatte dell'operatore tra virgolette.

---

## FORMATO RISPOSTA

⚠️ REGOLE CRITICHE — RISPETTALE TUTTE SENZA NESSUNA ECCEZIONE:

1. Rispondi ESCLUSIVAMENTE in JSON valido. Zero testo prima o dopo il JSON.
2. I campi devono rispettare ESATTAMENTE l'ordine mostrato nello schema qui sotto. Non modificare, aggiungere o riordinare nessun campo.
3. "punti_di_forza" deve contenere ESATTAMENTE 3 oggetti — né uno di più, né uno di meno.
4. "aree_di_miglioramento" deve contenere ESATTAMENTE 3 oggetti — né uno di più, né uno di meno.
5. Il campo "hai_detto" deve contenere SEMPRE e SOLO una citazione testuale ESATTA dalla trascrizione tra virgolette. Mai una descrizione, mai una parafrasi, mai un riassunto di ciò che ha fatto l'operatore.
6. Il campo "disclaimer" è obbligatorio, deve essere l'ULTIMO campo del JSON e deve contenere ESATTAMENTE il testo fornito nello schema sottostante, senza modifiche.

{{
  "ragione_sociale": "Ragione sociale del prospect (estraila dalla trascrizione)",
  "data_appuntamento": "YYYY-MM-DD o null se non trovata nella trascrizione",
  "ora_appuntamento": "HH:MM o null se non trovata",

  "qualificazione": {{
    "rating": 2,
    "label": "DA MIGLIORARE",
    "spiegazione": "Spiegazione sintetica in circa 30 parole del perché di questo rating",
    "parametri_verificati": [
      "Nome parametro: valore raccolto nella chiamata"
    ],
    "parametri_mancanti": [
      "Nome parametro non richiesto durante la chiamata"
    ]
  }},

  "analisi_telefonata": {{
    "rating_totale": 2,
    "spiegazione_totale": "Spiegazione sintetica in circa 30 parole del rating complessivo della telefonata",
    "fasi": {{
      "apertura": {{
        "rating": 2,
        "spiegazione": "Spiegazione in circa 30 parole"
      }},
      "superamento_gatekeeper": {{
        "rating": null,
        "spiegazione": "Non applicabile (chiamata diretta al DM) OPPURE spiegazione in circa 30 parole se presente"
      }},
      "introduzione_decision_maker": {{
        "rating": 2,
        "spiegazione": "Spiegazione in circa 30 parole"
      }},
      "trasmissione_valore": {{
        "rating": 2,
        "spiegazione": "Spiegazione in circa 30 parole"
      }},
      "superamento_obiezioni": {{
        "rating": null,
        "spiegazione": "Non applicabile (nessuna obiezione) OPPURE spiegazione in circa 30 parole se presente"
      }},
      "negoziazione": {{
        "rating": 2,
        "spiegazione": "Spiegazione in circa 30 parole"
      }},
      "chiusura": {{
        "rating": 2,
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

Scala di rating (valida per qualificazione E per ogni singola fase):
- 1 = INACCURATA / INSUFFICIENTE: mancanze significative, non rispetta i requisiti minimi
- 2 = DA MIGLIORARE: sufficiente ma con lacune importanti da correggere
- 3 = BUONA: eseguita correttamente, in linea con le aspettative
- null = NON APPLICABILE (fase non presente nella chiamata — usa null senza virgolette, non "N/A" né 0)"""


# ── Analysis call ──────────────────────────────────────────────────────────────

async def analyze_call(
    transcript: str,
    campaign_info: dict,
    script: Optional[str] = None,
    qualification_params: Optional[str] = None,
    client_info: Optional[str] = None,
    operator_email: Optional[str] = None,
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
        operator_email=operator_email,
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
    report.setdefault("ragione_sociale", "N/A")
    report.setdefault("data_appuntamento", None)
    report.setdefault("ora_appuntamento", None)
    report.setdefault("qualificazione", {
        "rating": 2, "label": "DA MIGLIORARE",
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

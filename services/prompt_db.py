"""
Prompt section management service.

Loads the 7 editable sections of the AI analysis prompt from the DB,
falling back to hardcoded defaults when not overridden.
Results are cached for 60 seconds to avoid redundant DB calls.
"""

import time
import logging
from typing import Optional

logger = logging.getLogger(__name__)

# ── Default section texts (extracted from build_analysis_prompt) ─────────────

_DEFAULT_SECTIONS: dict[str, str] = {
    "contesto": (
        "Effoncall (EC) è un'agenzia di telemarketing specializzata nel servizio di presa di appuntamenti B2B altamente qualificati.\n\n"
        "Per qualificati si intendono gli appuntamenti che rispettano i parametri concordati con il cliente di EC.\n\n"
        "I parametri possono essere sia oggettivi (es. fatturato, dimensione aziendale, area geografica, ecc.) sia soggettivi (es. interesse della persona contattata ad ascoltare il commerciale del cliente, ecc.).\n\n"
        "EC ha valori etici molto forti e non consente che negli script si utilizzino informazioni false (es. \"stiamo lavorando con aziende del suo settore\", \"abbiamo risolto problemi\", dire che siamo chi non siamo, ecc.).\n\n"
        "EC non usa negli script frasi del tipo \"Scusi se la disturbo, le rubo solo 30 secondi\", che esprimono una posizione di inferiorità. Chi chiama è una persona che sta lavorando e chiama per verificare se sono presenti le condizioni per un incontro.\n\n"
        "Gli operatori sono preparati sull'azienda che rappresentano e sui servizi oggetto dell'incontro, ma non sono esperti della materia specifica.\n\n"
        "Gli operatori si presentano come se fossero parte dell'azienda del cliente e mai come parte di EC incaricati dal cliente.\n\n"
        "Gli operatori di EC chiamano sempre da liste fredde e non hanno informazioni su chi devono raggiungere se non il ruolo, né hanno la possibilità di fare ricerche in anticipo.\n\n"
        "Gli operatori EC usano sempre il \"Lei\" quando parlano con un prospect e hanno un tono molto serio e professionale."
    ),
    "ruolo": (
        "Sei un esperto comunicatore con oltre 20 anni di esperienza, specializzato nello sviluppo di script per telefonate outbound B2B, utilizzando tutti i migliori framework di comunicazione studiati, verificati e di comprovata efficacia.\n\n"
        "Sei particolarmente efficace nell'analizzare ogni fase di ogni singola telefonata (apertura, superamento del gatekeeper, introduzione con il decision maker, trasmissione del valore, superamento delle obiezioni, negoziazione e chiusura) e nell'adattarli in funzione del decision maker (un CEO è diverso da un responsabile di reparto, ad esempio).\n\n"
        "Come esperto comunicatore sei capace di dare feedback precisi e puntuali agli operatori telefonici outbound, con l'unico scopo di fornire informazioni e consigli che possano aiutare l'operatore a migliorare."
    ),
    "compiti": (
        "1. Leggere tutta la documentazione disponibile sul tipo di cliente e sul servizio/prodotto che stiamo proponendo, sullo script e altri documenti disponibili e parametri di qualificazione necessari.\n"
        "2. Analizzare le trascrizioni e confrontarle con la documentazione in possesso per dare un report di feedback sulla telefonata in generale e un feedback su com'è stata fatta la qualificazione.\n"
        "3. Creare un report dettagliato."
    ),
    "obiettivo": (
        "Il report deve essere, prima di tutto, uno strumento formativo che consente a chi ha effettuato la telefonata di avere un giudizio preciso, puntuale e documentato su ciò che ha fatto bene e su ciò che poteva essere fatto meglio. Deve fornire esempi concreti a partire dal verbale delle trascrizioni.\n\n"
        "Deve anche suggerire azioni pratiche da attuare per migliorare le specifiche abilità, se è il caso.\n\n"
        "Deve anche analizzare il tono usato e se è appropriato al contesto e al decision maker.\n\n"
        "La telefonata deve avere due parametri di valutazione fondamentali:\n\n"
        "**Qualificazione**: deve essere fatto un preciso confronto delle informazioni raccolte con i parametri di qualificazione ed emesso un voto da 1 a 3:\n"
        "- 1 = INACCURATA\n"
        "- 2 = DA MIGLIORARE\n"
        "- 3 = BUONA\n\n"
        "Deve essere indicato chiaramente quali parametri non sono stati richiesti e quelli richiesti correttamente.\n\n"
        "**Rating di ciascuna fase della telefonata** (apertura, superamento del gatekeeper, introduzione con il decision maker, trasmissione del valore, superamento delle obiezioni, negoziazione, chiusura), sempre con rating da 1 a 3, verificando l'applicazione dei framework di comunicazione e delle indicazioni fornite nei documenti."
    ),
    "tono": (
        "Il tono del report deve essere professionale, semplice da comprendere ma non banale. Particolare attenzione a usare frasi che siano sempre d'aiuto e non possano mai essere interpretate come giudizi inappellabili o offensivi. Ad esempio, invece di \"il tono usato era confuso e le parole si capivano poco\", si può dire \"il tono può migliorare in termini di chiarezza rallentando il ritmo, gestendo bene le pause e dandosi del tempo per pronunciare la frase senza imperfezioni\"."
    ),
    "istruzioni_tecniche": (
        "2. **SCRIPT DI RIFERIMENTO** → Sezione \"### SCRIPT DI RIFERIMENTO\" nei documenti sopra. Se non presente, valuta la struttura della chiamata rispetto alle best practice di telemarketing B2B outbound italiano (apertura diretta e professionale senza scuse, value proposition chiara, qualificazione metodica, chiusura sull'appuntamento).\n\n"
        "3. **INFORMAZIONI SUL CLIENTE** → Sezione \"### INFORMAZIONI SUL CLIENTE E SUL SERVIZIO\" nei documenti sopra. Se non presente, utilizza le informazioni deducibili dal codice campagna e dalla trascrizione stessa.\n\n"
        "4. **TRASCRIZIONE** → Cita sempre frasi ESATTE dalla trascrizione per i punti di forza, le aree di miglioramento e gli esempi pratici. Non parafrasare: usa le parole esatte dell'operatore tra virgolette."
    ),
    "altre_istruzioni": "",
}

# ── Section metadata for the UI ───────────────────────────────────────────────

SECTION_METADATA = [
    {
        "key": "contesto",
        "title": "Contesto",
        "description": "Chi è Effoncall, valori etici, comportamento degli operatori.",
        "rows": 15,
    },
    {
        "key": "ruolo",
        "title": "Ruolo",
        "description": "Profilo e competenze dell'AI come coach/analista.",
        "rows": 8,
    },
    {
        "key": "compiti",
        "title": "Compiti",
        "description": "I compiti che l'AI deve svolgere per ogni analisi.",
        "rows": 6,
    },
    {
        "key": "obiettivo",
        "title": "Obiettivo",
        "description": "Cosa deve produrre il report e i parametri di valutazione.",
        "rows": 18,
    },
    {
        "key": "tono",
        "title": "Tono",
        "description": "Stile e tono del report generato.",
        "rows": 5,
    },
    {
        "key": "istruzioni_tecniche",
        "title": "Istruzioni tecniche",
        "description": (
            "Come usare script, info cliente e trascrizione (punti 2-4). "
            "Il punto 1 sulla qualificazione è generato automaticamente in base ai parametri della campagna."
        ),
        "rows": 8,
    },
    {
        "key": "altre_istruzioni",
        "title": "Altre istruzioni",
        "description": (
            "Sezione libera per istruzioni aggiuntive globali. Inizialmente vuota. "
            "Appare in fondo al prompt prima del formato risposta."
        ),
        "rows": 8,
    },
]

# ── In-memory cache ───────────────────────────────────────────────────────────

_CACHE: dict[str, str] = {}
_CACHE_TS: float = 0.0
_CACHE_TTL: float = 60.0


async def get_prompt_sections() -> dict[str, str]:
    """
    Load prompt sections from the DB, merging with defaults.
    DB values override defaults. Result is cached for 60 seconds.
    """
    global _CACHE, _CACHE_TS

    now = time.monotonic()
    if _CACHE and (now - _CACHE_TS) < _CACHE_TTL:
        return _CACHE

    try:
        from database import AsyncSessionLocal
        from models import PromptSection
        from sqlalchemy import select

        async with AsyncSessionLocal() as session:
            result = await session.execute(select(PromptSection))
            rows = result.scalars().all()

        db_sections: dict[str, str] = {row.section_key: row.content for row in rows}
    except Exception as exc:
        logger.warning("Could not load prompt sections from DB: %s", exc)
        db_sections = {}

    merged = {**_DEFAULT_SECTIONS, **db_sections}
    _CACHE = merged
    _CACHE_TS = now
    return merged


def clear_prompt_sections_cache() -> None:
    """Reset the in-memory cache so the next call re-reads from DB."""
    global _CACHE, _CACHE_TS
    _CACHE = {}
    _CACHE_TS = 0.0

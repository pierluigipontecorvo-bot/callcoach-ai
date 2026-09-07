# CallCoach AI — Contesto Progetto

> **Prima di lavorare, leggi `CONTESTO_CALLCOACH.md`** (nella cartella del progetto).
> È il dossier completo e autonomo su CallCoach e l'analisi delle telefonate:
> come si recuperano le registrazioni dalla replica MariaDB Sidial, la pipeline,
> le regole di dominio, i difetti verificati. Aggiornato e verificato in
> produzione il 07/09/2026. Questo file qui sotto resta il promemoria breve.

## Flusso Business (NON DIMENTICARE MAI)

1. L'**operatore** (es. 91-ROSAMARIA) chiama il cliente per vendere un servizio
2. La **chiamata viene registrata su Sidial** (CRM con registrazioni VoIP)
3. Se il cliente accetta, l'operatore **crea un appuntamento su Acuity Scheduling**
   con etichetta "PRESO" (o "CONFERMATO" se confermato successivamente)
4. La data dell'appuntamento e la data della CHIAMATA sono DIVERSE:
   - La chiamata avviene PRIMA (es. 23/03)
   - L'appuntamento e in futuro (es. 26/03)
   - **Le registrazioni Sidial esistono GIA quando l'appuntamento viene creato**
5. Il webhook Acuity scatta → la pipeline di analisi parte
6. La pipeline cerca le registrazioni su Sidial per NUMERO DI TELEFONO del cliente
7. Scarica le registrazioni, le trascrive, e le analizza con Claude AI
8. Produce un report di coaching per l'operatore (qualita della chiamata)
9. Invia il report via email

## Architettura

- **Backend**: FastAPI + Jinja2 + Starlette, deploy su Railway
- **Database**: PostgreSQL su Supabase, pooler sulla porta 5432 (session mode —
  verificato 07/09/2026; la nota precedente diceva transaction mode ed era sbagliata)
  - CRITICO: asyncpg deve usare `statement_cache_size=0`
  - CRITICO: `pool_pre_ping=False`
  - Nessun sistema di migrazioni: lo schema si costruisce con DDL idempotenti a ogni avvio.
    `schema.sql` è fermo alla prima versione e NON descrive il DB reale.
- **Acuity Scheduling**: 2 account (account 1 e 2), webhook su etichetta "PRESO"
- **Sidial CRM**: API REST per cercare lead e scaricare registrazioni
  - Ricerca per: telefono (phone1-4), ragione sociale, P.IVA
  - Le registrazioni possono essere "in conversione" (wav→mp3) — retry con attesa
- **Trascrizione**: OpenAI Whisper (default) o AssemblyAI (con speaker diarization)
  - AssemblyAI: NON usare `speech_model: "best"` (slam-1 e solo inglese)
- **Analisi AI**: Claude (Anthropic API) con prompt strutturato per campagna
- **Email**: Brevo HTTP API (Railway blocca SMTP)

## Pipeline 14 Step

```
1  webhook      — ricevuto e validato
2  firma        — HMAC signature verificata
3  acuity       — appuntamento recuperato da Acuity
4  form         — form fields estratti (P.IVA, Ragione Sociale)
5  etichetta    — etichetta Acuity estratta (PRESO, CONFERMATO, etc.)
6  data         — data appuntamento parsata
7  campagna     — campagna identificata (prefix matching)
8  operatore    — operatore identificato (da email/form → tabella operators)
9  sidial       — lead trovati su Sidial per telefono
10 download     — registrazioni scaricate
11 trascrizione — audio trascritto (Whisper/AssemblyAI)
12 analisi      — analisi AI con Claude
13 salvataggio  — risultato salvato nel DB
14 email        — report inviato via email
```

## Campagne (Prefix Matching)

Codice campagna tipo: `INTER-CER-2908-LUCA-(MI)`
- Matching dal piu specifico al meno: INTER-CER-2908-LUCA-(MI) → INTER-CER-2908 → INTER-CER → INTER
- Una riga "INTER" copre tutte le campagne INTER di default
- Righe piu specifiche sovrascrivono il default

## Etichette Acuity (Colori)

```
PRESO:         #f9a825 (amber)     — Acuity color: "yellow"
CONFERMATO:    #1e88e5 (blue)      — Acuity color: "sky"
APP.TO OK:     #388e3c (green)     — Acuity color: "green"
APP.TO KO:     #c62828 (red)       — Acuity color: "red"
ANNULLATO:     #f06292 (pink)      — Acuity color: "pink"
DA RICHIAMARE: #7c3aed (purple)    — Acuity color: "purple"
NO SHOW:       #6b7280 (gray)      — Acuity color: "gray"
```

Definiti in `routers/admin_ui.py` → `LABEL_COLORS` e `_ACUITY_CSS_COLOR_MAP`.
Passati come variabili al template per uniformita su tutte le pagine.

## Qualificazione Analisi

```
5 = eccellente     (verde)
4 = buona          (blu)
3 = sufficiente    (azzurro)
2 = da_migliorare  (arancio)
1 = inaccurata     (rosso)
non_in_target      (grigio) — fuori parametro
errore_tecnico     (nero)   — trascrizione troppo breve/fallita
```

## Note Tecniche Importanti

- `update_step()` e `init_steps()` NON rilanciano MAI eccezioni (catch interno)
- Tutti i DB save intermedi nella pipeline sono non-fatali (try/except)
- `run_analysis_pipeline()` ha un global try/except wrapper
- Il periodo default nella pagina principale e "Mese" (non "Oggi")
- Le analisi duplicate per lo stesso appointment vengono gestite mostrando solo la piu recente (ORDER BY id DESC)
- Sidial retry registrazioni in conversione: 6 tentativi ogni 10 minuti (valori scritti
  nel codice; le impostazioni `sidial_retry_count`/`sidial_retry_wait_seconds` sono lette
  e mai usate). ATTENZIONE: al 07/09/2026 `retry_conversion_analysis` è ROTTA — usa nomi
  non importati nel modulo e solleva NameError a ogni giro, quindi le analisi in
  `pending_conversion` non ripartono mai. Dettaglio in CONTESTO_CALLCOACH.md §5.1.
- Il webhook analizza SOLO appuntamenti creati oggi e SOLO con etichetta PRESO
- Registrazioni: da oggi si trovano sulla REPLICA MariaDB Sidial (non più cercando via API);
  il download del file resta su `a=getLeadRec`. Procedura completa in CONTESTO_CALLCOACH.md §3.

## Accesso admin: dov'è la password e come si recupera

L'area admin (`/admin/ui/login`) ha **una sola password condivisa**, senza
utenti né email: sta nella variabile d'ambiente `ADMIN_PASSWORD` del servizio
Railway `CALLCOACH` (progetto EFFONCALL-CALLCOACH) e viene confrontata in
chiaro in `utils/auth.py` → `verify_admin_password`. Il login rilascia un JWT
di 24 ore in un cookie HttpOnly `callcoach_token`, firmato con `SECRET_KEY`.

Non esiste (per scelta) un «password dimenticata» via email: **la password si
legge o si cambia da Railway**.

```bash
# leggerla
railway variables --service CALLCOACH | grep ADMIN_PASSWORD

# cambiarla (il servizio si riavvia da solo; i cookie già emessi restano
# validi fino a scadenza, per invalidarli subito cambia anche SECRET_KEY)
railway variables --service CALLCOACH --set 'ADMIN_PASSWORD=<nuova>'
```

In alternativa: Railway → progetto EFFONCALL-CALLCOACH → servizio CALLCOACH →
scheda *Variables*. La password **non va mai scritta nel repo**.

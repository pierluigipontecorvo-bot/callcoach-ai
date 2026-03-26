# CONTESTO CALLCOACH — Regole, Logiche e Decisioni di Progetto

> Documento di riferimento per lo sviluppo continuo di CallCoach AI.
> Va letto all'inizio di ogni sessione per garantire continuità e non rimettere
> in discussione decisioni già consolidate.
>
> Aggiornato: 2026-03-26

---

## A. AUTOMAZIONI
*Cosa succede quando si verifica un evento*

### A.1 Evento: appuntamento creato/aggiornato su Acuity con etichetta "PRESO"
Scatta il webhook Acuity → parte la pipeline di analisi in background (14 step):

```
1  Webhook ricevuto e validato
2  Firma HMAC verificata
3  Appuntamento recuperato da Acuity API
4  Form fields estratti (P.IVA, Ragione Sociale, telefono)
5  Etichetta Acuity estratta (PRESO, CONFERMATO, ecc.)
6  Data appuntamento parsata
7  Campagna identificata (prefix matching sul codice)
8  Operatore identificato (da email/form → tabella operators DB)
9  Lead trovati su Sidial per numero di telefono del cliente
10 registrazioni scaricate da Sidial
11 Audio trascritto (OpenAI Whisper o AssemblyAI)
12 Analisi AI eseguita con Claude
13 Risultato salvato nel database
14 Report inviato via email all'operatore
```

**Regola critica sulle date:**
- La **chiamata** avviene PRIMA (es. 23/03)
- L'**appuntamento** è nel futuro (es. 26/03)
- Le registrazioni Sidial **esistono già** quando l'appuntamento viene creato
- NON cercare registrazioni in base alla data dell'appuntamento

### A.2 Evento: analisi già esistente per lo stesso appuntamento
Se arriva un secondo webhook per lo stesso `appointment_id`, viene creata una
nuova analisi. Nella UI viene mostrata **solo la più recente** (ORDER BY id DESC).

### A.3 Evento: registrazioni in conversione (wav→mp3)
Solo per appuntamenti **di oggi**: retry automatico 5 volte × 3 minuti di attesa.
Per appuntamenti passati: nessun retry, le registrazioni sono già convertite.

### A.4 Evento: motore di trascrizione che fallisce
Fallback automatico: se AssemblyAI fallisce → tenta con OpenAI Whisper (e viceversa).
Se il fallback funziona, diventa il motore attivo per quella sessione.

### A.5 Evento: analisi in elaborazione nella UI
- Pallino verde pulsante animato visibile sulla riga
- Timer mm:ss aggiornato ogni secondo
- Polling automatico ogni 2 secondi per aggiornare lo stato

---

## B. LOGICHE
*I diversi tipi di logica applicati nei vari momenti*

### B.1 Logica di ricerca lead su Sidial

**Regola fondamentale:** il lead con le registrazioni può essere salvato in una
campagna Sidial diversa da quella dell'appuntamento Acuity — questo è normale.

**Ordine di ricerca (senza short-circuit):**
1. Telefono: TUTTE le varianti × TUTTI e 4 i campi phone
2. P.IVA: 7 nomi di campo possibili
3. Ragione Sociale: 7 nomi di campo, operatore LIKE

**Varianti telefono:** `023655651` → `023655651`, `39023655651`, `0039023655651`

**NON fare short-circuit:** anche se la prima variante trova dei lead, continuare
a cercare con tutte le altre. Il lead corretto (con registrazioni) potrebbe essere
salvato con una variante diversa (es. in phone2 invece che phone1).

**Raccogliere tutti i lead da tutte le fonti, poi selezionare quelli con registrazioni.**

### B.2 Logica di prefix matching delle campagne
```
INTER-CER-2908-LUCA-(MI) → INTER-CER-2908 → INTER-CER → INTER
```
Dal più specifico al meno specifico. La riga più specifica nel DB vince.
Una riga `INTER` copre tutte le sotto-campagne INTER di default.

### B.3 Logica di qualificazione prospect (AI)

Esistono tre tipi di parametri — l'AI deve trattarli diversamente:

**OBBLIGATORI** — tutti devono essere soddisfatti (logica AND)
Esempio: tipo incontro, sede, vettori, esclusioni assolute.

**FLESSIBILI** — valori numerici con tolleranza ±15-20%
Esempio: "circa €1.000/mese" — non escludere per differenze marginali.
Valori vicini alla soglia → segnalare come "borderline", non escludere.

**ALTERNATIVI (OPPURE)** — basta soddisfarne uno (logica OR)
Esempio: spesa nazionale > €1.000 OPPURE spesa internazionale > €250.
Il prospect è IN TARGET se soddisfa almeno uno dei criteri alternativi.

### B.4 Logica di valutazione operatore (AI) — separata dalla qualifica

**REGOLA FONDAMENTALE: parametro non chiesto ≠ parametro non soddisfatto.**
Sono due cose distinte che l'AI non deve mai confondere:

| Situazione | Significato corretto |
|---|---|
| Parametro non chiesto | Lacuna dell'operatore → segnalare nel coaching |
| Parametro esplicitamente negativo | Possibile esclusione o mancata qualifica |

L'operatore DEVE raccogliere TUTTI i parametri, anche se il prospect sembra
già qualificato da un solo criterio alternativo. Ogni parametro mancante è
una lacuna da segnalare nel coaching, indipendentemente dall'esito della qualifica.

**L'AI non deve mai penalizzare la qualifica del prospect per lacune dell'operatore.**

### B.5 Logica delle due dimensioni di analisi (NON confonderle)

| Dimensione | Logica | Cosa valuta |
|---|---|---|
| **Qualifica prospect** | OR per alternativi, AND per obbligatori | IN TARGET o NO |
| **Qualità operatore** | Deve aver raccolto tutto + seguito script | Punteggio coaching |

### B.6 Logica "ieri lavorativo" — regola globale su tutto il sito
"Ieri" è sempre l'ultimo giorno lavorativo, non il giorno precedente di calendario:
- **Lunedì** → ieri = venerdì (−3 giorni)
- **Domenica** → ieri = venerdì (−2 giorni)
- **Tutti gli altri giorni** → ieri = ieri (−1 giorno)
Questa regola si applica a TUTTE le pagine, filtri e calcoli del sito.

### B.7 Logica selezione periodo nella UI
L'ultima selezione viene salvata in `localStorage` (chiave: `callcoach_main_period`).
Al ricaricamento o rientro nella pagina, viene ripristinata automaticamente.
Default se nessuna selezione salvata: **"Mese"**.

### B.8 Logica fallback trascrizione
```
Motore primario (campaign override → global setting)
    ↓ se fallisce
Motore alternativo (openai→assemblyai o assemblyai→openai)
    ↓ se funziona
Diventa il nuovo motore attivo per la sessione
```

---

## C. CALCOLI
*Valori derivati, soglie e trasformazioni*

### C.1 Normalizzazione numero di telefono
```
Input:  +39 023 655 651  o  0039023655651  o  39023655651
Output: 023655651  (rimuove prefisso paese)
Varianti generate: 023655651 | 39023655651 | 0039023655651
```

### C.2 Calcolo "ieri lavorativo"
```python
weekday = today.weekday()  # 0=lun, 6=dom
if weekday == 0:   ieri = today - 3 giorni  # lunedì → venerdì
elif weekday == 6: ieri = today - 2 giorni  # domenica → venerdì
else:              ieri = today - 1 giorno
```

### C.3 Calcolo durata totale parlato
Somma dei `callLength` (secondi) di tutte le registrazioni scaricate.
Mostrato in UI come `Xm YYs`.

### C.4 Selezione registrazioni da scaricare
- Ordinare per `callLength` DESC (più lunga prima)
- Scaricare massimo 10 registrazioni per appuntamento
- NON limitare a 1: se la linea cade, ci sono più segmenti della stessa chiamata

### C.5 Livelli di qualificazione analisi
```
5 = eccellente     → badge verde scuro
4 = buona          → badge verde
3 = sufficiente    → badge azzurro
2 = da_migliorare  → badge arancio
1 = inaccurata     → badge rosso
non_in_target      → badge grigio
errore_tecnico     → badge nero
```

### C.6 Timeout e deadline
```
Timeout singola chiamata HTTP Sidial:  12 secondi
Deadline globale Sidial:              180 secondi
Timeout outer webhook:                200 secondi  (> deadline Sidial)
Timeout progress callback:              3 secondi
```
Regola: ogni timeout esterno deve essere > del timeout/deadline interno.

---

## D. DATABASE
*Struttura, regole e vincoli*

### D.1 Infrastruttura
- **PostgreSQL** su Supabase con PgBouncer/Supavisor in Transaction Mode
- **Parametri obbligatori asyncpg:**
  - `statement_cache_size=0` — richiesto da PgBouncer
  - `pool_pre_ping=False` — evita problemi con PgBouncer
  - `command_timeout=15`

### D.2 Tabelle principali
- `analyses` — risultati analisi pipeline (una riga per elaborazione)
- `campaigns` — configurazioni campagne con prompt AI
- `global_documents` — documenti iniettati in tutti i prompt
- `operators` — mapping email/nome operatori
- `settings` — configurazioni globali chiave-valore

### D.3 Regole sui salvataggi in pipeline
- I salvataggi intermedi durante la pipeline sono **non-fatali** (try/except)
- `update_step()` e `init_steps()` non rilanciano mai eccezioni
- Solo il salvataggio finale (step 13) è critico

### D.4 Gestione analisi duplicate
Per lo stesso `appointment_id` possono esistere più analisi nel DB.
Nella UI mostrare **solo la più recente** (ORDER BY id DESC).

---

## E. OUTPUT — FRONTEND
*Come deve presentarsi l'interfaccia*

### E.1 Brand e identità visiva

**Logo:** `logo_dark.png` — versione chiara, usata su sfondo navy
**Favicon:** `favicon.png`
**Nome applicazione in navbar:** "CallCoach Admin"
**Azienda:** Effoncall

### E.2 Palette colori brand (CSS variables)
```css
--ec-navy:  #001126   /* sfondo navbar, sidebar, pulsanti primari */
--ec-slate: #708090   /* testo secondario, bordi, icone */
--ec-grey:  #d3d2d2   /* bordi leggeri, hover secondari */
--ec-white: #ffffff   /* card, contenuto */
--ec-bg:    #f4f5f7   /* sfondo pagina */
```

**Colore testo corpo:** `#1a1a2e`
**Colore card header:** `#f8f9fc` (sfondo), `#e2e4e9` (bordo)
**Colore link hover tabella:** `#f0f3f8`

### E.3 Tipografia
```
Font body:      Poppins (300, 400, 500, 600, 700) — testo corrente
Font headings:  Space Grotesk (400, 500, 600, 700) — titoli, navbar, tabelle, sidebar
Font size base: 14px
```

Elementi che usano **Space Grotesk:**
h1–h6, `.navbar-brand`, `.fw-bold`, `.fw-semibold`, `th`, `.card-header`,
sidebar links, form labels, badge headers

### E.4 Layout struttura
```
Navbar (56px, navy) — logo sx, "CallCoach Admin" + Esci dx
    ↓
Sidebar (col-md-2, navy, min-height 100vh)  |  Main content (col-md-10, padding 1.5rem)
```

**Sidebar — voci di menu:**
- Gestione: Campagne, Documenti Globali, Prompt AI, ↳ Anteprima, Appuntamenti, Analisi, Archivio
- Configurazione: Impostazioni

**Barra di caricamento navigazione:** linea sottile `#42a5f5` (3px) in cima alla pagina,
animata al click su link sidebar.

### E.5 Pulsanti
```
Primario (azione principale):
  background: #001126 (navy), testo bianco
  hover: #002244
  classe: .btn-dark o .btn-primary

Secondario (azione alternativa):
  outline grigio, testo slate
  hover: sfondo grigio chiaro, testo navy
  classe: .btn-outline-secondary

Piccolo (filtri, azioni inline):
  classe aggiuntiva: .btn-sm
  font-size: 12px
```

**Pulsanti periodo (filtro appuntamenti):**
```css
default: border #ccc, background white, colore #333
hover:   background #f0f3f8, colore navy
active:  background #001126 (navy), colore white
```

### E.6 Card
```
border:        1px solid #e2e4e9
border-radius: 10px
background:    white
header bg:     #f8f9fc
header font:   Space Grotesk, 13px, weight 600
```

### E.7 Tabelle
```
Header: Space Grotesk, 11px, uppercase, letter-spacing .6px, colore slate
        sfondo #f8f9fc, border-bottom 2px #e2e4e9
Righe:  vertical-align middle
Hover:  #f0f3f8
```

### E.8 Form
```
border-color:  #dde1e9
font-size:     14px
focus border:  slate (#708090)
focus shadow:  rgba(112,128,144,.2)
label font:    Space Grotesk, 13px
helper text:   11.5px, slate
```

### E.9 Colori etichette Acuity
```
PRESO:         #f9a825   amber   (Acuity: "yellow")
CONFERMATO:    #1e88e5   blue    (Acuity: "sky")
APP.TO OK:     #388e3c   green   (Acuity: "green")
APP.TO KO:     #c62828   red     (Acuity: "red")
ANNULLATO:     #f06292   pink    (Acuity: "pink")
DA RICHIAMARE: #7c3aed   purple  (Acuity: "purple")
NO SHOW:       #6b7280   gray    (Acuity: "gray")
```
**Fonte unica:** `routers/admin_ui.py` → `LABEL_COLORS` e `_ACUITY_CSS_COLOR_MAP`.
Aggiornare solo lì — si propaga automaticamente a tutte le pagine.

### E.10 Badge qualificazione analisi
```
eccellente:    sfondo #1565c0  (blu scuro)    ⭐ Eccellente
buona:         sfondo #27ae60  (verde)        ✅ Buona
sufficiente:   sfondo #2e7d32  (verde scuro)  ✅ Sufficiente
da_migliorare: sfondo #e67e22  (arancio)      ⚠ Da migliorare
inaccurata:    sfondo #c0392b  (rosso)        ❌ Inaccurata
non_in_target: sfondo #c0392b  (rosso)        ⛔ Non in target
errore_tecnico: sfondo grigio scuro           ⚙ Errore tecnico
```

### E.11 Indicatori stato pipeline in tempo reale
```
In corso:  pallino verde pulsante (animazione pulse-glow) + timer mm:ss
OK:        ✅ verde
Warning:   ⚠️ arancio
Stop/Error: 🔴 rosso
```

### E.12 Barra di progresso navigazione
Linea sottile `#42a5f5` in posizione `fixed` a top:0, height 3px.
Animata al click sui link sidebar (0→65%), completa al page load (→100%), poi sparisce.

### E.13 Misc elementi UI
```css
badge-prefix:  bg #eef0f5, colore #495057, border-radius 4px, padding 2px 6px
code inline:   colore #405080, bg #eef0f8, border-radius 4px, font-size 12px
alert flash:   border-radius 8px, font-size 13px
sidebar label: Space Grotesk, 10px, uppercase, letter-spacing 1.2px, colore slate
```

---

## F. ARCHITETTURA TECNICA

### F.1 Stack
```
Backend:     FastAPI + Jinja2 + Starlette
Deploy:      Railway
Database:    PostgreSQL su Supabase (PgBouncer/Supavisor transaction mode)
Scheduling:  Acuity Scheduling (2 account)
CRM:         Sidial (API REST)
Trascrizione: OpenAI Whisper (default) o AssemblyAI (speaker diarization)
AI:          Claude (Anthropic API) — modello configurabile
Email:       Brevo HTTP API (Railway blocca SMTP)
```

### F.2 URL produzione
```
App:      https://web-production-181160.up.railway.app
Admin:    https://web-production-181160.up.railway.app/admin/ui/login
Health:   https://web-production-181160.up.railway.app/health
```
⚠️ NON usare `callcoach-ai-production.up.railway.app` — è un'altra applicazione.

### F.3 Versioning pipeline
La versione è visibile nello step 1 del webhook: `v2024-03-24g`.
Formato: data + lettera progressiva. Utile per verificare i deploy.

### F.4 Trascrizione — parametro AssemblyAI corretto
```python
# SBAGLIATO (deprecato):
"speech_model": "universal-2"
# CORRETTO:
"speech_models": ["universal-2"]
```

---

## G. CONFIGURAZIONE CAMPAGNE

### G.1 Campi configurabili per campagna
- **Script di vendita** — l'AI confronta la chiamata con lo script
- **Parametri di qualificazione** — criteri OBBLIGATORI / FLESSIBILI / ALTERNATIVI
- **Info cliente / Contesto campagna** — aiuta l'AI a interpretare il contesto
- **Istruzioni specifiche per l'AI** — override locale al prompt globale
- **Motore trascrizione** — override del default globale
- **Destinatari email** — `inoltro@effoncall.com` sempre incluso automaticamente

### G.2 Struttura standard "Parametri di qualificazione"
```
=== PARAMETRI OBBLIGATORI (TUTTI devono essere soddisfatti) ===
[P1] Nome: descrizione + regola di valutazione

=== ESCLUSIONI ASSOLUTE ===
[E1] Nome: condizione + eventuali eccezioni

=== CRITERI ALTERNATIVI — basta soddisfarne UNO ===
[B1] Criterio A: valore soglia
[B2] Criterio B: valore soglia
LOGICA: IN TARGET se soddisfa B1 OPPURE B2 OPPURE B3.

=== PARAMETRI CHE L'OPERATORE DEVE RACCOGLIERE (tutti) ===
1. Lista completa numerata
```

### G.3 Documenti Globali AI
Accessibili da `/admin/ui/global`. Vengono iniettati in OGNI analisi
indipendentemente dalla campagna. Contengono le regole meta-logica
(OR/AND/FLESSIBILE, parametro non chiesto ≠ non soddisfatto, ecc.).

---

## H. DECISIONI CONSOLIDATE — NON RIMETTERE IN DISCUSSIONE

1. **NON limitare a 1 registrazione** — se la linea cade, ci sono più segmenti
2. **NON fare short-circuit** sulla ricerca telefono — cercare sempre tutte le varianti
3. **NON usare `speech_model`** (stringa) con AssemblyAI — usare `speech_models` (lista)
4. **NON usare** `callcoach-ai-production.up.railway.app` — è un'altra app
5. **NON mettere `pool_pre_ping=True`** — rompe PgBouncer
6. **NON mettere `statement_cache_size` ≠ 0** — rompe PgBouncer
7. **NON confondere** "parametro non chiesto" con "parametro non soddisfatto"
8. **NON penalizzare la qualifica del prospect** per lacune dell'operatore
9. **NON escludere per ADR** se il prospect spedisce anche merce non-ADR
10. **Il periodo default** nella pagina principale è "Mese", non "Oggi"

---

## I. LAVORI IN CORSO / TODO

- [ ] Creare Documento Globale AI con regole logica OR/AND nella UI (`/admin/ui/global`)
- [ ] Ristrutturare "Parametri di qualificazione" di INTER con le categorie standard
- [ ] Meccanismo feedback/correzione analisi — pulsante "analisi errata" nella UI
- [ ] Uniformare colori etichette su tutte le pagine
- [ ] Pulsanti di reinvio email per analisi completate
- [ ] Risolvere problema crediti Anthropic (dopo ricarica $40 ancora errore billing)

---

*Aggiornare questo documento ogni volta che viene presa una decisione significativa,
risolto un problema ricorrente, o aggiunta una nuova funzionalità rilevante.*

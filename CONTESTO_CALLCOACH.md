# CONTESTO CALLCOACH — analisi delle telefonate

> Documento autonomo: contiene tutto quello che serve per lavorare su CallCoach
> in una chat nuova, **senza avere il repo sott'occhio**. Riguarda solo CallCoach
> e l'analisi delle telefonate; il gestionale, l'Academy e la fatturazione hanno
> documenti propri.
>
> Aggiornato: **7 settembre 2026**. Tutti i numeri e le procedure qui dentro sono
> stati verificati in produzione quel giorno, non ricostruiti a memoria.
> Dove una cosa non è stata verificata, è scritto.

---

## 1. A che cosa serve

L'operatore chiama un'azienda per venderle un servizio. Se l'azienda accetta, fissa
un appuntamento per un consulente. CallCoach ascolta quella telefonata e produce
due giudizi distinti: **se il prospect era davvero in target** e **come ha lavorato
l'operatore**. Il risultato arriva per email all'operatore ed è consultabile
nell'area admin.

Il giro completo:

1. L'operatore chiama dal CRM **Sidial**, che registra la conversazione.
2. Se il cliente accetta, l'operatore crea l'appuntamento su **Acuity** con
   etichetta `PRESO`.
3. Il webhook di Acuity sveglia CallCoach.
4. CallCoach cerca su Sidial le registrazioni **di quel numero di telefono**.
5. Le scarica, le trascrive, le fa analizzare a Claude.
6. Salva il risultato e manda il report all'operatore.

**Regola sulle date, la più sbagliata di tutte:** la telefonata è avvenuta
**prima**, l'appuntamento è nel **futuro**. Il 4 settembre si prende un
appuntamento per il 9. Le registrazioni esistono già quando il webhook arriva.
Non cercare mai registrazioni alla data dell'appuntamento.

---

## 2. Stato di fatto, oggi

Numeri letti dal database di produzione il 7 settembre 2026.

| Dato | Valore |
|---|---|
| Analisi in archivio | 406 |
| Completate | 326 |
| In errore | 50 |
| Ferme in attesa di conversione | 30 |
| Ultima analisi | 4 settembre 2026 |
| Campagne configurate | 8 |

Una analisi su cinque non arriva in fondo. È il motivo per cui il programma va
riscritto, e quasi tutte quelle 80 si fermano nel punto 4 del giro: trovare e
scaricare le registrazioni. Il capitolo 3 esiste per questo.

Volume di riferimento: circa **580-610 registrazioni al giorno** in tutta
l'azienda, di cui ~70% sopra i 30 secondi. Durata media 67-75 secondi, la più
lunga misurata 798 secondi.

---

## 3. ⭐ Come si recuperano le registrazioni

Questo è il capitolo che conta. La procedura **è cambiata**: prima si passava
solo dall'API di Sidial, adesso c'è una **replica del database** che rende la
ricerca certa e immediata.

### 3.1 Come si faceva fino a ieri, e perché non regge

Tutto via HTTP sull'API `https://effoncall.sidial.cloud/api.php`, con
`apiToken` nella query:

| Azione | Chiamata | Cosa torna |
|---|---|---|
| Cerca il lead | `a=searchLeads` con filtro su `phone1..phone4` | elenco lead |
| Registrazioni del lead | `a=searchRecs` su tabella `leadsRecs`, campo `lead` | elenco registrazioni |
| Scarica il file | `a=getLeadRec&id=<recId>` | i byte dell'MP3 |

Il numero viene normalizzato togliendo il prefisso internazionale e poi cercato
in **tre varianti** (`023655651`, `39023655651`, `0039023655651`) per **quattro
campi** telefono, senza fermarsi al primo esito. Se non basta, si cerca anche per
partita IVA su sette possibili nomi di campo e per ragione sociale su altri sette
con `LIKE`. In tutto **fino a 26 chiamate HTTP in fila solo per trovare il
lead**, più una per ogni lead trovato, più una per ogni file da scaricare.

> Nel codice sembra esserci una quarta variante col numero grezzo, ma il ramo che
> la aggiunge non può mai eseguire: le varianti sono sempre esattamente tre.

Misurato il 7 settembre: **una sola** `searchLeads` impiega ~1,05 secondi. Una
ricerca completa sta quindi fra i 15 e i 20 secondi quando tutto va bene. Il
codice si difende con tre livelli di timeout in cascata (12 secondi per chiamata,
90 di scadenza interna, 120 sul totale), che esistono solo perché l'API a volte
si blocca del tutto.

I limiti veri, in ordine di gravità:

- **Incerto.** Se il numero è scritto in una forma non prevista, il lead non si
  trova e l'analisi muore. Sulla base ci sono 2.395 numeri con `+39` e 221 con
  caratteri non numerici: la normalizzazione va indovinata ogni volta.
- **Lento.** 15-20 secondi buoni, dentro un webhook.
- **Ambiguo.** Il campo `converted` restituito dall'API può arrivare nullo o in
  forme diverse, quindi il codice deve tirare a indovinare se il file è pronto.

### 3.2 Come si fa adesso: la replica

Esiste una **replica MariaDB in sola lettura** del database Sidial. Contiene le
stesse tabelle che l'API espone, ma interrogabili con SQL.

```
host      effoncallrepl.sidial.cloud
porta     3306
utente    replclientuser
password  variabile SIDIAL_REPL_PASSWORD (Railway, servizio gestionale)
database  sidial
versione  MariaDB 10.5.29
```

**La replica è chiusa per indirizzo IP.** Non è una password sbagliata: MariaDB
rifiuta proprio l'host. Verificato il 7 settembre:

| Servizio | IP in uscita | Ammesso |
|---|---|---|
| Gestionale (Railway) | `152.55.184.241` | sì |
| **CallCoach (Railway)** | **`208.77.244.155`** | **no** |

Provando a connettersi da CallCoach, il server risponde testualmente:
`Host '208.77.244.155' is not allowed to connect to this MariaDB server`.

**Quindi, prima di scrivere una riga di codice che usa la replica, serve una di
queste due cose:**

1. **Far aggiungere `208.77.244.155` alla whitelist di Sidial** (è il fornitore
   che concede il permesso a livello di utente MariaDB). È la strada giusta:
   dopo, CallCoach parla con la replica direttamente.
2. Oppure passare per il gestionale, che è già ammesso, esponendo un piccolo
   endpoint interno che gira la query. Funziona subito ma aggiunge un salto e
   lega CallCoach al gestionale.

Se l'IP di uscita di CallCoach cambia (succede se si ricrea il servizio), si
rilegge così, e va rifatta la whitelist:

```bash
railway ssh --service CALLCOACH "python -c \"import urllib.request;print(urllib.request.urlopen('https://api.ipify.org').read().decode())\""
```

### 3.3 Le due tabelle che servono

**`leads`** — le anagrafiche. Campi utili: `id`, `campaign`, `name`,
`phone1`, `phone2`, `phone3`, `phone4`, `createdWhen`.
**Tutti e quattro i campi telefono hanno un indice**, quindi la ricerca è
istantanea. Circa 186.000 righe.

**`leadsRecs`** — le registrazioni, circa 63.000 righe. Colonne che contano:

| Colonna | Tipo | Significato |
|---|---|---|
| `id` | bigint | identificativo della registrazione, è quello che serve per scaricare |
| `lead` | int, indicizzato | il lead a cui appartiene |
| `createdWhen` | datetime | quando è stata registrata, **ora italiana** |
| `username` | varchar | l'operatore Sidial |
| `callLength` | **varchar** | durata in secondi, **scritta come testo** |
| `fileName` | varchar | nome del file |
| `converted` | enum('y','n') | conversione in MP3 completata |
| `movedToS3` | enum('y','n') | file spostato sullo storage remoto |
| `removedLocal` | enum('y','n') | file cancellato dal disco locale |
| `recType` | enum | tipo di registrazione |

Il nome del file è parlante e utile per i controlli:

```
20260904_171528_135_247022_0365897947_OSD_64
   data     ora  camp  lead    telefono
```

### 3.4 La ricetta, provata

Da numero di telefono a elenco di registrazioni, **una query sola**:

```sql
SELECT r.id                              AS rec_id,
       r.createdWhen                     AS quando,
       r.username                        AS operatore,
       CAST(r.callLength AS UNSIGNED)    AS secondi,
       r.converted, r.movedToS3, r.fileName,
       l.id                              AS lead_id,
       c.description                     AS campagna
FROM sidial.leadsRecs r
JOIN sidial.leads     l ON l.id = r.lead
LEFT JOIN sidial.campaigns c ON c.id = l.campaign
WHERE l.phone1 = ? OR l.phone2 = ? OR l.phone3 = ? OR l.phone4 = ?
ORDER BY r.createdWhen DESC;
```

Provata il 7 settembre sul numero `0365897947`: risponde in **0,55-0,62 secondi**
comprensivi del giro in rete, e restituisce lead, campagna, operatore, durata e
identificativo di ogni registrazione. La stessa cosa via API richiede una quindicina
di chiamate e una ventina di secondi.

Il nome della campagna sta in `campaigns.description` ed è esattamente il codice
che si vede in Acuity, per esempio `TELEM-EHD-0000-0001-CAMP1+1-(IT)`.

### 3.5 Scaricare il file audio

Questo passo **resta sull'API**: nella replica c'è solo l'anagrafica della
registrazione, non i byte. I file sono su storage remoto (`movedToS3 = y` per
tutte le registrazioni recenti, e `removedLocal = y`).

```
GET https://effoncall.sidial.cloud/api.php?a=getLeadRec&id=<rec_id>&apiToken=<token>
```

Verificato sulla registrazione 300020: risponde `200`, `content-type: audio/mpeg`,
386 KB, un MP3 mono 8 kHz a 16 kbps di 193 secondi. Il download non ha bisogno di
whitelist: è HTTP normale, funziona già da CallCoach.

> Attenzione: `fileSizeKb` nella tabella non è la dimensione dell'MP3 scaricato
> (per quella registrazione dice 1.131 KB contro i 377 KB reali). Verosimilmente
> è la dimensione del WAV originale. Non usarlo per validare il download.

### 3.6 I tranelli, tutti verificati

- **`callLength` è testo.** Ordinare o confrontare senza conversione dà risultati
  falsi: `"99"` risulta maggiore di `"100"`. Chiedendo il massimo senza cast
  viene fuori 99 secondi, con `CAST(... AS UNSIGNED)` viene fuori 798. Convertire
  sempre.
- **Gli orari sono ora italiana.** Il server della replica gira in CEST:
  `NOW()` restituisce l'ora di Roma. Nessuna conversione di fuso, mai. Se il
  client legge le date con una libreria che le etichetta come UTC, l'etichetta è
  sbagliata ma il valore è giusto.
- **`campaignId` in `leadsRecs` è sempre vuoto.** Su 3.571 registrazioni recenti,
  tutte nulle. La campagna si prende dal lead: `leads.campaign` →
  `campaigns.description`.
- **Una telefonata può avere più registrazioni.** Su 2.100 lead recenti: 392 ne
  hanno 2, 163 ne hanno 3, e si arriva a 8 e oltre. Se la linea cade e si
  richiama, sono segmenti della stessa conversazione. **Prenderle tutte**, non
  solo la più lunga.
- **Il lead può stare in una campagna diversa** da quella dell'appuntamento
  Acuity. È normale e non è un errore.
- **I numeri non sono normalizzati alla fonte.** Su 206.000 lead: 203.890 con
  sole cifre, 2.395 con `+39`, 221 con caratteri non numerici, 21 con `0039`,
  11 vuoti. Cercare comunque per varianti, oppure — meglio — normalizzare in SQL.
- **Nella replica `converted` è un enum stretto**, `y` o `n`, mai nullo. Sparisce
  l'ambiguità che l'API aveva e con essa la logica a indovinare del codice attuale.

### 3.7 Il ponte con le analisi già fatte

Nella tabella `analyses` di CallCoach il campo `sidial_call_id` contiene gli
identificativi delle registrazioni separati da virgola, per esempio
`299388,299383,299003,299004`. Sono esattamente le chiavi `leadsRecs.id`.

Verificato: quei quattro identificativi risolvono nella replica sul lead 205058,
telefono `0558874564`, campagna `AVANZ-AVI-0000-0720-MARCO-(PO)`, operatore
`francesca`, con durate 53 + 36 + 207 + 402 = **698 secondi**, che è esattamente
il valore salvato in `total_talk_seconds`. Qualunque analisi passata si può
quindi ricostruire dalla replica, e le due basi si possono riconciliare.

### 3.8 Cosa cambia, in sintesi

| | API (oggi) | Replica (proposta) |
|---|---|---|
| Trovare le registrazioni di un numero | 12-16 chiamate, 15-20 s | 1 query, 0,6 s |
| Certezza | dipende dalla forma del numero | esatta, indici su tutti i campi |
| Stato di conversione | ambiguo | enum netto |
| Campagna e operatore | altra chiamata | nella stessa query |
| Scarico del file | `getLeadRec` | invariato, resta `getLeadRec` |
| Prerequisito | nessuno | whitelist dell'IP di CallCoach |

Il capitolo J del vecchio documento proponeva di costruire una cache locale
delle registrazioni sincronizzata ogni cinque minuti. **Non serve più**: la
replica è già quella cache, tenuta aggiornata da Sidial.

---

## 4. La pipeline attuale, in quattordici passi

I nomi sono quelli veri usati dal codice, perché compaiono nell'interfaccia e nei
log.

```
1  webhook       ricevuto e validato
2  firma         HMAC verificata (disattivabile)
3  acuity        appuntamento riletto dall'API Acuity
4  form          campi del modulo estratti (telefono, P.IVA, ragione sociale)
5  etichetta     etichetta Acuity letta
6  data          data appuntamento interpretata
7  campagna      campagna riconosciuta dal codice
8  operatore     operatore riconosciuto da email o modulo
9  sidial        lead trovati per numero di telefono
10 download      registrazioni scaricate
11 trascrizione  audio trascritto
12 analisi       analisi con Claude
13 salvataggio   risultato scritto a database
14 email         report inviato
```

Come leggerli davvero:

- **I passi 1, 2 e 3 sono decorativi.** Vengono scritti «ok» tutti insieme appena
  parte la pipeline: webhook, firma e lettura da Acuity sono già avvenuti prima,
  nell'endpoint che chiama. Non possono mai risultare rossi.
- **I passi da 4 a 8 non fermano mai** l'analisi: al massimo diventano gialli.
- **Fermano tutto** i passi 9 (Sidial), 10 (download), 11 (trascrizione),
  12 (analisi) e 13 (salvataggio). L'email che non parte è solo un avviso.
- Quando un passo si ferma, **quelli dopo restano grigi per sempre**: grigio
  vuol dire tanto «non ancora fatto» quanto «non sarà mai fatto».
- Il webhook lavora **solo su appuntamenti creati oggi** e **solo sull'etichetta
  `PRESO`**. Un appuntamento di ieri che cambia etichetta non produce nulla.
- Se il codice campagna non si riesce a leggere, o la campagna non è configurata,
  la pipeline **esce prima di creare il record**: in interfaccia non compare
  niente, nemmeno un errore. È il buco di osservabilità più fastidioso.

Regole di robustezza già imparate a caro prezzo, da mantenere:

- l'aggiornamento dello stato di un passo **non solleva mai eccezioni**: se
  fallisce, la pipeline prosegue;
- i salvataggi intermedi sono tutti protetti;
- se le registrazioni risultano ancora in conversione, si riprova ogni 10 minuti
  per un massimo di 6 volte, poi si chiude con errore tecnico (**oggi questo
  ritentativo non funziona: vedi capitolo 5**);
- la rianalisi manuale **riusa lo stesso record**, non ne crea uno nuovo, e
  **non rimanda l'email** se era già stata inviata.

---

## 5. Difetti verificati il 7 settembre 2026

Non sono sospetti: sono stati riprodotti sul codice o letti nei log di
produzione. Chi riscrive deve saperli, chi tocca il programma attuale anche.

### 5.1 Il ritentativo delle registrazioni in conversione non funziona

`retry_conversion_analysis` usa cinque nomi che nel suo file non esistono
(`AsyncSessionLocal`, `Analysis`, `update_step`, `get_setting`, `_INOLTRO`):
sono importati dentro **altre** funzioni, non a livello di modulo. La funzione
fallisce quindi alla prima riga utile, a ogni giro, per ogni analisi.

Nei log di produzione, ogni dieci minuti:

```
ERROR | routers.webhook | [retry_conv] Load analysis 317 failed: name 'AsyncSessionLocal' is not defined
```

**Conseguenza:** le **30 analisi ferme in `pending_conversion` non riparteranno
mai**, e il contatore dei tentativi non avanza, quindi non arrivano nemmeno a
chiudersi con un errore. Restano lì. Si sblocca aggiungendo gli import mancanti
dentro la funzione, oppure — meglio — sparisce da sola con la replica, perché lo
stato di conversione lì è certo.

### 5.2 L'analisi blocca tutto il server

`analyze_call` è dichiarata asincrona ma usa il client **sincrono** di Anthropic.
Il processo gira con **un solo worker**: per tutta la durata della chiamata a
Claude, l'applicazione intera è ferma. Nessuna pagina risponde, nessun altro
webhook viene servito.

### 5.3 Impostazioni che non fanno niente

- `sidial_retry_count` (5) e `sidial_retry_wait_seconds` (180) si modificano
  dall'interfaccia, vengono lette dal codice e **non vengono mai usate**. I
  valori veri sono scritti nel codice: 6 tentativi ogni 600 secondi.
  **`CLAUDE.md` su questo è sbagliato** e dice «5 tentativi × 3 minuti».
- `whisper_model_size`, le quattro variabili SMTP e `supabase_service_key` non
  sono lette da nessuna riga: la trascrizione passa dalle API, l'email da Brevo.
- `campaign_code` e `appointment_dt` compaiono nella firma della ricerca
  registrazioni ma **non arrivano mai** alla funzione che lavora: la selezione
  non tiene conto né della campagna né della data dell'appuntamento.

### 5.4 Cose che si vedono e non corrispondono

- L'etichetta salvata in `acuity_label` è **sempre `PRESO`**, scritta a mano nel
  codice. L'etichetta vera sta in `label_name` e `label_color`.
- `email_sent_at` non viene scritta da nessuna parte: è sempre vuota. Per sapere
  se l'email è partita vale solo `email_sent`.
- La griglia dei quattordici semafori **non si aggiorna da sola**: la pagina
  cerca un campo che l'endpoint non restituisce. Bisogna ricaricare.
- I pulsanti «carica audio» e «carica trascrizione», che compaiono quando il
  download si ferma, **non sono collegati a niente**.
- La pagina di prova Sidial (`/admin/ui/sidial-test`) è rotta: importa due
  funzioni che nel codice non esistono più.
- Nel prompt convivono due scale di voto, da 1 a 3 e da 1 a 5, e il livello 1 si
  chiama «insufficiente» nel prompt e «inaccurata» nel database.

### 5.5 Il database non è come descritto

`CLAUDE.md` dice che si passa dal pooler in *transaction mode*. La stringa di
connessione usa la porta 5432, che è la *session mode*. I due vincoli
(`statement_cache_size=0`, `pool_pre_ping=False`) restano comunque, ma la
motivazione scritta è imprecisa.

Non esiste un sistema di migrazioni: lo schema si costruisce con una ventina di
istruzioni eseguite a ogni avvio, tutte non fatali. `schema.sql` è fermo alla
prima versione e **non descrive il database vero**: non usarlo per ricrearlo.

---

## 6. Le regole di dominio che devono sopravvivere alla riscrittura

Questa è la parte che non si deduce dal codice e che costa di più riscoprire.

### 6.1 Due giudizi separati, da non confondere mai

| Dimensione | Logica | Che cosa decide |
|---|---|---|
| Qualifica del prospect | OR fra gli alternativi, AND fra gli obbligatori | se era in target |
| Qualità dell'operatore | ha raccolto tutto e seguito la traccia | il punteggio di coaching |

**L'analisi non deve mai penalizzare la qualifica del prospect per una lacuna
dell'operatore.** Sono due cose diverse.

### 6.2 Tre tipi di parametro

- **Obbligatori:** devono essere tutti soddisfatti.
- **Flessibili:** valori numerici con tolleranza del 15-20%. Un valore vicino
  alla soglia è «borderline», non un'esclusione.
- **Alternativi:** ne basta uno. Spesa nazionale sopra mille euro **oppure**
  internazionale sopra duecentocinquanta.

### 6.3 Non chiesto non vuol dire non soddisfatto

| Situazione | Lettura corretta |
|---|---|
| L'operatore non ha chiesto il parametro | lacuna dell'operatore, va nel coaching |
| Il prospect ha risposto di no | possibile esclusione dalla qualifica |

L'operatore deve raccogliere **tutti** i parametri anche quando il prospect è già
qualificato da un criterio alternativo. Ogni parametro mancante è una lacuna,
comunque vada la qualifica.

Due casi già corretti in passato e da non far tornare: una **dichiarazione
sul futuro** del prospect non vale come valore attuale, e una **risposta
negativa esplicita** non è un «parametro mancante».

### 6.4 I livelli di giudizio

```
5  eccellente      verde scuro
4  buona           verde
3  sufficiente     azzurro
2  da_migliorare   arancio
1  inaccurata      rosso
   non_in_target   grigio    — fuori parametro, non è un voto all'operatore
   errore_tecnico  nero      — trascrizione troppo breve o fallita
```

Distribuzione reale delle 406 analisi: 181 buone, 76 non in target, 36 errore
tecnico, 27 da migliorare, 9 eccellenti, 8 sufficienti, 4 inaccurate.

### 6.5 Le campagne si riconoscono dal prefisso

```
INTER-CER-2908-LUCA-(MI) → INTER-CER-2908 → INTER-CER → INTER
```

Dal più specifico al meno specifico: vince la riga più specifica presente a
database. Una riga `INTER` fa da impostazione predefinita per tutte le sue
sotto-campagne.

### 6.6 «Ieri» è l'ultimo giorno lavorativo

Vale su tutte le pagine e tutti i filtri: il lunedì «ieri» è venerdì, la domenica
è venerdì, negli altri giorni è il giorno prima.

---

## 7. Trascrizione e analisi

**Trascrizione.** Due motori, scelti per impostazione o per campagna:

- **OpenAI Whisper** via API (`whisper-1`, lingua italiana): è il predefinito.
- **AssemblyAI** (`language_code: it`, `speech_models: ["universal-2"]`,
  etichette dei parlanti attive): utile quando serve distinguere chi parla.

Il motore attivo si sceglie in quest'ordine: scelta fatta per quella analisi,
poi impostazione della campagna, poi la riga `transcription_engine` nella tabella
`settings`. **Non** dalla variabile d'ambiente, che pure esiste. Se il motore
scelto fallisce o produce meno di venti caratteri si passa all'altro, che diventa
quello attivo per il resto della sessione.

> Trappola nota: il parametro giusto è `speech_models` come **lista**. Il vecchio
> `speech_model` come stringa punta a un modello solo inglese.

Whisper in locale gira solo nella taglia `tiny` per via della CPU condivisa di
Railway: le taglie superiori vanno sempre in timeout.

**Analisi.** Il testo va a Claude con un prompt costruito a strati: prompt di
base, documenti globali, configurazione della campagna e parametri di
qualificazione. La risposta è un JSON con un ordine dei campi obbligatorio e un
campo `disclaimer` finale da riportare alla lettera.

> Da rivedere nella riscrittura: il modello configurato per l'analisi è
> **Haiku 4.5**, il più piccolo della famiglia. Per un giudizio di coaching su
> una trascrizione lunga è una scelta da rimettere in discussione.

---

## 8. Il database di CallCoach

PostgreSQL su Supabase, sette tabelle: `analyses`, `campaigns`, `documents`,
`global_documents`, `operators`, `prompt_sections`, `settings`.

`analyses` è il cuore. Campi che contano: `appointment_id`, `campaign_code`,
`client_phone`, `client_company`, `operator_name`, `operator_email`,
`acuity_account`, `acuity_label`, `label_name`, `label_color`,
`sidial_call_id` (gli identificativi delle registrazioni, separati da virgola),
`num_recordings`, `total_talk_seconds`, `transcript`, `qualification_level`,
`report_json`, `report_html`, `email_sent`, `email_sent_at`,
`processing_status`, `pipeline_steps`, `progress`, `step_message`,
`error_message`.

La connessione passa dal pooler di Supabase sulla porta 5432 (*session mode*), e
impone due vincoli **da non toccare**: `statement_cache_size=0` e
`pool_pre_ping=False`. Cambiarli rompe la connessione in produzione. Non c'è un
sistema di migrazioni: vedi 5.5.

Le analisi duplicate sullo stesso appuntamento si gestiscono mostrando solo la
più recente.

---

## 9. Area admin e accesso

Interfaccia a pagine servite dal server, sotto `/admin/ui`: elenco e dettaglio
delle analisi, archivio, stampa del report, gestione campagne (creazione,
modifica, duplicazione), prompt, documenti globali, elenco appuntamenti con
analisi su richiesta, più alcune pagine di diagnostica (`/debug`,
`/acuity-debug`, `/sidial-test`, `/test-email`).

**Accesso:** una sola password condivisa, senza utenti né email, nella variabile
`ADMIN_PASSWORD`. Il login rilascia un token firmato valido 24 ore in un cookie.
Non esiste un «password dimenticata»: la password si legge e si cambia dalle
variabili del servizio Railway `CALLCOACH`. La procedura è in `CLAUDE.md`.

**Email:** i report partono via **Brevo** in HTTP, non via SMTP, perché Railway
blocca le porte SMTP in uscita.

---

## 10. Configurazione e messa in opera

Applicazione FastAPI con Jinja2, in produzione su Railway
(`https://web-production-181160.up.railway.app`). Variabili attese:

| Gruppo | Variabili |
|---|---|
| Database | `DATABASE_URL`, `SUPABASE_SERVICE_KEY` |
| Intelligenza artificiale | `ANTHROPIC_API_KEY`, `ANTHROPIC_MODEL` |
| Sidial | `SIDIAL_API_URL`, `SIDIAL_API_TOKEN` |
| Replica Sidial (da aggiungere) | `SIDIAL_REPL_HOST`, `SIDIAL_REPL_PORT`, `SIDIAL_REPL_USER`, `SIDIAL_REPL_PASSWORD` |
| Acuity, due account | `ACUITY_ACCOUNT{1,2}_USER_ID`, `ACUITY_ACCOUNT{1,2}_API_KEY`, `ACUITY_ACCOUNT{1,2}_WEBHOOK_SECRET`, `ACUITY_VERIFY_WEBHOOK` |
| Trascrizione | `OPENAI_API_KEY`, `ASSEMBLYAI_API_KEY`, `TRANSCRIPTION_ENGINE`, `WHISPER_MODEL_SIZE` |
| Email | `BREVO_API_KEY`, `EMAIL_FROM_ADDRESS`, `FALLBACK_EMAIL` |
| Sicurezza | `SECRET_KEY`, `ADMIN_PASSWORD` |

Le variabili della replica **non sono ancora configurate** sul servizio CallCoach.

Per guardare dentro al servizio in produzione, senza aspettare i log:

```bash
railway ssh --service CALLCOACH "python -c \"...\""
```

---

## 11. Decisioni consolidate — non rimetterle in discussione

1. **Mai una sola registrazione:** se la linea cade ci sono più segmenti.
2. **Mai fermarsi alla prima variante** del numero: cercarle tutte.
3. **`speech_models` come lista**, mai `speech_model` come stringa.
4. **`pool_pre_ping` resta falso** e **`statement_cache_size` resta zero**.
5. **Parametro non chiesto ≠ parametro non soddisfatto.**
6. **La qualifica del prospect non si abbassa** per le lacune dell'operatore.
7. **L'email non si rimanda** se già inviata per quell'appuntamento.
8. **La rianalisi riusa lo stesso record**, non ne crea uno nuovo.
9. **Il periodo predefinito** nella pagina principale è «Mese».
10. **Le registrazioni non si cercano alla data dell'appuntamento**, ma prima.

---

## 12. Per la riscrittura

Cosa tenere così com'è: il giro di lavoro del capitolo 1, le regole di dominio
del capitolo 5, le decisioni del capitolo 10.

Cosa cambiare, in ordine di guadagno:

1. **Trovare le registrazioni dalla replica**, non dall'API. Toglie il punto in
   cui si rompe una analisi su cinque. Primo passo concreto: far mettere in
   whitelist l'IP `208.77.244.155`.
2. **Semplificare i timeout.** I tre livelli in cascata esistono per difendersi
   dalla lentezza dell'API. Con una query da mezzo secondo diventano inutili.
3. **Sbloccare le trenta analisi ferme.** Oggi il ritentativo è rotto (5.1) e
   non ripartiranno mai da sole. Con `sidial_call_id` che punta a `leadsRecs.id`
   si possono ripescare e rifare.
4. **Togliere il blocco del server durante l'analisi** (5.2): con un worker solo
   e il client sincrono, ogni analisi ferma tutta l'applicazione.
5. **Rivedere il modello dell'analisi**, oggi Haiku 4.5.
6. **Ripensare l'accesso.** Una password condivisa in una variabile d'ambiente
   va bene per una persona, non per una squadra.
7. **Fare pulizia.** Il capitolo 5 elenca impostazioni che non fanno niente,
   pulsanti scollegati, una pagina di diagnostica rotta e un'immagine che
   installa duecentocinquanta megabyte di librerie mai importate.

---

*Aggiornare questo documento a ogni decisione presa, problema risolto in modo
non ovvio o funzionalità aggiunta. Le cose verificate vanno segnate come tali,
con la data.*

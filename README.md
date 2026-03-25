# Data Vault Demo Environment

Komplett dockerisierte Demo-Umgebung für **dbt**, **AutomateDV** (Data Vault 2.0) und **Airflow 3** Orchestrierung auf **PostgreSQL**. Gedacht für Schulungen, Workshops und Team-Demos.

## Was zeigt diese Demo?

### Data Vault 2.0 mit AutomateDV
Ein vollständiges Data-Vault-Modell auf Basis eines Northwind-ähnlichen Bestellsystems:

| Schicht | Schema | Inhalt |
|---------|--------|--------|
| **Raw** | `raw` | Rohdaten aus CSV-Dateien (5 Tabellen) |
| **Staging** | `staging` | Bereinigte Views mit Hash-Keys und Metadaten (AutomateDV `stage` Macro) |
| **Raw Vault** | `raw_vault` | 4 Hubs, 3 Links, 5 Satellites, 2 PIT-Tabellen |
| **Marts** | `mart` | 3 Business-Tabellen (Umsatz/Kunde, Bestellübersicht, Produktverkäufe) |

### Airflow-Orchestrierung: Drei Ansätze im Vergleich

- **`dbt_classic`** - BashOperator-Kette: `dbt deps` → `dbt seed` → `dbt run` (staging → raw_vault → marts) → `dbt test`. Einfach, aber ein Task pro Phase.
- **`dbt_cosmos`** - Astronomer Cosmos parst das dbt-Projekt automatisch und erstellt **43 individuelle Airflow-Tasks** mit vollständigem Dependency-Graph. Retry und Monitoring auf Modell-Ebene. https://astronomer.github.io/astronomer-cosmos/index.html
- **`cosmos_master` → `cosmos_orders` → `cosmos_marts`** - Aufgeteilte Cosmos-DAGs pro fachlicher Domain, verkettet über **Airflow Datasets**. Mit **DatasetOrTimeSchedule** (Cron-Fallback) und **Freshness-Checks** für robuste Pipelines. Siehe [Demo: DAG-Splitting mit Datasets](#demo-dag-splitting-mit-datasets-cosmos_split).

> **Performance-Hinweis:** `dbt_classic` läuft in ~40s, `dbt_cosmos` in ~2 min. Cosmos erstellt pro Modell einen eigenen Task mit separatem dbt-Prozess (Startup, Parsing, Profil-Laden). Das ist der Preis für die Granularität (Retry, Monitoring, Parallelisierung auf Modell-Ebene). Mit dem experimentellen **Watcher Execution Mode** (ab Cosmos 1.11) lässt sich dieser Overhead auf nahezu Classic-Niveau reduzieren.

### Quelldaten
Northwind-ähnliches Bestellsystem mit 5 Tabellen:

| Tabelle | Zeilen | Beschreibung |
|---------|--------|-------------|
| customers | 100 | Kunden mit Adressen |
| employees | 20 | Mitarbeiter mit Abteilungen |
| orders | 600 | Bestellungen mit Versand- und Zahlungsdaten |
| order_details | 700 | Bestellpositionen mit Preisen und Rabatten |
| products | 10 | Produkte mit Kategorien und Preisen |

---

## Installation

### Voraussetzungen
- **Docker Desktop** (macOS/Windows/Linux) mit mindestens 16 GB RAM
- **Git** (optional, zum Klonen)
- **DBeaver** oder anderer SQL-Client (optional, pgAdmin ist integriert)

### Setup
```bash
# Repository klonen
git clone <repo-url> && cd new_env

# .env-Datei erstellen (WICHTIG - wird nicht mit Git ausgeliefert!)
cp .env.example .env
```

**Secrets generieren und in `.env` eintragen:**
```bash
# Fernet Key (für Airflow Connection/Variable-Verschlüsselung)
python3 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"

# Falls cryptography nicht installiert: alternativ mit openssl
openssl rand -base64 32

# API Secret + JWT Secret (für Airflow 3 Container-Kommunikation)
python3 -c "import secrets; print(secrets.token_urlsafe(32))"
python3 -c "import secrets; print(secrets.token_urlsafe(32))"
```

> **Warum 3 verschiedene Keys?**
> - `FERNET_KEY`: Verschlüsselt Passwörter in Airflow Connections/Variables in der DB
> - `API__SECRET_KEY`: Signiert die API-Session (wie Flask SECRET_KEY)
> - `API_AUTH__JWT_SECRET`: Signiert JWT-Tokens zwischen Scheduler/Worker und API-Server
>
> **Alle 3 müssen auf allen Airflow-Containern identisch sein!** Siehe [Airflow 3 Upgrade-Hinweise](#airflow-3-vs-2x-upgrade-hinweise) für Details.

```bash
# Alle Container bauen und starten
docker compose up -d --build

# Erster Start dauert ca. 3-5 Minuten (Image-Download + Build)
# Status prüfen:
docker compose ps
```

Beim ersten Start passiert automatisch:
1. PostgreSQL startet mit den Schemas `raw`, `staging`, `raw_vault`, `mart`
2. Airflow-Datenbank wird migriert, Admin-User wird erstellt
3. dbt Docs werden generiert und served
4. Alle DAGs werden registriert (unpaused)

### Services nach dem Start

| Service | URL | Login |
|---------|-----|-------|
| **Streamlit Portal** | http://localhost:8572 | - |
| **Airflow UI** | http://localhost:8080 | admin / admin |
| **Flower** (Celery Monitor) | http://localhost:5555 | - |
| **dbt Docs** | http://localhost:8081 | - |
| **pgAdmin** | http://localhost:5050 | admin@demo.com / admin |
| **PostgreSQL** | localhost:5432 | demo_user / demo_pass / DB: demo |

#### Option 1: Metabase (BI + DQ-Dashboards)

Metabase ergänzt die Demo um ein professionelles BI-Tool für Dashboards auf den Mart-Tabellen und DQ-Visualisierung — und ist der empfohlene nächste Schritt nach dem Core-Stack:

```bash
docker compose -f docker-compose.yml -f docker-compose.bi.yml up -d
```

| Service | URL | Login |
|---------|-----|-------|
| **Metabase** | http://localhost:3000 | admin@demo.com / admin2pistor |

> **RAM-Hinweis:** Metabase benötigt ca. 500-700 MB zusätzlich. Gesamtbedarf: ~10 GB.

**Erster Start (frisches Volume):** Setup-Script ausführen — richtet Admin-User, PostgreSQL-Verbindung und MCP API Key automatisch ein:
```bash
./metabase/setup-metabase.sh
```
Danach ist Metabase sofort einsatzbereit mit der Verbindung "Demo (Data Vault)" und allen Schemas (raw, staging, raw_vault, mart, dq).

**MCP API Key** (für Claude Desktop Integration): Das Script speichert den Key in `metabase/mcp-api-key.txt` (gitignored):
```bash
cat metabase/mcp-api-key.txt
# → mb_xxxx...  →  in claude_desktop_config.json unter METABASE_API_KEY eintragen
```

**DuckDB-Dateien einbinden:** Das DuckDB Community-Plugin ist bereits vorinstalliert (`metabase/plugins/duckdb.metabase-driver.jar`). Dateien einfach in `./duckdb/` ablegen — das Verzeichnis ist in den Container gemountet:
```bash
cp meine_analyse.duckdb ./duckdb/
# In Metabase: Einstellungen → Datenbanken → DuckDB → Pfad: /duckdb/meine_analyse.duckdb
```

**DQ-Monitoring:** Testergebnisse in PostgreSQL persistieren und in Metabase visualisieren:
```bash
# In Airflow UI: dq_persist_results triggern
# → schreibt alle run_results.json nach dq.test_results
# Dann in Metabase: http://localhost:3000 → Dashboards bauen
# Beispiel-Abfragen und Vergleich Streamlit vs. Metabase → siehe Abschnitt "Datenqualität"
```

> **Hinweis:** Nach `docker compose down -v` (Volume-Reset) Setup-Script erneut ausführen und neuen API Key eintragen.

#### Option 2: MCP + KI-Assistenz (Open WebUI + Ollama)

MCP (Model Context Protocol) verbindet KI-Assistenten direkt mit dem Data Stack — als "Surprise Goodie" für den Ausblick auf AI-gestützte Datenpipelines.

**Weg 1 — Claude Desktop / Claude Code (einfachste Option):**
```bash
# mcp/claude-desktop-config.json als Vorlage verwenden.  (MacOS!)
cp mcp/claude-desktop-config.json ~/Library/Application\ Support/Claude/claude_desktop_config.json
# Pfad zu dbt_project anpassen, Metabase API Key eintragen, dann Claude Desktop neu starten
```

**Weg 2 — Open WebUI + Ollama (Browser-Chat, komplett lokal):**
```bash
# Ollama auf dem Host: https://ollama.com
ollama pull qwen2.5:7b   # ~4.7 GB

# Mit MCP-Overlay starten (zusammen mit Core + Metabase empfohlen)
docker compose -f docker-compose.yml -f docker-compose.bi.yml -f docker-compose.mcp.yml up -d
```

| Service | URL | Login |
|---------|-----|-------|
| **Open WebUI** | http://localhost:3001 | Beim ersten Start Account erstellen |
| **MCP-Gateway API** | http://localhost:8200/docs | - |

> **RAM-Hinweis:** Open WebUI + MCP-Gateway ca. +800 MB. Gesamtbedarf mit allem: ~11 GB.

### Worker skalieren
```bash
# Mehrere Celery-Worker starten (z.B. 3 parallele Worker)
docker compose up -d --scale airflow-worker=3

# Worker-Status in Flower beobachten: http://localhost:5555
```

### Stoppen und Aufräumen
```bash
# Stoppen (Daten bleiben erhalten)
docker compose down
# Mit Metabase:
docker compose -f docker-compose.yml -f docker-compose.bi.yml down

# Stoppen + alle Daten löschen (frischer Neustart)
docker compose down -v
# Mit Metabase:
docker compose -f docker-compose.yml -f docker-compose.bi.yml down -v
```

---

## Demo-Ablauf

Die Demo besteht aus **5 aufbauenden Szenarien**. Szenarien 1-3 sind Alternativen (gleicher Effekt, unterschiedliche Orchestrierung). Szenarien 4-5 sind Erweiterungen.

### Grundlage: Rohdaten laden
In der Airflow UI den DAG **`init_raw_data`** triggern. Dieser:
- Löscht alle bestehenden Raw-Tabellen (idempotent)
- Erstellt die Tabellen neu (Postgres-DDL)
- Lädt die 5 CSV-Dateien per `COPY`

→ Ergebnis in pgAdmin prüfen: `raw.customers`, `raw.orders`, etc.

### Szenario 1: Klassisch (BashOperator)
DAG **`dbt_classic`** triggern:
- `dbt seed` lädt CSV-Daten als Alternative in Raw-Schema
- `dbt run --select staging` erstellt Staging-Views mit Hash-Keys
- `dbt run --select raw_vault` baut Hubs, Links, Satellites, PITs
- `dbt run --select marts` erstellt Business-Tabellen
- `dbt test` validiert alle Modelle

→ **Zeigt:** Einfacher, sequentieller Ansatz. Ein Task pro Phase. So fängt jeder an.

### Szenario 2: Cosmos (monolithisch)
DAG **`dbt_cosmos`** triggern:
- Cosmos parst das dbt-Projekt automatisch
- Jedes dbt-Modell wird ein eigener Airflow-Task
- Der Dependency-Graph ist in der Airflow UI sichtbar
- 43 Tasks mit vollständiger Parallelisierung

→ **Zeigt:** Gleicher Effekt wie Szenario 1, aber mit automatischem Task-Graph. Retry und Monitoring auf Modell-Ebene.

### Szenario 3: Cosmos Split (Domain-DAGs mit Datasets)
DAG **`cosmos_master`** manuell triggern:
- Baut nur Stammdaten auf (Kunden, Mitarbeiter, Produkte)
- Publiziert `Dataset("dbt://domain_master")`
- **`cosmos_orders`** startet **automatisch** (Dataset-Trigger oder Cron-Fallback 06:00)
  - Freshness-Check: Sind Stammdaten frisch? Warnung wenn nicht, aber kein Abbruch
  - Baut Bestellungen, Links, PITs auf
  - Publiziert `Dataset("dbt://domain_orders")`
- **`cosmos_marts`** startet **automatisch** (Dataset-Trigger oder Cron-Fallback 07:00)
  - Baut Business-Tabellen auf

→ **Zeigt:** Domain-Trennung, Dataset-Verkettung, Cron-Fallback, Graceful Degradation. Siehe [Demo: DAG-Splitting mit Datasets](#demo-dag-splitting-mit-datasets-cosmos_split) und Streamlit Tab "DAG-Splitting".

In der **Airflow UI** sieht man:
- **Datasets-Tab:** Die Kette `dbt://domain_master` → `dbt://domain_orders`
- **Graph View** von `cosmos_orders`: Den Freshness-Check-Branch
- **DAG-Runs:** Drei separate Läufe mit unabhängigem Status

### Szenario 4a: Delta-Load klassisch
DAG **`load_delta`** triggern (setzt Szenario 1, 2 oder 3 voraus):
- **Kein DROP** - die Delta-CSVs werden per UPSERT in die bestehenden Raw-Tabellen eingefügt
- `dbt run` läuft **ohne** `--full-refresh`, sodass AutomateDVs Incremental-Logik greift:
  - Hubs: nur neue Business Keys werden eingefügt
  - Satellites: nur geänderte Hashdiffs erzeugen neue Versionen (Historisierung!)
  - Links: nur neue Beziehungs-Kombinationen

**Delta-Daten (Batch 2, `change_date` = 2024-01-15):**
| Datei | Inhalt | Effekt im Vault |
|-------|--------|----------------|
| `CUSTOMERS_DELTA.csv` | 3 neue + 2 geänderte Kunden | Hub: +3, Sat: +5 |
| `ORDERS_DELTA.csv` | 5 neue Bestellungen | Hub: +5, alle Links: +5 |
| `ORDER_DETAILS_DELTA.csv` | 8 neue Positionen | Link Order-Product: +8, Sat: +8 |

→ Im **Streamlit Portal** (Tab "Inkrementelle Loads") können die Vorher/Nachher-Zahlen und die Satellite-Historisierung live geprüft werden.

### Szenario 4b: Delta-Load + Split-Kette
DAG **`load_delta_split`** triggern (setzt Szenario 3 voraus):
- Gleiche UPSERT-Logik wie 4a
- **Kein eigener dbt-Lauf** — stattdessen publiziert der DAG ein Dataset
- Die **gesamte Cosmos-Split-Kette** startet automatisch:
  `cosmos_master` → `cosmos_orders` → `cosmos_marts`

→ **Unterschied zu 4a:** Der dbt-Teil wird von den Domain-DAGs übernommen, nicht vom Delta-DAG selbst. Jede Domain testet ihre eigenen Modelle separat.

### Szenario 5a: PSA klassisch (BashOperator)
1. DAG **`psa_flow`** triggern (setzt `init_raw_data` voraus):
   - Baut die Persistent Staging Area auf (SCD2, Delete Detection)
   - Baut einen alternativen Data Vault aus der PSA via `dbt run --select tag:psa`
2. DAG **`psa_rebuild_demo`** triggern:
   - Droppt die PSA-Vault-Tabellen
   - Baut sie aus der PSA komplett neu auf
   - **Beweist:** Kein Datenverlust, kein Zurückgreifen auf CSV nötig

→ Im **Streamlit Portal** (Tab "PSA-Pfad") die Side-by-Side-Vergleiche und SCD2-Historie einsehen.

### Szenario 5b: PSA + Cosmos (produktionsnahe Variante)
DAG **`psa_cosmos_flow`** triggern (setzt `init_raw_data` voraus):
1. **NG Generator** (BashOperator): PSA-Objekte erstellen, SCD2-Load, Delete Detection
2. **Cosmos TaskGroup** (tag:psa): Staging + Vault aus PSA mit individuellen Tasks pro Modell
3. **Dataset publizieren**: `Dataset("dbt://psa_loaded")`
4. **`cosmos_master`** startet **automatisch** (Dataset-Trigger) → baut Stammdaten
5. **`cosmos_orders`** startet **automatisch** (Dataset-Trigger) → baut Bestellungen
6. **`cosmos_marts`** startet **automatisch** (Dataset-Trigger) → baut Marts

→ **Volle Dataset-Kette:** `dbt://psa_loaded` → `dbt://domain_master` → `dbt://domain_orders`

Das ist die **produktionsnahe Architektur**: PSA als stabile Vorstufe (NG Generator), Cosmos für den dbt-Teil, Datasets für die Verkettung. Der Unterschied zu 5a:

| Aspekt | `psa_flow` (5a) | `psa_cosmos_flow` (5b) |
|--------|-----------------|----------------------|
| dbt-Ausführung | Ein BashOperator (`dbt run --select tag:psa`) | Cosmos TaskGroup (individuelle Tasks) |
| Nachfolge-DAGs | Keine (standalone) | Triggert die gesamte Split-Kette via Datasets |
| Retry | Gesamter dbt-Lauf | Pro Modell einzeln |
| Monitoring | Ein Task-Status | Pro Modell sichtbar im Graph |

### Ergebnisse erkunden
- **Streamlit Portal** (localhost:8572): Zentrale Übersicht mit allen Tabs
- **pgAdmin** (localhost:5050): SQL-Abfragen auf alle Schemas
- **dbt Docs** (localhost:8081): Lineage-Graph und Modell-Dokumentation
- **Metabase** (localhost:3000): BI-Dashboards auf Marts + DQ-Monitoring (optional, siehe unten)
- **DBeaver**: Direktverbindung zu PostgreSQL

### Demo-Reset
Für einen sauberen Neustart einfach `init_raw_data` erneut triggern - der DAG macht `DROP TABLE IF EXISTS` bevor er neu lädt.

---

## Architektur

```
┌───────────────────────────────────────────────────────────────────┐
│  Docker Compose                                                   │
│                                                                   │
│  ┌──────────┐  ┌────────────────────────────────────────────┐     │
│  │ Postgres │  │ Airflow 3.0.2 (CeleryExecutor)             │     │
│  │          │  │  ┌────────────┐  ┌───────────────┐         │     │
│  │ demo DB  │◄─┤  │ API-Server │  │ DAG-Processor │         │     │
│  │  raw     │  │  └────────────┘  └───────────────┘         │     │
│  │  staging │  │  ┌────────────┐  ┌───────────────┐         │     │
│  │  raw_vlt │  │  │ Scheduler  │  │  Triggerer    │         │     │
│  │  mart    │  │  └────────────┘  └───────────────┘         │     │
│  │          │  │  ┌────────────┐  ┌───────────────┐         │     │
│  │ airflow  │  │  │   Worker   │  │    Flower     │ :5555   │     │
│  │   DB     │  │  │ (skalierb.)│  │  (Monitoring) │         │     │
│  └──────────┘  │  └────────────┘  └───────────────┘         │     │
│                └────────────────────────────────────────────┘     │
│  ┌──────────┐                                                     │
│  │  Redis   │  ┌──────────┐ ┌─────────┐ ┌──────────┐             │
│  │ (Broker) │  │ dbt Docs │ │ pgAdmin │ │Streamlit │             │
│  └──────────┘  │  :8081   │ │  :5050  │ │  :8572   │             │
│                └──────────┘ └─────────┘ └──────────┘             │
│                                                                   │
│  Optional (docker-compose.bi.yml):                                │
│  ┌──────────┐                                                     │
│  │ Metabase │  BI-Dashboards (Marts) + DQ-Monitoring              │
│  │  :3000   │  Liest: mart.*, dq.test_results                    │
│  └──────────┘                                                     │
│                                                                   │
│  Optional (docker-compose.mcp.yml):                               │
│  ┌──────────┐  ┌─────────────────────────────────────────────┐   │
│  │Open WebUI│  │ MCP-Gateway :8200 (mcpo)                    │   │
│  │  :3001   │◄─┤  /dbt      → dbt-mcp (offiziell, dbt Labs)  │   │
│  │ + Ollama │  └─────────────────────────────────────────────┘   │
│  └──────────┘                                                     │
│                                                                   │
│  Lokal auf Host (kein Docker):                                    │
│  Claude Desktop / Claude Code mit mcp/claude-desktop-config.json │
│  → PostgreSQL-MCP, dbt-MCP, Metabase-MCP                         │
└───────────────────────────────────────────────────────────────────┘

Lokale Volumes (gemountet):
  ./dbt_project  →  dbt-Modelle, Seeds, Config
  ./airflow/dags →  Airflow DAG-Dateien
  ./daten        →  CSV-Quelldaten
  ./streamlit    →  Portal-App
```

### Data Vault Modell

```
                    ┌──────────────┐
                    │ HUB_CUSTOMER │
                    │   (100)      │
                    └──────┬───────┘
                           │
              ┌────────────┤
              │            │
   ┌──────────▼─────┐  ┌──▼──────────────┐
   │ SAT_CUSTOMER   │  │LNK_ORDER_CUST   │
   │   (100)        │  │   (600)          │
   └────────────────┘  └──┬──────────────┘
                           │
                    ┌──────▼───────┐
                    │  HUB_ORDER   │──────┐
                    │   (600)      │      │
                    └──────┬───────┘      │
                           │        ┌─────▼──────────┐
              ┌────────────┤        │LNK_ORDER_EMP   │
              │            │        │   (600)         │
   ┌──────────▼─────┐  ┌──▼────────┴──┐    ┌────────▼──────┐
   │ SAT_ORDER      │  │LNK_ORDER_PROD│    │ HUB_EMPLOYEE  │
   │   (600)        │  │   (700)      │    │   (20)        │
   └────────────────┘  └──┬───────────┘    └───────┬───────┘
                           │                       │
                    ┌──────▼───────┐    ┌──────────▼─────┐
                    │ HUB_PRODUCT  │    │ SAT_EMPLOYEE   │
                    │   (10)       │    │   (20)         │
                    └──────┬───────┘    └────────────────┘
                           │
                ┌──────────▼─────┐
                │ SAT_PRODUCT    │
                │   (10)         │
                └────────────────┘
```

---

## Airflow 3 vs. 2.x: Upgrade-Hinweise

Diese Demo läuft auf **Airflow 3.0.2**. Bei einem Upgrade von 2.x auf 3.x gibt es mehrere kritische Unterschiede, die wir beim Aufbau dieser Umgebung erfahren haben:

### Architekturänderungen

| Komponente | Airflow 2.x | Airflow 3.x |
|-----------|-------------|-------------|
| Web UI | `airflow webserver` | **`airflow api-server`** (Webserver existiert nicht mehr) |
| DAG Parsing | Im Scheduler integriert | **`airflow dag-processor`** als separater Prozess |
| Task Execution | Scheduler startet Tasks direkt | Tasks kommunizieren über **Execution API** mit dem API-Server |
| User Management | `airflow users create` | Nur mit **FAB-Provider** (`apache-airflow-providers-fab`) |
| Triggerer | Optional | **Pflicht** als eigener Service |

### Kritisches Problem: JWT-Signatur zwischen Containern

**Das grösste Problem bei Airflow 3 im Multi-Container-Setup:**

In Airflow 3 kommunizieren Task-Worker (die vom Scheduler gestartet werden) mit dem API-Server über eine **interne Execution API**. Diese API verwendet **JWT-Tokens** zur Authentifizierung. Die JWT-Tokens werden mit einem `secret_key` und `jwt_secret` signiert.

**Das Problem:** Wenn `api-server` und `scheduler` in **separaten Containern** laufen, generiert jeder Container beim ersten Start **eigene, zufällige Secrets** in seiner lokalen `airflow.cfg`. Der Scheduler signiert den JWT mit seinem Secret, der API-Server prüft mit seinem eigenen → **Signature verification failed**.

**Die Lösung:** Alle Airflow-Container müssen dieselbe `airflow.cfg` mit identischen Secrets teilen:

```ini
# airflow/config/airflow.cfg
[api]
secret_key = <identischer-key-für-alle-container>

[api_auth]
jwt_secret = <identischer-key-für-alle-container>
```

Diese Datei wird per Volume in alle Container gemountet:
```yaml
volumes:
  - ./airflow/config:/opt/airflow/config
environment:
  AIRFLOW_CONFIG: "/opt/airflow/config/airflow.cfg"
```

**In Airflow 2.x war das kein Problem**, weil der Webserver und Scheduler über die gemeinsame Datenbank kommunizierten und keine API-basierte Task-Execution stattfand.

### REST API Authentifizierung in Airflow 3

Airflow 3 hat die Authentifizierung für die **REST API** grundlegend geändert. Für den täglichen Betrieb ist es wichtig, zwei völlig unabhängige Auth-Systeme zu unterscheiden:

| Zugang | Methode | Geändert? |
|--------|---------|-----------|
| **Web-UI (Browser)** | admin/admin wie bisher | Nein |
| **REST API** (`/api/v2/...`) | Bearer Token (JWT) | **Ja** — Basic Auth entfernt |

#### Web-UI: Keine Änderung

Login über den Browser mit Benutzername und Passwort funktioniert wie in Airflow 2.x. Davon ist nichts betroffen.

#### REST API: Nur noch Bearer Token

In Airflow 2.x akzeptierte die API Basic Auth (Benutzername + Passwort direkt im Request). In Airflow 3 ist das **entfernt**. Wer die API programmatisch nutzt — eigene Skripte, MCP-Server, CI/CD Pipelines, externe Tools — muss zuerst einen Token holen:

```bash
# Schritt 1: Token holen (einmalig, gültig 24h)
TOKEN=$(curl -s -X POST http://localhost:8080/auth/token \
  -H "Content-Type: application/json" \
  -d '{"username":"admin","password":"admin"}' | \
  python3 -c "import sys,json; print(json.load(sys.stdin)['access_token'])")

# Schritt 2: Token bei jedem API-Call mitschicken
curl -H "Authorization: Bearer $TOKEN" http://localhost:8080/api/v2/dags
```

#### Token-Ablauf im Betrieb

Der Token ist **24 Stunden gültig**. Für eigene Skripte oder Tools die die API verwenden: Token einmal zu Beginn holen, dann für alle Calls des Tages verwenden. Läuft ein langlebiger Prozess länger als 24h, muss der Token erneuert werden.

> **Airflow 3.1+:** Permanente API Keys (kein Ablauf) sind als experimentelles Feature in Vorbereitung und werden dieses Token-Handling künftig vereinfachen.

### Astronomer Cosmos + Airflow 3: Zwei kritische Bugs

Beim Einsatz von **astronomer-cosmos 1.13.1** mit Airflow 3.0.2 traten zwei zusammenhängende Probleme auf:

**Problem 1: `ParamValidationError` auf `__cosmos_telemetry_metadata__`**

Cosmos fügt jedem DAG einen versteckten Parameter `__cosmos_telemetry_metadata__` hinzu, der Telemetrie-Daten als Base64-encodierten String enthält. Dieser wird mit einem JSON-Schema `const`-Constraint validiert. In Airflow 3 ist die Param-Validierung **strikt**: Der Hash wird beim DAG-Parsing generiert, ändert sich aber bis zur Task-Execution (z.B. durch Cache-Invalidierung), wodurch die Validierung fehlschlägt.

```
ParamValidationError: Invalid input for param __cosmos_telemetry_metadata__:
'eNpl...' was expected
Failed validating 'const' in schema
```

**Lösung:**
```yaml
AIRFLOW__COSMOS__PROPAGATE_TELEMETRY: "False"
```

**Problem 2: DAG-Parse-Timeout bei `dbt ls` ohne Cache**

Wenn man den Cosmos-Cache deaktiviert (als Workaround für Problem 1), führt Cosmos bei **jedem DAG-Parse** `dbt deps` + `dbt ls` aus. In einem Container mit dbt im isolierten venv dauert das >30 Sekunden und überschreitet den Airflow `dagbag_import_timeout`:

```
AirflowTaskTimeout: DagBag import timeout for dag_dbt_cosmos.py after 30.0s
```

**Lösung: `LoadMode.DBT_MANIFEST`**

Statt `dbt ls` beim Parsen liest Cosmos ein vorbereitetes `manifest.json`:
```python
from cosmos.constants import LoadMode

render_config=RenderConfig(
    load_method=LoadMode.DBT_MANIFEST,
)
project_config=ProjectConfig(
    dbt_project_path=DBT_PROJECT_PATH,
    manifest_path=DBT_PROJECT_PATH / "target" / "manifest.json",
)
```

Das Manifest wird beim Container-Start (`airflow-init`) und bei Bedarf im laufenden Betrieb generiert. Dazu den Utility-DAG **`refresh_cosmos_manifest`** in der Airflow UI triggern — kein Restart oder Docker-Rebuild nötig. Der nächste DAG-Parse-Zyklus (~5 Min) übernimmt die Änderungen automatisch. **Vorteil**: Sofortiges DAG-Parsing ohne Subprocess-Aufruf.

> **Beide Probleme existieren in Airflow 2.x nicht**, weil dort (a) die Param-Validierung weniger strikt ist und (b) längere Parse-Zeiten toleriert werden.

### Weitere Upgrade-Stolpersteine

1. **`EXECUTION_API_SERVER_URL` ist Pflicht**
   ```yaml
   AIRFLOW__CORE__EXECUTION_API_SERVER_URL: "http://airflow-apiserver:8080/execution/"
   ```
   Ohne diese Variable wissen Scheduler-Worker nicht, wo der API-Server läuft.

2. **BashOperator Import-Pfad geändert**
   ```python
   # Alt (Airflow 2.x) - funktioniert noch, gibt Deprecation-Warning
   from airflow.operators.bash import BashOperator
   # Neu (Airflow 3.x)
   from airflow.providers.standard.operators.bash import BashOperator
   ```

3. **FAB-Provider für User Management**
   ```
   pip install apache-airflow-providers-fab
   ```
   Ohne diesen Provider gibt es keinen `airflow users create` Befehl und keine Web-Login-Seite.

4. **`schedule_interval` entfernt** - nur noch `schedule=None` oder `schedule="@daily"` etc.

5. **Offizielles Docker-Compose als Referenz nutzen**
   Das offizielle Template unter `https://airflow.apache.org/docs/apache-airflow/3.0.2/docker-compose.yaml` ist die beste Ausgangsbasis. Diese Demo verwendet CeleryExecutor + Redis + Flower für Worker-Skalierung und Monitoring.

---

## Technische Details

### dbt + AutomateDV

- **dbt-postgres** läuft in einem isolierten Python-venv (`/home/airflow/dbt_venv/`) innerhalb der Airflow-Container, um Dependency-Konflikte mit Airflow 3 zu vermeiden (insb. `protobuf`).
- **AutomateDV 0.11.1** - die Macros `hub`, `link`, `sat`, `stage` und `pit` funktionieren auf Postgres. Die Macros `bridge` und `eff_sat` sind [deprecated](https://github.com/Datavault-UK/automate-dv/blob/master/macros/tables/postgres/bridge.sql) und wurden daher nicht verwendet.
- **Staging-Modelle** nutzen das `automate_dv.stage` Macro mit `source()`-Referenzen für automatisches Hashing (MD5) der Business Keys und Hashdiffs.

### Projekt-Struktur
```
new_env/
├── docker-compose.yml          # Core: 11 Services (Postgres, Redis, 6x Airflow, dbt-docs, pgAdmin, Streamlit)
├── docker-compose.bi.yml       # Optional: Metabase für BI + DQ-Dashboards (~+700 MB)
├── docker-compose.mcp.yml      # Optional: Open WebUI + MCP-Gateway für KI-Assistenz (~+800 MB)
├── Dockerfile.airflow          # Airflow 3 + Cosmos + dbt (isolierter venv)
├── Dockerfile.dbt              # dbt für Docs-Server
├── Dockerfile.mcp              # MCP-Gateway (mcpo + dbt-mcp + airflow-mcp)
├── .env                        # Shared Secrets (Fernet Key, JWT)
├── daten/                      # CSV-Quelldaten
├── dbt_project/                # Komplettes dbt-Projekt
│   ├── models/staging/         # 5 Staging-Views (AutomateDV stage macro)
│   ├── models/raw_vault/       # 4 Hubs, 3 Links, 5 Sats, 2 PITs
│   └── models/marts/           # 3 Business-Tabellen
├── airflow/
│   ├── dags/                   # 12 DAGs (init, classic, cosmos, cosmos_split x3, delta x2, psa x3, dq_persist)
│   │   └── scripts/            # Python-Utilities für DAGs
│   └── config/airflow.cfg      # Shared Secrets für JWT-Auth
├── mcp/
│   ├── mcp-config.json         # mcpo-Konfiguration (Airflow-MCP + dbt-MCP für Open WebUI)
│   └── claude-desktop-config.json  # Template: Alle 4 MCPs für Claude Desktop / Claude Code
├── metabase/
│   ├── metabase-config.yml     # Admin-User + PostgreSQL-Verbindung (auto-setup)
│   ├── plugins/                # Community-Plugins (duckdb.metabase-driver.jar)
│   └── setup-metabase.sh       # Einrichtungs-Script (erster Start)
├── duckdb/                     # DuckDB-Dateien hier ablegen → /duckdb/ im Container
├── postgres/init/              # DB-Init-Scripte (Schemas inkl. dq)
└── streamlit/                  # Portal-App
```

---

## Datenqualität mit dbt-expectations

Neben den Standard-Tests (`unique`, `not_null`) setzt diese Demo [dbt-expectations](https://github.com/calogica/dbt-expectations) ein - eine dbt-native Portierung von Great Expectations mit über 50 fachlichen Test-Macros.

### Test-Übersicht (19 Tests)

| Schicht | Test | Zweck | Ergebnis |
|---------|------|-------|----------|
| **Staging** | `expect_column_pair_values_A_to_be_greater_than_B` | shipped_date >= order_date | **WARN: 304 Verletzungen!** |
| **Staging** | `expect_column_values_to_match_regex` | Email-Format-Validierung | PASS |
| **Staging** | `expect_column_values_to_be_between` | Versandgebühr 0-100, Rabatt 0-100%, Preis > 0, Menge > 0 | PASS |
| **Staging** | `expect_column_distinct_count_to_equal` | Genau 4 Bestellstatus | PASS |
| **Raw Vault** | `expect_column_proportion_of_unique_values` | Hub: 100% unique, Satellite: < 100% (Historisierung!) | PASS |
| **Raw Vault** | `expect_compound_columns_to_be_unique` | Link-Kombinationen eindeutig | PASS |
| **Raw Vault** | `expect_row_values_to_have_recent_data` | Frische Daten nach Delta-Load | PASS |
| **Marts** | `expect_table_row_count_to_be_between` | Plausible Zeilenanzahl | PASS |
| **Marts** | `expect_column_values_to_be_between` | Revenue > 0, Orders >= 1, Menge >= 0 | PASS |

### Demo-Highlight: shipped_date < order_date

Der Test `expect_column_pair_values_A_to_be_greater_than_B` deckt auf, dass **304 von 605 Bestellungen** ein `shipped_date` haben, das **vor** dem `order_date` liegt - ein echtes Datenqualitätsproblem, das kein `not_null`/`unique`-Test je finden würde.

```bash
# Tests ausführen (innerhalb des Airflow-Workers):
dbt test --select tag:quality tag:vault-integrity tag:freshness tag:business-logic
```

### DQ-Monitoring: Streamlit vs. Metabase

Die Demo bietet **zwei Wege** zur Visualisierung der Datenqualität:

| | Streamlit (integriert) | Metabase (optional) |
|--|----------------------|-------------------|
| **Datenquelle** | `run_results.json` (Dateisystem) | `dq.test_results` (PostgreSQL) |
| **Historie** | Nur letzter Run pro Domain | Alle Runs (akkumuliert) |
| **Setup** | Eingebaut, keine Konfiguration | `docker-compose.bi.yml` aktivieren |
| **Stärke** | Sofort sichtbar, kein extra Schritt | Trends über Zeit, interaktive Dashboards |
| **Zielgruppe** | Entwickler, schnelle Prüfung | Fachbereich, Management-Reporting |

**Typischer Schulungsablauf:**
1. Streamlit-Tab zeigen: "So sieht das integriert aus"
2. Metabase zeigen: "So sieht das in der Praxis aus - mit Historie und BI-Dashboards"

> **Setup:** Metabase starten und einrichten → siehe [Option 1: Metabase](#option-1-metabase-bi--dq-dashboards) im Installations-Abschnitt.

#### DuckDB-Dateien in Metabase

DuckDB-Dateien aus `./duckdb/` sind direkt in Metabase verfügbar (Plugin vorinstalliert, Volume gemountet). Verbindung anlegen unter: Einstellungen → Datenbanken → DuckDB → Pfad: `/duckdb/dateiname.duckdb`

> **Hinweis:** Der Ordner `./duckdb/` ist gitignored — DuckDB-Dateien werden nicht ins Repository eingecheckt.

**DQ-Dashboard in Metabase (Beispiel-Abfragen):**

```sql
-- Übersicht: Pass/Fail pro Schicht
SELECT layer, status, COUNT(*) AS anzahl
FROM dq.test_results
WHERE run_at = (SELECT MAX(run_at) FROM dq.test_results)
GROUP BY layer, status
ORDER BY layer, status;

-- Trend: Fehlgeschlagene Tests über alle Runs
SELECT DATE(run_at) AS datum, COUNT(*) AS fehler
FROM dq.test_results
WHERE status IN ('fail', 'error')
GROUP BY DATE(run_at)
ORDER BY datum;

-- Detail: Alle Warnungen und Fehler
SELECT run_at, domain, model_name, test_name, status, failures, message
FROM dq.test_results
WHERE status IN ('fail', 'warn', 'error')
ORDER BY run_at DESC;
```

**BI-Dashboards auf Mart-Tabellen:**

Metabase erkennt automatisch die Mart-Tabellen (`mart.mart_revenue_per_customer`, `mart.mart_order_overview`, `mart.mart_product_sales`). Damit lassen sich in wenigen Klicks BI-Dashboards erstellen - z.B. Umsatz pro Kunde, Top-Produkte, Bestelltrends.

---

## Plattform-Kompatibilität (macOS, Windows, Ubuntu)

Die gesamte Umgebung läuft in Docker-Containern und ist plattformunabhängig. **Kein einziges File im Projekt muss geändert werden** - Docker löst die Architektur (ARM vs. Intel) automatisch auf. Alle verwendeten Base-Images (`postgres:16-alpine`, `apache/airflow:3.0.2`, `python:3.11-slim`, `redis:7.2-bookworm`, `dpage/pgadmin4`) bieten Multi-Arch-Images an.

| Thema | macOS (ARM) | Windows (Intel) | Ubuntu (Intel) |
|-------|-------------|-----------------|----------------|
| **Docker** | Docker Desktop | Docker Desktop (WSL2) | Docker Engine (nativ) |
| **Images** | `linux/arm64` (auto) | `linux/amd64` (auto) | `linux/amd64` (auto) |
| **Performance** | Gut (native ARM) | Etwas langsamer (WSL2-Layer) | Am schnellsten (nativer Kernel) |
| **Pfade/Ports** | Identisch | Identisch | Identisch |

### Windows-spezifische Hinweise

- **Docker Desktop mit WSL2-Backend** muss aktiviert sein (nicht Hyper-V)
- Repo am besten unter WSL2 klonen (`/home/user/`, nicht `/mnt/c/`) für deutlich bessere I/O-Performance bei Volume-Mounts
- Line-Endings: `git config core.autocrlf input` setzen, damit CSVs und Shell-Scripts LF behalten

### Ubuntu-spezifische Hinweise

- Docker Engine statt Docker Desktop (leichtgewichtiger):
  ```bash
  sudo apt install docker.io docker-compose-v2
  sudo usermod -aG docker $USER
  ```
- Nach dem `usermod`-Befehl neu einloggen oder `newgrp docker` ausführen

---

## Schutz historischer Daten im Data Vault

### Das Problem

In einem Data Vault speichern Satellites die **komplette Änderungshistorie** jeder Entität. Diese Historisierung ist der Kernwert des Modells - einmal verloren, ist sie nicht aus den Quelldaten rekonstruierbar (die Quellsysteme halten typischerweise nur den aktuellen Stand).

**Gefahr:** Ein versehentliches `dbt run --full-refresh` auf den Raw Vault Modellen würde alle Satellite-Tabellen droppen und neu aufbauen - nur mit den aktuellen Daten aus Staging. Die gesamte Historie wäre unwiderruflich verloren.

In klassischen dbt-Projekten (ohne Data Vault) nutzt man `dbt snapshot` für SCD2-Historisierung. Snapshots haben einen eingebauten Schutzmechanismus: Sie sind als eigenständiger Befehl (`dbt snapshot`) von `dbt run` getrennt. Bei AutomateDV sind die Satellites jedoch normale `incremental`-Modelle, die über `dbt run` gesteuert werden - und damit anfällig für `--full-refresh`.

### Schutzmechanismen in dieser Demo

#### 1. `full_refresh: false` (aktiv)

In `dbt_project.yml` ist für alle Raw Vault Modelle `full_refresh: false` konfiguriert:

```yaml
raw_vault:
  +materialized: incremental
  +full_refresh: false   # Blockiert --full-refresh komplett
```

**Effekt:** Wenn jemand `dbt run --full-refresh` ausführt, werden Raw Vault Modelle **übersprungen** statt neu aufgebaut. dbt gibt eine Warnung aus:
```
Refusing to full-refresh model 'hub_customer' because full_refresh is set to false.
```

Das ist der stärkste dbt-native Schutz und seit dbt 1.6 verfügbar.

#### 2. Airflow DAGs ohne --full-refresh

Die DAGs `dbt_classic` und `dbt_cosmos` verwenden **bewusst kein** `--full-refresh` Flag auf den Raw Vault Modellen. Der `load_delta` DAG nutzt `dbt run` (ohne Flag), damit AutomateDVs eingebaute Incremental-Logik greift.

#### 3. Weitere Mitigationen (für Produktionsumgebungen)

| Massnahme | Beschreibung | Aufwand |
|-----------|-------------|---------|
| **Postgres-Permissions** | `REVOKE TRUNCATE, DROP ON ALL TABLES IN SCHEMA raw_vault FROM dbt_user` - der dbt-User darf nur INSERT/UPDATE. Ein separater Admin-User behält DROP-Rechte. | Mittel |
| **Backup vor Full-Refresh** | Pre-Hook in dbt der vor jedem Full-Refresh ein `pg_dump` des raw_vault Schemas erstellt. | Mittel |
| **CI/CD Guard** | In der CI-Pipeline prüfen ob ein PR `--full-refresh` auf Raw Vault Modelle anwendet und den Build blockieren. | Niedrig |
| **dbt tags + selectors** | Raw Vault Modelle taggen (`+tags: ["protected"]`) und Team-Konvention: `--full-refresh` nur mit explizitem `--exclude tag:protected`. | Niedrig |
| **Monitoring** | Alert wenn die Zeilenanzahl eines Satellites sinkt (sollte monoton steigen). Umsetzbar mit `dbt_expectations.expect_table_row_count_to_be_between` und einem dynamischen Minimum aus der letzten Ausführung. | Mittel |

#### 4. Architektonischer Schutz: Vorgeschaltete PSA

Die stärkste Variante ist **architektonischer Schutz** durch eine vorgeschaltete **Persistent Staging Area (PSA)**. Statt die Historisierung nur im Data Vault zu halten (wo sie von `--full-refresh` bedroht ist), wird sie in einer separaten, von dbt unabhängigen Schicht verwaltet:

```
OHNE PSA:   CSV → Raw → Data Vault (Historisierung HIER, verwundbar)
MIT PSA:    CSV → Raw → PSA (Historisierung HIER, stabil) → Data Vault (rebuild-fähig)
```

**Wie das funktioniert:**
- Die PSA wird durch Stored Procedures (z.B. aus einem Code-Generator) verwaltet, nicht durch dbt
- SCD2-Historisierung, Content-Hashing und Delete Detection passieren in der PSA
- dbt liest aus der PSA (`source('psa', 'v_customers_cur')`) und baut den Data Vault daraus auf
- Der Data Vault wird zur **deterministischen Funktion der PSA** — jederzeit neu generierbar

**Warum das schützt:**
- `dbt run --full-refresh` auf den Vault? Kein Problem — Rebuild aus PSA, kein Datenverlust
- Modellumbau im Data Vault? PSA bleibt unangetastet, neues Modell wird aus derselben Datenbasis gebaut
- Bug in einer Transformation? PSA-Daten sind korrekt, Core wird idempotent neu aufgebaut

Diese Demo enthält einen optionalen PSA-Pfad (DAGs `psa_flow` und `psa_rebuild_demo`), der genau dieses Konzept demonstriert. Siehe [PSA-Pfad mit NG Generator](#psa-pfad-mit-ng-generator-optionaler-alternativer-datenfluss) für Details.

> **Zusammenfassung:** Für die Demo-Umgebung reicht `full_refresh: false` als Minimalschutz. In Produktionsumgebungen empfehlen wir eine vorgeschaltete PSA als architektonischen Schutz, ergänzt durch Postgres-Permissions und Monitoring.

---

## DAG-Splitting-Strategie für Produktion

In einer Produktionsumgebung reicht ein einzelner dbt-DAG nicht aus. Verschiedene Quellen, Fachbereiche und Konsumenten haben unterschiedliche Ladezeiten, Abhängigkeiten und Verantwortlichkeiten. Hier ist die empfohlene Strategie:

### Prinzip: Tags als zentraler Steuerungsmechanismus

dbt-Tags auf Modell-Ebene definieren die fachliche Zugehörigkeit. Einmal definiert, können sie überall verwendet werden - in Cosmos DAGs, dbt selectors und CI/CD:

```yaml
# dbt_project.yml
models:
  northwind_vault:
    raw_vault:
      crm:
        +tags: ["domain_crm"]
      logistics:
        +tags: ["domain_logistics"]
    marts:
      sales:
        +tags: ["consumption_sales"]
      controlling:
        +tags: ["consumption_controlling"]
```

### Empfohlene DAG-Struktur

```
PSA (pro Quelle)                    dbt/Cosmos (pro Domain)           Consumption
┌─────────────┐                    ┌──────────────────────┐      ┌─────────────────┐
│ psa_crm     │──┐                 │ dv_crm               │──┐   │ cons_sales      │
│ @02:00      │  │  Dataset/       │ @03:00 oder Dataset  │  │   │ @06:00          │
└─────────────┘  │  Sensor         │ Tag: domain_crm      │  │   │ Tag: cons_sales │
┌─────────────┐  ├────────────────>│                      │  ├──>│                 │
│ psa_erp     │──┘                 └──────────────────────┘  │   └─────────────────┘
│ @02:00      │──┐                 ┌──────────────────────┐  │   ┌─────────────────┐
└─────────────┘  ├────────────────>│ dv_logistics         │──┘   │ cons_ctrl       │
┌─────────────┐  │                 │ @04:00 oder Dataset  │──┐   │ @06:30          │
│ psa_wms     │──┘                 │ Tag: domain_logistics│  ├──>│                 │
│ @02:30      │                    └──────────────────────┘  │   └─────────────────┘
└─────────────┘                    ┌──────────────────────┐  │
                                   │ dv_nightly_full      │──┘
                                   │ @So 01:00 (weekly)   │
                                   │ Alles, full rebuild  │
                                   └──────────────────────┘
```

**Drei Schichten, drei Verantwortlichkeiten:**

| Schicht | Splitting | Schedule | Trigger |
|---------|-----------|----------|---------|
| **PSA** (NG Generator) | Pro Quelle | Zeitgesteuert | Unabhängig |
| **Core/Vault** (dbt/Cosmos) | Pro fachliche Domain | Nach PSA | Dataset oder Sensor |
| **Consumption** (dbt/Cosmos) | Pro Konsument/Bereich | Nach Core | Dataset oder zeitversetzt |

### Cosmos: Mehrere DAGs mit Tag-Selektion

Pro Domain ein eigener Cosmos-DAG mit `select`:

```python
# dag_dv_crm.py
DbtDag(
    dag_id="dv_crm",
    schedule="0 3 * * *",
    render_config=RenderConfig(
        select=["tag:domain_crm"],
    ),
    ...
)
```

### Cosmos: DbtTaskGroup für Sub-Graphs

Falls mehrere Domains in einem DAG mit expliziter Reihenfolge laufen sollen:

```python
with DAG("dv_nightly") as dag:
    psa_done = ExternalTaskSensor(task_id="wait_psa", ...)

    with DbtTaskGroup(group_id="crm",
         select=["tag:domain_crm"], ...) as crm:
        pass
    with DbtTaskGroup(group_id="logistics",
         select=["tag:domain_logistics"], ...) as logistics:
        pass
    with DbtTaskGroup(group_id="sales",
         select=["tag:consumption_sales"], ...) as sales:
        pass

    psa_done >> [crm, logistics] >> sales
```

### Cross-DAG Dependencies: PSA triggert dbt

**Variante A - Airflow Datasets (empfohlen für Airflow 3):**
```python
# PSA-DAG publiziert Dataset nach erfolgreichem Load:
psa_dataset = Dataset("psa://crm_loaded")

@task(outlets=[psa_dataset])
def mark_psa_done(): pass

# dbt-DAG wird automatisch getriggert:
DbtDag(
    dag_id="dv_crm",
    schedule=[psa_dataset],  # Startet wenn PSA fertig
    ...
)
```

**Variante B - ExternalTaskSensor (klassisch):**
```python
wait_for_psa = ExternalTaskSensor(
    task_id="wait_psa",
    external_dag_id="psa_flow",
    external_task_id="load_customers",
)
wait_for_psa >> crm_task_group
```

### dbt Selectors für komplexe Auswahl

Für fortgeschrittene Szenarien (z.B. nur inkrementelle Modelle einer Domain):

```yaml
# selectors.yml
selectors:
  - name: crm_incremental
    definition:
      intersection:
        - method: tag
          value: domain_crm
        - method: config.materialized
          value: incremental
```

Referenz in Cosmos: `select=["selector:crm_incremental"]`

### Konfiguration von Code trennen (Best Practice)

In der Praxis werden DAGs betrieblich angepasst - Ladezeitpunkte verschieben sich, Abhängigkeiten ändern sich, einzelne Modelle werden temporär ausgeschlossen. Diese Anpassungen gehen verloren wenn DAG-Dateien über CI/CD redeployed oder regeneriert werden.

**Das Problem:**

| Anpassung | Wo gespeichert | Risiko bei Redeployment |
|-----------|---------------|------------------------|
| Schedule `0 3 * * *` → `0 3,15 * * *` | Hardcodiert im DAG-File | überschrieben |
| Zusätzliche Sensor-Dependency | Hardcodiert im DAG-File | überschrieben |
| `exclude` für experimentelle Modelle | Hardcodiert im DAG-File | überschrieben |
| Airflow Variable `dv_crm_config` | Airflow-Datenbank | **Persistiert** |

**Lösung: Betriebliche Parameter in Airflow Variables auslagern:**

```python
# dag_dv_crm.py - Code ist generierbar/wiederherstellbar
from airflow.models import Variable
import json

config = json.loads(Variable.get("dv_crm_config", default_var='{}'))

DbtDag(
    dag_id="dv_crm",
    schedule=config.get("schedule", "0 3 * * *"),       # Default im Code
    render_config=RenderConfig(
        select=config.get("select", ["tag:domain_crm"]),
        exclude=config.get("exclude", []),
    ),
    ...
)
```

**Konfiguration in der Airflow UI** (Admin → Variables):

```json
// Variable: dv_crm_config
{
  "schedule": "0 3,15 * * *",
  "select": ["tag:domain_crm"],
  "exclude": ["tag:experimental"],
  "notes": "2x täglich seit 2026-03-10, Ticket OPS-1234"
}
```

**Vorteile dieses Patterns:**
- **DAG-Code ist regenerierbar** - Git-Deployment überschreibt keine betrieblichen Anpassungen
- *Änderungen ohne Git-Commit** - Betrieb kann Schedules/Excludes sofort anpassen
- **Audit-Trail** - Variable Änderungen werden in der Airflow-DB geloggt
- **Defaults im Code** - Wenn keine Variable gesetzt ist, greifen sinnvolle Defaults
- **Dokumentation im Wert** - Das `notes`-Feld erklärt warum eine Anpassung gemacht wurde

> **Faustregel:** Alles was sich zwischen Deployments ändern kann (Schedules, Excludes, Timeouts, Retry-Counts) gehört in Airflow Variables. Alles was sich nur bei Modell-Änderungen ändert (Tags, Pfade, Projekt-Config) gehört in den Code.

> **Zusammenfassung:** Tags auf den dbt-Modellen sind die Grundlage. Darauf aufbauend erstellt man pro Fachbereich einen eigenen Cosmos-DAG (oder DbtTaskGroup). Cross-DAG-Abhängigkeiten (PSA → Core → Consumption) werden über Airflow Datasets oder ExternalTaskSensors gesteuert. Betriebliche Anpassungen (Schedules, Excludes) werden in Airflow Variables ausgelagert, damit sie Redeployments überleben. Ein wöchentlicher Full-Rebuild-DAG sichert die Gesamtkonsistenz.

---

## Debugging und Run-Dokumentation

### Das 3-Schichten-Problem

In einer Architektur mit Code-Generator + dbt + AutomateDV durchläuft jedes SQL-Statement drei Generierungsschichten:

```
Schicht 1: Code-Generator    YAML-Metadaten + Jinja-Templates  ->  dbt .sql/.yml Dateien
Schicht 2: dbt + AutomateDV  dbt-Jinja + AutomateDV Macros     ->  compiled SQL
Schicht 3: PostgreSQL         Führt das SQL aus                ->  Ergebnis / Fehler
```

Wenn ein Fehler auftritt, muss man zurückverfolgen:
- Ist das **generierte dbt-Modell** korrekt? (Schicht 1 → 2 Übergang)
- Ist das **compilierte SQL** korrekt? (Schicht 2 → 3 Übergang)
- Oder ist die **YAML-Metadaten-Definition** falsch? (Schicht 1 Eingang)

### dbt Artefakte verstehen

dbt schreibt bei jedem Lauf mehrere Artefakte in das `target/`-Verzeichnis:

| Verzeichnis / Datei | Inhalt | Debugging-Nutzen |
|---------------------|--------|-----------------|
| `target/compiled/` | Reines SELECT nach Jinja-Auflösung | "Was hat mein Jinja produziert?" |
| `target/run/` | Vollständiges SQL inkl. DDL (`CREATE TABLE AS`, `MERGE`) | "Was hat Postgres tatsächlich bekommen?" |
| `manifest.json` | Kompletter Projekt-Graph inkl. `compiled_code` pro Node | Programmatisch auswertbar, enthält compiled SQL |
| `run_results.json` | Status, Laufzeit, Fehler pro Node | Test-/Run-Ergebnisse |
| `sources.json` | Freshness-Ergebnisse pro Source | Datenaktualität |
| `catalog.json` | Warehouse-Katalog (Spalten, Typen, Statistiken) | Für dbt Docs |

**Wichtig:** Das `target/`-Verzeichnis wird bei **jedem** dbt-Lauf überschrieben. Für Audit-Zwecke müssen die Artefakte nach jedem Lauf archiviert werden (siehe unten).

### Debugging-Workflow

**1. Schicht isolieren:**

```bash
# Was hat der Code-Generator erzeugt? (Schicht 1 Output)
cat dbt_project/models/raw_vault/hubs/hub_customer.sql

# Was hat dbt daraus kompiliert? (Schicht 2 Output, reines SELECT)
dbt compile --select hub_customer
cat target/compiled/northwind_vault/models/raw_vault/hubs/hub_customer.sql

# Was wurde tatsächlich ausgeführt? (Schicht 3 Input, mit DDL)
cat target/run/northwind_vault/models/raw_vault/hubs/hub_customer.sql
```

**2. SQL-Diff vor/nach Änderungen:**

```bash
# Zustand VOR der Änderung sichern:
dbt compile
cp -r target/compiled target/compiled_BEFORE

# Code-Generator Config ändern, neu generieren, dann:
dbt compile
diff -r target/compiled_BEFORE target/compiled
```

Dieser Workflow ist besonders wertvoll bei AutomateDV-Konfigurationsänderungen - man sieht exakt wie sich eine YAML-Änderung auf das generierte SQL auswirkt.

**3. Debug-Kommentare in Generator-Templates:**

```sql
-- NG_TEMPLATE: hub.sql.j2 v1.3
-- NG_SOURCE: customers.yml
-- NG_GENERATED: 2026-03-17T08:00:00
{{ automate_dv.hub(src_pk=src_pk, ...) }}
```

Diese Kommentare überleben die dbt-Kompilierung und erscheinen im `target/compiled/` Output. So kann man bei einem Fehler sofort sehen, welches Template und welche Metadaten-Version den Code erzeugt haben.

**4. Jinja-Logging für Runtime-Debugging:**

```sql
{%- set my_var = some_expression -%}
{{ log("DEBUG my_var = " ~ my_var, info=True) }}
```

Gibt Werte während der Kompilierung auf die Konsole aus, ohne das SQL zu beeinflussen.

### Run-Dokumentation: Artefakte archivieren

Das `target/`-Verzeichnis wird bei jedem Lauf überschrieben. Für eine lückenlose Dokumentation müssen die Artefakte nach jedem Lauf archiviert werden.

**Ansatz 1: Archiv-Task in Airflow (einfach)**

```python
archive = BashOperator(
    task_id="archive_artifacts",
    bash_command="""
        TIMESTAMP=$(date +%Y%m%d_%H%M%S)
        mkdir -p /usr/app/dbt/artifact_archive/${TIMESTAMP}
        cp /usr/app/dbt/target/manifest.json /usr/app/dbt/artifact_archive/${TIMESTAMP}/
        cp /usr/app/dbt/target/run_results.json /usr/app/dbt/artifact_archive/${TIMESTAMP}/
        cp -r /usr/app/dbt/target/compiled /usr/app/dbt/artifact_archive/${TIMESTAMP}/
        cp -r /usr/app/dbt/target/run /usr/app/dbt/artifact_archive/${TIMESTAMP}/
    """,
    trigger_rule=TriggerRule.ALL_DONE,  # Läuft IMMER, auch bei Fehlern
)
```

**Ansatz 2: dbt-artifacts Package (Daten in Postgres)**

```yaml
# packages.yml
packages:
  - package: brooklyn-data/dbt_artifacts
    version: [">=2.6.0", "<3.0.0"]

# dbt_project.yml
on-run-end:
  - "{{ dbt_artifacts.upload_results(results) }}"
```

Schreibt Run-Resultate als Tabellen in die Datenbank. Damit sind historische Laufergebnisse per SQL abfragbar und können in Streamlit visualisiert werden.

**Ansatz 3: Elementary (umfassende Reports)**

```yaml
# packages.yml
packages:
  - package: elementary-data/elementary
    version: [">=0.15.0", "<1.0.0"]
```

Elementary generiert mit `edr report` ein **komplettes HTML-Dashboard** pro Run: Teststatus, Laufzeiten, Anomalie-Erkennung, Lineage. Der Befehl kann als Post-Run-Task in Airflow ausgeführt werden.

### Immer-laufender Report-Task in Airflow

Um einen Dokumentations-Task anzuhängen der **unabhängig vom Erfolg** der vorherigen Tasks läuft, muss man von `DbtDag` auf `DbtTaskGroup` wechseln:

```python
from airflow.utils.trigger_rule import TriggerRule

with DAG("dbt_cosmos_with_reporting") as dag:

    dbt_tasks = DbtTaskGroup(
        group_id="dbt_run",
        ...
    )

    archive = BashOperator(
        task_id="archive_artifacts",
        bash_command="...",
        trigger_rule=TriggerRule.ALL_DONE,  # Läuft IMMER
    )

    report = PythonOperator(
        task_id="generate_report",
        python_callable=generate_run_report,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    dbt_tasks >> archive >> report
```

**Relevante Trigger Rules:**

| Rule | Verhalten | Einsatz |
|------|-----------|---------|
| `all_success` | Nur wenn alle Upstream-Tasks erfolgreich (Default) | Normale Abhängigkeiten |
| `all_done` | Wenn alle Upstream-Tasks fertig (egal ob success/fail/skip) | Archivierung, Reports |
| `one_failed` | Mindestens ein Upstream-Task fehlgeschlagen | Frühe Alarmierung |
| `none_failed` | Kein Upstream-Task fehlgeschlagen (skip erlaubt) | Bedingte Pfade |

### Empfehlung je nach Reifegrad

| Reifegrad | Lösung | Aufwand |
|-----------|---------|---------|
| **Einstieg** | `target/compiled/` manuell inspizieren + `dbt compile --select` | Kein Aufwand |
| **Standard** | Archiv-Task in Airflow + Streamlit-Datenqualitäts-Tab (bereits vorhanden) | Niedrig |
| **Fortgeschritten** | dbt-artifacts Package → Run-Historie in Postgres → Trend-Dashboards | Mittel |
| **Enterprise** | Elementary + `edr report` → HTML-Reports pro Run + Slack-Benachrichtigung | Mittel-Hoch |

---

## Multi-Environment Deployment (DEV / INT / PROD)

### Grundprinzip: Ein Branch, Environment per Variable

Statt branch-per-environment (führt zu Drift und Merge-Konflikten) wird empfohlen, einen einzigen `main`-Branch zu verwenden. Das Verhalten pro Umgebung wird durch Environment-Variablen gesteuert:

```python
# In jedem DAG:
import os
ENV = os.getenv("AIRFLOW_ENV", "dev")

SCHEDULES = {
    "dev": None,           # nur manuell
    "int": "@daily",       # täglich für Integrationstests
    "prod": "0 6 * * *",   # 06:00 in Produktion
}
```

### Dateistruktur für Multi-Environment

```
new_env/
├── docker-compose.yml                  # Basis-Konfiguration (alle Umgebungen)
├── environments/
│   ├── dev/
│   │   ├── .env                        # Secrets + AIRFLOW_ENV=dev
│   │   └── docker-compose.override.yml # Dev-spezifische Overrides
│   ├── int/
│   │   ├── .env
│   │   └── docker-compose.override.yml
│   └── prod/
│       ├── .env
│       └── docker-compose.override.yml
├── dbt_project/
│   └── profiles.yml                    # Multi-Target (dev/int/prod)
└── airflow/dags/                       # Lesen AIRFLOW_ENV für Verhalten
```

Start pro Umgebung:
```bash
docker compose -f docker-compose.yml -f environments/prod/docker-compose.override.yml up -d
```

### Environment-spezifische Overrides

```yaml
# environments/prod/docker-compose.override.yml
services:
  airflow-scheduler:
    environment:
      AIRFLOW_ENV: prod
      DBT_TARGET: prod
      POSTGRES_HOST: prod-db.internal
      AIRFLOW_CONN_DEMO_POSTGRES: postgresql://prod_user:${PROD_DB_PASS}@prod-db:5432/prod_warehouse
      AIRFLOW_VAR_DBT_TARGET: prod
      AIRFLOW_VAR_ALERT_EMAIL: team@company.com
```

### Connections und Variables: Nie von DEV kopieren

Jede Umgebung hat eigene Credentials. Die empfohlene Methode ist `AIRFLOW_CONN_*` und `AIRFLOW_VAR_*` Environment-Variablen:

| Konfiguration | Methode | Beispiel |
|---------------|---------|---------|
| **DB-Credentials** | `AIRFLOW_CONN_*` Env-Var | `AIRFLOW_CONN_DEMO_POSTGRES=postgresql://...` |
| **Nicht-geheime Config** | `AIRFLOW_VAR_*` Env-Var | `AIRFLOW_VAR_DBT_TARGET=prod` |
| **Secrets** | `.env`-Datei (gitignored) | `PROD_DB_PASS=...` |
| **Runtime-veränderbare Werte** | Airflow Variables in der DB | Feature-Flags, Schedules |

### dbt profiles.yml: Multi-Target

```yaml
northwind_vault:
  target: "{{ env_var('DBT_TARGET', 'dev') }}"
  outputs:
    dev:
      type: postgres
      host: "{{ env_var('POSTGRES_HOST', 'localhost') }}"
      port: 5432
      user: "{{ env_var('POSTGRES_USER', 'demo_user') }}"
      password: "{{ env_var('POSTGRES_PASSWORD', 'demo_pass') }}"
      dbname: "{{ env_var('POSTGRES_DB', 'demo') }}"
      schema: public
      threads: 4
    int:
      type: postgres
      host: "{{ env_var('POSTGRES_HOST', 'int-db') }}"
      port: 5432
      user: "{{ env_var('POSTGRES_USER', 'int_user') }}"
      password: "{{ env_var('POSTGRES_PASSWORD', '') }}"
      dbname: "{{ env_var('POSTGRES_DB', 'int_warehouse') }}"
      schema: public
      threads: 4
    prod:
      type: postgres
      host: "{{ env_var('POSTGRES_HOST', 'prod-db') }}"
      port: 5432
      user: "{{ env_var('POSTGRES_USER', 'prod_user') }}"
      password: "{{ env_var('POSTGRES_PASSWORD', '') }}"
      dbname: "{{ env_var('POSTGRES_DB', 'prod_warehouse') }}"
      schema: public
      threads: 8
```

### Schema-Isolation zwischen Umgebungen

**Option A - Separate Datenbanken (empfohlen):**

| Umgebung | Datenbank | Schemas |
|----------|-----------|---------|
| DEV | `dev_warehouse` | `staging`, `raw_vault`, `mart` |
| INT | `int_warehouse` | `staging`, `raw_vault`, `mart` |
| PROD | `prod_warehouse` | `staging`, `raw_vault`, `mart` |

**Option B - Schema-Prefix (gleiche Datenbank):**

Über ein angepasstes `generate_schema_name` Macro:
```sql
{% macro generate_schema_name(custom_schema_name, node) %}
    {% set env_prefix = var('env_prefix', '') %}
    {% if env_prefix and custom_schema_name %}
        {{ env_prefix }}_{{ custom_schema_name }}
    {% elif custom_schema_name %}
        {{ custom_schema_name }}
    {% else %}
        {{ target.schema }}
    {% endif %}
{% endmacro %}
```

Ergibt z.B. `int_staging`, `int_raw_vault` etc.

### Cosmos-DAG: Environment-aware

```python
ENV = os.getenv("AIRFLOW_ENV", "dev")
DBT_TARGET = os.getenv("DBT_TARGET", "dev")

CONN_IDS = {"dev": "demo_postgres", "int": "int_postgres", "prod": "prod_postgres"}
SCHEDULES = {"dev": None, "int": "@daily", "prod": "0 6 * * *"}

profile_config = ProfileConfig(
    profile_name="northwind_vault",
    target_name=DBT_TARGET,
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id=CONN_IDS[ENV],
    ),
)

DbtDag(
    dag_id="dbt_cosmos",
    schedule=SCHEDULES[ENV],
    profile_config=profile_config,
    tags=["dbt", "cosmos", ENV],
    ...
)
```

### CI/CD Pipeline

```
PR erstellt   →  DAG-Parse-Check + dbt compile + Unit-Tests
Merge          →  Auto-Deploy DEV
               →  Auto-Deploy INT
               →  Manual Gate → Deploy PROD
```

**dbt Slim CI** (nur geänderte Modelle testen):
```bash
# Vergleich gegen Produktions-Manifest:
dbt build --select state:modified+ --state ./prod-artifacts/ --target int
```

Das verkürzt einen 10-Minuten-Full-Build auf ~30 Sekunden für inkrementelle Änderungen.

**DAG-Validierung in CI:**
```bash
# Prüft ob alle DAGs ohne Import-Fehler parsbar sind:
docker compose run --rm airflow-scheduler airflow dags list
# Exit-Code != 0 bei Fehlern
```

### Rollback-Strategie

```bash
# Jedes Deployment taggen:
git tag "deploy/prod/$(git rev-parse --short HEAD)"

# Rollback = vorherigen Tag auschecken:
git checkout deploy/prod/<previous-tag>
docker compose -f docker-compose.yml -f environments/prod/docker-compose.override.yml up -d --build
```

### Zusammenfassung

| Thema | Empfehlung |
|-------|------------|
| **Branching** | Ein Branch + `AIRFLOW_ENV` Environment-Variable |
| **Docker** | dbt-Projekt ins Image baken; DAGs per Volume-Mount |
| **Connections** | `AIRFLOW_CONN_*` Env-Vars pro Umgebung (nie von DEV kopieren) |
| **Secrets** | `.env`-Dateien pro Umgebung, gitignored |
| **dbt Targets** | Multi-Target `profiles.yml` mit `env_var()`, gesteuert durch `DBT_TARGET` |
| **Schema-Isolation** | Separate Datenbanken oder Schema-Prefix via Macro |
| **Cosmos** | Environment-aware DAGs (Schedule, Connection, Target) |
| **CI: DAGs** | `airflow dags list` für Parse-Check |
| **CI: dbt** | Slim CI mit `state:modified+` gegen Produktions-Manifest |
| **Promotion** | Auto-Deploy DEV/INT bei Merge; manuelles Gate für PROD |
| **Rollback** | Git-Tags pro Deploy + explizite Docker Image Tags |

---

## Demo: DAG-Splitting mit Datasets (cosmos_split)

### Überblick

Diese Demo zeigt den Unterschied zwischen einem **monolithischen Cosmos-DAG** (`dbt_cosmos` - alle 43 Tasks in einem DAG) und einer **aufgeteilten Version** (3 DAGs mit Dataset-Dependencies). In der Praxis mit 100+ DAGs ist die aufgeteilte Variante Standard.

**Vergleich auf einen Blick:**

| Aspekt | `dbt_cosmos` (monolithisch) | `cosmos_master` → `cosmos_orders` → `cosmos_marts` (split) |
|--------|---------------------------|-----------------------------------------------------------|
| **Anzahl DAGs** | 1 | 3 |
| **Trigger** | Manuell | Dataset-Kette + Cron-Fallback |
| **Parallelität** | Innerhalb eines DAG | Über DAG-Grenzen hinweg |
| **Fehler-Isolation** | Ein Fehler stoppt alles | Nur die betroffene Domain stoppt |
| **Skalierung** | Ein Worker-Pool | Verschiedene Pools pro Domain möglich |
| **Monitoring** | Eine Statusanzeige | Pro Domain separat überwachbar |
| **Retry** | Gesamter Graph oder manuell | Pro Domain unabhängig |

### Architektur

```
┌────────────────────┐     Dataset: dbt://domain_master     ┌────────────────────┐     Dataset: dbt://domain_orders     ┌────────────────────┐
│   cosmos_master    │ ─────────────────────────────────────►│   cosmos_orders    │ ─────────────────────────────────────►│   cosmos_marts     │
│                    │                                       │                    │                                       │                    │
│ stg_customers      │  Fallback: auch ohne Dataset          │ stg_orders         │  Fallback: auch ohne Dataset          │ mart_revenue       │
│ stg_employees      │  um 06:00 (DatasetOrTimeSchedule)     │ stg_order_details  │  um 07:00 (DatasetOrTimeSchedule)     │ mart_orders        │
│ stg_products       │                                       │ hub_order          │                                       │ mart_products      │
│ hub_customer       │                                       │ alle Links         │  Freshness-Check:                     │                    │
│ hub_employee       │                                       │ sat_order          │  - frisch → normaler Lauf             │                    │
│ hub_product        │                                       │ sat_order_detail   │  - veraltet → Lauf mit Warnung        │                    │
│ sat_customer       │                                       │ pit_customer       │                                       │                    │
│ sat_employee       │                                       │ pit_order          │  Source Freshness:                    │                    │
│ sat_product        │                                       │                    │  - warn_after ohne error_after        │                    │
│                    │                                       │                    │  → dokumentiert, blockiert nicht       │                    │
│ + dbt test         │                                       │ + dbt test         │                                       │ + dbt test         │
└────────────────────┘                                       └────────────────────┘                                       └────────────────────┘
   DatasetOrTimeSchedule                                          DatasetOrTimeSchedule                                       DatasetOrTimeSchedule
   (PSA-Dataset ODER 05:00)                                       (Dataset ODER 06:00)                                        (Dataset ODER 07:00)
```

**Optionale Vorstufe: PSA mit Cosmos (`psa_cosmos_flow`)**

```
┌────────────────────┐     Dataset: dbt://psa_loaded
│  psa_cosmos_flow   │ ─────────────────────────────────► cosmos_master (s.o.)
│                    │
│ [BashOperator]     │   NG Generator: SQL-Procedures
│  create_psa_objects│   (nicht dbt - daher BashOperator)
│  create_procedures │
│  run_psa_load      │
│  run_delete_detect │
│                    │
│ [Cosmos TaskGroup] │   dbt via Cosmos (tag:psa):
│  stg_customers_psa │   individuelle Tasks pro Modell
│  hub_customer_psa  │
│  sat_customer_psa  │
│                    │
│ + dbt test (psa)   │
└────────────────────┘
   schedule=None (manuell)
```

### Domain-Tags

Die Aufteilung basiert auf **dbt-Tags** in `dbt_project.yml`:

```yaml
models:
  northwind_vault:
    staging:
      stg_customers:
        +tags: ["domain_master"]
      stg_orders:
        +tags: ["domain_orders"]
      # ...
    raw_vault:
      hubs:
        hub_customer:
          +tags: ["domain_master"]
        hub_order:
          +tags: ["domain_orders"]
      links:
        +tags: ["domain_orders"]     # alle Links gehören zur Order-Domain
      satellites:
        sat_customer:
          +tags: ["domain_master"]
        sat_order:
          +tags: ["domain_orders"]
      pit:
        +tags: ["domain_orders"]
    marts:
      +tags: ["consumption"]
```

Cosmos selektiert die Modelle per `select=["tag:domain_master"]` etc.

### Demo-Ablauf

```bash
# 1. Umgebung starten (Manifest wird beim Start automatisch generiert)
docker compose up -d
# Bei Modellaenderungen im laufenden Betrieb: DAG 'refresh_cosmos_manifest' triggern

# 2. Rohdaten laden
#    In Airflow UI: init_raw_data triggern

# 3a. Monolithisch: dbt_cosmos triggern → alle 43 Tasks in einem DAG

# 3b. Split: cosmos_master triggern →
#     cosmos_orders startet automatisch (Dataset-Trigger) →
#     cosmos_marts startet automatisch (Dataset-Trigger)
```

In der **Airflow UI** sieht man:
- Unter **Datasets** die Kette `dbt://domain_master` → `dbt://domain_orders`
- Unter **DAGs** die drei Split-DAGs mit ihren jeweiligen Task-Graphen
- Im **Graph View** von `cosmos_orders` den Freshness-Check-Branch

### DatasetOrTimeSchedule: Der Cron-Fallback

Das zentrale Pattern für robuste Pipelines mit 100+ DAGs:

```python
from airflow.timetables.datasets import DatasetOrTimeSchedule
from airflow.timetables.trigger import CronTriggerTimetable

schedule=DatasetOrTimeSchedule(
    timetable=CronTriggerTimetable("0 6 * * *", timezone="Europe/Zurich"),
    datasets=[MASTER_DATASET],
)
```

**Verhalten:**
- **Normalfall:** `cosmos_master` läuft erfolgreich → publiziert Dataset → `cosmos_orders` startet sofort
- **Fallback:** `cosmos_master` ist nicht gelaufen (Quelle nicht geliefert, Fehler, etc.) → `cosmos_orders` startet trotzdem um 06:00 mit den bestehenden Daten

Damit wird die Pipeline **nie blockiert**, auch wenn eine Quelle fehlt. Die nachfolgenden dbt-Tests und Source-Freshness-Checks dokumentieren die Veraltung.

---

## Graceful Degradation: Umgang mit fehlenden Quellen

### Das Problem

In einer Umgebung mit 100+ DAGs kommt es regelmässig vor, dass eine Quelle nicht rechtzeitig liefert. Der Produkt-Master ist nicht bereit, das CRM-System hatte Wartung, eine Datei fehlt. **Nicht jeder Ladeausfall sollte die gesamte Pipeline blockieren.**

### Lösungsansätze in dieser Demo

#### 1. DatasetOrTimeSchedule (in cosmos_orders und cosmos_marts)

Siehe oben. Der Cron-Fallback stellt sicher, dass der DAG auch ohne Dataset-Event läuft.

#### 2. Freshness-Check mit BranchPythonOperator (in cosmos_orders)

```python
def check_master_freshness(**context):
    hook = PostgresHook(postgres_conn_id="demo_postgres")
    result = hook.get_first(
        "SELECT MAX(load_datetime) FROM raw_vault.hub_customer"
    )
    last_load = result[0] if result and result[0] else None
    is_fresh = last_load and (datetime.now() - last_load) < timedelta(hours=12)

    if is_fresh:
        return "proceed_fresh"      # normaler Lauf
    else:
        return "proceed_stale"      # Lauf mit Warnung, kein Abbruch
```

**Ergebnis in der Airflow UI:**
- **Frisch:** `check_master_freshness` → `proceed_fresh` → `dbt_orders` → Tests
- **Veraltet:** `check_master_freshness` → `proceed_stale` (gelb/skipped) → `dbt_orders` → Tests

In beiden Fällen laufen die dbt-Modelle und Tests. Die Tests **dokumentieren** den Zustand der Daten.

#### 3. dbt Source Freshness mit warn_after (ohne error_after)

In `_staging__sources.yml`:

```yaml
sources:
  - name: raw
    schema: raw
    freshness:
      warn_after: {count: 12, period: hour}
      # error_after bewusst WEGGELASSEN → Exit-Code immer 0
    loaded_at_field: "change_date"
    tables:
      - name: products
        freshness:
          warn_after: {count: 48, period: hour}   # Stammdaten dürfen älter sein
```

**Schlüsselentscheidung:** `warn_after` ohne `error_after` bedeutet:
- `dbt source freshness` gibt Exit-Code **0** (Erfolg), auch bei veralteten Quellen
- Veraltung wird in `target/sources.json` dokumentiert und in den dbt Docs angezeigt
- Die Pipeline wird **nie blockiert**

Würde man `error_after` hinzufügen, gäbe es Exit-Code 1 → Airflow-Task schlägt fehl → Pipeline blockiert.

#### 4. trigger_rule für nachfolgende Tasks

```python
# Tests laufen IMMER, unabhängig vom Freshness-Branch
dbt_test = BashOperator(
    task_id="dbt_test",
    trigger_rule="none_failed_min_one_success",
    ...
)
```

| trigger_rule | Verhalten | Einsatz |
|-------------|-----------|---------|
| `all_success` | Nur wenn alle Vorgänger erfolgreich (Default) | Normale Dependencies |
| `none_failed_min_one_success` | Kein Vorgänger fehlgeschlagen, mind. einer erfolgreich | Nach Branches |
| `all_done` | Alle Vorgänger fertig (egal ob Erfolg/Fehler/Skip) | Archivierung, Reports |
| `none_failed` | Kein Fehler, Skips erlaubt | Bedingte Pfade |

### Weitere Patterns für Produktionsumgebungen

#### Circuit-Breaker DAG

Ein dedizierter DAG der um 04:00 alle Quellen prüft und den Status als Dataset publiziert:

```python
SOURCE_CHECKS = {
    "raw.customers": {"query": "SELECT MAX(change_date) FROM raw.customers", "max_age_hours": 12},
    "raw.products":  {"query": "SELECT MAX(change_date) FROM raw.products",  "max_age_hours": 48},
}
# → Publiziert Dataset("internal://source-health-status")
# → Downstream-DAGs lesen den Status per XCom
```

#### ExternalTaskSensor mit soft_fail=True

Für Cross-DAG-Dependencies mit Timeout statt Blockade:

```python
wait_for_vault = ExternalTaskSensor(
    task_id="wait_for_vault",
    external_dag_id="cosmos_orders",
    timeout=3600,           # max 1 Stunde warten
    mode="reschedule",      # Worker-Slot freigeben während Wartezeit
    soft_fail=True,         # Bei Timeout: SKIP statt FAIL
)
```

#### Reusable Soft-Load TaskGroup

Für 100+ DAGs lohnt es sich, das Pattern in eine wiederverwendbare TaskGroup zu extrahieren:

```python
def create_soft_load_group(group_id, load_command, freshness_query, max_age):
    with TaskGroup(group_id=group_id) as group:
        check = BranchPythonOperator(...)   # Freshness prüfen
        run_load = BashOperator(...)         # Laden wenn frisch
        skip_load = BashOperator(...)        # Warnung wenn veraltet
        done = EmptyOperator(trigger_rule="none_failed_min_one_success")
        check >> [run_load, skip_load] >> done
    return group
```

### Zusammenfassung: Empfohlene Strategie

| Schicht | Pattern | Bei fehlender Quelle |
|---------|---------|---------------------|
| **Source-Load** | Emittiert Dataset bei Erfolg | Kein Dataset-Event → kein Trigger |
| **Core/Vault** | `DatasetOrTimeSchedule` + Freshness-Check | Cron-Fallback, Lauf mit Warnung |
| **Consumption** | `DatasetOrTimeSchedule` + `soft_fail` Sensor | Cron-Fallback, Marts mit bestehenden Daten |
| **dbt Sources** | `warn_after` ohne `error_after` | Warnung in Logs, kein harter Fehler |
| **dbt Tests** | `--store-failures` + `trigger_rule=all_done` | Tests dokumentieren Veraltung |
| **Monitoring** | Circuit-Breaker DAG um 04:00 | Zentrale Übersicht aller Quellen |

> **Kernprinzip:** Fehlende Quellen sind ein **Dokumentations-Event**, kein **Fehler-Event**. Die Pipeline läuft weiter, Tests zeigen was veraltet ist, und das Monitoring alarmiert das Team. Harte Fehler gibt es nur bei technischen Problemen (DB nicht erreichbar, SQL-Fehler), nicht bei fachlichen Verspätungen.

---

## Bekannte Einschränkungen

- **AutomateDV Bridge + Effectivity Satellite**: Beide Macros sind in AutomateDV deprecated und wurden aus dem Projekt entfernt. Siehe [GitHub Issue](https://github.com/Datavault-UK/automate-dv/blob/master/macros/tables/postgres/bridge.sql).
- **dbt Docs Server**: Nutzt `python -m http.server` statt `dbt docs serve`, da letzterer Verbindungsabbrüche auf Docker/macOS verursacht.
- **Airflow 3 CeleryExecutor**: Die Demo nutzt CeleryExecutor + Redis + Flower. Worker können mit `docker compose up -d --scale airflow-worker=3` skaliert werden.
- **Inkrementelle Loads**: Der DAG `load_delta` demonstriert Delta-Loads mit AutomateDVs eingebauter Incremental-Logik. Die Delta-CSVs in `daten/delta/` enthalten neue und geänderte Datensätze.

---

## Ausblick: Optionale Erweiterungen

### KI-Assistenz mit MCP (Model Context Protocol)

MCP verbindet KI-Assistenten (Claude, Ollama, Copilot) direkt mit den Tools des Data Stacks. Statt Screenshots oder Copy-Paste kann man per natürlicher Sprache mit der gesamten Datenpipeline interagieren:

- *"Trigger den DAG `dbt_classic` und zeig mir die Task-Logs"*
- *"Wie viele Zeilen hat `mart_revenue_per_customer`? Zeig mir die Top 5 Kunden."*
- *"Generiere ein YAML für ein neues Staging-Modell `stg_suppliers`"*
- *"Welche dbt-Tests sind zuletzt fehlgeschlagen und warum?"*
- *"Zeig mir die Lineage von `hub_customer` bis zum Mart"*

#### MCP-Landschaft für diesen Stack

```
KI-Client (Claude Desktop / Claude Code / Open WebUI + Ollama / Cursor)
       |
       |-- PostgreSQL MCP  → Schema-Inspektion, read-only SQL          [Anthropic, offiziell]
       |-- dbt MCP         → run/test/compile, Lineage, Codegen        [dbt Labs, offiziell]
       |-- Airflow MCP     → (noch kein reifer Airflow-3-Support)      [ausstehend]
       |-- Metabase MCP    → Dashboards, Questions, Schema erkunden    [Community]
```

| MCP-Server | Offiziell? | Reife | Repo |
|-----------|-----------|-------|------|
| **PostgreSQL** | Anthropic | Stabil | [modelcontextprotocol/servers](https://github.com/modelcontextprotocol/servers/tree/main/src/postgres) |
| **dbt** | dbt Labs | Stabil | [dbt-labs/dbt-mcp](https://github.com/dbt-labs/dbt-mcp) — siehe Hinweis unten |
| **Airflow** | — | Ausstehend | Kein reifer Airflow-3-MCP verfügbar (Stand März 2026). Verfügbare Community-Server entweder API v1 only oder reine Navigations-/Dokumentations-Server ohne direkte Tool-Execution. Airflow bleibt bis auf weiteres über Web-UI und REST API zugänglich. |
| **Metabase** | Community | Feature-reich | [CognitionAI/metabase-mcp-server](https://github.com/CognitionAI/metabase-mcp-server) |

> **Installation:** Siehe [Option 2: MCP + KI-Assistenz](#option-2-mcp--ki-assistenz-open-webui--ollama) im Installations-Abschnitt.

#### dbt MCP — zwei Betriebsmodi

Der dbt MCP Server verhält sich unterschiedlich je nachdem ob lokal eine dbt-Installation vorhanden ist:

| Modus | Voraussetzung | Verfügbare Features |
|-------|--------------|---------------------|
| **Dockerisiert** (diese Demo) | dbt läuft in Docker, kein lokales Binary nötig | Codegen: `generate_model_yaml`, `generate_source`, `generate_staging_model` |
| **Lokal mit dbt** | dbt in PATH oder `DBT_PATH` gesetzt (z.B. Conda-Env) | Zusätzlich: `dbt run`, `dbt test`, `dbt ls`, Lineage-Abfragen |

**Dockerisierter Modus (Standard in dieser Demo):**

dbt-mcp wird mit `DBT_MCP_ENABLE_DBT_CLI=false` und `DBT_PATH=/opt/anaconda3/envs/dbt/bin/dbt` gestartet. Das ist kein schmerzhafter Verlust — die fehlenden Features werden durch andere MCP-Server kompensiert:

- **Lineage & Run-History** → `airflow-meta` PostgreSQL-MCP (Tabellen `dag_run`, `task_instance`)
- **Schema & Datenqualität** → `postgres-demo` PostgreSQL-MCP (direkte Abfragen auf Mart-Tabellen)
- **Was wirklich fehlt:** `dbt test` Ergebnisse live abfragen — diese landen aber in der `demo`-DB und sind via `postgres-demo` erreichbar

**Lokaler Entwicklungsmodus (volle Funktionalität):**

In der Praxis findet die dbt-Entwicklung lokal statt — mit VSCode, dbt Power User Extension und einem externen Code-Generator (in dieser Demo NG Generator, in Produktion z.B. ITB). Der Workflow:

```
Lokal (VSCode + dbt + MCP)  →  git push  →  DEV/INT/PROD (Docker, nur Execution via Airflow)
```

Wer dbt lokal installiert hat (z.B. via Conda), aktiviert CLI-Features in der MCP-Config:
```json
"DBT_MCP_ENABLE_DBT_CLI": "true",
"DBT_PATH": "/opt/anaconda3/envs/dbt/bin/dbt"
```
Dann sind alle Tools verfügbar: Lineage, `dbt run/test/compile`, direkte Modell-Ausführung via Claude — ergänzend zu VSCode und dbt Power User.

> **Sicherheitshinweis:** dbt-MCP mit aktiviertem CLI erlaubt KI-Assistenten, dbt-Befehle auszuführen die Datenbank-Objekte verändern können. In der Demo-Umgebung unkritisch, in Produktion die aktivierten Tool-Kategorien einschränken.

#### Demo-Szenarien mit MCP

**Szenario "Airflow per Chat steuern"** (Claude Desktop oder Open WebUI):
> *"Pausiere alle DAGs ausser `dbt_classic`. Dann triggere `init_raw_data` und warte bis er fertig ist. Zeig mir danach die Task-Logs."*

**Szenario "dbt per Chat entwickeln"** (Claude Desktop oder Claude Code):
> *"Schau dir die Lineage von `mart_order_overview` an. Welche Staging-Modelle fliessen rein? Generiere dann ein YAML-Schema für das Modell mit sinnvollen Beschreibungen."*

**Szenario "SQL per Sprache"** (PostgreSQL-MCP in Claude):
> *"Wie viele Satellite-Versionen hat der Kunde mit der höchsten Bestellanzahl? Zeig mir die Hash-Diff-Änderungen chronologisch."*

**Szenario "Vollständig lokal"** (Open WebUI + Ollama):
> Gleiche Szenarien, aber ohne Cloudanbindung — ideal für Umgebungen mit Datenschutzanforderungen.

### PSA-Pfad mit NG Generator (optionaler alternativer Datenfluss)

Die Demo enthält einen optionalen zweiten Pfad, der eine **Persistent Staging Area (PSA)** als stabile, historisierte Grundlage vor dbt einschiebt. Die PSA wird durch einen metadatengetriebenen Code-Generator ("NG Generator") erzeugt.

**Architektur-Vergleich:**
```
KLASSISCH:  CSV -> Raw -> Staging (dbt) -> Raw Vault (dbt) -> Marts
PSA-PFAD:   CSV -> Raw -> PSA (NG Gen) -> Staging (dbt) -> Raw Vault (dbt)
                          stabil!
```

**Kernvorteil:** Der Data Vault kann jederzeit aus der PSA komplett neu aufgebaut werden, ohne auf die Quelldaten (CSV) zurückgreifen zu müssen. Die PSA schützt die historischen Daten **architektonisch** (nicht nur konfigurativ via `full_refresh: false`).

**PSA-Objekte (Schema `psa`, am Beispiel `customers`):**

| Objekt | Typ | Zweck |
|--------|-----|-------|
| `v_customers_ifc` | View | Interface-Abbild der Quelle mit Typ-Casting |
| `v_customers_cln` | View | Dedupliziert + Content-Hash (sha256) |
| `customers_psa` | Tabelle | SCD2-historisiert (ng_valid_from/to, ng_is_current) |
| `v_customers_cur` | View | Nur aktuelle, nicht gelöschte Version |
| `v_customers_fhi` | View | Alle Versionen (Full History) |
| `run_customers_psa_load()` | Procedure | SCD2-Load via Hash-Vergleich |
| `run_customers_delete_detection()` | Procedure | Markiert gelöschte Records |

**Demo-Ablauf:**
1. `init_raw_data` ausführen (CSV-Daten laden)
2. `dbt_classic` ausführen (klassischer Pfad zum Vergleich)
3. `psa_flow` ausführen (PSA aufbauen + Data Vault aus PSA)
4. Im Streamlit-Tab "PSA-Pfad" die Ergebnisse vergleichen
5. `psa_rebuild_demo` ausführen → beweist Vault-Rebuild aus PSA ohne CSV

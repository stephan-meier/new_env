# Data Vault Demo Environment

Komplett dockerisierte Demo-Umgebung fuer **dbt**, **AutomateDV** (Data Vault 2.0) und **Airflow 3** Orchestrierung auf **PostgreSQL**. Gedacht fuer Schulungen, Workshops und Team-Demos.

## Was zeigt diese Demo?

### Data Vault 2.0 mit AutomateDV
Ein vollstaendiges Data-Vault-Modell auf Basis eines Northwind-aehnlichen Bestellsystems:

| Schicht | Schema | Inhalt |
|---------|--------|--------|
| **Raw** | `raw` | Rohdaten aus CSV-Dateien (5 Tabellen) |
| **Staging** | `staging` | Bereinigte Views mit Hash-Keys und Metadaten (AutomateDV `stage` Macro) |
| **Raw Vault** | `raw_vault` | 4 Hubs, 3 Links, 5 Satellites, 2 PIT-Tabellen |
| **Marts** | `mart` | 3 Business-Tabellen (Umsatz/Kunde, Bestelluebersicht, Produktverkaeufe) |

### Airflow-Orchestrierung: Klassisch vs. Cosmos
Zwei alternative Ansaetze im direkten Vergleich:

- **`dbt_classic`** - BashOperator-Kette: `dbt deps` → `dbt seed` → `dbt run` (staging → raw_vault → marts) → `dbt test`. Einfach, aber ein Task pro Phase.
- **`dbt_cosmos`** - Astronomer Cosmos parst das dbt-Projekt automatisch und erstellt **43 individuelle Airflow-Tasks** mit vollstaendigem Dependency-Graph. Retry und Monitoring auf Modell-Ebene.

### Quelldaten
Northwind-aehnliches Bestellsystem mit 5 Tabellen:

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
# Fernet Key (fuer Airflow Connection/Variable-Verschluesselung)
python3 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"

# Falls cryptography nicht installiert: alternativ mit openssl
openssl rand -base64 32

# API Secret + JWT Secret (fuer Airflow 3 Container-Kommunikation)
python3 -c "import secrets; print(secrets.token_urlsafe(32))"
python3 -c "import secrets; print(secrets.token_urlsafe(32))"
```

> **Warum 3 verschiedene Keys?**
> - `FERNET_KEY`: Verschluesselt Passwoerter in Airflow Connections/Variables in der DB
> - `API__SECRET_KEY`: Signiert die API-Session (wie Flask SECRET_KEY)
> - `API_AUTH__JWT_SECRET`: Signiert JWT-Tokens zwischen Scheduler/Worker und API-Server
>
> **Alle 3 muessen auf allen Airflow-Containern identisch sein!** Siehe [Airflow 3 Upgrade-Hinweise](#airflow-3-vs-2x-upgrade-hinweise) fuer Details.

```bash
# Alle Container bauen und starten
docker compose up -d --build

# Erster Start dauert ca. 3-5 Minuten (Image-Download + Build)
# Status pruefen:
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

### Worker skalieren
```bash
# Mehrere Celery-Worker starten (z.B. 3 parallele Worker)
docker compose up -d --scale airflow-worker=3

# Worker-Status in Flower beobachten: http://localhost:5555
```

### Stoppen und Aufraeumen
```bash
# Stoppen (Daten bleiben erhalten)
docker compose down

# Stoppen + alle Daten loeschen (frischer Neustart)
docker compose down -v
```

---

## Demo-Ablauf

### 1. Rohdaten laden
In der Airflow UI den DAG **`init_raw_data`** triggern. Dieser:
- Loescht alle bestehenden Raw-Tabellen (idempotent)
- Erstellt die Tabellen neu (Postgres-DDL)
- Laedt die 5 CSV-Dateien per `COPY`

→ Ergebnis in pgAdmin pruefen: `raw.customers`, `raw.orders`, etc.

### 2. Data Vault aufbauen (Variante A: Klassisch)
DAG **`dbt_classic`** triggern:
- `dbt seed` laedt CSV-Daten als Alternative in Raw-Schema
- `dbt run --select staging` erstellt Staging-Views mit Hash-Keys
- `dbt run --select raw_vault` baut Hubs, Links, Satellites, PITs
- `dbt run --select marts` erstellt Business-Tabellen
- `dbt test` validiert alle Modelle

### 3. Data Vault aufbauen (Variante B: Cosmos)
DAG **`dbt_cosmos`** triggern:
- Cosmos parst das dbt-Projekt automatisch
- Jedes dbt-Modell wird ein eigener Airflow-Task
- Der Dependency-Graph ist in der Airflow UI sichtbar
- 43 Tasks mit vollstaendiger Parallelisierung

### 4. Inkrementellen Delta-Load ausfuehren
DAG **`load_delta`** triggern:
- **Kein DROP** - die Delta-CSVs werden per COPY in die bestehenden Raw-Tabellen **angehaengt**
- `dbt run` laeuft **ohne** `--full-refresh`, sodass AutomateDVs Incremental-Logik greift:
  - Hubs: nur neue Business Keys werden eingefuegt
  - Satellites: nur geaenderte Hashdiffs erzeugen neue Versionen (Historisierung!)
  - Links: nur neue Beziehungs-Kombinationen

**Delta-Daten (Batch 2, `change_date` = 2024-01-15):**
| Datei | Inhalt | Effekt im Vault |
|-------|--------|----------------|
| `CUSTOMERS_DELTA.csv` | 3 neue + 2 geaenderte Kunden | Hub: +3, Sat: +5 |
| `ORDERS_DELTA.csv` | 5 neue Bestellungen | Hub: +5, alle Links: +5 |
| `ORDER_DETAILS_DELTA.csv` | 8 neue Positionen | Link Order-Product: +8, Sat: +8 |

→ Im **Streamlit Portal** (Tab "Inkrementelle Loads") koennen die Vorher/Nachher-Zahlen und die Satellite-Historisierung live geprueft werden.

### 5. Ergebnisse erkunden
- **pgAdmin** (localhost:5050): SQL-Abfragen auf alle Schemas
- **dbt Docs** (localhost:8081): Lineage-Graph und Modell-Dokumentation
- **DBeaver**: Direktverbindung zu PostgreSQL

### Demo-Reset
Fuer einen sauberen Neustart einfach `init_raw_data` erneut triggern - der DAG macht `DROP TABLE IF EXISTS` bevor er neu laedt.

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

Diese Demo laeuft auf **Airflow 3.0.2**. Bei einem Upgrade von 2.x auf 3.x gibt es mehrere kritische Unterschiede, die wir beim Aufbau dieser Umgebung erfahren haben:

### Architektur-Aenderungen

| Komponente | Airflow 2.x | Airflow 3.x |
|-----------|-------------|-------------|
| Web UI | `airflow webserver` | **`airflow api-server`** (Webserver existiert nicht mehr) |
| DAG Parsing | Im Scheduler integriert | **`airflow dag-processor`** als separater Prozess |
| Task Execution | Scheduler startet Tasks direkt | Tasks kommunizieren ueber **Execution API** mit dem API-Server |
| User Management | `airflow users create` | Nur mit **FAB-Provider** (`apache-airflow-providers-fab`) |
| Triggerer | Optional | **Pflicht** als eigener Service |

### Kritisches Problem: JWT-Signatur zwischen Containern

**Das groesste Problem bei Airflow 3 im Multi-Container-Setup:**

In Airflow 3 kommunizieren Task-Worker (die vom Scheduler gestartet werden) mit dem API-Server ueber eine **interne Execution API**. Diese API verwendet **JWT-Tokens** zur Authentifizierung. Die JWT-Tokens werden mit einem `secret_key` und `jwt_secret` signiert.

**Das Problem:** Wenn `api-server` und `scheduler` in **separaten Containern** laufen, generiert jeder Container beim ersten Start **eigene, zufaellige Secrets** in seiner lokalen `airflow.cfg`. Der Scheduler signiert den JWT mit seinem Secret, der API-Server prueft mit seinem eigenen → **Signature verification failed**.

**Die Loesung:** Alle Airflow-Container muessen dieselbe `airflow.cfg` mit identischen Secrets teilen:

```ini
# airflow/config/airflow.cfg
[api]
secret_key = <identischer-key-fuer-alle-container>

[api_auth]
jwt_secret = <identischer-key-fuer-alle-container>
```

Diese Datei wird per Volume in alle Container gemountet:
```yaml
volumes:
  - ./airflow/config:/opt/airflow/config
environment:
  AIRFLOW_CONFIG: "/opt/airflow/config/airflow.cfg"
```

**In Airflow 2.x war das kein Problem**, weil der Webserver und Scheduler ueber die gemeinsame Datenbank kommunizierten und keine API-basierte Task-Execution stattfand.

### Astronomer Cosmos + Airflow 3: Zwei kritische Bugs

Beim Einsatz von **astronomer-cosmos 1.13.1** mit Airflow 3.0.2 traten zwei zusammenhaengende Probleme auf:

**Problem 1: `ParamValidationError` auf `__cosmos_telemetry_metadata__`**

Cosmos fuegt jedem DAG einen versteckten Parameter `__cosmos_telemetry_metadata__` hinzu, der Telemetrie-Daten als Base64-encodierten String enthaelt. Dieser wird mit einem JSON-Schema `const`-Constraint validiert. In Airflow 3 ist die Param-Validierung **strikt**: Der Hash wird beim DAG-Parsing generiert, aendert sich aber bis zur Task-Execution (z.B. durch Cache-Invalidierung), wodurch die Validierung fehlschlaegt.

```
ParamValidationError: Invalid input for param __cosmos_telemetry_metadata__:
'eNpl...' was expected
Failed validating 'const' in schema
```

**Loesung:**
```yaml
AIRFLOW__COSMOS__PROPAGATE_TELEMETRY: "False"
```

**Problem 2: DAG-Parse-Timeout bei `dbt ls` ohne Cache**

Wenn man den Cosmos-Cache deaktiviert (als Workaround fuer Problem 1), fuehrt Cosmos bei **jedem DAG-Parse** `dbt deps` + `dbt ls` aus. In einem Container mit dbt im isolierten venv dauert das >30 Sekunden und ueberschreitet den Airflow `dagbag_import_timeout`:

```
AirflowTaskTimeout: DagBag import timeout for dag_dbt_cosmos.py after 30.0s
```

**Loesung: `LoadMode.DBT_MANIFEST`**

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

Das Manifest wird im Dockerfile mit `dbt parse` generiert und per Volume-Mount bereitgestellt. **Vorteil**: Sofortiges DAG-Parsing ohne Subprocess-Aufruf.

> **Beide Probleme existieren in Airflow 2.x nicht**, weil dort (a) die Param-Validierung weniger strikt ist und (b) laengere Parse-Zeiten toleriert werden.

### Weitere Upgrade-Stolpersteine

1. **`EXECUTION_API_SERVER_URL` ist Pflicht**
   ```yaml
   AIRFLOW__CORE__EXECUTION_API_SERVER_URL: "http://airflow-apiserver:8080/execution/"
   ```
   Ohne diese Variable wissen Scheduler-Worker nicht, wo der API-Server laeuft.

2. **BashOperator Import-Pfad geaendert**
   ```python
   # Alt (Airflow 2.x) - funktioniert noch, gibt Deprecation-Warning
   from airflow.operators.bash import BashOperator
   # Neu (Airflow 3.x)
   from airflow.providers.standard.operators.bash import BashOperator
   ```

3. **FAB-Provider fuer User Management**
   ```
   pip install apache-airflow-providers-fab
   ```
   Ohne diesen Provider gibt es keinen `airflow users create` Befehl und keine Web-Login-Seite.

4. **`schedule_interval` entfernt** - nur noch `schedule=None` oder `schedule="@daily"` etc.

5. **Offizielles Docker-Compose als Referenz nutzen**
   Das offizielle Template unter `https://airflow.apache.org/docs/apache-airflow/3.0.2/docker-compose.yaml` ist die beste Ausgangsbasis. Diese Demo verwendet CeleryExecutor + Redis + Flower fuer Worker-Skalierung und Monitoring.

---

## Technische Details

### dbt + AutomateDV

- **dbt-postgres** laeuft in einem isolierten Python-venv (`/home/airflow/dbt_venv/`) innerhalb der Airflow-Container, um Dependency-Konflikte mit Airflow 3 zu vermeiden (insb. `protobuf`).
- **AutomateDV 0.11.1** - die Macros `hub`, `link`, `sat`, `stage` und `pit` funktionieren auf Postgres. Die Macros `bridge` und `eff_sat` sind [deprecated](https://github.com/Datavault-UK/automate-dv/blob/master/macros/tables/postgres/bridge.sql) und wurden daher nicht verwendet.
- **Staging-Modelle** nutzen das `automate_dv.stage` Macro mit `source()`-Referenzen fuer automatisches Hashing (MD5) der Business Keys und Hashdiffs.

### Projekt-Struktur
```
new_env/
├── docker-compose.yml          # 11 Services (Postgres, Redis, 6x Airflow, dbt-docs, pgAdmin, Streamlit)
├── Dockerfile.airflow          # Airflow 3 + Cosmos + dbt (isolierter venv)
├── Dockerfile.dbt              # dbt fuer Docs-Server
├── .env                        # Shared Secrets (Fernet Key, JWT)
├── daten/                      # CSV-Quelldaten
├── dbt_project/                # Komplettes dbt-Projekt
│   ├── models/staging/         # 5 Staging-Views (AutomateDV stage macro)
│   ├── models/raw_vault/       # 4 Hubs, 3 Links, 5 Sats, 2 PITs
│   └── models/marts/           # 3 Business-Tabellen
├── airflow/
│   ├── dags/                   # 4 DAGs (init, classic, cosmos, delta)
│   └── config/airflow.cfg      # Shared Secrets fuer JWT-Auth
├── postgres/init/              # DB-Init-Scripte (Schemas)
└── streamlit/                  # Portal-App
```

---

## Datenqualitaet mit dbt-expectations

Neben den Standard-Tests (`unique`, `not_null`) setzt diese Demo [dbt-expectations](https://github.com/calogica/dbt-expectations) ein - eine dbt-native Portierung von Great Expectations mit ueber 50 fachlichen Test-Macros.

### Test-Uebersicht (19 Tests)

| Schicht | Test | Zweck | Ergebnis |
|---------|------|-------|----------|
| **Staging** | `expect_column_pair_values_A_to_be_greater_than_B` | shipped_date >= order_date | **WARN: 304 Verletzungen!** |
| **Staging** | `expect_column_values_to_match_regex` | Email-Format-Validierung | PASS |
| **Staging** | `expect_column_values_to_be_between` | Versandgebuehr 0-100, Rabatt 0-100%, Preis > 0, Menge > 0 | PASS |
| **Staging** | `expect_column_distinct_count_to_equal` | Genau 4 Bestellstatus | PASS |
| **Raw Vault** | `expect_column_proportion_of_unique_values` | Hub: 100% unique, Satellite: < 100% (Historisierung!) | PASS |
| **Raw Vault** | `expect_compound_columns_to_be_unique` | Link-Kombinationen eindeutig | PASS |
| **Raw Vault** | `expect_row_values_to_have_recent_data` | Frische Daten nach Delta-Load | PASS |
| **Marts** | `expect_table_row_count_to_be_between` | Plausible Zeilenanzahl | PASS |
| **Marts** | `expect_column_values_to_be_between` | Revenue > 0, Orders >= 1, Menge >= 0 | PASS |

### Demo-Highlight: shipped_date < order_date

Der Test `expect_column_pair_values_A_to_be_greater_than_B` deckt auf, dass **304 von 605 Bestellungen** ein `shipped_date` haben, das **vor** dem `order_date` liegt - ein echtes Datenqualitaetsproblem, das kein `not_null`/`unique`-Test je finden wuerde.

```bash
# Tests ausfuehren (innerhalb des Airflow-Workers):
dbt test --select tag:quality tag:vault-integrity tag:freshness tag:business-logic
```

---

## Plattform-Kompatibilitaet (macOS, Windows, Ubuntu)

Die gesamte Umgebung laeuft in Docker-Containern und ist plattformunabhaengig. **Kein einziges File im Projekt muss geaendert werden** - Docker loest die Architektur (ARM vs. Intel) automatisch auf. Alle verwendeten Base-Images (`postgres:16-alpine`, `apache/airflow:3.0.2`, `python:3.11-slim`, `redis:7.2-bookworm`, `dpage/pgadmin4`) bieten Multi-Arch-Images an.

| Thema | macOS (ARM) | Windows (Intel) | Ubuntu (Intel) |
|-------|-------------|-----------------|----------------|
| **Docker** | Docker Desktop | Docker Desktop (WSL2) | Docker Engine (nativ) |
| **Images** | `linux/arm64` (auto) | `linux/amd64` (auto) | `linux/amd64` (auto) |
| **Performance** | Gut (native ARM) | Etwas langsamer (WSL2-Layer) | Am schnellsten (nativer Kernel) |
| **Pfade/Ports** | Identisch | Identisch | Identisch |

### Windows-spezifische Hinweise

- **Docker Desktop mit WSL2-Backend** muss aktiviert sein (nicht Hyper-V)
- Repo am besten unter WSL2 klonen (`/home/user/`, nicht `/mnt/c/`) fuer deutlich bessere I/O-Performance bei Volume-Mounts
- Line-Endings: `git config core.autocrlf input` setzen, damit CSVs und Shell-Scripts LF behalten

### Ubuntu-spezifische Hinweise

- Docker Engine statt Docker Desktop (leichtgewichtiger):
  ```bash
  sudo apt install docker.io docker-compose-v2
  sudo usermod -aG docker $USER
  ```
- Nach dem `usermod`-Befehl neu einloggen oder `newgrp docker` ausfuehren

---

## Schutz historischer Daten im Data Vault

### Das Problem

In einem Data Vault speichern Satellites die **komplette Aenderungshistorie** jeder Entitaet. Diese Historisierung ist der Kernwert des Modells - einmal verloren, ist sie nicht aus den Quelldaten rekonstruierbar (die Quellsysteme halten typischerweise nur den aktuellen Stand).

**Gefahr:** Ein versehentliches `dbt run --full-refresh` auf den Raw Vault Modellen wuerde alle Satellite-Tabellen droppen und neu aufbauen - nur mit den aktuellen Daten aus Staging. Die gesamte Historie waere unwiderruflich verloren.

In klassischen dbt-Projekten (ohne Data Vault) nutzt man `dbt snapshot` fuer SCD2-Historisierung. Snapshots haben einen eingebauten Schutzmechanismus: Sie sind als eigenstaendiger Befehl (`dbt snapshot`) von `dbt run` getrennt. Bei AutomateDV sind die Satellites jedoch normale `incremental`-Modelle, die ueber `dbt run` gesteuert werden - und damit anfaellig fuer `--full-refresh`.

### Schutzmechanismen in dieser Demo

#### 1. `full_refresh: false` (aktiv)

In `dbt_project.yml` ist fuer alle Raw Vault Modelle `full_refresh: false` konfiguriert:

```yaml
raw_vault:
  +materialized: incremental
  +full_refresh: false   # Blockiert --full-refresh komplett
```

**Effekt:** Wenn jemand `dbt run --full-refresh` ausfuehrt, werden Raw Vault Modelle **uebersprungen** statt neu aufgebaut. dbt gibt eine Warnung aus:
```
Refusing to full-refresh model 'hub_customer' because full_refresh is set to false.
```

Das ist der staerkste dbt-native Schutz und seit dbt 1.6 verfuegbar.

#### 2. Airflow DAGs ohne --full-refresh

Die DAGs `dbt_classic` und `dbt_cosmos` verwenden **bewusst kein** `--full-refresh` Flag auf den Raw Vault Modellen. Der `load_delta` DAG nutzt `dbt run` (ohne Flag), damit AutomateDVs eingebaute Incremental-Logik greift.

#### 3. Weitere Mitigationen (fuer Produktionsumgebungen)

| Massnahme | Beschreibung | Aufwand |
|-----------|-------------|---------|
| **Postgres-Permissions** | `REVOKE TRUNCATE, DROP ON ALL TABLES IN SCHEMA raw_vault FROM dbt_user` - der dbt-User darf nur INSERT/UPDATE. Ein separater Admin-User behaelt DROP-Rechte. | Mittel |
| **Backup vor Full-Refresh** | Pre-Hook in dbt der vor jedem Full-Refresh ein `pg_dump` des raw_vault Schemas erstellt. | Mittel |
| **CI/CD Guard** | In der CI-Pipeline pruefen ob ein PR `--full-refresh` auf Raw Vault Modelle anwendet und den Build blockieren. | Niedrig |
| **dbt tags + selectors** | Raw Vault Modelle taggen (`+tags: ["protected"]`) und Team-Konvention: `--full-refresh` nur mit explizitem `--exclude tag:protected`. | Niedrig |
| **Monitoring** | Alert wenn die Zeilenanzahl eines Satellites sinkt (sollte monoton steigen). Umsetzbar mit `dbt_expectations.expect_table_row_count_to_be_between` und einem dynamischen Minimum aus der letzten Ausfuehrung. | Mittel |

#### 4. Architektonischer Schutz: Vorgeschaltete PSA

Die staerkste Variante ist **architektonischer Schutz** durch eine vorgeschaltete **Persistent Staging Area (PSA)**. Statt die Historisierung nur im Data Vault zu halten (wo sie von `--full-refresh` bedroht ist), wird sie in einer separaten, von dbt unabhaengigen Schicht verwaltet:

```
OHNE PSA:   CSV → Raw → Data Vault (Historisierung HIER, verwundbar)
MIT PSA:    CSV → Raw → PSA (Historisierung HIER, stabil) → Data Vault (rebuild-faehig)
```

**Wie das funktioniert:**
- Die PSA wird durch Stored Procedures (z.B. aus einem Code-Generator) verwaltet, nicht durch dbt
- SCD2-Historisierung, Content-Hashing und Delete Detection passieren in der PSA
- dbt liest aus der PSA (`source('psa', 'v_customers_cur')`) und baut den Data Vault daraus auf
- Der Data Vault wird zur **deterministischen Funktion der PSA** — jederzeit neu generierbar

**Warum das schuetzt:**
- `dbt run --full-refresh` auf den Vault? Kein Problem — Rebuild aus PSA, kein Datenverlust
- Modellumbau im Data Vault? PSA bleibt unangetastet, neues Modell wird aus derselben Datenbasis gebaut
- Bug in einer Transformation? PSA-Daten sind korrekt, Core wird idempotent neu aufgebaut

Diese Demo enthaelt einen optionalen PSA-Pfad (DAGs `psa_flow` und `psa_rebuild_demo`), der genau dieses Konzept demonstriert. Siehe [PSA-Pfad mit NG Generator](#psa-pfad-mit-ng-generator-optionaler-alternativer-datenfluss) fuer Details.

> **Zusammenfassung:** Fuer die Demo-Umgebung reicht `full_refresh: false` als Minimalschutz. In Produktionsumgebungen empfehlen wir eine vorgeschaltete PSA als architektonischen Schutz, ergaenzt durch Postgres-Permissions und Monitoring.

---

## DAG-Splitting-Strategie fuer Produktion

In einer Produktionsumgebung reicht ein einzelner dbt-DAG nicht aus. Verschiedene Quellen, Fachbereiche und Konsumenten haben unterschiedliche Ladezeiten, Abhaengigkeiten und Verantwortlichkeiten. Hier ist die empfohlene Strategie:

### Prinzip: Tags als zentraler Steuerungsmechanismus

dbt-Tags auf Modell-Ebene definieren die fachliche Zugehoerigkeit. Einmal definiert, koennen sie ueberall verwendet werden - in Cosmos DAGs, dbt selectors und CI/CD:

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
| **PSA** (NG Generator) | Pro Quelle | Zeitgesteuert | Unabhaengig |
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

### Cosmos: DbtTaskGroup fuer Sub-Graphs

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

**Variante A - Airflow Datasets (empfohlen fuer Airflow 3):**
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

### dbt Selectors fuer komplexe Auswahl

Fuer fortgeschrittene Szenarien (z.B. nur inkrementelle Modelle einer Domain):

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

In der Praxis werden DAGs betrieblich angepasst - Ladezeitpunkte verschieben sich, Abhaengigkeiten aendern sich, einzelne Modelle werden temporaer ausgeschlossen. Diese Anpassungen gehen verloren wenn DAG-Dateien ueber CI/CD redeployed oder regeneriert werden.

**Das Problem:**

| Anpassung | Wo gespeichert | Risiko bei Redeployment |
|-----------|---------------|------------------------|
| Schedule `0 3 * * *` → `0 3,15 * * *` | Hardcodiert im DAG-File | Ueberschrieben |
| Zusaetzliche Sensor-Dependency | Hardcodiert im DAG-File | Ueberschrieben |
| `exclude` fuer experimentelle Modelle | Hardcodiert im DAG-File | Ueberschrieben |
| Airflow Variable `dv_crm_config` | Airflow-Datenbank | **Persistiert** |

**Loesung: Betriebliche Parameter in Airflow Variables auslagern:**

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
  "notes": "2x taeglich seit 2026-03-10, Ticket OPS-1234"
}
```

**Vorteile dieses Patterns:**
- **DAG-Code ist regenerierbar** - Git-Deployment ueberschreibt keine betrieblichen Anpassungen
- **Aenderungen ohne Git-Commit** - Betrieb kann Schedules/Excludes sofort anpassen
- **Audit-Trail** - Variable-Aenderungen werden in der Airflow-DB geloggt
- **Defaults im Code** - Wenn keine Variable gesetzt ist, greifen sinnvolle Defaults
- **Dokumentation im Wert** - Das `notes`-Feld erklaert warum eine Anpassung gemacht wurde

> **Faustregel:** Alles was sich zwischen Deployments aendern kann (Schedules, Excludes, Timeouts, Retry-Counts) gehoert in Airflow Variables. Alles was sich nur bei Modell-Aenderungen aendert (Tags, Pfade, Projekt-Config) gehoert in den Code.

> **Zusammenfassung:** Tags auf den dbt-Modellen sind die Grundlage. Darauf aufbauend erstellt man pro Fachbereich einen eigenen Cosmos-DAG (oder DbtTaskGroup). Cross-DAG-Abhaengigkeiten (PSA → Core → Consumption) werden ueber Airflow Datasets oder ExternalTaskSensors gesteuert. Betriebliche Anpassungen (Schedules, Excludes) werden in Airflow Variables ausgelagert, damit sie Redeployments ueberleben. Ein woechentlicher Full-Rebuild-DAG sichert die Gesamtkonsistenz.

---

## Debugging und Run-Dokumentation

### Das 3-Schichten-Problem

In einer Architektur mit Code-Generator + dbt + AutomateDV durchlaeuft jedes SQL-Statement drei Generierungsschichten:

```
Schicht 1: Code-Generator    YAML-Metadaten + Jinja-Templates  ->  dbt .sql/.yml Dateien
Schicht 2: dbt + AutomateDV  dbt-Jinja + AutomateDV Macros     ->  compiled SQL
Schicht 3: PostgreSQL         Fuehrt das SQL aus                ->  Ergebnis / Fehler
```

Wenn ein Fehler auftritt, muss man zurueckverfolgen:
- Ist das **generierte dbt-Modell** korrekt? (Schicht 1 → 2 Uebergang)
- Ist das **compilierte SQL** korrekt? (Schicht 2 → 3 Uebergang)
- Oder ist die **YAML-Metadaten-Definition** falsch? (Schicht 1 Eingang)

### dbt Artefakte verstehen

dbt schreibt bei jedem Lauf mehrere Artefakte in das `target/`-Verzeichnis:

| Verzeichnis / Datei | Inhalt | Debugging-Nutzen |
|---------------------|--------|-----------------|
| `target/compiled/` | Reines SELECT nach Jinja-Aufloesung | "Was hat mein Jinja produziert?" |
| `target/run/` | Vollstaendiges SQL inkl. DDL (`CREATE TABLE AS`, `MERGE`) | "Was hat Postgres tatsaechlich bekommen?" |
| `manifest.json` | Kompletter Projekt-Graph inkl. `compiled_code` pro Node | Programmatisch auswertbar, enthaelt compiled SQL |
| `run_results.json` | Status, Laufzeit, Fehler pro Node | Test-/Run-Ergebnisse |
| `sources.json` | Freshness-Ergebnisse pro Source | Datenaktualitaet |
| `catalog.json` | Warehouse-Katalog (Spalten, Typen, Statistiken) | Fuer dbt Docs |

**Wichtig:** Das `target/`-Verzeichnis wird bei **jedem** dbt-Lauf ueberschrieben. Fuer Audit-Zwecke muessen die Artefakte nach jedem Lauf archiviert werden (siehe unten).

### Debugging-Workflow

**1. Schicht isolieren:**

```bash
# Was hat der Code-Generator erzeugt? (Schicht 1 Output)
cat dbt_project/models/raw_vault/hubs/hub_customer.sql

# Was hat dbt daraus kompiliert? (Schicht 2 Output, reines SELECT)
dbt compile --select hub_customer
cat target/compiled/northwind_vault/models/raw_vault/hubs/hub_customer.sql

# Was wurde tatsaechlich ausgefuehrt? (Schicht 3 Input, mit DDL)
cat target/run/northwind_vault/models/raw_vault/hubs/hub_customer.sql
```

**2. SQL-Diff vor/nach Aenderungen:**

```bash
# Zustand VOR der Aenderung sichern:
dbt compile
cp -r target/compiled target/compiled_BEFORE

# Code-Generator Config aendern, neu generieren, dann:
dbt compile
diff -r target/compiled_BEFORE target/compiled
```

Dieser Workflow ist besonders wertvoll bei AutomateDV-Konfigurationsaenderungen - man sieht exakt wie sich eine YAML-Aenderung auf das generierte SQL auswirkt.

**3. Debug-Kommentare in Generator-Templates:**

```sql
-- NG_TEMPLATE: hub.sql.j2 v1.3
-- NG_SOURCE: customers.yml
-- NG_GENERATED: 2026-03-17T08:00:00
{{ automate_dv.hub(src_pk=src_pk, ...) }}
```

Diese Kommentare ueberleben die dbt-Kompilierung und erscheinen im `target/compiled/` Output. So kann man bei einem Fehler sofort sehen, welches Template und welche Metadaten-Version den Code erzeugt haben.

**4. Jinja-Logging fuer Runtime-Debugging:**

```sql
{%- set my_var = some_expression -%}
{{ log("DEBUG my_var = " ~ my_var, info=True) }}
```

Gibt Werte waehrend der Kompilierung auf die Konsole aus, ohne das SQL zu beeinflussen.

### Run-Dokumentation: Artefakte archivieren

Das `target/`-Verzeichnis wird bei jedem Lauf ueberschrieben. Fuer eine lueckenlose Dokumentation muessen die Artefakte nach jedem Lauf archiviert werden.

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
    trigger_rule=TriggerRule.ALL_DONE,  # Laeuft IMMER, auch bei Fehlern
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

Schreibt Run-Resultate als Tabellen in die Datenbank. Damit sind historische Laufergebnisse per SQL abfragbar und koennen in Streamlit visualisiert werden.

**Ansatz 3: Elementary (umfassende Reports)**

```yaml
# packages.yml
packages:
  - package: elementary-data/elementary
    version: [">=0.15.0", "<1.0.0"]
```

Elementary generiert mit `edr report` ein **komplettes HTML-Dashboard** pro Run: Teststatus, Laufzeiten, Anomalie-Erkennung, Lineage. Der Befehl kann als Post-Run-Task in Airflow ausgefuehrt werden.

### Immer-laufender Report-Task in Airflow

Um einen Dokumentations-Task anzuhaengen der **unabhaengig vom Erfolg** der vorherigen Tasks laeuft, muss man von `DbtDag` auf `DbtTaskGroup` wechseln:

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
        trigger_rule=TriggerRule.ALL_DONE,  # Laeuft IMMER
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
| `all_success` | Nur wenn alle Upstream-Tasks erfolgreich (Default) | Normale Abhaengigkeiten |
| `all_done` | Wenn alle Upstream-Tasks fertig (egal ob success/fail/skip) | Archivierung, Reports |
| `one_failed` | Mindestens ein Upstream-Task fehlgeschlagen | Fruehe Alarmierung |
| `none_failed` | Kein Upstream-Task fehlgeschlagen (skip erlaubt) | Bedingte Pfade |

### Empfehlung je nach Reifegrad

| Reifegrad | Loesung | Aufwand |
|-----------|---------|---------|
| **Einstieg** | `target/compiled/` manuell inspizieren + `dbt compile --select` | Kein Aufwand |
| **Standard** | Archiv-Task in Airflow + Streamlit-Datenqualitaets-Tab (bereits vorhanden) | Niedrig |
| **Fortgeschritten** | dbt-artifacts Package → Run-Historie in Postgres → Trend-Dashboards | Mittel |
| **Enterprise** | Elementary + `edr report` → HTML-Reports pro Run + Slack-Benachrichtigung | Mittel-Hoch |

---

## Multi-Environment Deployment (DEV / INT / PROD)

### Grundprinzip: Ein Branch, Environment per Variable

Statt branch-per-environment (fuehrt zu Drift und Merge-Konflikten) wird empfohlen, einen einzigen `main`-Branch zu verwenden. Das Verhalten pro Umgebung wird durch Environment-Variablen gesteuert:

```python
# In jedem DAG:
import os
ENV = os.getenv("AIRFLOW_ENV", "dev")

SCHEDULES = {
    "dev": None,           # nur manuell
    "int": "@daily",       # taeglich fuer Integrationstests
    "prod": "0 6 * * *",   # 06:00 in Produktion
}
```

### Dateistruktur fuer Multi-Environment

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
└── airflow/dags/                       # Lesen AIRFLOW_ENV fuer Verhalten
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
| **Runtime-veraenderbare Werte** | Airflow Variables in der DB | Feature-Flags, Schedules |

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

Ueber ein angepasstes `generate_schema_name` Macro:
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

**dbt Slim CI** (nur geaenderte Modelle testen):
```bash
# Vergleich gegen Produktions-Manifest:
dbt build --select state:modified+ --state ./prod-artifacts/ --target int
```

Das verkuerzt einen 10-Minuten-Full-Build auf ~30 Sekunden fuer inkrementelle Aenderungen.

**DAG-Validierung in CI:**
```bash
# Prueft ob alle DAGs ohne Import-Fehler parsbar sind:
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
| **CI: DAGs** | `airflow dags list` fuer Parse-Check |
| **CI: dbt** | Slim CI mit `state:modified+` gegen Produktions-Manifest |
| **Promotion** | Auto-Deploy DEV/INT bei Merge; manuelles Gate fuer PROD |
| **Rollback** | Git-Tags pro Deploy + explizite Docker Image Tags |

---

## Bekannte Einschraenkungen

- **AutomateDV Bridge + Effectivity Satellite**: Beide Macros sind in AutomateDV deprecated und wurden aus dem Projekt entfernt. Siehe [GitHub Issue](https://github.com/Datavault-UK/automate-dv/blob/master/macros/tables/postgres/bridge.sql).
- **dbt Docs Server**: Nutzt `python -m http.server` statt `dbt docs serve`, da letzterer Verbindungsabbrueche auf Docker/macOS verursacht.
- **Airflow 3 CeleryExecutor**: Die Demo nutzt CeleryExecutor + Redis + Flower. Worker koennen mit `docker compose up -d --scale airflow-worker=3` skaliert werden.
- **Inkrementelle Loads**: Der DAG `load_delta` demonstriert Delta-Loads mit AutomateDVs eingebauter Incremental-Logik. Die Delta-CSVs in `daten/delta/` enthalten neue und geaenderte Datensaetze.

---

## Ausblick: Optionale Erweiterungen

### dbt-MCP - KI-gestuetzte dbt-Entwicklung

[dbt-MCP](https://github.com/dbt-labs/dbt-mcp) ist ein MCP-Server (Model Context Protocol) von dbt Labs, der dbt-Funktionalitaet als strukturierte Tools fuer KI-Assistenten bereitstellt. Damit kann man z.B. in **Claude Desktop**, **VS Code Copilot Chat** oder **Cursor** per natuerlicher Sprache mit dem dbt-Projekt interagieren:

- *"Zeig mir die Lineage von mart_revenue_per_customer"*
- *"Generiere ein YAML fuer ein neues Staging-Modell"*
- *"Fuehre die Tests mit Tag 'quality' aus und zeig mir die Ergebnisse"*
- *"Compiliere hub_customer und zeig mir das generierte SQL"*

**Funktionsumfang ohne dbt Cloud (lokal nutzbar):**

| Kategorie | Was es kann |
|-----------|-------------|
| **dbt CLI** | `run`, `test`, `compile`, `show`, `parse`, `docs` - alles per Chat steuerbar |
| **Codegen** | YAML-Definitionen und Staging-Modelle automatisch generieren |
| **Lineage** | Abhaengigkeiten aus `manifest.json` inspizieren |

**Funktionen die dbt Cloud voraussetzen** (nicht in dieser Demo verfuegbar):
Discovery API (Model Health, Semantic Search), Semantic Layer (Metriken), Admin API (Job-Management), `text_to_sql`.

**Setup (optional):**
```bash
# Python >= 3.12 erforderlich
pip install dbt-mcp

# In Claude Desktop config (~/.claude/claude_desktop_config.json):
{
  "mcpServers": {
    "dbt": {
      "command": "uvx",
      "args": ["dbt-mcp"],
      "env": {
        "DBT_PROJECT_DIR": "/pfad/zu/new_env/dbt_project",
        "DBT_MCP_ENABLE_DBT_CLI": "true",
        "DBT_MCP_ENABLE_DBT_CODEGEN": "true"
      }
    }
  }
}
```

> **Hinweis:** dbt-MCP erlaubt KI-Assistenten, dbt-Befehle auszufuehren die Datenbank-Objekte veraendern koennen. In einer Demo-Umgebung ist das unkritisch, in Produktionsumgebungen sollte man die aktivierten Tool-Kategorien sorgfaeltig einschraenken.

### PSA-Pfad mit NG Generator (optionaler alternativer Datenfluss)

Die Demo enthaelt einen optionalen zweiten Pfad, der eine **Persistent Staging Area (PSA)** als stabile, historisierte Grundlage vor dbt einschiebt. Die PSA wird durch einen metadatengetriebenen Code-Generator ("NG Generator") erzeugt.

**Architektur-Vergleich:**
```
KLASSISCH:  CSV -> Raw -> Staging (dbt) -> Raw Vault (dbt) -> Marts
PSA-PFAD:   CSV -> Raw -> PSA (NG Gen) -> Staging (dbt) -> Raw Vault (dbt)
                          stabil!
```

**Kernvorteil:** Der Data Vault kann jederzeit aus der PSA komplett neu aufgebaut werden, ohne auf die Quelldaten (CSV) zurueckgreifen zu muessen. Die PSA schuetzt die historischen Daten **architektonisch** (nicht nur konfigurativ via `full_refresh: false`).

**PSA-Objekte (Schema `psa`, am Beispiel `customers`):**

| Objekt | Typ | Zweck |
|--------|-----|-------|
| `v_customers_ifc` | View | Interface-Abbild der Quelle mit Typ-Casting |
| `v_customers_cln` | View | Dedupliziert + Content-Hash (sha256) |
| `customers_psa` | Tabelle | SCD2-historisiert (ng_valid_from/to, ng_is_current) |
| `v_customers_cur` | View | Nur aktuelle, nicht geloeschte Version |
| `v_customers_fhi` | View | Alle Versionen (Full History) |
| `run_customers_psa_load()` | Procedure | SCD2-Load via Hash-Vergleich |
| `run_customers_delete_detection()` | Procedure | Markiert geloeschte Records |

**Demo-Ablauf:**
1. `init_raw_data` ausfuehren (CSV-Daten laden)
2. `dbt_classic` ausfuehren (klassischer Pfad zum Vergleich)
3. `psa_flow` ausfuehren (PSA aufbauen + Data Vault aus PSA)
4. Im Streamlit-Tab "PSA-Pfad" die Ergebnisse vergleichen
5. `psa_rebuild_demo` ausfuehren → beweist Vault-Rebuild aus PSA ohne CSV

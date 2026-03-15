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
| **Streamlit Portal** | http://localhost:8501 | - |
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
│  └──────────┘  │  :8081   │ │  :5050  │ │  :8501   │             │
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

> **Fuer die Demo-Umgebung** reicht `full_refresh: false` in Kombination mit dem `init_raw_data` DAG fuer einen sauberen Reset. In Produktionsumgebungen empfehlen wir mindestens zusaetzlich Postgres-Permissions und Monitoring.

---

## Bekannte Einschraenkungen

- **AutomateDV Bridge + Effectivity Satellite**: Beide Macros sind in AutomateDV deprecated und wurden aus dem Projekt entfernt. Siehe [GitHub Issue](https://github.com/Datavault-UK/automate-dv/blob/master/macros/tables/postgres/bridge.sql).
- **dbt Docs Server**: Nutzt `python -m http.server` statt `dbt docs serve`, da letzterer Verbindungsabbrueche auf Docker/macOS verursacht.
- **Airflow 3 CeleryExecutor**: Die Demo nutzt CeleryExecutor + Redis + Flower. Worker koennen mit `docker compose up -d --scale airflow-worker=3` skaliert werden.
- **Inkrementelle Loads**: Der DAG `load_delta` demonstriert Delta-Loads mit AutomateDVs eingebauter Incremental-Logik. Die Delta-CSVs in `daten/delta/` enthalten neue und geaenderte Datensaetze.

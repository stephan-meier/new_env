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
- **Docker Desktop** (macOS/Windows/Linux) mit mindestens 4 GB RAM
- **Git** (optional, zum Klonen)
- **DBeaver** oder anderer SQL-Client (optional, pgAdmin ist integriert)

### Setup
```bash
# Repository klonen
git clone <repo-url> && cd new_env

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

### 4. Ergebnisse erkunden
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
   Das offizielle Template unter `https://airflow.apache.org/docs/apache-airflow/3.0.2/docker-compose.yaml` ist die beste Ausgangsbasis. Es verwendet CeleryExecutor + Redis, kann aber auf LocalExecutor vereinfacht werden (wie in dieser Demo).

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
│   ├── dags/                   # 3 DAGs (init, classic, cosmos)
│   └── config/airflow.cfg      # Shared Secrets fuer JWT-Auth
├── postgres/init/              # DB-Init-Scripte (Schemas)
└── streamlit/                  # Portal-App
```

---

## Bekannte Einschraenkungen

- **AutomateDV Bridge + Effectivity Satellite**: Beide Macros sind in AutomateDV deprecated und wurden aus dem Projekt entfernt. Siehe [GitHub Issue](https://github.com/Datavault-UK/automate-dv/blob/master/macros/tables/postgres/bridge.sql).
- **dbt Docs Server**: Nutzt `python -m http.server` statt `dbt docs serve`, da letzterer Verbindungsabbrueche auf Docker/macOS verursacht.
- **Airflow 3 CeleryExecutor**: Die Demo nutzt CeleryExecutor + Redis + Flower. Worker koennen mit `docker compose up -d --scale airflow-worker=3` skaliert werden.
- **Keine inkrementellen Loads**: Die Demo zeigt einen Full-Load-Ansatz. Fuer inkrementelle Loads muessten die Staging-Modelle und die `init_raw_data`-Logik angepasst werden.

# CLAUDE.md — new_env Projektkontext

## Projektübersicht

Dockerisierte Demo- und Schulungsumgebung für moderne Datenarchitektur mit:
- **Apache Airflow 3** (Orchestrierung, Cosmos-Integration)
- **dbt** (Transformation, Data Vault, AutomateDV, dbt-expectations)
- **PostgreSQL** (Datenbank: `demo` + `airflow` Metadaten-DB)
- **Streamlit** (Demo-Portal mit DQ-Tab)
- **Metabase** (BI-Tool, optional via `docker-compose.bi.yml`)
- **MCP-Server** (Claude Desktop Integration, optional via `docker-compose.mcp.yml`)

## Docker Compose Struktur

```bash
# Core (immer):
docker compose -f docker-compose.yml up -d

# + BI (Metabase):
docker compose -f docker-compose.yml -f docker-compose.bi.yml up -d

# + MCP Gateway (Open WebUI + Ollama):
docker compose -f docker-compose.yml -f docker-compose.bi.yml -f docker-compose.mcp.yml up -d
```

## MCP-Konfiguration (Claude Desktop)

Konfigurationsdatei: `~/Library/Application Support/Claude/claude_desktop_config.json`
Template: `mcp/claude-desktop-config.json`

### Aktive MCP-Server

| Server | Package | Zweck |
|--------|---------|-------|
| `postgres-demo` | `@modelcontextprotocol/server-postgres` | Read-only SQL auf `demo` DB |
| `airflow-meta` | `@modelcontextprotocol/server-postgres` | Read-only auf Airflow Metadaten-DB |
| `dbt-demo` | `dbt-mcp` (dbt Labs) | Codegen: sources.yml, model YAML, staging SQL |
| `metabase-demo` | `@cognitionai/metabase-mcp-server` | Dashboards, Questions, Schema |

### Postgres MCP — Wichtig: mcp_reader User

Beide Postgres-MCP-Server verbinden sich als `mcp_reader` (read-only, kein Superuser):
```
postgresql://mcp_reader:mcp_readonly@localhost:5432/demo
postgresql://mcp_reader:mcp_readonly@localhost:5432/airflow
```
Init-Script: `postgres/init/04_create_mcp_reader.sql`

### dbt MCP — Einschränkungen

`DBT_MCP_ENABLE_DBT_CLI=false` — CLI ist deaktiviert wegen eines Bugs in `dbt-mcp`:
das Package prüft das dbt-Binary auch wenn CLI deaktiviert ist → Absturz ohne `DBT_PATH`.

**Workaround:** `DBT_PATH=/opt/anaconda3/envs/dbt/bin/dbt` setzen (Binary muss existieren,
wird aber nicht aufgerufen).

Verfügbare Tools: `generate_model_yaml`, `generate_source`, `generate_staging_model`
NICHT verfügbar: Lineage, `dbt run`, `dbt test` (→ Airflow macht das)

### Airflow MCP — Kein reifer Server für Airflow 3

Stand März 2026: Kein produktionsreifer MCP-Server für Airflow 3 / API v2 verfügbar.
- `yangkyeongmo/mcp-server-apache-airflow` → nur API v1 (in Airflow 3 entfernt)
- `abhishekbhakat/airflow-mcp-server` → Kategorie-Navigation statt echter Tools
- **Lösung:** `airflow-meta` Postgres-MCP direkt auf Airflow-Metadaten-DB

Airflow 3 API-Token (Hintergrund für Schulung):
- Web-UI: weiterhin admin/admin (Session-Cookie)
- REST-API: nur noch Bearer Token via `POST /auth/token`
- Token gültig 24h, kein permanentes Basic-Auth mehr in API v2

### Metabase MCP — API Key Pflicht

`@cognitionai/metabase-mcp-server` unterstützt **nur API Key Auth** (kein Username/Password).
API Key wird beim ersten Setup automatisch erstellt:
```bash
./metabase/setup-metabase.sh
# → speichert Key in metabase/mcp-api-key.txt (gitignored)
# → Key in claude_desktop_config.json unter METABASE_API_KEY eintragen
```

## Docker & Infrastructure

- **WICHTIG:** Bei Multi-Container Airflow 3 müssen Scheduler, API-Server und Worker
  dieselbe `airflow.cfg` mit identischen JWT/Fernet-Secrets teilen.
  Konfiguration: `airflow/config/airflow.cfg` (Volume-Mount in alle Container)

- **Cosmos-Abhängigkeiten:** `apache-airflow-providers-google` kann Konflikte verursachen.
  Nur explizit benötigte Provider installieren.

- **dbt-Projekt Volume:** Das dbt-Projekt muss in den airflow-worker Container gemountet sein
  damit Cosmos die Modelle finden kann.

## Streamlit Portal

- DQ-Tab bleibt erhalten (eingebaute Datenqualitäts-Visualisierung)
- Metabase ist der "Praxis-Upgrade" für die Schulung: Streamlit = eingebaut, Metabase = professionell
- README.md muss im Container gemountet sein für korrekte Anzeige

## Projektstruktur (wichtige Pfade)

```
new_env/
├── docker-compose.yml              # Core
├── docker-compose.bi.yml           # Metabase + DuckDB
├── docker-compose.mcp.yml          # Open WebUI + MCP Gateway
├── mcp/
│   ├── claude-desktop-config.json  # Template für Claude Desktop
│   ├── mcp-config.json             # mcpo-Config für Open WebUI Gateway
│   └── airflow-mcp-wrapper.sh      # Wrapper für zukünftigen Airflow MCP
├── metabase/
│   ├── setup-metabase.sh           # Auto-Setup + API Key Erstellung
│   ├── mcp-api-key.txt             # Gitignored! API Key für MCP
│   └── plugins/                    # duckdb.metabase-driver.jar
├── postgres/init/
│   ├── 01_create_databases.sql
│   ├── 02_create_schemas.sql
│   ├── 03_create_dq_schema.sql
│   └── 04_create_mcp_reader.sql    # mcp_reader User (read-only)
├── dbt_project/                    # dbt Northwind Data Vault
└── airflow/dags/                   # Airflow DAGs (Cosmos + native)
```

## RAM-Übersicht (Demo-Laptop 16GB)

| Stack | RAM |
|-------|-----|
| Core (Airflow + dbt + PostgreSQL + Streamlit) | ~9 GB |
| + Metabase | ~10 GB |
| + Open WebUI + MCP Gateway | ~11 GB |
| Ollama-Modell (qwen2.5:7b, auf Host) | ~5 GB zusätzlich |

## Schulungskonzept MCP

MCP ist **kein Teil der eigentlichen Schulung** — es ist ein "Surprise Goodie" am Ende
das den Ausblick auf AI-gestützte Datenpipelines zeigt.

Empfohlene Demo-Fragen die echte Tool-Calls triggern:
- *"Welche DAGs sind aktiv?"* → airflow-meta SQL auf `dag` Tabelle
- *"Zeig mir die letzten fehlgeschlagenen DAG Runs"* → airflow-meta SQL auf `dag_run`
- *"Wie viele Zeilen hat mart_revenue_per_customer?"* → postgres-demo SQL
- *"Erstelle eine sources.yml für das raw Schema"* → dbt-demo generate_source
- *"Was zeigt das Revenue Dashboard?"* → metabase-demo get_dashboard

## Voraussetzungen Host

```bash
# Bereits installiert:
uv / uvx    → ~/.local/bin/uvx
npx         → /opt/homebrew/bin/npx
Anaconda    → /opt/anaconda3/envs/dbt/bin/dbt
Ollama      → lokal installiert
```

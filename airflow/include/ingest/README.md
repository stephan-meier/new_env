# Ingest-Layer (dlt, metadatengetrieben)

Leichtgewichtiger Ingest auf Basis von [dlt](https://dlthub.com/). Quellen werden
als YAML deklariert, die Airflow-DAG-Factory generiert pro YAML einen DAG.

## Eine neue Quelle anbinden

1. YAML anlegen unter `airflow/config/ingest_sources/<name>.yml`
2. Airflow DAG-Processor (laeuft als Container) parst in < 30s → DAG erscheint
3. DAG manuell triggern oder Schedule abwarten

Beispiele siehe `airflow/config/ingest_sources/example_*.yml(.disabled)`.

## Unterstuetzte Quellentypen

| Type | Package | Treiber (falls nachzuinstallieren) |
|---|---|---|
| `postgres` | core | psycopg2 (bereits vorhanden) |
| `mssql` | core | `pip install pymssql` |
| `oracle` | core | `pip install oracledb` |
| `mysql` | core | `pip install pymysql` |
| `duckdb` | core | `pip install duckdb duckdb-engine` |
| `sqlite` | core | - |
| `rest_api` | core | - |
| `filesystem` | core | - |

Zusaetzliche DB-Treiber in `Dockerfile.airflow` erweitern.

## Konfigurationsschema

### SQL-Quellen (postgres/mssql/oracle/...)

```yaml
name: <dag_name_suffix>
type: postgres | mssql | oracle | mysql | duckdb | sqlite
connection_id: <airflow_conn_id>   # oder: connection_url: "..."
source_schema: <schema_in_source>
schedule: "<cron>"

target:
  destination: postgres
  connection_id: demo_postgres
  schema: <target_schema>
  write_disposition: append | replace | merge

tables:
  - name: <table>
    primary_key: <col> | [<col1>, <col2>]
    write_disposition: merge     # optional, ueberschreibt target-default
    incremental:                 # optional
      cursor: <column>
      initial_value: "<value>"
```

### REST-API

```yaml
name: <name>
type: rest_api
base_url: https://api.example.com
auth:
  type: bearer | api_key | basic
  token: "..."
target: { ... }
resources:
  - name: <resource_name>
    endpoint: /path/{param}
    params: { key: value }
    primary_key: id
    incremental:
      cursor: updated_at
      param: since                # Query-Param fuer Delta
```

### Filesystem

```yaml
name: <name>
type: filesystem
path: /data/drop
file_glob: "*.csv"
format: csv | jsonl | parquet
resource_name: <target_table>
primary_key: <col>
target: { ... }
```

## Architektur

```
airflow/config/ingest_sources/*.yml   ← Metadaten (deine Hand)
airflow/dags/ingest_factory.py        ← DAG-Factory (generisch)
airflow/include/ingest/runner.py      ← Dispatcher (generisch)
airflow/include/ingest/sources/*.py   ← Quellentyp-Builder (generisch)
```

Downstream (unveraendert): NG Generator kann die `raw_*` Schemas discovern
und PSA/Vault-Code generieren.

## Design-Entscheidungen

- **Modul in `include/`, nicht `plugins/`:** Airflow 3 scannt `plugins/`
  rekursiv und versucht jede `.py`-Datei als Plugin-Klasse zu laden. Das
  fuehrt Top-Level-Imports (`import dlt`) bereits beim Scheduler-Start aus
  und bricht bei fehlender Dependency den gesamten Airflow-Start. `include/`
  ist reiner PYTHONPATH-Eintrag ohne Airflow-Scan-Logik.
- **Runtime DAG-Factory statt Code-Generierung:** YAML → DAG wird zur Parse-Zeit
  des Schedulers erzeugt. Keine separate Generator-Pipeline, keine commiteten
  generierten Files. Neue Quelle anbinden = ein YAML committen.
- **Lazy Import von dlt:** dlt wird erst im Task-Worker geladen, nicht beim
  DAG-Parsing. Haelt Scheduler-Parse-Zeit niedrig.
- **State im Destination-Schema:** `_dlt_pipeline_state`-Tabelle liegt im
  Ziel-Schema (dlt-Default). Keine zusaetzliche State-Infrastruktur noetig,
  State wandert mit der DB (Backup/Restore, Replikation).
- **`DLT_DATA_DIR=/tmp/dlt_data`:** lokaler dlt-Arbeitsordner pro Run neu;
  Source-of-Truth fuer Inkremental-State bleibt die DB.
- **Runner gibt Load-Metadaten zurueck:** Load-IDs + Row-Counts via Airflow
  XCom verfuegbar fuer Reconciliation-Tasks (Pre-/Post-Checks).
- **`connection_id` bevorzugt gegenueber `connection_url`:** Secrets bleiben
  in Airflow Connections als Single Source of Truth, nicht verteilt in YAMLs.
- **`.disabled`-Suffix fuer Beispiele:** Factory ignoriert sie, Beispiele
  bleiben im Repo ohne DAG-Clutter.
- **Ein Task pro DAG (nicht pro Tabelle):** einfacher Start, weniger Overhead.
  Bei Bedarf (granulare Retries) spaeter aufdroeseln.
- **Quellentyp-Dispatch im Runner:** `ingest.sources.*` waechst linear mit
  Quellentypen, nicht mit Quellen-Instanzen. SQL-Dialekte teilen sich einen
  Builder, weil dlt's `sql_database` SQLAlchemy-basiert ist.

## Troubleshooting

- **DAG erscheint nicht:** YAML-Syntax pruefen (`python -c "import yaml; yaml.safe_load(open('...'))"`). DAG-Processor-Logs ansehen.
- **Connection-Fehler:** `airflow connections list` im Worker — `demo_postgres` muss existieren (kommt aus `AIRFLOW_CONN_DEMO_POSTGRES` Env).
- **dlt-State verloren:** State liegt im Destination-Schema (`_dlt_pipeline_state`-Tabelle). Nicht manuell loeschen.
- **Schema-Drift:** dlt evolviert automatisch. Fuer strikte Contracts siehe dlt-Docs zu `schema_contract="freeze"`.

"""
DAG: load_delta
Manuell triggerbarer DAG fuer inkrementelle Delta-Loads.
Laedt Delta-CSVs per COPY in temporaere Tabellen und fuehrt dann
ein UPSERT (INSERT ... ON CONFLICT DO UPDATE) in die Raw-Tabellen durch.
Anschliessend dbt run ohne --full-refresh, sodass AutomateDVs
eingebaute Incremental-Logik greift:
  - Hubs: INSERT nur neue Business Keys
  - Satellites: INSERT nur geaenderte Hashdiffs (neue Versionen)
  - Links: INSERT nur neue Beziehungen
"""

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime

PSQL = "PGPASSWORD=demo_pass psql -h postgres -U demo_user -d demo"
DBT = "cd /usr/app/dbt && /home/airflow/dbt_venv/bin/dbt"

# (table, csv, pk_columns, all_columns)
DELTA_TABLES = [
    (
        "customers",
        "delta/CUSTOMERS_DELTA.csv",
        ["id"],
        ["id", "last_name", "first_name", "email", "company", "phone",
         "address1", "address2", "city", "state", "postal_code", "country",
         "change_date"],
    ),
    (
        "orders",
        "delta/ORDERS_DELTA.csv",
        ["id"],
        ["id", "employee_id", "customer_id", "order_date", "shipped_date",
         "ship_name", "ship_address1", "ship_address2", "ship_city",
         "ship_state", "ship_postal_code", "ship_country", "shipping_fee",
         "payment_type", "paid_date", "order_status", "change_date"],
    ),
    (
        "order_details",
        "delta/ORDER_DETAILS_DELTA.csv",
        ["order_id", "product_id"],
        ["order_id", "product_id", "quantity", "unit_price", "discount",
         "order_detail_status", "date_allocated", "change_date"],
    ),
]


def build_upsert_command(table, csv_file, pk_cols, all_cols):
    """Erstellt ein Bash-Kommando das: temp table -> COPY -> UPSERT -> drop temp."""
    non_pk_cols = [c for c in all_cols if c not in pk_cols]
    pk_list = ", ".join(pk_cols)
    col_list = ", ".join(all_cols)
    update_set = ", ".join(f"{c} = EXCLUDED.{c}" for c in non_pk_cols)

    sql = (
        f"CREATE TEMP TABLE _delta (LIKE raw.{table} INCLUDING ALL); "
        f"COPY _delta FROM '/data/{csv_file}' WITH (FORMAT csv, HEADER true); "
        f"INSERT INTO raw.{table} ({col_list}) "
        f"SELECT {col_list} FROM _delta "
        f"ON CONFLICT ({pk_list}) DO UPDATE SET {update_set}; "
        f"DROP TABLE _delta;"
    )
    return f'{PSQL} -c "{sql}"'


with DAG(
    dag_id="load_delta",
    description="Delta-Load: CSV-Daten upsert + dbt incremental run",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["delta", "incremental"],
) as dag:

    # --- Phase 1: Delta-CSVs per Upsert in Raw-Tabellen ---
    upsert_tasks = []
    for table, csv_file, pk_cols, all_cols in DELTA_TABLES:
        task = BashOperator(
            task_id=f"upsert_{table}",
            bash_command=build_upsert_command(table, csv_file, pk_cols, all_cols),
        )
        upsert_tasks.append(task)

    # --- Phase 2: dbt run (incremental, kein full-refresh) ---
    dbt_run_staging = BashOperator(
        task_id="dbt_run_staging",
        bash_command=f"{DBT} run --select staging --profiles-dir /usr/app/dbt",
    )

    dbt_run_raw_vault = BashOperator(
        task_id="dbt_run_raw_vault",
        bash_command=f"{DBT} run --select raw_vault --profiles-dir /usr/app/dbt",
    )

    dbt_run_marts = BashOperator(
        task_id="dbt_run_marts",
        bash_command=f"{DBT} run --select marts --profiles-dir /usr/app/dbt",
    )

    # --- Phase 3: dbt test (mit store-failures fuer Visualisierung) ---
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"{DBT} test --store-failures --profiles-dir /usr/app/dbt",
    )

    # Alle Upsert-Tasks parallel, dann dbt sequentiell
    upsert_tasks >> dbt_run_staging >> dbt_run_raw_vault >> dbt_run_marts >> dbt_test

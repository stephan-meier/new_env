"""
DAG: load_delta_split
Delta-Load fuer die Split-DAG-Architektur.

Unterschied zum klassischen load_delta:
- load_delta:       Upsert + eigener dbt run (staging → raw_vault → marts → test)
- load_delta_split: NUR Upsert + Dataset publizieren → Split-Kette uebernimmt den dbt-Teil

Nach dem Upsert publiziert dieser DAG das PSA-Dataset, was die gesamte
Cosmos-Split-Kette ausloest:
  Dataset("dbt://psa_loaded") → cosmos_master → cosmos_orders → cosmos_marts

So wird der Delta-Load zum ersten Glied der Dataset-Kette - genau wie
psa_cosmos_flow es fuer den PSA-Pfad tut.

Hinweis: Wir verwenden hier bewusst das gleiche Dataset wie psa_cosmos_flow
(dbt://psa_loaded), weil cosmos_master als Consumer beides gleichbehandelt:
neue Daten sind da, egal ob via PSA oder via direktem Raw-Load.
"""

from airflow import DAG, Dataset
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime

PSQL = "PGPASSWORD=demo_pass psql -h postgres -U demo_user -d demo"

# Gleiche Delta-Tabellen wie load_delta
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

# Dataset: triggert die gesamte Split-Kette
RAW_LOADED_DATASET = Dataset("dbt://psa_loaded")


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
    dag_id="load_delta_split",
    description="Delta-Load + Dataset-Trigger fuer Cosmos-Split-Kette",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["delta", "incremental", "split"],
) as dag:

    # --- Phase 1: Delta-CSVs per Upsert in Raw-Tabellen (identisch zu load_delta) ---
    upsert_tasks = []
    for table, csv_file, pk_cols, all_cols in DELTA_TABLES:
        task = BashOperator(
            task_id=f"upsert_{table}",
            bash_command=build_upsert_command(table, csv_file, pk_cols, all_cols),
        )
        upsert_tasks.append(task)

    # --- Phase 2: Dataset publizieren (statt eigenem dbt run) ---
    # Triggert: cosmos_master → cosmos_orders → cosmos_marts
    publish_dataset = BashOperator(
        task_id="publish_raw_loaded",
        bash_command=(
            "echo 'Delta-Load complete. "
            "Publishing dataset to trigger Cosmos-Split-Kette.'"
        ),
        outlets=[RAW_LOADED_DATASET],
    )

    upsert_tasks >> publish_dataset

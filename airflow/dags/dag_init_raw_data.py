"""
DAG: init_raw_data
Manuell triggerbarer DAG zur sauberen (Re-)Initialisierung der Raw-Schicht.
Loescht bestehende Tabellen und laedt CSV-Daten per COPY.
"""

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime

PSQL = "PGPASSWORD=demo_pass psql -h postgres -U demo_user -d demo"
SQL_DIR = "/opt/airflow/dags/sql"

TABLES = [
    ("customers", "CUSTOMERS.csv"),
    ("employees", "EMPLOYEES.csv"),
    ("orders", "ORDERS.csv"),
    ("order_details", "ORDER_DETAILS.csv"),
    ("products", "PRODUCTS.csv"),
]

with DAG(
    dag_id="init_raw_data",
    description="Drop + Create Raw-Tabellen und lade CSV-Daten",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["init", "raw"],
) as dag:

    # Alle dbt-verwalteten Objekte aufraumen (Staging, Vault, Marts).
    # Verhindert Typ-Konflikte wenn Raw-Tabellen mit neuem Schema geladen werden.
    drop_dbt_objects = BashOperator(
        task_id="drop_dbt_objects",
        bash_command=f"{PSQL} -f {SQL_DIR}/drop_dbt_objects.sql",
    )

    create_tables = BashOperator(
        task_id="create_raw_tables",
        bash_command=f"{PSQL} -f {SQL_DIR}/create_raw_tables.sql",
    )

    for table_name, csv_file in TABLES:
        copy_task = BashOperator(
            task_id=f"load_{table_name}",
            bash_command=(
                f"""{PSQL} -c "COPY raw.{table_name} FROM '/data/{csv_file}' """
                f"""WITH (FORMAT csv, HEADER true);" """
            ),
        )
        create_tables >> copy_task

    drop_dbt_objects >> create_tables

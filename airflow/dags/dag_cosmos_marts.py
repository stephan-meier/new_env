"""
DAG: cosmos_marts (Consumption Layer)
Baut die Business-Tabellen (Marts) auf.

ZWEI Trigger-Modi (DatasetOrTimeSchedule):
  1. Dataset-Trigger: startet wenn cosmos_orders sein Dataset publiziert
  2. Cron-Fallback:   startet um 07:00 auch OHNE Dataset-Event
     (z.B. wenn cosmos_orders nicht lief - Marts werden mit bestehenden
     Vault-Daten gebaut, dbt-Tests dokumentieren die Veraltung)

Die Marts referenzieren Hubs, Links und Satellites aus BEIDEN Domains.
Da cosmos_orders bereits von cosmos_master abhaengt (Dataset-Kette),
reicht es theoretisch, nur auf domain_orders zu warten. Der Cron-Fallback
faengt den Fall ab, dass die Kette nicht ausgeloest wurde.

Tags: consumption (in dbt_project.yml definiert)
Modelle: mart_revenue_per_customer, mart_order_overview, mart_product_sales
"""

from datetime import datetime
from pathlib import Path

from airflow import DAG, Dataset
from airflow.timetables.datasets import DatasetOrTimeSchedule
from airflow.timetables.trigger import CronTriggerTimetable
from airflow.providers.standard.operators.bash import BashOperator
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig, RenderConfig
from cosmos.constants import LoadMode
from cosmos.profiles import PostgresUserPasswordProfileMapping

DBT_PROJECT_PATH = Path("/usr/app/dbt")
DBT_BIN = "/home/airflow/dbt_venv/bin/dbt"
DBT_CMD = f"cd {DBT_PROJECT_PATH} && {DBT_BIN}"

# Dataset-Dependency
ORDERS_DATASET = Dataset("dbt://domain_orders")

profile_config = ProfileConfig(
    profile_name="northwind_vault",
    target_name="dev",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id="demo_postgres",
        profile_args={"schema": "public"},
    ),
)

with DAG(
    dag_id="cosmos_marts",
    description="Consumption: Marts fuer Reporting (Split-Demo, mit Fallback)",
    # --- DatasetOrTimeSchedule: Dataset-Event ODER 07:00, was zuerst kommt ---
    schedule=DatasetOrTimeSchedule(
        timetable=CronTriggerTimetable("0 7 * * *", timezone="Europe/Zurich"),
        datasets=[ORDERS_DATASET],
    ),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["dbt", "cosmos", "split", "consumption"],
) as dag:

    # --- dbt TaskGroup: nur Modelle mit Tag consumption ---
    dbt_marts = DbtTaskGroup(
        group_id="dbt_marts",
        project_config=ProjectConfig(
            dbt_project_path=DBT_PROJECT_PATH,
            manifest_path=DBT_PROJECT_PATH / "target" / "manifest.json",
        ),
        profile_config=profile_config,
        execution_config=ExecutionConfig(
            dbt_executable_path=DBT_BIN,
        ),
        render_config=RenderConfig(
            load_method=LoadMode.DBT_MANIFEST,
            select=["tag:consumption"],
            emit_datasets=False,
        ),
        operator_args={
            "install_deps": False,  # Packages im Docker-Image vorinstalliert
        },
    )

    # --- dbt test: Mart-Tests dokumentieren ob Daten aktuell sind ---
    dbt_test_marts = BashOperator(
        task_id="dbt_test_marts",
        bash_command=(
            f"{DBT_CMD} test --select tag:consumption "
            f"--store-failures "
            f"--target-path {DBT_PROJECT_PATH}/target/consumption "
            f"--profiles-dir {DBT_PROJECT_PATH}"
        ),
    )

    dbt_marts >> dbt_test_marts

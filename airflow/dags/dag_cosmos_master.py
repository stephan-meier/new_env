"""
DAG: cosmos_master (Domain: Stammdaten)
Verarbeitet Kunden-, Mitarbeiter- und Produkt-Stammdaten.
Teil des Split-DAG-Demos: Zeigt wie ein grosses dbt-Projekt nach
fachlichen Domains aufgeteilt und ueber Datasets verkettet wird.

ZWEI Trigger-Modi (DatasetOrTimeSchedule):
  1. Dataset-Trigger: startet wenn psa_cosmos_flow sein Dataset publiziert
  2. Manuell/Cron:    startet auch ohne PSA (z.B. bei klassischem Raw-Pfad)

Damit funktioniert cosmos_master in BEIDEN Szenarien:
  - Klassisch:  init_raw_data -> [manuell] cosmos_master -> cosmos_orders -> cosmos_marts
  - PSA-Pfad:   init_raw_data -> psa_cosmos_flow -> [Dataset] cosmos_master -> ...

Flow: cosmos_master -> Dataset("domain_master") -> cosmos_orders -> cosmos_marts

Tags: domain_master (in dbt_project.yml definiert)
Modelle: stg_customers, stg_employees, stg_products,
         hub_customer, hub_employee, hub_product,
         sat_customer, sat_employee, sat_product
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

# Dataset-Dependencies
PSA_DATASET = Dataset("dbt://psa_loaded")       # consumed (optionaler Trigger von PSA)
MASTER_DATASET = Dataset("dbt://domain_master")  # produced (Trigger fuer cosmos_orders)

profile_config = ProfileConfig(
    profile_name="northwind_vault",
    target_name="dev",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id="demo_postgres",
        profile_args={"schema": "public"},
    ),
)

with DAG(
    dag_id="cosmos_master",
    description="Domain Stammdaten: Kunden, Mitarbeiter, Produkte (Split-Demo)",
    # --- DatasetOrTimeSchedule: PSA-Dataset ODER 05:00 Cron ---
    # Bei PSA-Pfad: psa_cosmos_flow publiziert Dataset -> cosmos_master startet
    # Bei klassischem Pfad: cosmos_master startet um 05:00 oder manuell
    schedule=DatasetOrTimeSchedule(
        timetable=CronTriggerTimetable("0 5 * * *", timezone="Europe/Zurich"),
        datasets=[PSA_DATASET],
    ),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["dbt", "cosmos", "split", "domain_master"],
) as dag:

    # --- dbt seed: laedt as_of_date (benoetigt fuer PIT-Tabellen) ---
    # Seeds werden nicht von Cosmos verwaltet, daher separater BashOperator.
    # Idempotent: dbt seed prueft ob Daten bereits vorhanden sind.
    dbt_seed = BashOperator(
        task_id="dbt_seed",
        bash_command=f"{DBT_CMD} seed --profiles-dir {DBT_PROJECT_PATH}",
    )

    # --- dbt TaskGroup: nur Modelle mit Tag domain_master ---
    dbt_master = DbtTaskGroup(
        group_id="dbt_master",
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
            select=["tag:domain_master"],
            emit_datasets=False,
        ),
        operator_args={
            "install_deps": True,
        },
    )

    # --- dbt test: nur Modelle mit Tag domain_master ---
    # --target-path: separates Artefakt-Verzeichnis pro Domain,
    # damit run_results.json nicht ueberschrieben wird (Fix fuer Streamlit)
    dbt_test_master = BashOperator(
        task_id="dbt_test_master",
        bash_command=(
            f"{DBT_CMD} test --select tag:domain_master "
            f"--store-failures "
            f"--target-path {DBT_PROJECT_PATH}/target/domain_master "
            f"--profiles-dir {DBT_PROJECT_PATH}"
        ),
    )

    # --- Dataset publizieren: signalisiert dass Stammdaten bereit sind ---
    publish_dataset = BashOperator(
        task_id="publish_master_dataset",
        bash_command="echo 'Domain master complete - publishing dataset'",
        outlets=[MASTER_DATASET],
    )

    dbt_seed >> dbt_master >> dbt_test_master >> publish_dataset

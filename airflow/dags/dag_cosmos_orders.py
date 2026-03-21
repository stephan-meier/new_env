"""
DAG: cosmos_orders (Domain: Bestellungen)
Verarbeitet Bestellungen und Bestellpositionen inkl. Links zu Stammdaten.

ZWEI Trigger-Modi (DatasetOrTimeSchedule):
  1. Dataset-Trigger: startet wenn cosmos_master sein Dataset publiziert
  2. Cron-Fallback:   startet um 06:00 auch OHNE Dataset-Event
     (z.B. wenn cosmos_master nicht lief oder fehlgeschlagen ist)

Zusaetzlich: Freshness-Check als erste Aufgabe. Wenn die Stammdaten
veraltet sind, wird trotzdem geladen (mit Warnung), nicht blockiert.

Flow: cosmos_master -> Dataset("domain_master") -> [dieser DAG] -> Dataset("domain_orders") -> cosmos_marts

Tags: domain_orders (in dbt_project.yml definiert)
Modelle: stg_orders, stg_order_details,
         hub_order, lnk_order_customer, lnk_order_employee, lnk_order_product,
         sat_order, sat_order_detail, pit_customer, pit_order
"""

from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG, Dataset
from airflow.timetables.datasets import DatasetOrTimeSchedule
from airflow.timetables.trigger import CronTriggerTimetable
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import BranchPythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig, RenderConfig
from cosmos.constants import LoadMode
from cosmos.profiles import PostgresUserPasswordProfileMapping

DBT_PROJECT_PATH = Path("/usr/app/dbt")
DBT_BIN = "/home/airflow/dbt_venv/bin/dbt"
DBT_CMD = f"cd {DBT_PROJECT_PATH} && {DBT_BIN}"

# Dataset-Dependencies
MASTER_DATASET = Dataset("dbt://domain_master")   # consumed (Trigger)
ORDERS_DATASET = Dataset("dbt://domain_orders")    # produced (Outlet)

profile_config = ProfileConfig(
    profile_name="northwind_vault",
    target_name="dev",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id="demo_postgres",
        profile_args={"schema": "public"},
    ),
)


def check_master_freshness(**context):
    """
    Prueft ob die Stammdaten-Hubs aktuell sind.
    Bei Ausloesung durch Dataset: immer frisch (cosmos_master hat gerade geladen).
    Bei Ausloesung durch Cron-Fallback: koennte veraltet sein.

    Gibt den Branch-Task-ID zurueck:
    - 'proceed_fresh': Stammdaten sind frisch, normaler Lauf
    - 'proceed_stale': Stammdaten veraltet, Lauf mit Warnung
    """
    hook = PostgresHook(postgres_conn_id="demo_postgres")
    try:
        result = hook.get_first(
            "SELECT MAX(load_datetime) FROM raw_vault.hub_customer"
        )
        last_load = result[0] if result and result[0] else None
    except Exception:
        last_load = None

    threshold = timedelta(hours=12)
    is_fresh = last_load and (datetime.now() - last_load) < threshold

    context["ti"].xcom_push(key="master_fresh", value=is_fresh)
    context["ti"].xcom_push(key="master_last_load", value=str(last_load))

    if is_fresh:
        print(f"Stammdaten frisch (letzter Load: {last_load})")
        return "proceed_fresh"
    else:
        print(f"WARNUNG: Stammdaten veraltet (letzter Load: {last_load}). "
              f"Lade trotzdem mit bestehenden Daten weiter.")
        return "proceed_stale"


with DAG(
    dag_id="cosmos_orders",
    description="Domain Bestellungen: Orders, Links, PITs (Split-Demo, mit Fallback)",
    # --- DatasetOrTimeSchedule: Dataset-Event ODER Cron, was zuerst kommt ---
    schedule=DatasetOrTimeSchedule(
        timetable=CronTriggerTimetable("0 6 * * *", timezone="Europe/Zurich"),
        datasets=[MASTER_DATASET],
    ),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["dbt", "cosmos", "split", "domain_orders"],
) as dag:

    # --- Schritt 1: Freshness-Check mit Branch ---
    check_freshness = BranchPythonOperator(
        task_id="check_master_freshness",
        python_callable=check_master_freshness,
    )

    proceed_fresh = EmptyOperator(task_id="proceed_fresh")
    proceed_stale = BashOperator(
        task_id="proceed_stale",
        bash_command=(
            'echo "WARNUNG: Stammdaten nicht frisch. '
            'Lauf mit bestehenden Hub-Daten. '
            'Tests werden Veraltung dokumentieren."'
        ),
    )

    # Join-Punkt: laeuft unabhaengig vom Branch
    ready_to_run = EmptyOperator(
        task_id="ready_to_run",
        trigger_rule="none_failed_min_one_success",
    )

    # --- Schritt 2: dbt TaskGroup fuer domain_orders ---
    dbt_orders = DbtTaskGroup(
        group_id="dbt_orders",
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
            select=["tag:domain_orders"],
            emit_datasets=False,
        ),
        operator_args={
            "install_deps": True,
        },
    )

    # --- Schritt 3: Tests (laufen IMMER, dokumentieren Veraltung) ---
    dbt_test_orders = BashOperator(
        task_id="dbt_test_orders",
        bash_command=(
            f"{DBT_CMD} test --select tag:domain_orders "
            f"--store-failures "
            f"--target-path {DBT_PROJECT_PATH}/target/domain_orders "
            f"--profiles-dir {DBT_PROJECT_PATH}"
        ),
    )

    # --- Schritt 4: Source Freshness Check (nicht blockierend) ---
    dbt_source_freshness = BashOperator(
        task_id="dbt_source_freshness",
        bash_command=(
            f"{DBT_CMD} source freshness --select source:raw "
            f"--profiles-dir {DBT_PROJECT_PATH} "
            f"--target-path {DBT_PROJECT_PATH}/target/domain_orders "
            f"--output {DBT_PROJECT_PATH}/target/domain_orders/sources.json"
            # warn_after ohne error_after = Exit-Code immer 0
        ),
    )

    # --- Schritt 5: Dataset publizieren ---
    publish_dataset = BashOperator(
        task_id="publish_orders_dataset",
        bash_command="echo 'Domain orders complete - publishing dataset'",
        outlets=[ORDERS_DATASET],
    )

    # --- Ablauf ---
    check_freshness >> [proceed_fresh, proceed_stale]
    [proceed_fresh, proceed_stale] >> ready_to_run
    ready_to_run >> dbt_orders >> [dbt_test_orders, dbt_source_freshness] >> publish_dataset

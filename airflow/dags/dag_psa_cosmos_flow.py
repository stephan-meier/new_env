"""
DAG: psa_cosmos_flow
PSA-Pfad mit Cosmos-Integration: NG Generator (SQL) + dbt via Cosmos TaskGroup.

Zeigt den Unterschied zum klassischen psa_flow (reiner BashOperator):
- Phase 1 (NG Generator): identisch - SQL-Procedures per BashOperator
- Phase 2 (dbt):          Cosmos TaskGroup statt BashOperator
                          → individuelle Tasks pro Modell, Retry auf Modell-Ebene
- Phase 3 (Dataset):      publiziert Dataset fuer nachfolgende Split-DAGs

Architektur (reale Umgebung):
  Quellsystem -> Raw -> PSA (NG Gen) -> [Dataset] -> cosmos_master -> cosmos_orders -> cosmos_marts
                          ↑ stabil!                        ↑ dieses Dataset

In der Demo nur fuer customers implementiert (tag:psa).
In Produktion: pro Quelltabelle ein eigener PSA-DAG (psa_crm, psa_erp, psa_wms, ...),
jeder publiziert ein Dataset das die nachfolgenden Domain-DAGs triggert.
"""

from datetime import datetime
from pathlib import Path

from airflow import DAG, Dataset
from airflow.providers.standard.operators.bash import BashOperator
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig, RenderConfig
from cosmos.constants import LoadMode
from cosmos.profiles import PostgresUserPasswordProfileMapping

PSQL = "PGPASSWORD=demo_pass psql -h postgres -U demo_user -d demo"
DBT_PROJECT_PATH = Path("/usr/app/dbt")
DBT_BIN = "/home/airflow/dbt_venv/bin/dbt"

# Dataset: signalisiert dass PSA-Daten bereit sind
PSA_DATASET = Dataset("dbt://psa_loaded")

profile_config = ProfileConfig(
    profile_name="northwind_vault",
    target_name="dev",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id="demo_postgres",
        profile_args={"schema": "public"},
    ),
)

with DAG(
    dag_id="psa_cosmos_flow",
    description="PSA-Pfad mit Cosmos: NG Generator + dbt TaskGroup + Dataset",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["psa", "cosmos", "ng-generator"],
) as dag:

    # ===================================================================
    # Phase 1: NG Generator (SQL) - identisch zum klassischen psa_flow
    # Diese Phase ist NICHT dbt - das sind Stored Procedures die von
    # einem Code-Generator erzeugt werden. Daher BashOperator.
    # ===================================================================
    create_psa_objects = BashOperator(
        task_id="create_psa_objects",
        bash_command=f"{PSQL} -f /opt/airflow/dags/sql/create_psa_objects.sql",
    )

    create_psa_procedures = BashOperator(
        task_id="create_psa_procedures",
        bash_command=f"{PSQL} -f /opt/airflow/dags/sql/create_psa_procedures.sql",
    )

    run_psa_load = BashOperator(
        task_id="run_psa_load",
        bash_command=f'{PSQL} -c "CALL psa.run_customers_psa_load();"',
    )

    run_delete_detection = BashOperator(
        task_id="run_delete_detection",
        bash_command=f'{PSQL} -c "CALL psa.run_customers_delete_detection();"',
    )

    # ===================================================================
    # Phase 2: dbt via Cosmos TaskGroup (tag:psa)
    # Unterschied zum klassischen psa_flow: statt einem BashOperator
    # mit "dbt run --select tag:psa" erstellt Cosmos individuelle Tasks:
    #   - stg_customers_psa (Staging aus PSA-View)
    #   - hub_customer_psa  (Hub aus PSA-Staging)
    #   - sat_customer_psa  (Satellite aus PSA-Staging)
    # Jedes Modell ist separat retry-bar und im Graph sichtbar.
    # ===================================================================
    dbt_psa = DbtTaskGroup(
        group_id="dbt_psa",
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
            select=["tag:psa"],
            emit_datasets=False,
        ),
        operator_args={
            "install_deps": False,  # Packages im Docker-Image vorinstalliert
        },
    )

    # --- dbt test auf PSA-Modelle ---
    dbt_test_psa = BashOperator(
        task_id="dbt_test_psa",
        bash_command=(
            f"cd {DBT_PROJECT_PATH} && {DBT_BIN} test "
            f"--select tag:psa --store-failures "
            f"--target-path {DBT_PROJECT_PATH}/target/psa "
            f"--profiles-dir {DBT_PROJECT_PATH}"
        ),
    )

    # ===================================================================
    # Phase 3: Dataset publizieren
    # Signalisiert: PSA-Daten sind geladen und der dbt-Vault aus PSA
    # ist aktuell. Nachfolgende DAGs (z.B. cosmos_master) koennen starten.
    # ===================================================================
    publish_dataset = BashOperator(
        task_id="publish_psa_dataset",
        bash_command="echo 'PSA load complete - publishing dataset'",
        outlets=[PSA_DATASET],
    )

    # --- Ablauf ---
    (
        create_psa_objects
        >> create_psa_procedures
        >> run_psa_load
        >> run_delete_detection
        >> dbt_psa
        >> dbt_test_psa
        >> publish_dataset
    )

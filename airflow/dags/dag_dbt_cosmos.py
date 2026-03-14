"""
DAG: dbt_cosmos
dbt-Workflow mit Astronomer Cosmos.
Cosmos parst das dbt-Projekt automatisch und erstellt
individuelle Airflow-Tasks pro dbt-Modell mit Abhaengigkeitsgraph.
"""

from datetime import datetime
from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig, RenderConfig
from cosmos.profiles import PostgresUserPasswordProfileMapping

profile_config = ProfileConfig(
    profile_name="northwind_vault",
    target_name="dev",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id="demo_postgres",
        profile_args={"schema": "public"},
    ),
)

dbt_cosmos_dag = DbtDag(
    project_config=ProjectConfig("/usr/app/dbt"),
    profile_config=profile_config,
    execution_config=ExecutionConfig(
        dbt_executable_path="/home/airflow/dbt_venv/bin/dbt",
    ),
    render_config=RenderConfig(
        emit_datasets=False,
    ),
    operator_args={
        "install_deps": True,
    },
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    dag_id="dbt_cosmos",
    description="dbt via Astronomer Cosmos - automatischer Task-Graph",
    tags=["dbt", "cosmos"],
)

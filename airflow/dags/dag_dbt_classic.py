"""
DAG: dbt_classic
Klassischer dbt-Workflow mit BashOperator: seed -> run -> test
Demonstriert den 'traditionellen' Ansatz der dbt-Orchestrierung.
"""

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime

DBT_DIR = "/usr/app/dbt"
DBT_BIN = "/home/airflow/dbt_venv/bin/dbt"
DBT_CMD = f"cd {DBT_DIR} && {DBT_BIN}"

with DAG(
    dag_id="dbt_classic",
    description="dbt via BashOperator: seed -> run -> test",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["dbt", "classic"],
) as dag:

    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=f"rm -rf {DBT_DIR}/dbt_packages && {DBT_CMD} deps --profiles-dir {DBT_DIR}",
    )

    dbt_seed = BashOperator(
        task_id="dbt_seed",
        bash_command=f"{DBT_CMD} seed --profiles-dir {DBT_DIR}",
    )

    dbt_run_staging = BashOperator(
        task_id="dbt_run_staging",
        bash_command=f"{DBT_CMD} run --select staging --profiles-dir {DBT_DIR}",
    )

    dbt_run_raw_vault = BashOperator(
        task_id="dbt_run_raw_vault",
        bash_command=f"{DBT_CMD} run --select raw_vault --profiles-dir {DBT_DIR}",
    )

    dbt_run_marts = BashOperator(
        task_id="dbt_run_marts",
        bash_command=f"{DBT_CMD} run --select marts --profiles-dir {DBT_DIR}",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"{DBT_CMD} test --store-failures --profiles-dir {DBT_DIR}",
    )

    dbt_deps >> dbt_seed >> dbt_run_staging >> dbt_run_raw_vault >> dbt_run_marts >> dbt_test

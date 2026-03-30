"""
DAG: refresh_cosmos_manifest
Aktualisiert das dbt Manifest (manifest.json) im laufenden Betrieb.

Cosmos DAGs nutzen LoadMode.DBT_MANIFEST und lesen das Manifest beim
DAG-Parse-Zyklus (alle ~5 Min). Nach Aenderungen an dbt-Modellen
diesen DAG triggern — kein Airflow-Restart oder Docker-Rebuild noetig.

Ablauf:
  1. dbt deps  (falls neue Packages in packages.yml)
  2. dbt parse (generiert manifest.json)
  3. Naechster DAG-Parse-Zyklus liest das neue Manifest automatisch

Das Manifest liegt auf dem shared Volume (dbt-target), das alle
Airflow-Services mounten. Daher reicht ein einmaliger Parse.
"""

from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator

DBT_PROJECT_PATH = Path("/usr/app/dbt")
DBT_BIN = "/home/airflow/dbt_venv/bin/dbt"
DBT_CMD = f"cd {DBT_PROJECT_PATH} && {DBT_BIN}"

with DAG(
    dag_id="refresh_cosmos_manifest",
    description="dbt Manifest aktualisieren (kein Restart noetig)",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["dbt", "cosmos", "utility"],
) as dag:

    # .DS_Store (macOS) blockiert dbt deps rmtree auf gemounteten Volumes.
    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=(
            f"find {DBT_PROJECT_PATH}/dbt_packages -name '.DS_Store' -delete 2>/dev/null; "
            f"{DBT_CMD} deps --profiles-dir {DBT_PROJECT_PATH}"
        ),
    )

    dbt_parse = BashOperator(
        task_id="dbt_parse",
        bash_command=(
            f"{DBT_CMD} parse --profiles-dir {DBT_PROJECT_PATH} --target dev && "
            f"echo 'Manifest aktualisiert:' && "
            f"ls -la {DBT_PROJECT_PATH}/target/manifest.json"
        ),
    )

    dbt_deps >> dbt_parse

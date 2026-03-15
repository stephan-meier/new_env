"""
DAG: psa_rebuild_demo
Demonstriert: Data Vault kann aus der PSA jederzeit neu aufgebaut werden.

Ablauf:
1. DROP der PSA-Pfad Vault-Tabellen (hub_customer_psa, sat_customer_psa)
2. dbt run --full-refresh auf tag:psa → Vault wird aus PSA rebuilt
3. Verify: Row-Counts pruefen

Das ist der Kernvorteil der PSA-Architektur:
- Die PSA bleibt unangetastet (historisiert, stabil)
- Der Data Vault ist eine deterministische Funktion der PSA
- Kein Zurueckgreifen auf die Quelldaten (CSV) noetig
"""

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime

PSQL = "PGPASSWORD=demo_pass psql -h postgres -U demo_user -d demo"
DBT = "cd /usr/app/dbt && /home/airflow/dbt_venv/bin/dbt"

with DAG(
    dag_id="psa_rebuild_demo",
    description="Demo: Data Vault aus PSA neu aufbauen (ohne Quelldaten)",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["psa", "demo"],
) as dag:

    drop_psa_vault = BashOperator(
        task_id="drop_psa_vault",
        bash_command=(
            f'{PSQL} -c "'
            "DROP TABLE IF EXISTS raw_vault.hub_customer_psa CASCADE; "
            "DROP TABLE IF EXISTS raw_vault.sat_customer_psa CASCADE; "
            "DROP VIEW IF EXISTS staging.stg_customers_psa CASCADE;"
            '"'
        ),
    )

    rebuild_vault = BashOperator(
        task_id="rebuild_vault",
        bash_command=(
            f"{DBT} deps --profiles-dir /usr/app/dbt && "
            f"{DBT} run --select tag:psa --full-refresh --profiles-dir /usr/app/dbt"
        ),
    )

    verify_counts = BashOperator(
        task_id="verify_counts",
        bash_command=(
            f'{PSQL} -c "'
            "SELECT 'psa.customers_psa' AS source, count(*) FROM psa.customers_psa "
            "UNION ALL SELECT 'hub_customer_psa (rebuilt)', count(*) FROM raw_vault.hub_customer_psa "
            "UNION ALL SELECT 'sat_customer_psa (rebuilt)', count(*) FROM raw_vault.sat_customer_psa;"
            '"'
        ),
    )

    drop_psa_vault >> rebuild_vault >> verify_counts

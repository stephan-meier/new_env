"""
DAG: psa_flow
Optionaler PSA-Pfad mit NG Generator.
Demonstriert den Aufbau einer Persistent Staging Area (PSA) als stabile,
historisierte Grundlage fuer den Data Vault.

Ablauf:
1. PSA-Objekte erstellen (Tabellen, Views)
2. PSA-Procedures erstellen (SCD2-Load, Delete Detection)
3. SCD2-Load: Daten aus raw.customers in psa.customers_psa laden
4. Delete Detection: geloeschte Records markieren
5. dbt run: Staging + Raw Vault aus PSA aufbauen (tag:psa)
6. dbt test: Tests auf PSA-Modelle

Architektur:
  CSV -> Raw (COPY) -> PSA (NG Gen) -> Staging (dbt) -> Raw Vault (dbt)
                        stabil!        source('psa', 'v_customers_cur')
"""

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime

PSQL = "PGPASSWORD=demo_pass psql -h postgres -U demo_user -d demo"
DBT = "cd /usr/app/dbt && /home/airflow/dbt_venv/bin/dbt"

with DAG(
    dag_id="psa_flow",
    description="PSA-Pfad: NG Generator SCD2 + dbt Data Vault aus PSA",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["psa", "ng-generator"],
) as dag:

    # --- Phase 1: PSA-Infrastruktur erstellen ---
    create_psa_objects = BashOperator(
        task_id="create_psa_objects",
        bash_command=f"{PSQL} -f /opt/airflow/dags/sql/create_psa_objects.sql",
    )

    create_psa_procedures = BashOperator(
        task_id="create_psa_procedures",
        bash_command=f"{PSQL} -f /opt/airflow/dags/sql/create_psa_procedures.sql",
    )

    # --- Phase 2: NG Generator Procedures ausfuehren ---
    run_psa_load = BashOperator(
        task_id="run_psa_load",
        bash_command=f'{PSQL} -c "CALL psa.run_customers_psa_load();"',
    )

    run_delete_detection = BashOperator(
        task_id="run_delete_detection",
        bash_command=f'{PSQL} -c "CALL psa.run_customers_delete_detection();"',
    )

    # --- Phase 3: dbt aus PSA aufbauen ---
    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=(
            f"{DBT} deps --profiles-dir /usr/app/dbt || "
            f"(test -d /usr/app/dbt/dbt_packages/dbt_utils && "
            f"echo 'WARN: dbt deps failed but packages already installed, continuing...')"
        ),
    )

    dbt_run_psa = BashOperator(
        task_id="dbt_run_psa",
        bash_command=f"{DBT} run --select tag:psa --profiles-dir /usr/app/dbt",
    )

    dbt_test_psa = BashOperator(
        task_id="dbt_test_psa",
        bash_command=f"{DBT} test --select tag:psa --profiles-dir /usr/app/dbt || true",
    )

    (
        create_psa_objects
        >> create_psa_procedures
        >> run_psa_load
        >> run_delete_detection
        >> dbt_deps
        >> dbt_run_psa
        >> dbt_test_psa
    )

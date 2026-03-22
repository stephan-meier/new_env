"""
DAG: dq_persist_results
Persistiert dbt-Testergebnisse aus run_results.json in dq.test_results.
Dient als Datenquelle fuer Metabase DQ-Dashboards.

Nutzung: Manuell ausloesen nachdem ein dbt-Test gelaufen ist
         (z.B. nach dbt_classic, cosmos_master/orders/marts).
"""

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime

from scripts.persist_dbt_results import persist_test_results

with DAG(
    dag_id="dq_persist_results",
    description="Persistiert dbt-Testergebnisse in dq.test_results (fuer Metabase)",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["dq", "metabase"],
) as dag:

    persist = PythonOperator(
        task_id="persist_dbt_test_results",
        python_callable=persist_test_results,
    )

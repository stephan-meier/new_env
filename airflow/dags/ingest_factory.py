"""DAG-Factory fuer metadatengetriebenen Ingest.

Liest alle *.yml-Dateien aus /opt/airflow/config/ingest_sources/ und
generiert pro Datei einen Airflow-DAG. Der eigentliche dlt-Pipeline-Code
liegt in /opt/airflow/plugins/ingest/ (ingest.runner).

Neue Quelle anbinden = neue YAML-Datei committen. Nach DAG-Parsing
(Scheduler/DAG-Processor) erscheint der neue DAG automatisch.
"""
from __future__ import annotations

from datetime import datetime
from pathlib import Path

import yaml
from airflow.decorators import dag, task

CONFIG_DIR = Path("/opt/airflow/config/ingest_sources")


def _load_configs():
    if not CONFIG_DIR.exists():
        return []
    return [
        (p, yaml.safe_load(p.read_text()))
        for p in sorted(CONFIG_DIR.glob("*.yml"))
    ]


def _build_dag(cfg: dict):
    dag_id = f"ingest_{cfg['name']}"
    source_type = cfg["type"]

    @dag(
        dag_id=dag_id,
        description=f"Ingest {source_type} -> {cfg.get('target', {}).get('schema', 'raw')}",
        schedule=cfg.get("schedule"),
        start_date=datetime(2024, 1, 1),
        catchup=False,
        tags=["ingest", source_type, "dlt"],
        doc_md=f"Generiert aus YAML-Config fuer Quelle '{cfg['name']}'.",
    )
    def _ingest_dag():

        @task
        def run_pipeline(config: dict) -> dict:
            # Lazy Import: dlt wird erst im Task-Worker geladen, nicht beim
            # DAG-Parsing (haelt die Scheduler-Parse-Zeit niedrig).
            from ingest.runner import run_from_config
            return run_from_config(config)

        run_pipeline(cfg)

    return _ingest_dag()


# DAGs generieren und im Modul-Namespace registrieren
for _path, _cfg in _load_configs():
    globals()[f"ingest_{_cfg['name']}"] = _build_dag(_cfg)

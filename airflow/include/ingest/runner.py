"""Ingest-Runner: YAML-Konfig -> dlt-Pipeline.

Einstiegspunkt ist run_from_config(). Wird von der Airflow-Task aufgerufen
und ruft quellentyp-spezifische Builder in ingest.sources.* auf.
"""
from __future__ import annotations

import logging
import os
from typing import Any, Dict

import dlt

log = logging.getLogger(__name__)


def run_from_config(cfg: Dict[str, Any]) -> Dict[str, Any]:
    """Fuehrt eine dlt-Pipeline anhand eines Source-Configs aus.

    Returns:
        dict mit Load-Metadaten (load_id, row_counts) fuer Logging/Reconciliation.
    """
    source_type = cfg["type"]
    name = cfg["name"]
    target = cfg.get("target", {})

    log.info("Starte Ingest '%s' (type=%s)", name, source_type)

    # dlt-State ausserhalb des Home-Dirs ablegen (Worker-Container ist nicht
    # persistent, Home wird zwischen Runs nicht garantiert erhalten).
    os.environ.setdefault("DLT_DATA_DIR", "/tmp/dlt_data")

    source = _build_source(cfg)

    pipeline = dlt.pipeline(
        pipeline_name=f"ingest_{name}",
        destination=_build_destination(target),
        dataset_name=target.get("schema", f"raw_{name}"),
        progress="log",
    )

    default_write_disposition = target.get("write_disposition", "append")
    load_info = pipeline.run(source, write_disposition=default_write_disposition)

    log.info("Load abgeschlossen: %s", load_info)

    # Rueckgabe fuer Airflow XCom / Reconciliation
    return {
        "pipeline": pipeline.pipeline_name,
        "dataset": pipeline.dataset_name,
        "loads": [p.load_id for p in load_info.load_packages],
        "row_counts": dict(load_info.metrics) if hasattr(load_info, "metrics") else {},
    }


def _build_source(cfg: Dict[str, Any]):
    """Dispatcht auf quellentyp-spezifische Builder."""
    source_type = cfg["type"]

    if source_type in ("postgres", "mssql", "oracle", "mysql", "duckdb", "sqlite"):
        from ingest.sources.sql import build_sql_source
        return build_sql_source(cfg)

    if source_type == "rest_api":
        from ingest.sources.rest import build_rest_source
        return build_rest_source(cfg)

    if source_type == "filesystem":
        from ingest.sources.filesystem import build_filesystem_source
        return build_filesystem_source(cfg)

    raise ValueError(
        f"Unbekannter source type: {source_type}. "
        f"Erlaubt: postgres, mssql, oracle, mysql, duckdb, sqlite, rest_api, filesystem"
    )


def _build_destination(target: Dict[str, Any]):
    """Baut das dlt-Destination-Objekt. Default: Postgres demo-DB."""
    dest_type = target.get("destination", "postgres")

    if dest_type == "postgres":
        # Credentials aus Airflow-Env (AIRFLOW_CONN_DEMO_POSTGRES) oder expliziter URL.
        credentials = target.get("credentials") or _airflow_conn_to_url(
            target.get("connection_id", "demo_postgres")
        )
        return dlt.destinations.postgres(credentials=credentials)

    if dest_type == "duckdb":
        return dlt.destinations.duckdb(target.get("path", "/tmp/dlt_ingest.duckdb"))

    raise ValueError(f"Unbekannter destination type: {dest_type}")


def _airflow_conn_to_url(conn_id: str) -> str:
    """Liest eine Airflow-Connection und gibt SQLAlchemy-URL zurueck."""
    from airflow.hooks.base import BaseHook
    conn = BaseHook.get_connection(conn_id)
    # dlt erwartet postgresql:// (nicht postgres://)
    uri = conn.get_uri().replace("postgres://", "postgresql://", 1)
    # Airflow haengt ggf. __extra__ an - abschneiden
    return uri.split("?")[0] if "?__extra__" in uri else uri

"""SQL-Database Source-Builder.

Unterstuetzt alle SQLAlchemy-kompatiblen Quellen: Postgres, MSSQL, Oracle,
MySQL, DuckDB, SQLite. Der Quellentyp bestimmt nur den Dialekt-Teil der
Connection-URL; die Logik ist identisch.

Treiber-Hinweise:
  - postgres: psycopg2 (bereits in apache-airflow-providers-postgres)
  - mssql:    pip install pymssql
  - oracle:   pip install oracledb
  - mysql:    pip install pymysql
  - duckdb:   pip install duckdb duckdb-engine
"""
from __future__ import annotations

from typing import Any, Dict


def build_sql_source(cfg: Dict[str, Any]):
    """Baut eine dlt sql_database Source aus einer YAML-Konfiguration.

    Erwartet in cfg:
      - type: postgres | mssql | oracle | mysql | duckdb | sqlite
      - connection_id: Airflow Connection ID (bevorzugt)
        oder
        connection_url: SQLAlchemy-URL (explizit)
      - source_schema: Schema in der Quell-DB (optional, Default: public/dbo/...)
      - tables: Liste von Tabellen-Configs
    """
    from dlt.sources.sql_database import sql_database

    credentials = _resolve_credentials(cfg)

    # sql_database laedt alle Tabellen aus schema, wir filtern per table_names
    table_names = [t["name"] for t in cfg["tables"]]

    source = sql_database(
        credentials=credentials,
        schema=cfg.get("source_schema"),
        table_names=table_names,
    )

    # Pro Tabelle: Incremental-Config, Primary Key, Write-Disposition
    for table_cfg in cfg["tables"]:
        name = table_cfg["name"]
        resource = source.resources[name]

        if "primary_key" in table_cfg:
            resource.apply_hints(primary_key=table_cfg["primary_key"])

        if "write_disposition" in table_cfg:
            resource.apply_hints(write_disposition=table_cfg["write_disposition"])

        incremental = table_cfg.get("incremental")
        if incremental:
            import dlt
            resource.apply_hints(
                incremental=dlt.sources.incremental(
                    cursor_path=incremental["cursor"],
                    initial_value=_coerce_initial_value(incremental.get("initial_value")),
                )
            )

    return source


def _coerce_initial_value(value):
    """Wandelt YAML-Werte in Python-Typen die dlt mit DB-Spalten vergleichen kann.

    dlt vergleicht initial_value typgleich mit den Quelldaten. Ein YAML-String
    "1996-01-01" scheitert bei einer datetime-Spalte. Wir konvertieren:
      - ISO-Date/Datetime-Strings -> datetime
      - YAML date (unquoted) -> datetime (mit 00:00:00)
      - alles andere unveraendert
    """
    if value is None:
        return None

    from datetime import date, datetime

    if isinstance(value, datetime):
        return value
    if isinstance(value, date):
        return datetime.combine(value, datetime.min.time())
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value)
        except ValueError:
            return value
    return value


def _resolve_credentials(cfg: Dict[str, Any]) -> str:
    """Liefert SQLAlchemy-URL entweder aus Airflow-Connection oder expliziter URL."""
    if "connection_url" in cfg:
        return cfg["connection_url"]

    if "connection_id" in cfg:
        from airflow.hooks.base import BaseHook
        conn = BaseHook.get_connection(cfg["connection_id"])
        # SQLAlchemy-Dialekt je nach type erzwingen
        dialect = _sqlalchemy_dialect(cfg["type"])
        return (
            f"{dialect}://{conn.login}:{conn.password}"
            f"@{conn.host}:{conn.port or _default_port(cfg['type'])}/{conn.schema}"
        )

    raise ValueError("SQL source benoetigt 'connection_id' oder 'connection_url'")


def _sqlalchemy_dialect(source_type: str) -> str:
    return {
        "postgres": "postgresql+psycopg2",
        "mssql": "mssql+pymssql",
        "oracle": "oracle+oracledb",
        "mysql": "mysql+pymysql",
        "duckdb": "duckdb",
        "sqlite": "sqlite",
    }[source_type]


def _default_port(source_type: str) -> int:
    return {
        "postgres": 5432,
        "mssql": 1433,
        "oracle": 1521,
        "mysql": 3306,
    }.get(source_type, 0)

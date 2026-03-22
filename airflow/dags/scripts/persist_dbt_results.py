"""
persist_dbt_results.py
Liest alle run_results.json (klassisch + Domain-Split) und
schreibt Testergebnisse in dq.test_results (PostgreSQL).

Wird als PythonOperator-Task im DAG dag_dq_persist aufgerufen.
"""

import json
import os
from pathlib import Path

import psycopg2

# ---------- Pfade (identisch zum Streamlit-Mapping) ----------
DBT_TARGET = Path("/usr/app/dbt/target")
DOMAIN_DIRS = ["domain_master", "domain_orders", "consumption", "psa"]


def _get_connection():
    """Verbindung zur Demo-Datenbank ueber Airflow-Umgebungsvariablen."""
    return psycopg2.connect(
        host=os.environ.get("POSTGRES_HOST", "postgres"),
        port=5432,
        dbname="demo",
        user="demo_user",
        password="demo_pass",
    )


def _layer_from_uid(uid: str) -> str:
    """Leitet die Schicht aus dem unique_id ab."""
    if "stg_" in uid:
        return "staging"
    if any(p in uid for p in ("hub_", "sat_", "lnk_", "pit_")):
        return "raw_vault"
    if "mart_" in uid:
        return "mart"
    return "other"


def _test_type_from_uid(uid: str) -> str:
    return "dbt-expectations" if "dbt_expectations" in uid else "standard"


def _readable_test_name(uid: str) -> str:
    """Extrahiert einen lesbaren Testnamen aus der unique_id."""
    # test.northwind_vault.<testname>.<hash> -> <testname>
    parts = uid.split(".")
    if len(parts) >= 3:
        return parts[2]
    return uid


def _model_name_from_uid(uid: str) -> str:
    """Extrahiert den Modellnamen aus der unique_id."""
    prefixes = ["stg_", "hub_", "sat_", "lnk_", "pit_", "mart_"]
    name = uid.split(".")[-1] if "." in uid else uid
    for p in prefixes:
        if p in name:
            idx = name.index(p)
            model_parts = name[idx:].split("_")
            return "_".join(model_parts[:2])
    return ""


def _collect_run_results():
    """Sammelt alle run_results.json: klassisch + pro Domain."""
    files = []
    classic = DBT_TARGET / "run_results.json"
    if classic.exists():
        files.append(("klassisch", classic))
    for domain in DOMAIN_DIRS:
        p = DBT_TARGET / domain / "run_results.json"
        if p.exists():
            files.append((domain, p))
    return files


def _parse_results(path: Path, domain: str) -> tuple:
    """Parst eine run_results.json und gibt (rows, run_id, run_at) zurueck."""
    with open(path) as f:
        data = json.load(f)

    meta = data.get("metadata", {})
    run_id = meta.get("invocation_id", "")
    run_at = meta.get("generated_at", "")

    rows = []
    for r in data.get("results", []):
        uid = r.get("unique_id", "")
        if not uid.startswith("test."):
            continue
        rows.append((
            run_id,
            run_at,
            domain,
            _readable_test_name(uid),
            _model_name_from_uid(uid),
            _layer_from_uid(uid),
            _test_type_from_uid(uid),
            r.get("status", "unknown"),
            r.get("failures", 0) or 0,
            r.get("execution_time", 0),
            r.get("message") or "",
        ))
    return rows


def _parse_freshness():
    """Parst sources.json (Split bevorzugt, Fallback klassisch)."""
    split_path = DBT_TARGET / "domain_orders" / "sources.json"
    classic_path = DBT_TARGET / "sources.json"
    path = split_path if split_path.exists() else classic_path
    if not path.exists():
        return []

    with open(path) as f:
        data = json.load(f)

    rows = []
    for r in data.get("results", []):
        rows.append((
            r.get("max_loaded_at", ""),
            r.get("unique_id", "").split(".")[-1] if "." in r.get("unique_id", "") else "",
            r.get("unique_id", "").split(".")[-2] if r.get("unique_id", "").count(".") >= 2 else "",
            r.get("max_loaded_at"),
            r.get("status", "unknown"),
            r.get("max_loaded_at_time_ago_in_s", 0),
        ))
    return rows


def persist_test_results(**kwargs):
    """Hauptfunktion: Liest alle run_results und persistiert in dq.test_results."""
    files = _collect_run_results()
    if not files:
        print("Keine run_results.json gefunden. Zuerst einen dbt-Test ausfuehren.")
        return

    all_rows = []
    for domain, path in files:
        rows = _parse_results(path, domain)
        all_rows.extend(rows)
        print(f"  {domain}: {len(rows)} Testergebnisse gelesen ({path})")

    if not all_rows:
        print("Keine Testergebnisse zum Persistieren gefunden.")
        return

    conn = _get_connection()
    try:
        with conn.cursor() as cur:
            # Ergebnisse einfuegen (keine Duplikat-Pruefung — jeder Run wird gespeichert)
            cur.executemany(
                """
                INSERT INTO dq.test_results
                    (run_id, run_at, domain, test_name, model_name,
                     layer, test_type, status, failures, execution_time, message)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                all_rows,
            )
            print(f"  {len(all_rows)} Testergebnisse in dq.test_results geschrieben.")

        # Source Freshness (optional)
        freshness_rows = _parse_freshness()
        if freshness_rows:
            with conn.cursor() as cur:
                cur.executemany(
                    """
                    INSERT INTO dq.source_freshness
                        (checked_at, source_name, table_name,
                         max_loaded_at, status, age_seconds)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    """,
                    freshness_rows,
                )
                print(f"  {len(freshness_rows)} Freshness-Ergebnisse geschrieben.")

        conn.commit()
    finally:
        conn.close()

    print("Persistierung abgeschlossen.")

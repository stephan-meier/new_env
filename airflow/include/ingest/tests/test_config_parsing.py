"""Offline-Tests fuer Config-Parsing und Typ-Coercion.

Laeuft ohne Airflow und ohne dlt. Validiert nur die Logik die wir selbst
geschrieben haben. Aufruf aus dem Repo-Root:

    python airflow/include/ingest/tests/test_config_parsing.py
"""
from __future__ import annotations

import sys
from datetime import date, datetime
from pathlib import Path

# Modul-Pfad einbinden (include/ -> sys.path)
REPO_ROOT = Path(__file__).resolve().parents[4]
AIRFLOW_DIR = REPO_ROOT / "airflow"
sys.path.insert(0, str(AIRFLOW_DIR / "include"))


def test_coerce_initial_value():
    """Alle relevanten YAML-Input-Typen muessen richtig konvertieren."""
    # Direkter Import um dlt-Abhaengigkeit in sql.py zu umgehen
    # _coerce_initial_value braucht kein dlt.
    import importlib.util
    spec = importlib.util.spec_from_file_location(
        "sql_module",
        AIRFLOW_DIR / "include" / "ingest" / "sources" / "sql.py",
    )
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    coerce = mod._coerce_initial_value

    # None bleibt None
    assert coerce(None) is None

    # datetime bleibt datetime
    dt = datetime(2020, 1, 1, 10, 30)
    assert coerce(dt) == dt

    # date -> datetime (00:00:00)
    result = coerce(date(1996, 1, 1))
    assert result == datetime(1996, 1, 1, 0, 0, 0)
    assert isinstance(result, datetime)

    # ISO-Date-String -> datetime
    assert coerce("1996-01-01") == datetime(1996, 1, 1, 0, 0, 0)

    # ISO-Datetime-String -> datetime
    assert coerce("2020-06-15T14:30:00") == datetime(2020, 6, 15, 14, 30, 0)

    # Integer bleibt Integer (numerischer Cursor)
    assert coerce(42) == 42

    # Nicht-ISO-String bleibt String
    assert coerce("not-a-date") == "not-a-date"

    print("OK _coerce_initial_value")


def test_yaml_parsing():
    """Alle mitgelieferten Beispiel-YAMLs muessen parsebar sein."""
    import yaml
    config_dir = AIRFLOW_DIR / "config" / "ingest_sources"
    assert config_dir.exists(), f"Config-Dir nicht gefunden: {config_dir}"

    active = list(config_dir.glob("*.yml"))
    disabled = list(config_dir.glob("*.yml.disabled"))
    assert active, "Keine aktiven YAML-Configs gefunden"

    for p in active + disabled:
        cfg = yaml.safe_load(p.read_text())
        assert "name" in cfg, f"{p.name}: 'name' fehlt"
        assert "type" in cfg, f"{p.name}: 'type' fehlt"
        assert cfg["type"] in (
            "postgres", "mssql", "oracle", "mysql", "duckdb", "sqlite",
            "rest_api", "filesystem",
        ), f"{p.name}: unbekannter type={cfg['type']}"
        print(f"OK YAML {p.name} (type={cfg['type']}, name={cfg['name']})")


def test_example_postgres_config_structure():
    """Die aktive Beispiel-Config muss alle erwarteten Felder haben."""
    import yaml
    config_path = AIRFLOW_DIR / "config" / "ingest_sources" / "example_demo_postgres.yml"
    cfg = yaml.safe_load(config_path.read_text())

    assert cfg["type"] == "postgres"
    assert "connection_id" in cfg
    assert "tables" in cfg and len(cfg["tables"]) >= 1
    assert "target" in cfg
    assert cfg["target"]["schema"].startswith("raw")

    for t in cfg["tables"]:
        assert "name" in t
        # Wenn incremental: cursor ist Pflicht
        if "incremental" in t:
            assert "cursor" in t["incremental"]

    print("OK example_demo_postgres.yml Struktur")


def test_source_builder_dispatch_logic():
    """Runner-Dispatch muss alle erlaubten types mappen (ohne dlt-Import)."""
    # Wir lesen die Dispatch-Logik aus dem Quelltext, weil der Import
    # von runner.py dlt triggered.
    runner_src = (AIRFLOW_DIR / "include" / "ingest" / "runner.py").read_text()

    for src_type in (
        "postgres", "mssql", "oracle", "mysql", "duckdb", "sqlite",
        "rest_api", "filesystem",
    ):
        assert f'"{src_type}"' in runner_src, \
            f"Runner kennt type '{src_type}' nicht"
    print("OK Runner-Dispatch deckt alle Quellentypen ab")


def test_dag_factory_importable_without_airflow():
    """DAG-Factory soll beim statischen Lesen keine Syntax-Fehler haben."""
    factory_path = AIRFLOW_DIR / "dags" / "ingest_factory.py"
    import ast
    tree = ast.parse(factory_path.read_text())
    # Check: _build_dag und _load_configs sind definiert
    func_names = {n.name for n in ast.walk(tree) if isinstance(n, ast.FunctionDef)}
    assert "_build_dag" in func_names
    assert "_load_configs" in func_names
    print("OK ingest_factory.py Syntax + Struktur")


def test_filesystem_format_detection():
    """Format-Auto-Detection aus file_glob-Extension."""
    import importlib.util
    spec = importlib.util.spec_from_file_location(
        "fs_module",
        AIRFLOW_DIR / "include" / "ingest" / "sources" / "filesystem.py",
    )
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    detect = mod._detect_format

    assert detect("*.csv") == "csv"
    assert detect("sales_*.csv") == "csv"
    assert detect("data.CSV") == "csv"
    assert detect("*.parquet") == "parquet"
    assert detect("*.jsonl") == "jsonl"
    assert detect("*.ndjson") == "jsonl"
    assert detect("*.txt") is None
    assert detect("*") is None
    assert detect("nested/path/*.csv") == "csv"
    print("OK _detect_format (filesystem)")


if __name__ == "__main__":
    test_coerce_initial_value()
    test_yaml_parsing()
    test_example_postgres_config_structure()
    test_source_builder_dispatch_logic()
    test_dag_factory_importable_without_airflow()
    test_filesystem_format_detection()
    print("\nAlle Tests OK")

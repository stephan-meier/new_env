"""Filesystem Source-Builder (CSV, JSONL, Parquet).

Liest Dateien aus einem Verzeichnis. Fuer Webcrawler-Output, manuelle
Drops oder Batch-Lieferungen.

Minimal-Config:
    name: sales_drops
    type: filesystem
    path: /data/drops
    file_glob: "sales_*.csv"
    target: { schema: raw_drops }

Alles andere hat sinnvolle Defaults:
    - format: aus Dateiendung im file_glob abgeleitet
    - resource_name: aus cfg['name'] abgeleitet
    - write_disposition: append (Default fuer Drop-Verzeichnisse)
    - primary_key: nicht gesetzt (kein Dedup)
"""
from __future__ import annotations

from pathlib import PurePosixPath
from typing import Any, Dict, Optional

FORMAT_BY_EXTENSION = {
    ".csv": "csv",
    ".jsonl": "jsonl",
    ".ndjson": "jsonl",
    ".parquet": "parquet",
}


def build_filesystem_source(cfg: Dict[str, Any]):
    """Baut eine dlt-Source aus einer Filesystem-Konfiguration."""
    from dlt.sources.filesystem import filesystem, read_csv, read_jsonl, read_parquet

    file_glob = cfg.get("file_glob", "*")
    fmt = cfg.get("format") or _detect_format(file_glob)
    if fmt is None:
        raise ValueError(
            f"Format konnte nicht aus file_glob '{file_glob}' abgeleitet werden. "
            f"Bitte 'format' explizit setzen (csv | jsonl | parquet)."
        )

    files = filesystem(
        bucket_url=f"file://{cfg['path']}",
        file_glob=file_glob,
    )

    readers = {"csv": read_csv, "jsonl": read_jsonl, "parquet": read_parquet}
    if fmt not in readers:
        raise ValueError(
            f"Unbekanntes filesystem format: {fmt}. Erlaubt: {list(readers)}"
        )
    reader = files | readers[fmt]()

    # resource_name default = DAG-/Pipeline-Name
    reader = reader.with_name(cfg.get("resource_name") or cfg["name"])

    if "primary_key" in cfg:
        reader.apply_hints(primary_key=cfg["primary_key"])

    if "write_disposition" in cfg:
        reader.apply_hints(write_disposition=cfg["write_disposition"])

    return reader


def _detect_format(file_glob: str) -> Optional[str]:
    """Format aus Dateiendung im Glob ableiten (*.csv -> csv)."""
    suffix = PurePosixPath(file_glob).suffix.lower()
    return FORMAT_BY_EXTENSION.get(suffix)

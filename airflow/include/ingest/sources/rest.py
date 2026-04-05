"""REST-API Source-Builder.

Nutzt dlt's rest_api-Source mit Pagination- und Auth-Support.
"""
from __future__ import annotations

from typing import Any, Dict


def build_rest_source(cfg: Dict[str, Any]):
    """Baut eine dlt rest_api Source aus einer YAML-Konfiguration.

    Erwartet in cfg:
      - base_url: API-Basis-URL
      - auth: optional (type: bearer|api_key|basic, token/key/user/password)
      - resources: Liste von Endpoint-Configs
    """
    from dlt.sources.rest_api import rest_api_source

    config = {
        "client": {
            "base_url": cfg["base_url"],
        },
        "resources": _build_resources(cfg["resources"]),
    }

    auth_cfg = cfg.get("auth")
    if auth_cfg:
        config["client"]["auth"] = _build_auth(auth_cfg)

    return rest_api_source(config)


def _build_resources(resources_cfg):
    result = []
    for r in resources_cfg:
        resource = {
            "name": r["name"],
            "endpoint": {
                "path": r["endpoint"],
                "params": r.get("params", {}),
            },
        }

        if "primary_key" in r:
            resource["primary_key"] = r["primary_key"]
        if "write_disposition" in r:
            resource["write_disposition"] = r["write_disposition"]

        incremental = r.get("incremental")
        if incremental:
            resource["endpoint"]["incremental"] = {
                "cursor_path": incremental["cursor"],
                "initial_value": incremental.get("initial_value"),
                "param": incremental.get("param", "since"),
            }

        result.append(resource)
    return result


def _build_auth(auth_cfg: Dict[str, Any]):
    auth_type = auth_cfg["type"]
    if auth_type == "bearer":
        return {"type": "bearer", "token": auth_cfg["token"]}
    if auth_type == "api_key":
        return {
            "type": "api_key",
            "api_key": auth_cfg["key"],
            "name": auth_cfg.get("header_name", "X-API-Key"),
            "location": auth_cfg.get("location", "header"),
        }
    if auth_type == "basic":
        return {
            "type": "http_basic",
            "username": auth_cfg["username"],
            "password": auth_cfg["password"],
        }
    raise ValueError(f"Unbekannter auth type: {auth_type}")

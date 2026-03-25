#!/bin/bash
# Build-Script: Kopiert aktuelle Dateien aus dem ng_work Projekt
# Aufruf: ./build.sh [--docker]

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
NG_WORK="/Users/stephanmeier/Documents/ng_work"

echo "Kopiere Dateien aus $NG_WORK ..."

# App
cp "$NG_WORK/streamlit_access.py" "$SCRIPT_DIR/app.py"

# Parser
cp "$NG_WORK/access/lib/__init__.py" "$SCRIPT_DIR/access/lib/"
cp "$NG_WORK/access/lib/dv_parser.py" "$SCRIPT_DIR/access/lib/"

# Templates
cp "$NG_WORK/access/templates/"*.tmpl "$SCRIPT_DIR/access/templates/"

echo "Dateien kopiert."

# Optional: Docker Build
if [ "$1" = "--docker" ]; then
    echo "Docker Build ..."
    cd "$SCRIPT_DIR/.."
    docker compose -f docker-compose.yml -f docker-compose.access.yml build dbt-access
    echo "Docker Build fertig."
fi

#!/bin/bash
# ============================================================
# Metabase Auto-Setup: Admin-User + PostgreSQL-Verbindung
# Wird nach dem ersten Start von Metabase ausgefuehrt.
# Nur noetig bei frischem Volume (docker compose down -v).
# ============================================================

METABASE_URL="${METABASE_URL:-http://localhost:3000}"
MAX_WAIT=120

echo "Warte auf Metabase..."
for i in $(seq 1 $MAX_WAIT); do
    STATUS=$(curl -s -o /dev/null -w "%{http_code}" "$METABASE_URL/api/health" 2>/dev/null)
    if [ "$STATUS" = "200" ]; then
        echo "Metabase ist bereit."
        break
    fi
    if [ "$i" = "$MAX_WAIT" ]; then
        echo "FEHLER: Metabase nicht erreichbar nach ${MAX_WAIT}s"
        exit 1
    fi
    sleep 1
done

# Pruefen ob Setup bereits abgeschlossen
HAS_SETUP=$(curl -s "$METABASE_URL/api/session/properties" | python3 -c "import sys,json; print(json.load(sys.stdin).get('has-user-setup', False))" 2>/dev/null)

if [ "$HAS_SETUP" = "True" ]; then
    echo "Metabase ist bereits eingerichtet. Setup uebersprungen."
    exit 0
fi

# Setup-Token holen
SETUP_TOKEN=$(curl -s "$METABASE_URL/api/session/properties" | python3 -c "import sys,json; print(json.load(sys.stdin)['setup-token'])")

echo "Richte Metabase ein (Admin-User + PostgreSQL-Verbindung)..."
RESULT=$(curl -s -X POST "$METABASE_URL/api/setup" \
  -H "Content-Type: application/json" \
  -d "{
    \"token\": \"${SETUP_TOKEN}\",
    \"user\": {
      \"first_name\": \"Admin\",
      \"last_name\": \"Demo\",
      \"email\": \"admin@demo.com\",
      \"password\": \"admin2pistor\",
      \"site_name\": \"Data Vault Demo\"
    },
    \"database\": {
      \"engine\": \"postgres\",
      \"name\": \"Demo (Data Vault)\",
      \"details\": {
        \"host\": \"postgres\",
        \"port\": 5432,
        \"dbname\": \"demo\",
        \"user\": \"demo_user\",
        \"password\": \"demo_pass\"
      }
    },
    \"prefs\": {
      \"site_name\": \"Data Vault Demo\",
      \"site_locale\": \"de\",
      \"allow_tracking\": false
    }
  }")

# Session-ID aus Setup-Antwort extrahieren
SESSION_ID=$(echo "$RESULT" | python3 -c "import sys,json; print(json.load(sys.stdin).get('id',''))" 2>/dev/null)

if [ -z "$SESSION_ID" ]; then
    echo "FEHLER: Setup fehlgeschlagen"
    echo "$RESULT"
    exit 1
fi

echo "Admin-User erstellt."

# DB-Verbindung sicherstellen (Setup-API fuegt DB nicht immer zuverlaessig hinzu)
DB_COUNT=$(curl -s "$METABASE_URL/api/database" \
  -H "X-Metabase-Session: $SESSION_ID" | python3 -c "
import sys, json
data = json.load(sys.stdin)
pg_dbs = [d for d in data.get('data', []) if d['engine'] == 'postgres']
print(len(pg_dbs))
" 2>/dev/null)

if [ "$DB_COUNT" = "0" ]; then
    echo "PostgreSQL-Verbindung wird hinzugefuegt..."
    curl -s -X POST "$METABASE_URL/api/database" \
      -H "Content-Type: application/json" \
      -H "X-Metabase-Session: $SESSION_ID" \
      -d '{
        "engine": "postgres",
        "name": "Demo (Data Vault)",
        "details": {
          "host": "postgres",
          "port": 5432,
          "dbname": "demo",
          "user": "demo_user",
          "password": "demo_pass"
        }
      }' > /dev/null 2>&1
fi

echo ""
echo "Setup erfolgreich!"
echo "  URL:   ${METABASE_URL}"
echo "  Login: admin@demo.com / admin2pistor"
echo "  DB:    Demo (Data Vault) - postgres:5432/demo"

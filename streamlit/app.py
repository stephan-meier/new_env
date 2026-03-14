import streamlit as st
import psycopg2
import pandas as pd
from pathlib import Path

st.set_page_config(
    page_title="Data Vault Demo Portal",
    page_icon="🏗️",
    layout="wide",
)

# --- DB-Verbindung (wiederverwendbar) ---
def get_connection():
    return psycopg2.connect(
        host="postgres", port=5432, dbname="demo",
        user="demo_user", password="demo_pass",
    )

# --- Navigation ---
tab_portal, tab_incremental, tab_readme = st.tabs(["Portal", "Inkrementelle Loads", "Dokumentation"])

# ==================== TAB: PORTAL ====================
with tab_portal:
    st.title("Data Vault Demo Portal")
    st.markdown("Zentrale Uebersicht fuer die dbt + AutomateDV + Airflow Demo-Umgebung")
    st.divider()

    # --- Service Links ---
    col1, col2, col3, col4, col5 = st.columns(5)

    with col1:
        st.subheader("Airflow")
        st.markdown("[Airflow Web UI](http://localhost:8080)")
        st.code("User: admin\nPasswort: admin", language=None)

    with col2:
        st.subheader("Flower")
        st.markdown("[Celery Monitor](http://localhost:5555)")
        st.caption("Worker-Status & Task-Monitoring")

    with col3:
        st.subheader("dbt Docs")
        st.markdown("[dbt Documentation](http://localhost:8081)")
        st.caption("Lineage Graph & Modell-Dokumentation")

    with col4:
        st.subheader("pgAdmin")
        st.markdown("[pgAdmin 4](http://localhost:5050)")
        st.caption("Datenbank-Administration & SQL-Editor")

    with col5:
        st.subheader("DBeaver / SQL")
        st.code(
            "Host:     localhost\n"
            "Port:     5432\n"
            "Database: demo\n"
            "User:     demo_user\n"
            "Password: demo_pass",
            language=None,
        )

    st.divider()

    # --- Quick Start ---
    st.subheader("Quick Start")
    st.markdown("""
    1. **Airflow** oeffnen und DAG `init_raw_data` triggern (laedt CSV-Daten in `raw` Schema)
    2. DAG `dbt_classic` oder `dbt_cosmos` triggern (baut Staging, Raw Vault und Marts)
    3. **pgAdmin** oder **DBeaver** verbinden und Schemas erkunden: `raw`, `staging`, `raw_vault`, `mart`
    4. **dbt Docs** oeffnen fuer den Lineage Graph
    """)

    st.divider()

    # --- Architektur ---
    st.subheader("Architektur")
    col_arch1, col_arch2 = st.columns(2)

    with col_arch1:
        st.markdown("**Datenschichten**")
        st.code(
            "CSV Dateien\n"
            "  |  (Airflow: COPY / dbt seed)\n"
            "  v\n"
            "raw Schema (Rohdaten)\n"
            "  |  (dbt staging models)\n"
            "  v\n"
            "staging Schema (bereinigt + Hash-Keys)\n"
            "  |  (AutomateDV macros)\n"
            "  v\n"
            "raw_vault Schema (Hubs, Links, Satellites)\n"
            "  |  (dbt mart models)\n"
            "  v\n"
            "mart Schema (Business Views)",
            language=None,
        )

    with col_arch2:
        st.markdown("**Data Vault Objekte**")
        st.markdown("""
        | Typ | Objekte |
        |-----|---------|
        | Hubs | Customer, Employee, Product, Order |
        | Links | Order-Customer, Order-Employee, Order-Product |
        | Satellites | 5 (je Entitaet + Order Details) |
        | PIT | Customer, Order |
        | Marts | Revenue/Customer, Order Overview, Product Sales |
        """)

    st.divider()

    # --- Live DB Status ---
    st.subheader("Datenbank Status")

    @st.cache_data(ttl=10)
    def get_schema_stats():
        try:
            conn = get_connection()
            query = """
                SELECT
                    schemaname AS schema,
                    COUNT(*) AS tables,
                    SUM(n_live_tup) AS rows
                FROM pg_stat_user_tables
                WHERE schemaname IN ('raw', 'staging', 'raw_vault', 'mart')
                GROUP BY schemaname
                ORDER BY schemaname
            """
            df = pd.read_sql(query, conn)
            conn.close()
            return df
        except Exception:
            return None

    if st.button("Status aktualisieren"):
        st.cache_data.clear()

    stats = get_schema_stats()
    if stats is not None and not stats.empty:
        st.dataframe(stats, use_container_width=True, hide_index=True)
    else:
        st.info("Keine Tabellen gefunden. Bitte zuerst die Airflow DAGs ausfuehren.")

    # --- DAG Uebersicht ---
    st.divider()
    st.subheader("Airflow DAGs")

    dag_data = {
        "DAG": ["init_raw_data", "dbt_classic", "dbt_cosmos", "load_delta"],
        "Beschreibung": [
            "DROP + CREATE raw-Tabellen, COPY CSV-Daten",
            "dbt via BashOperator: seed -> staging -> raw_vault -> marts -> test",
            "dbt via Astronomer Cosmos: automatischer Task-Graph (43 Tasks)",
            "Delta-CSVs appenden + dbt incremental run (keine Full-Refresh)",
        ],
        "Trigger": ["Manuell", "Manuell", "Manuell", "Manuell"],
        "Zweck": [
            "Saubere (Re-)Initialisierung",
            "Klassischer Ansatz",
            "Moderner Ansatz (Vergleich)",
            "Inkrementelle Loads demonstrieren",
        ],
    }
    st.dataframe(pd.DataFrame(dag_data), use_container_width=True, hide_index=True)

# ==================== TAB: INKREMENTELLE LOADS ====================
with tab_incremental:
    st.title("Inkrementelle Loads im Data Vault")
    st.markdown("""
    Diese Seite zeigt, wie AutomateDV mit **inkrementellen Loads** umgeht.
    Der DAG `load_delta` laedt Delta-CSVs (neue + geaenderte Datensaetze)
    in die bestehenden Raw-Tabellen und fuehrt dann `dbt run` **ohne** `--full-refresh` aus.
    """)
    st.divider()

    # --- Erklaerung ---
    st.subheader("Was passiert bei einem Delta-Load?")
    col_exp1, col_exp2, col_exp3 = st.columns(3)
    with col_exp1:
        st.markdown("**Hubs**")
        st.markdown("""
        - Neue Business Keys → INSERT
        - Bestehende Keys → ignoriert (LEFT JOIN anti-pattern)
        - Hubs wachsen nur bei neuen Entitaeten
        """)
    with col_exp2:
        st.markdown("**Satellites**")
        st.markdown("""
        - Neuer Hashdiff → INSERT (neue Version)
        - Gleicher Hashdiff → ignoriert
        - So entsteht die **Historisierung**
        """)
    with col_exp3:
        st.markdown("**Links**")
        st.markdown("""
        - Neue Beziehung → INSERT
        - Bestehende Kombination → ignoriert
        - Links wachsen nur bei neuen Verknuepfungen
        """)

    st.divider()

    # --- Delta-Daten Uebersicht ---
    st.subheader("Delta-Daten (Batch 2)")
    st.markdown("""
    | Datei | Inhalt | Effekt |
    |-------|--------|--------|
    | `CUSTOMERS_DELTA.csv` | 3 neue + 2 geaenderte Kunden | Hub: +3, Sat: +5 (3 neue + 2 neue Versionen) |
    | `ORDERS_DELTA.csv` | 5 neue Bestellungen | Hub: +5, Links: +5 je, Sat: +5 |
    | `ORDER_DETAILS_DELTA.csv` | 8 Positionen | Link Order-Product: +8, Sat: +8 |
    """)

    st.divider()

    # --- Live-Vergleich aus DB ---
    st.subheader("Vault-Status (Live)")

    @st.cache_data(ttl=10)
    def get_vault_counts():
        try:
            conn = get_connection()
            query = """
                SELECT relname AS tabelle, n_live_tup AS zeilen
                FROM pg_stat_user_tables
                WHERE schemaname = 'raw_vault'
                ORDER BY relname
            """
            df = pd.read_sql(query, conn)
            conn.close()
            return df
        except Exception:
            return None

    if st.button("Vault-Status aktualisieren", key="refresh_vault"):
        st.cache_data.clear()

    vault_stats = get_vault_counts()
    if vault_stats is not None and not vault_stats.empty:
        st.dataframe(vault_stats, use_container_width=True, hide_index=True)
    else:
        st.info("Keine Raw Vault Tabellen gefunden. Bitte zuerst dbt_classic oder dbt_cosmos ausfuehren.")

    st.divider()

    # --- Satellite-Historisierung Demo ---
    st.subheader("Historisierung: sat_customer (Beispiel)")
    st.markdown("""
    Nach dem Delta-Load haben die Kunden **ID 1** und **ID 2** jeweils **zwei Versionen**
    im Satellite - mit unterschiedlichem `hashdiff` und `load_datetime`.
    """)

    @st.cache_data(ttl=10)
    def get_sat_customer_history():
        try:
            conn = get_connection()
            query = """
                SELECT
                    s.customer_hk,
                    s.hashdiff,
                    s.first_name,
                    s.last_name,
                    s.email,
                    s.city,
                    s.load_datetime,
                    s.record_source
                FROM raw_vault.sat_customer s
                JOIN raw_vault.hub_customer h ON s.customer_hk = h.customer_hk
                WHERE h.customer_id IN ('1', '2', '101', '102', '103')
                ORDER BY h.customer_id::int, s.load_datetime
            """
            df = pd.read_sql(query, conn)
            conn.close()
            return df
        except Exception:
            return None

    sat_history = get_sat_customer_history()
    if sat_history is not None and not sat_history.empty:
        st.dataframe(sat_history, use_container_width=True, hide_index=True)
        if len(sat_history) > 5:
            st.success(
                f"Historisierung sichtbar: {len(sat_history)} Zeilen fuer 5 Kunden "
                f"(2 geaenderte mit je 2 Versionen + 3 neue)"
            )
    else:
        st.info(
            "Noch keine Daten. Demo-Ablauf: "
            "init_raw_data -> dbt_classic -> load_delta -> hier pruefen"
        )

    st.divider()

    # --- Demo-Anleitung ---
    st.subheader("Demo-Ablauf")
    st.markdown("""
    1. **`init_raw_data`** triggern → laedt Batch 1 (Initialdaten) in `raw`
    2. **`dbt_classic`** triggern → baut den kompletten Data Vault auf
    3. Hier den **Vault-Status** pruefen (Zeilen zaehlen)
    4. **`load_delta`** triggern → laedt Batch 2 (Delta) + inkrementeller dbt run
    5. **Vault-Status erneut pruefen** → Satellites sind gewachsen, Hubs nur minimal
    6. **Historisierung** unten pruefen → Kunden 1+2 haben je 2 Versionen
    """)

# ==================== TAB: DOKUMENTATION ====================
with tab_readme:
    readme_path = Path("/app/../README.md")
    if not readme_path.exists():
        # Fallback: im gemounteten Volume suchen
        readme_path = Path("/README.md")
    if readme_path.exists():
        st.markdown(readme_path.read_text(encoding="utf-8"))
    else:
        st.warning("README.md nicht gefunden. Bitte sicherstellen, dass die Datei im Projekt-Root liegt.")

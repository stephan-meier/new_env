import streamlit as st
import psycopg2
import pandas as pd
from pathlib import Path

st.set_page_config(
    page_title="Data Vault Demo Portal",
    page_icon="🏗️",
    layout="wide",
)

# --- Navigation ---
tab_portal, tab_readme = st.tabs(["Portal", "Dokumentation"])

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
            conn = psycopg2.connect(
                host="postgres",
                port=5432,
                dbname="demo",
                user="demo_user",
                password="demo_pass",
            )
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
        "DAG": ["init_raw_data", "dbt_classic", "dbt_cosmos"],
        "Beschreibung": [
            "DROP + CREATE raw-Tabellen, COPY CSV-Daten",
            "dbt via BashOperator: seed -> staging -> raw_vault -> marts -> test",
            "dbt via Astronomer Cosmos: automatischer Task-Graph (43 Tasks)",
        ],
        "Trigger": ["Manuell", "Manuell", "Manuell"],
        "Zweck": [
            "Saubere (Re-)Initialisierung",
            "Klassischer Ansatz",
            "Moderner Ansatz (Vergleich)",
        ],
    }
    st.dataframe(pd.DataFrame(dag_data), use_container_width=True, hide_index=True)

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

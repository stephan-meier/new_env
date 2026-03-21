import streamlit as st
import psycopg2
import pandas as pd
import json
from pathlib import Path
from datetime import datetime

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

# --- Logo ---
# st.image("/app/logo/full_r.png", width=300)

# --- Navigation ---
tab_portal, tab_quality, tab_incremental, tab_splitting, tab_psa, tab_devtips, tab_readme = st.tabs(
    ["Portal", "Datenqualitaet", "Inkrementelle Loads", "DAG-Splitting", "PSA-Pfad (NG Generator)", "Entwickler-Tipps", "Dokumentation"]
)

# ==================== TAB: PORTAL ====================
with tab_portal:
    st.title("Data Vault Demo Portal")
    st.markdown("Zentrale Übersicht für die dbt + AutomateDV + Airflow Demo-Umgebung")
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
        "DAG": [
            "init_raw_data",
            "dbt_classic",
            "dbt_cosmos",
            "cosmos_master",
            "cosmos_orders",
            "cosmos_marts",
            "load_delta",
            "load_delta_split",
            "psa_flow",
            "psa_cosmos_flow",
            "psa_rebuild_demo",
        ],
        "Beschreibung": [
            "DROP + CREATE Raw-Tabellen, COPY CSV-Daten",
            "dbt via BashOperator: seed → staging → raw_vault → marts → test",
            "dbt via Astronomer Cosmos: automatischer Task-Graph (43 Tasks)",
            "Domain Stammdaten (Cosmos Split): Kunden, Mitarbeiter, Produkte + dbt seed",
            "Domain Bestellungen (Cosmos Split): Orders, Links, PITs + Freshness-Check",
            "Consumption (Cosmos Split): Marts fuer Reporting",
            "Delta-CSVs upserten + eigener dbt run (klassisch, standalone)",
            "Delta-CSVs upserten + Dataset → triggert Cosmos-Split-Kette",
            "PSA klassisch (BashOperator): NG Generator + dbt run tag:psa",
            "PSA mit Cosmos: NG Generator + Cosmos TaskGroup + Dataset-Kette",
            "Vault aus PSA neu aufbauen (beweist Rebuild-Faehigkeit)",
        ],
        "Trigger": [
            "Manuell",
            "Manuell",
            "Manuell",
            "Dataset ODER 05:00",
            "Dataset ODER 06:00",
            "Dataset ODER 07:00",
            "Manuell",
            "Manuell",
            "Manuell",
            "Manuell",
            "Manuell",
        ],
        "Szenario": [
            "Grundlage",
            "1: Klassisch",
            "2: Cosmos",
            "3: Cosmos Split",
            "3: Cosmos Split",
            "3: Cosmos Split",
            "4a: Delta klassisch",
            "4b: Delta + Split",
            "5a: PSA klassisch",
            "5b: PSA + Cosmos",
            "5: PSA Rebuild",
        ],
    }
    st.dataframe(pd.DataFrame(dag_data), use_container_width=True, hide_index=True)

    st.divider()

    # --- Demo-Szenarien ---
    st.subheader("Demo-Szenarien (aufbauend)")
    st.markdown("""
    | # | Szenario | DAGs | Was es zeigt |
    |---|----------|------|-------------|
    | 1 | **Klassisch** | `init_raw_data` → `dbt_classic` | BashOperator-Kette, dbt-Basics |
    | 2 | **Cosmos** | `init_raw_data` → `dbt_cosmos` | Gleicher Effekt, 43 Tasks mit Auto-Graph |
    | 3 | **Cosmos Split** | `init_raw_data` → `cosmos_master` → *(auto)* → *(auto)* | Domain-Trennung, Datasets, Cron-Fallback |
    | 4a | **Delta klassisch** | `load_delta` | Upsert + eigener dbt run (standalone) |
    | 4b | **Delta + Split** | `load_delta_split` → *(auto)* Split-Kette | Upsert + Dataset → Cosmos-Kette |
    | 5a | **PSA klassisch** | `psa_flow` → `psa_rebuild_demo` | Architektonischer Schutz, BashOperator |
    | 5b | **PSA + Cosmos** | `psa_cosmos_flow` → *(auto)* ganze Split-Kette | PSA als Vorstufe + Cosmos + Dataset-Kette |

    Szenarien 1-3 sind **Alternativen** (gleicher Effekt, unterschiedliche Orchestrierung).
    Szenarien 4a/4b und 5a/5b zeigen jeweils die **klassische und die Split-Variante**.
    """)

# ==================== TAB: DATENQUALITAET ====================
with tab_quality:
    st.title("Datenqualitaet (dbt-expectations)")
    st.markdown("""
    Ergebnisse des letzten `dbt test --store-failures` Laufs.
    Die Resultate stammen aus `run_results.json` (dbt Artefakt) und den
    `dbt_test__audit`-Tabellen in PostgreSQL.
    """)
    st.divider()

    # --- run_results.json parsen ---
    # Klassische DAGs schreiben nach target/run_results.json,
    # Split-DAGs nach target/<domain>/run_results.json.
    # Wir lesen alle vorhandenen Dateien zusammen.
    TARGET_DIR = Path("/usr/app/dbt/target")
    RUN_RESULTS_PATH = TARGET_DIR / "run_results.json"
    DOMAIN_DIRS = ["domain_master", "domain_orders", "consumption", "psa"]

    def _readable_test_name(uid):
        """Erzeugt einen lesbaren Testnamen aus der unique_id."""
        # unique_id Beispiele:
        #   test.northwind_vault.not_null_stg_customers_customer_hk.abc123
        #   test.northwind_vault.dbt_expectations_expect_column_values_to_be_between_stg_orders_shipping_fee_0_100.def456
        parts = uid.split(".")
        # Der vorletzte Teil ist der eigentliche Testname (ohne Hash)
        raw_name = parts[2] if len(parts) > 2 else parts[-1]

        # Hash-Suffix entfernen (letzte 8-12 hex chars nach dem letzten Punkt)
        # In unique_id ist der Hash der letzte Teil nach dem letzten Punkt
        # raw_name selbst hat keinen Hash mehr, der ist in parts[-1]

        # dbt-expectations: Praefix kuerzen
        if raw_name.startswith("dbt_expectations_"):
            raw_name = raw_name[len("dbt_expectations_"):]

        # Bekannte Test-Praefixe lesbarer machen
        readable_prefixes = {
            "expect_column_pair_values_a_to_be_greater_than_b": "Spalte A >= Spalte B",
            "expect_column_values_to_match_regex": "Regex-Pruefung",
            "expect_column_values_to_be_between": "Wertebereich",
            "expect_column_distinct_count_to_equal": "Anzahl Distinct-Werte",
            "expect_column_proportion_of_unique_values_to_be_between": "Anteil Unique-Werte",
            "expect_compound_columns_to_be_unique": "Kombination eindeutig",
            "expect_row_values_to_have_recent_data": "Aktuelle Daten vorhanden",
            "expect_table_row_count_to_be_between": "Zeilenanzahl plausibel",
        }

        for prefix, label in readable_prefixes.items():
            if prefix in raw_name:
                # Modellname extrahieren (nach dem Praefix)
                rest = raw_name.split(prefix)[-1].strip("_")
                # Unterstrich-Kette in lesbaren Modellnamen
                model = rest.split("_")[0:3]  # z.B. stg_orders_shipping
                model_str = ".".join(model) if model else ""
                return f"{label} ({model_str})" if model_str else label

        # Standard-Tests: not_null, unique etc.
        if raw_name.startswith("not_null_"):
            col_info = raw_name[len("not_null_"):]
            return f"NOT NULL: {col_info}"
        if raw_name.startswith("unique_"):
            col_info = raw_name[len("unique_"):]
            return f"UNIQUE: {col_info}"
        if raw_name.startswith("accepted_values_"):
            col_info = raw_name[len("accepted_values_"):]
            return f"Erlaubte Werte: {col_info}"
        if raw_name.startswith("relationships_"):
            col_info = raw_name[len("relationships_"):]
            return f"Referenz: {col_info}"

        return raw_name

    def _get_model_name(uid):
        """Extrahiert den Modellnamen aus der unique_id."""
        parts = uid.split(".")
        if len(parts) > 2:
            name = parts[2]
            # Aus Testnamen das Modell ableiten (z.B. not_null_stg_customers_hk -> stg_customers)
            for prefix in ["not_null_", "unique_", "accepted_values_", "relationships_"]:
                if name.startswith(prefix):
                    rest = name[len(prefix):]
                    # stg_customers_customer_hk -> stg_customers
                    for p in ["stg_", "hub_", "sat_", "lnk_", "pit_", "mart_"]:
                        if p in rest:
                            idx = rest.index(p)
                            model_parts = rest[idx:].split("_")
                            return "_".join(model_parts[:2])
                    return rest
            # dbt-expectations: Modellname ist nach dem expect-Typ
            for p in ["stg_", "hub_", "sat_", "lnk_", "pit_", "mart_"]:
                if p in name:
                    idx = name.index(p)
                    model_parts = name[idx:].split("_")
                    return "_".join(model_parts[:2])
        return ""

    def _collect_run_results_files():
        """Sammelt alle run_results.json: klassisch + pro Domain."""
        files = []
        if RUN_RESULTS_PATH.exists():
            files.append(("klassisch", RUN_RESULTS_PATH))
        for domain in DOMAIN_DIRS:
            p = TARGET_DIR / domain / "run_results.json"
            if p.exists():
                files.append((domain, p))
        return files

    def _parse_single_run_results(path, source_label):
        """Parst eine einzelne run_results.json und gibt (rows, timestamp) zurueck."""
        with open(path) as f:
            data = json.load(f)
        rows = []
        for r in data.get("results", []):
            uid = r.get("unique_id", "")
            if not uid.startswith("test."):
                continue
            if "stg_" in uid:
                layer = "Staging"
            elif "hub_" in uid or "sat_" in uid or "lnk_" in uid or "pit_" in uid:
                layer = "Raw Vault"
            elif "mart_" in uid:
                layer = "Marts"
            else:
                layer = "Andere"
            is_expectation = "dbt_expectations" in uid
            test_type = "dbt-expectations" if is_expectation else "Standard"
            readable_name = _readable_test_name(uid)
            model_name = _get_model_name(uid)
            rows.append({
                "Quelle": source_label,
                "Schicht": layer,
                "Modell": model_name,
                "Typ": test_type,
                "Test": readable_name,
                "Status": r.get("status", "unknown"),
                "Fehler": r.get("failures", 0) or 0,
                "Laufzeit (s)": round(r.get("execution_time", 0), 3),
                "Meldung": r.get("message") or "",
            })
        ts = data.get("metadata", {}).get("generated_at", "")
        return rows, ts

    def load_run_results():
        files = _collect_run_results_files()
        if not files:
            return None, None
        all_rows = []
        latest_ts = ""
        for label, path in files:
            rows, ts = _parse_single_run_results(path, label)
            all_rows.extend(rows)
            if ts > latest_ts:
                latest_ts = ts
        if not all_rows:
            return None, None
        df = pd.DataFrame(all_rows)
        # Duplikate entfernen (gleicher Test kann in klassisch + domain vorkommen)
        df = df.drop_duplicates(subset=["Test", "Modell"], keep="last")
        return df, latest_ts

    # --- Freshness aus sources.json parsen ---
    # Klassisch: target/sources.json, Split: target/domain_orders/sources.json
    SOURCES_PATH = TARGET_DIR / "sources.json"
    SOURCES_PATH_SPLIT = TARGET_DIR / "domain_orders" / "sources.json"

    def load_freshness_results():
        # Bevorzuge Split-Ergebnis, Fallback auf klassisch
        path = SOURCES_PATH_SPLIT if SOURCES_PATH_SPLIT.exists() else SOURCES_PATH
        if not path.exists():
            return None
        with open(path) as f:
            data = json.load(f)
        rows = []
        for r in data.get("results", []):
            uid = r.get("unique_id", "")
            source_name = uid.split(".")[-1] if "." in uid else uid
            age_seconds = r.get("max_loaded_at_time_ago_in_s")
            if age_seconds is not None:
                hours = age_seconds / 3600
                if hours < 1:
                    age_str = f"{int(age_seconds / 60)} Min"
                elif hours < 48:
                    age_str = f"{hours:.1f} Std"
                else:
                    age_str = f"{hours / 24:.1f} Tage"
            else:
                age_str = "n/a"
            criteria = r.get("criteria", {})
            warn_h = criteria.get("warn_after", {}).get("count", "")
            error_h = criteria.get("error_after", {}).get("count", "")
            rows.append({
                "Source": source_name,
                "Status": r.get("status", "unknown"),
                "Alter": age_str,
                "Warn-Schwelle": f"{warn_h}h" if warn_h else "",
                "Error-Schwelle": f"{error_h}h" if error_h else "",
            })
        return pd.DataFrame(rows) if rows else None

    if st.button("Ergebnisse neu laden", key="refresh_quality"):
        st.cache_data.clear()

    results_df, generated_at = load_run_results()
    freshness_df = load_freshness_results()

    if results_df is not None and not results_df.empty:
        # --- Zusammenfassung mit Metriken ---
        if generated_at:
            st.caption(f"Letzter dbt test Lauf: {generated_at}")

        col_m1, col_m2, col_m3, col_m4 = st.columns(4)
        total = len(results_df)
        passed = len(results_df[results_df["Status"].isin(["pass", "success"])])
        warned = len(results_df[results_df["Status"] == "warn"])
        failed = len(results_df[results_df["Status"].isin(["fail", "error"])])
        col_m1.metric("Tests gesamt", total)
        col_m2.metric("Bestanden", passed)
        col_m3.metric("Warnungen", warned)
        col_m4.metric("Fehler", failed)

        # --- Freshness-Ergebnisse ---
        if freshness_df is not None and not freshness_df.empty:
            st.divider()
            st.subheader("Source Freshness (PSA)")
            st.caption("Ergebnisse von `dbt source freshness` - prüft ob Quelldaten aktuell sind")
            st.dataframe(freshness_df, hide_index=True, use_container_width=True)

        st.divider()

        # --- Filter ---
        col_f1, col_f2 = st.columns(2)
        with col_f1:
            layer_filter = st.multiselect(
                "Schicht", options=sorted(results_df["Schicht"].unique()),
                default=sorted(results_df["Schicht"].unique()),
                key="layer_filter",
            )
        with col_f2:
            status_filter = st.multiselect(
                "Status", options=sorted(results_df["Status"].unique()),
                default=sorted(results_df["Status"].unique()),
                key="status_filter",
            )

        filtered = results_df[
            results_df["Schicht"].isin(layer_filter)
            & results_df["Status"].isin(status_filter)
        ]

        # Farbcodierung
        def color_status(val):
            if val == "pass":
                return "background-color: #1a472a; color: #4ade80"
            elif val == "warn":
                return "background-color: #4a3728; color: #fbbf24"
            elif val in ("fail", "error"):
                return "background-color: #4a2028; color: #f87171"
            return ""

        styled = filtered.style.map(color_status, subset=["Status"])
        st.dataframe(styled, use_container_width=True, hide_index=True)

        # --- Demo-Highlight: shipped_date Fehler ---
        st.divider()
        st.subheader("Demo-Highlight: shipped_date < order_date")
        st.markdown("""
        Der Test `expect_column_pair_values_A_to_be_greater_than_B` hat aufgedeckt,
        dass zahlreiche Bestellungen ein Versanddatum **vor** dem Bestelldatum haben.
        Das ist ein fachliches Datenqualitätsproblem, das Standard-Tests (`not_null`, `unique`)
        nie finden würden.
        """)

        @st.cache_data(ttl=30)
        def get_shipped_before_ordered():
            try:
                conn = get_connection()
                query = """
                    SELECT
                        order_id,
                        order_date::date AS bestellt,
                        shipped_date::date AS versendet,
                        (order_date::date - shipped_date::date) AS tage_zu_frueh,
                        order_status,
                        ship_name,
                        ship_country
                    FROM staging.stg_orders
                    WHERE shipped_date IS NOT NULL
                      AND shipped_date < order_date
                    ORDER BY (order_date - shipped_date) DESC
                """
                df = pd.read_sql(query, conn)
                conn.close()
                return df
            except Exception:
                return None

        bad_orders = get_shipped_before_ordered()
        if bad_orders is not None and not bad_orders.empty:
            st.warning(f"{len(bad_orders)} Bestellungen mit Versand VOR Bestelldatum gefunden!")
            col_d1, col_d2 = st.columns(2)
            with col_d1:
                st.metric("Betroffene Bestellungen", len(bad_orders))
            with col_d2:
                st.metric("Max. Tage zu früh", int(bad_orders["tage_zu_frueh"].max()))
            st.dataframe(bad_orders.head(20), use_container_width=True, hide_index=True)
            if len(bad_orders) > 20:
                st.caption(f"Zeige 20 von {len(bad_orders)} betroffenen Bestellungen (sortiert nach Schwere)")
        else:
            st.info("Keine Anomalien gefunden oder noch kein Test-Lauf ausgeführt.")

    else:
        st.info(
            "Keine Test-Resultate gefunden. Bitte zuerst `dbt_classic` oder `load_delta` "
            "ausführen - die DAGs enthalten `dbt test --store-failures`."
        )

# ==================== TAB: INKREMENTELLE LOADS ====================
with tab_incremental:
    st.title("Inkrementelle Loads im Data Vault")
    st.markdown("""
    Diese Seite zeigt, wie AutomateDV mit **inkrementellen Loads** umgeht.
    Der DAG `load_delta` lädt Delta-CSVs (neue + geänderte Datensaetze)
    in die bestehenden Raw-Tabellen und führt dann `dbt run` **ohne** `--full-refresh` aus.
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
        - Links wachsen nur bei neuen Verknüpfungen
        """)

    st.divider()

    # --- Delta-Daten Uebersicht ---
    st.subheader("Delta-Daten (Batch 2)")
    st.markdown("""
    | Datei | Inhalt | Effekt |
    |-------|--------|--------|
    | `CUSTOMERS_DELTA.csv` | 3 neue + 2 geänderte Kunden | Hub: +3, Sat: +5 (3 neue + 2 neue Versionen) |
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
        st.info("Keine Raw Vault Tabellen gefunden. Bitte zuerst dbt_classic oder dbt_cosmos ausführen.")

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
                f"(2 geänderte mit je 2 Versionen + 3 neue)"
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
    1. **`init_raw_data`** triggern → lädt Batch 1 (Initialdaten) in `raw`
    2. **`dbt_classic`** triggern → baut den kompletten Data Vault auf
    3. Hier den **Vault-Status** prüfen (Zeilen zählen)
    4. **`load_delta`** triggern → lädt Batch 2 (Delta) + inkrementeller dbt run
    5. **Vault-Status erneut prüfen** → Satellites sind gewachsen, Hubs nur minimal
    6. **Historisierung** unten prüfen → Kunden 1+2 haben je 2 Versionen
    """)

# ==================== TAB: DAG-SPLITTING ====================
with tab_splitting:
    st.title("DAG-Splitting: Monolithisch vs. Aufgeteilt")
    st.markdown("""
    Vergleich zwischen dem monolithischen `dbt_cosmos` DAG (alles in einem)
    und der aufgeteilten Variante mit **3 Domain-DAGs**, verkettet ueber **Airflow Datasets**.
    """)
    st.divider()

    # --- Vergleichstabelle ---
    st.subheader("Vergleich auf einen Blick")
    comparison = {
        "Aspekt": [
            "Anzahl DAGs",
            "Trigger",
            "Fehler-Isolation",
            "Parallelitaet",
            "Monitoring",
            "Retry",
            "Skalierung",
            "Bei fehlender Quelle",
        ],
        "dbt_cosmos (monolithisch)": [
            "1 DAG mit 43 Tasks",
            "Manuell",
            "Ein Fehler stoppt alles",
            "Innerhalb eines DAG",
            "Eine Statusanzeige",
            "Gesamter Graph oder manuell",
            "Ein Worker-Pool",
            "Harter Fehler",
        ],
        "cosmos_split (3 DAGs)": [
            "3 DAGs (master, orders, marts)",
            "Dataset-Kette + Cron-Fallback",
            "Nur die betroffene Domain stoppt",
            "Ueber DAG-Grenzen hinweg",
            "Pro Domain separat",
            "Pro Domain unabhaengig",
            "Verschiedene Pools moeglich",
            "Cron-Fallback + Warnung",
        ],
    }
    st.dataframe(pd.DataFrame(comparison), use_container_width=True, hide_index=True)

    st.divider()

    # --- Architektur-Diagramm ---
    st.subheader("Architektur: 3 Domains mit Dataset-Kette")
    col_flow1, col_flow2, col_flow3 = st.columns(3)
    with col_flow1:
        st.markdown("**cosmos_master**")
        st.markdown("*Domain: Stammdaten*")
        st.code(
            "Trigger: Manuell\n"
            "\n"
            "stg_customers\n"
            "stg_employees\n"
            "stg_products\n"
            "hub_customer\n"
            "hub_employee\n"
            "hub_product\n"
            "sat_customer\n"
            "sat_employee\n"
            "sat_product\n"
            "\n"
            "-> dbt test (domain_master)\n"
            "-> Dataset: dbt://domain_master",
            language=None,
        )
    with col_flow2:
        st.markdown("**cosmos_orders**")
        st.markdown("*Domain: Bestellungen*")
        st.code(
            "Trigger: Dataset ODER 06:00\n"
            "\n"
            "[Freshness-Check]\n"
            "  frisch -> normaler Lauf\n"
            "  veraltet -> Lauf mit Warnung\n"
            "\n"
            "stg_orders, stg_order_details\n"
            "hub_order\n"
            "lnk_order_customer\n"
            "lnk_order_employee\n"
            "lnk_order_product\n"
            "sat_order, sat_order_detail\n"
            "pit_customer, pit_order\n"
            "\n"
            "-> dbt test + source freshness\n"
            "-> Dataset: dbt://domain_orders",
            language=None,
        )
    with col_flow3:
        st.markdown("**cosmos_marts**")
        st.markdown("*Domain: Consumption*")
        st.code(
            "Trigger: Dataset ODER 07:00\n"
            "\n"
            "mart_revenue_per_customer\n"
            "mart_order_overview\n"
            "mart_product_sales\n"
            "\n"
            "-> dbt test (consumption)",
            language=None,
        )

    st.info(
        "**Dataset-Kette:** `cosmos_master` publiziert `dbt://domain_master` → "
        "triggert `cosmos_orders` → publiziert `dbt://domain_orders` → "
        "triggert `cosmos_marts`. In der Airflow UI unter **Datasets** sichtbar.\n\n"
        "**Mit PSA-Vorstufe:** `psa_cosmos_flow` publiziert `dbt://psa_loaded` → "
        "triggert `cosmos_master` → die ganze Kette laeuft automatisch weiter."
    )

    st.divider()

    # --- Domain-Tags ---
    st.subheader("Grundlage: dbt Domain-Tags")
    st.markdown("""
    Die Aufteilung basiert auf **Tags** in `dbt_project.yml`. Cosmos selektiert
    die Modelle pro DAG mit `select=["tag:domain_master"]` etc.
    """)
    tag_data = {
        "Tag": ["domain_master", "domain_master", "domain_master",
                "domain_orders", "domain_orders", "domain_orders",
                "consumption", "consumption", "consumption"],
        "Modelle": [
            "stg_customers, stg_employees, stg_products",
            "hub_customer, hub_employee, hub_product",
            "sat_customer, sat_employee, sat_product",
            "stg_orders, stg_order_details",
            "hub_order, alle Links (lnk_order_*)",
            "sat_order, sat_order_detail, pit_customer, pit_order",
            "mart_revenue_per_customer",
            "mart_order_overview",
            "mart_product_sales",
        ],
        "Schicht": [
            "Staging", "Hubs", "Satellites",
            "Staging", "Hubs + Links", "Satellites + PITs",
            "Mart", "Mart", "Mart",
        ],
    }
    st.dataframe(pd.DataFrame(tag_data), use_container_width=True, hide_index=True)

    st.divider()

    # --- Graceful Degradation ---
    st.subheader("Graceful Degradation: Was passiert bei fehlenden Quellen?")
    st.markdown("""
    In einer Umgebung mit 100+ DAGs liefert nicht jede Quelle rechtzeitig.
    **Nicht jeder Ladeausfall sollte die Pipeline blockieren.**
    """)

    col_gd1, col_gd2 = st.columns(2)
    with col_gd1:
        st.markdown("**DatasetOrTimeSchedule**")
        st.markdown("""
        Jeder Split-DAG hat zwei Trigger:
        - **Dataset**: Startet sofort wenn die Vorstufe fertig ist
        - **Cron-Fallback**: Startet spaetestens um 06:00/07:00

        Wenn `cosmos_master` nicht laeuft (Quelle fehlt),
        starten `cosmos_orders` und `cosmos_marts` trotzdem
        per Cron und arbeiten mit den bestehenden Daten.
        """)
    with col_gd2:
        st.markdown("**Freshness-Check (BranchPythonOperator)**")
        st.markdown("""
        `cosmos_orders` prueft als Erstes, ob die Stammdaten frisch sind:
        - **Frisch**: Normaler Lauf (gruener Pfad)
        - **Veraltet**: Warnung, aber trotzdem Lauf (gelber Pfad)

        In beiden Faellen laufen Tests und dokumentieren den Zustand.
        Im Graph View der Airflow UI sieht man, welcher Branch genommen wurde.
        """)

    st.divider()

    st.markdown("**Source Freshness mit `warn_after` (ohne `error_after`)**")
    st.markdown("""
    In `_staging__sources.yml` ist fuer jede Quelle ein `warn_after`-Schwellwert
    konfiguriert, aber bewusst **kein** `error_after`:

    | Source | warn_after | error_after | Effekt |
    |--------|-----------|-------------|--------|
    | customers | 12h | - | Warnung wenn aelter als 12h, nie ein Fehler |
    | employees | 12h | - | dto. |
    | orders | 24h | - | Bestellungen koennen aelter sein |
    | products | 48h | - | Stammdaten aendern sich selten |

    `dbt source freshness` gibt **immer Exit-Code 0** (Erfolg).
    Veraltung wird in `target/sources.json` dokumentiert, nicht blockiert.
    """)

    st.success(
        "**Kernprinzip:** Fehlende Quellen sind ein Dokumentations-Event, "
        "kein Fehler-Event. Die Pipeline laeuft weiter, Tests zeigen was "
        "veraltet ist, und das Monitoring alarmiert das Team."
    )

    st.divider()

    # --- Live-Status der Vault-Schichten ---
    st.subheader("Vault-Status nach Domain")

    @st.cache_data(ttl=10)
    def get_domain_stats():
        try:
            conn = get_connection()
            query = """
                SELECT
                    CASE
                        WHEN relname IN ('hub_customer', 'hub_employee', 'hub_product',
                                         'sat_customer', 'sat_employee', 'sat_product')
                            THEN 'domain_master'
                        WHEN relname IN ('hub_order', 'lnk_order_customer', 'lnk_order_employee',
                                         'lnk_order_product', 'sat_order', 'sat_order_detail',
                                         'pit_customer', 'pit_order')
                            THEN 'domain_orders'
                        WHEN relname LIKE 'mart_%'
                            THEN 'consumption'
                        ELSE 'andere'
                    END AS domain,
                    relname AS tabelle,
                    n_live_tup AS zeilen
                FROM pg_stat_user_tables
                WHERE schemaname IN ('raw_vault', 'mart')
                ORDER BY domain, relname
            """
            df = pd.read_sql(query, conn)
            conn.close()
            return df
        except Exception:
            return None

    if st.button("Status aktualisieren", key="refresh_splitting"):
        st.cache_data.clear()

    domain_stats = get_domain_stats()
    if domain_stats is not None and not domain_stats.empty:
        for domain in ["domain_master", "domain_orders", "consumption"]:
            subset = domain_stats[domain_stats["domain"] == domain]
            if not subset.empty:
                label = {"domain_master": "Stammdaten", "domain_orders": "Bestellungen",
                         "consumption": "Consumption (Marts)"}
                st.markdown(f"**{label.get(domain, domain)}** ({len(subset)} Tabellen, "
                            f"{int(subset['zeilen'].sum())} Zeilen)")
                st.dataframe(subset[["tabelle", "zeilen"]], use_container_width=True, hide_index=True)
    else:
        st.info("Keine Vault-Tabellen gefunden. Bitte zuerst einen der dbt-DAGs ausfuehren.")

    st.divider()

    # --- Demo-Ablauf ---
    st.subheader("Demo-Ablauf: Monolithisch vs. Split")
    st.markdown("""
    **Variante A - Monolithisch (zum Vergleich):**
    1. `init_raw_data` triggern
    2. `dbt_cosmos` triggern → 1 DAG, 43 Tasks, alles auf einmal

    **Variante B - Split (das neue Pattern):**
    1. `init_raw_data` triggern
    2. `cosmos_master` triggern → nur Stammdaten
    3. `cosmos_orders` startet **automatisch** (Dataset-Trigger von master)
    4. `cosmos_marts` startet **automatisch** (Dataset-Trigger von orders)

    **Was man in der Airflow UI sieht:**
    - **Datasets-Tab**: Die Kette `dbt://domain_master` → `dbt://domain_orders`
    - **Graph View** von `cosmos_orders`: Der Freshness-Check-Branch
    - **DAG-Runs**: Drei separate Laeufe mit unabhaengigem Status
    """)

# ==================== TAB: PSA-PFAD (NG GENERATOR) ====================
with tab_psa:
    st.title("PSA-Pfad (NG Generator)")
    st.markdown("Optionaler alternativer Datenfluss mit einer **Persistent Staging Area** als stabile Grundlage.")
    st.divider()

    # --- Architektur-Vergleich ---
    st.subheader("Architektur-Vergleich")
    col_classic, col_psa = st.columns(2)
    with col_classic:
        st.markdown("**Klassischer Pfad**")
        st.code(
            "CSV -> Raw (COPY)\n"
            "  -> Staging (dbt Views, Hash-Keys)\n"
            "  -> Raw Vault (dbt incremental)\n"
            "  -> Marts (dbt table)\n"
            "\n"
            "Historisierung: erst im Data Vault\n"
            "Schutz: full_refresh: false (konfigurativ)",
            language=None,
        )
    with col_psa:
        st.markdown("**PSA-Pfad (NG Generator)**")
        st.code(
            "CSV -> Raw (COPY)\n"
            "  -> PSA (NG Gen: SCD2, Delete Detection)\n"
            "  -> Staging (dbt Views, Hash-Keys)\n"
            "  -> Raw Vault (dbt incremental)\n"
            "  -> Marts (dbt table)\n"
            "\n"
            "Historisierung: bereits in der PSA!\n"
            "Schutz: architektonisch (PSA unabhängig von dbt)",
            language=None,
        )

    st.success(
        "**Kernvorteil:** Die PSA ist eine stabile, historisierte Grundlage. "
        "Der Data Vault kann jederzeit aus der PSA neu aufgebaut werden - "
        "ohne Zurückgreifen auf die Quelldaten (CSV). "
        "Das ist architektonischer Schutz statt nur Konfiguration."
    )

    st.divider()

    # --- PSA-Status ---
    st.subheader("PSA-Status")
    try:
        conn = get_connection()
        cur = conn.cursor()

        # Pruefen ob PSA-Tabelle existiert
        cur.execute("""
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables
                WHERE table_schema = 'psa' AND table_name = 'customers_psa'
            )
        """)
        psa_exists = cur.fetchone()[0]

        if psa_exists:
            cur.execute("SELECT count(*) FROM psa.customers_psa")
            psa_total = cur.fetchone()[0]
            cur.execute("SELECT count(*) FROM psa.v_customers_cur")
            psa_current = cur.fetchone()[0]
            cur.execute("SELECT count(*) FROM psa.v_customers_del")
            psa_deleted = cur.fetchone()[0]
            psa_versions = psa_total - psa_current - psa_deleted

            col1, col2, col3, col4 = st.columns(4)
            col1.metric("PSA Gesamt", psa_total)
            col2.metric("Aktuelle Versionen", psa_current)
            col3.metric("Historische Versionen", psa_versions)
            col4.metric("Geloeschte Records", psa_deleted)

            # Side-by-Side Vergleich mit klassischem Pfad
            st.divider()
            st.subheader("Side-by-Side: Klassisch vs. PSA-Pfad")

            comparison_data = []
            for tbl in ["hub_customer", "sat_customer"]:
                try:
                    cur.execute(f"SELECT count(*) FROM raw_vault.{tbl}")
                    classic_count = cur.fetchone()[0]
                except Exception:
                    classic_count = "n/a"
                try:
                    cur.execute(f"SELECT count(*) FROM raw_vault.{tbl}_psa")
                    psa_count = cur.fetchone()[0]
                except Exception:
                    psa_count = "n/a"
                comparison_data.append({
                    "Tabelle": tbl,
                    "Klassisch (raw_vault)": classic_count,
                    "PSA-Pfad (raw_vault)": psa_count,
                    "Match": "Ja" if classic_count == psa_count else "Nein"
                })

            st.dataframe(
                pd.DataFrame(comparison_data),
                use_container_width=True,
                hide_index=True,
            )

            # PSA-Historien-Viewer
            st.divider()
            st.subheader("PSA-Historien-Viewer (SCD2)")
            st.markdown("Zeigt alle Versionen eines Kunden in der PSA:")

            cur.execute("""
                SELECT id, last_name, first_name, email, city,
                       ng_valid_from, ng_valid_to, ng_is_current, ng_is_deleted,
                       ng_rowhash
                FROM psa.customers_psa
                ORDER BY id, ng_valid_from
                LIMIT 50
            """)
            cols = [desc[0] for desc in cur.description]
            rows = cur.fetchall()
            if rows:
                df_hist = pd.DataFrame(rows, columns=cols)
                st.dataframe(df_hist, use_container_width=True, hide_index=True)
            else:
                st.info("Keine Daten in der PSA. Bitte zuerst den DAG 'psa_flow' ausführen.")

        else:
            st.warning(
                "PSA noch nicht initialisiert. Bitte folgende Schritte ausführen:\n\n"
                "1. DAG `init_raw_data` triggern (Rohdaten laden)\n"
                "2. DAG `psa_flow` triggern (PSA aufbauen)"
            )

        conn.close()
    except Exception as e:
        st.error(f"Datenbankfehler: {e}")

    st.divider()

    # --- Demo-Workflow ---
    st.subheader("Demo-Workflow")

    col_psa_classic, col_psa_cosmos = st.columns(2)
    with col_psa_classic:
        st.markdown("**Variante A: PSA klassisch (BashOperator)**")
        st.markdown("""
        1. `init_raw_data` ausfuehren
        2. `dbt_classic` ausfuehren (klassischer Vault)
        3. `psa_flow` ausfuehren (PSA + Vault aus PSA)
        4. Hier Ergebnisse vergleichen
        5. `psa_rebuild_demo` ausfuehren (Rebuild-Beweis)
        """)
    with col_psa_cosmos:
        st.markdown("**Variante B: PSA + Cosmos (empfohlen)**")
        st.markdown("""
        1. `init_raw_data` ausfuehren
        2. `psa_cosmos_flow` triggern:
           - NG Generator laedt PSA
           - Cosmos baut Vault aus PSA (individuelle Tasks)
           - Publiziert `Dataset("dbt://psa_loaded")`
        3. `cosmos_master` startet **automatisch** (Dataset)
        4. `cosmos_orders` startet **automatisch** (Dataset)
        5. `cosmos_marts` startet **automatisch** (Dataset)

        → Volle Kette: PSA → Stammdaten → Bestellungen → Marts
        """)

    st.info(
        "**Variante B** zeigt die produktionsnahe Architektur: "
        "PSA als stabile Vorstufe, Cosmos fuer den dbt-Teil, "
        "Datasets fuer die Verkettung. In der Airflow UI sieht man "
        "die komplette Dataset-Kette: `dbt://psa_loaded` → "
        "`dbt://domain_master` → `dbt://domain_orders`."
    )

    st.divider()

    # --- NG Generator Konzept ---
    st.subheader("NG Generator - Konzept")
    st.markdown("""
    Der **NG Generator** ist ein metadatengetriebener Code-Generator, der aus
    Tabellendefinitionen (YAML) und Jinja-Templates den kompletten PSA-Code erzeugt:

    | Objekt | Typ | Zweck |
    |--------|-----|-------|
    | `v_customers_ifc` | View | Interface: 1:1 Abbild der Quelle mit Typ-Casting |
    | `v_customers_cln` | View | Clean: Dedup + Content-Hash (sha256) |
    | `customers_psa` | Tabelle | PSA: SCD2-historisiert (ng_valid_from/to) |
    | `v_customers_cur` | View | Aktuelle Version (ng_is_current = 1) |
    | `v_customers_fhi` | View | Full History (alle Versionen) |
    | `v_customers_del` | View | Gelöschte Records |
    | `run_customers_psa_load()` | Procedure | SCD2-Load via Content-Hash-Vergleich |
    | `run_customers_delete_detection()` | Procedure | Markiert fehlende Records als gelöscht |

    **Vorteil der Codegenerierung:** Alle Tabellen folgen demselben Muster.
    Fuer 100 Quellen aendert sich nur die YAML-Metadaten-Datei, nicht der Code.
    """)

# ==================== TAB: ENTWICKLER-TIPPS ====================
with tab_devtips:
    st.title("Entwickler-Tipps")
    st.markdown("Hinweise für die lokale Entwicklung mit dieser Demo-Umgebung.")
    st.divider()

    # --- Lokales dbt Setup ---
    st.subheader("1. Lokales dbt Setup (venv oder Conda)")
    st.markdown("""
    Für die Entwicklung mit VSCode und **dbt Power User** braucht man dbt lokal
    installiert. Die Docker-Container reichen dafür nicht - die Extension ruft
    `dbt compile`, `dbt parse` und Autocomplete direkt auf.
    """)
    col_venv, col_conda = st.columns(2)
    with col_venv:
        st.markdown("**Python venv**")
        st.code(
            "cd ~/Documents/work/new_env\n"
            "python3 -m venv .venv\n"
            "source .venv/bin/activate\n"
            "pip install dbt-postgres dbt-expectations",
            language="bash",
        )
    with col_conda:
        st.markdown("**Conda**")
        st.code(
            "conda create -n dbt_demo python=3.11 -y\n"
            "conda activate dbt_demo\n"
            "pip install dbt-postgres dbt-expectations",
            language="bash",
        )
    st.info(
        "Die `profiles.yml` verwendet `POSTGRES_HOST` mit Default `localhost`. "
        "Solange die Docker-Container laufen, verbindet sich das lokale dbt "
        "direkt mit dem gleichen PostgreSQL auf Port 5432."
    )

    st.divider()

    # --- VSCode Extensions ---
    st.subheader("2. VSCode Extensions")
    extensions = {
        "Extension": [
            "dbt Power User",
            "SQLFluff",
            "YAML",
            "Python",
            "Rainbow CSV",
            "Docker",
            "GitLens",
            "Even Better TOML",
        ],
        "ID": [
            "innoverio.vscode-dbt-power-user",
            "dorzey.vscode-sqlfluff",
            "redhat.vscode-yaml",
            "ms-python.python",
            "mechatroner.rainbow-csv",
            "ms-azuretools.vscode-docker",
            "eamodio.gitlens",
            "tamasfe.even-better-toml",
        ],
        "Zweck": [
            "dbt Autocomplete, Lineage-Preview, Compile-on-Save, Go-to-Definition",
            "SQL Linting und Formatting (konfigurierbar fuer dbt/Jinja)",
            "YAML-Validierung fuer dbt Schema-Dateien",
            "Python-Interpreter für venv/Conda, Debugging",
            "CSV-Dateien farblich hervorgehoben (Seeds, Delta-CSVs)",
            "Docker-Container verwalten, Logs anzeigen",
            "Git-Blame, Diff-Ansicht, History",
            "TOML-Syntax fuer Streamlit-Config",
        ],
    }
    st.dataframe(pd.DataFrame(extensions), use_container_width=True, hide_index=True)
    st.code(
        "# Alle auf einmal installieren:\n"
        "code --install-extension innoverio.vscode-dbt-power-user\n"
        "code --install-extension dorzey.vscode-sqlfluff\n"
        "code --install-extension redhat.vscode-yaml\n"
        "code --install-extension mechatroner.rainbow-csv\n"
        "code --install-extension ms-azuretools.vscode-docker",
        language="bash",
    )

    st.divider()

    # --- dbt Power User Konfiguration ---
    st.subheader("3. dbt Power User konfigurieren")
    st.markdown("""
    In den VSCode Settings (`Cmd+,`) oder `.vscode/settings.json`:
    """)
    st.code(
        '{\n'
        '  "python.defaultInterpreterPath": "${workspaceFolder}/.venv/bin/python",\n'
        '  "dbt.dbtPythonPathOverride": "${workspaceFolder}/.venv/bin/python",\n'
        '  "dbt.projectDir": "${workspaceFolder}/dbt_project",\n'
        '  "dbt.profilesDir": "${workspaceFolder}/dbt_project",\n'
        '  "files.associations": {\n'
        '    "*.sql": "jinja-sql"\n'
        '  }\n'
        '}',
        language="json",
    )
    st.markdown("""
    **Was dbt Power User bietet:**
    - **Cmd+Click** auf `ref('hub_customer')` springt zum Modell
    - **Compile-on-Save**: Zeigt das kompilierte SQL in einem Side-Panel
    - **Lineage-Graph**: Visueller Dependency-Graph direkt in VSCode
    - **Autocomplete**: `ref()`, `source()`, Macros, Column-Names
    """)

    st.divider()

    # --- Nützliche dbt-Befehle ---
    st.subheader("4. Nützliche dbt-Befehle")
    st.markdown("Lokal ausführen (im `dbt_project/`-Verzeichnis mit aktiviertem venv):")
    commands = {
        "Befehl": [
            "dbt debug",
            "dbt compile",
            "dbt run --select stg_customers",
            "dbt run --select raw_vault",
            "dbt run --select +mart_revenue_per_customer",
            "dbt test --select tag:quality",
            "dbt test --store-failures",
            "dbt docs generate && dbt docs serve",
            "dbt deps",
            "dbt parse",
        ],
        "Beschreibung": [
            "Verbindung und Konfiguration prüfen",
            "Alle Modelle kompilieren (ohne auszuführen) - zeigt SQL in target/compiled/",
            "Ein einzelnes Modell ausführen",
            "Alle Modelle einer Schicht ausführen",
            "Ein Modell mit allen Upstream-Abhaengigkeiten ausführen (+)",
            "Nur Tests mit bestimmtem Tag ausführen",
            "Tests ausführen und fehlerhafte Zeilen in DB speichern",
            "Dokumentation generieren und lokal anzeigen (Port 8080)",
            "Packages installieren (automate_dv, dbt_expectations)",
            "Manifest generieren (nötig fuer Cosmos nach Modelländerungen)",
        ],
    }
    st.dataframe(pd.DataFrame(commands), use_container_width=True, hide_index=True)

    st.divider()

    # --- Cosmos Manifest aktualisieren ---
    st.subheader("5. Nach Modelländerungen: Cosmos Manifest aktualisieren")
    st.markdown("""
    Der `dbt_cosmos` DAG verwendet `LoadMode.DBT_MANIFEST` und liest ein
    vorbereitetes `manifest.json`. Nach Änderungen an den dbt-Modellen
    muss dieses Manifest aktualisiert werden:
    """)
    st.code(
        "# 1. Lokal Manifest generieren:\n"
        "cd dbt_project && dbt parse\n"
        "\n"
        "# 2. Airflow-Image neu bauen (Manifest wird eingebacken):\n"
        "docker compose build airflow-init\n"
        "\n"
        "# 3. Airflow-Services neu starten:\n"
        "docker compose up -d",
        language="bash",
    )
    st.warning(
        "Ohne Manifest-Update sieht der Cosmos DAG die neuen Modelle nicht! "
        "Der `dbt_classic` DAG ist davon nicht betroffen (nutzt BashOperator)."
    )

    st.divider()

    # --- Neues Data Vault Modell anlegen ---
    st.subheader("6. Neues Data Vault Modell anlegen")
    st.markdown("""
    Workflow für ein neues Hub/Link/Satellite mit AutomateDV:

    **Schritt 1:** Staging-Modell erstellen (`models/staging/stg_neue_quelle.sql`)
    - `automate_dv.stage()` Macro verwenden
    - Hash-Keys, Hashdiffs und derived columns definieren
    - Source in `_staging__sources.yml` registrieren

    **Schritt 2:** Vault-Objekt erstellen (z.B. `models/raw_vault/hubs/hub_neue_entitaet.sql`)
    - `automate_dv.hub()`, `.link()` oder `.sat()` Macro verwenden
    - `ref()` auf das Staging-Modell

    **Schritt 3:** Tests und Dokumentation in `_raw_vault__models.yml` ergänzen

    **Schritt 4:** Testen
    """)
    st.code(
        "# Staging kompilieren und prüfen:\n"
        "dbt compile --select stg_neue_quelle\n"
        "# In target/compiled/ das SQL pruefen\n"
        "\n"
        "# Modell ausführen:\n"
        "dbt run --select +hub_neue_entitaet\n"
        "\n"
        "# Tests laufen lassen:\n"
        "dbt test --select hub_neue_entitaet",
        language="bash",
    )

    st.divider()

    # --- dbt-MCP: KI-gestuetzte Entwicklung ---
    st.subheader("7. Ausblick: dbt-MCP (KI-gestützte dbt-Entwicklung)")
    st.markdown("""
    [dbt-MCP](https://github.com/dbt-labs/dbt-mcp) ist ein MCP-Server von dbt Labs,
    der dbt-Funktionalität fuer KI-Assistenten bereitstellt. Damit kann man in
    **Claude Desktop**, **VS Code Copilot Chat** oder **Cursor** per natürlicher
    Sprache mit dem dbt-Projekt interagieren.
    """)

    col_works, col_cloud = st.columns(2)
    with col_works:
        st.markdown("**Lokal nutzbar (ohne dbt Cloud)**")
        st.markdown("""
        - `run`, `test`, `compile`, `show`, `parse` per Chat
        - YAML und Staging-Modelle automatisch generieren
        - Lineage aus `manifest.json` inspizieren
        """)
    with col_cloud:
        st.markdown("**Nur mit dbt Cloud**")
        st.markdown("""
        - Discovery API (Model Health, Semantic Search)
        - Semantic Layer (Metriken-Abfragen)
        - `text_to_sql` (natuerliche Sprache → SQL)
        """)

    with st.expander("Setup-Anleitung (optional)"):
        st.code(
            "# Python >= 3.12 erforderlich\n"
            "pip install dbt-mcp\n"
            "\n"
            "# Claude Desktop: ~/.claude/claude_desktop_config.json\n"
            '{\n'
            '  "mcpServers": {\n'
            '    "dbt": {\n'
            '      "command": "uvx",\n'
            '      "args": ["dbt-mcp"],\n'
            '      "env": {\n'
            '        "DBT_PROJECT_DIR": "/pfad/zu/new_env/dbt_project",\n'
            '        "DBT_MCP_ENABLE_DBT_CLI": "true",\n'
            '        "DBT_MCP_ENABLE_DBT_CODEGEN": "true"\n'
            '      }\n'
            '    }\n'
            '  }\n'
            '}',
            language="json",
        )
        st.warning(
            "dbt-MCP erlaubt KI-Assistenten, dbt-Befehle auszuführen die "
            "Datenbank-Objekte verändern koennen. In einer Demo-Umgebung "
            "unkritisch, in Produktion Tool-Kategorien sorgfaeltig einschränken."
        )

    st.divider()

    # --- Troubleshooting ---
    st.subheader("8. Troubleshooting")
    troubles = {
        "Problem": [
            "dbt kann sich nicht mit Postgres verbinden",
            "Airflow DAG erscheint nicht in der UI",
            "Cosmos DAG zeigt alte Modelle",
            "dbt test schlägt fehl mit 'relation does not exist'",
            "Container startet nicht / hängt",
            "dbt Power User zeigt keine Autocomplete-Vorschläge",
        ],
        "Loesung": [
            "Docker-Container laufen? `docker compose ps` → Postgres muss 'healthy' sein. "
            "Lokal: `dbt debug --profiles-dir .` ausführen.",
            "DAG-Processor braucht bis zu 30s. Syntax prüfen: "
            "`docker compose exec airflow-scheduler python -c \"exec(open('/opt/airflow/dags/datei.py').read())\"` ",
            "`dbt parse` lokal ausführen, dann `docker compose build airflow-init && docker compose up -d`",
            "Zuerst `dbt run --select staging` ausführen, dann Raw Vault. "
            "Oder: `dbt run --full-refresh` für einen Neuaufbau.",
            "`docker compose logs <service>` prüfen. "
            "Neustart: `docker compose restart <service>`. "
            "Komplett neu: `docker compose down && docker compose up -d --build`",
            "Python-Interpreter auf venv setzen (Cmd+Shift+P → 'Python: Select Interpreter'). "
            "`dbt deps` lokal ausführen. VSCode neu starten.",
        ],
    }
    st.dataframe(pd.DataFrame(troubles), use_container_width=True, hide_index=True)

# ==================== TAB: DOKUMENTATION ====================
with tab_readme:
    readme_path = Path("/app/../README.md")
    if not readme_path.exists():
        # Fallback: im gemounteten Volume suchen
        readme_path = Path("/README.md")
    if readme_path.exists():
        content = readme_path.read_text(encoding="utf-8")
        # Streamlit hat ein Rendering-Limit fuer grosse Markdown-Bloecke.
        # Daher splitten wir nach H2-Ueberschriften in einzelne Abschnitte.
        sections = content.split("\n## ")
        st.markdown(sections[0])  # Titel + erster Abschnitt
        for section in sections[1:]:
            st.markdown("## " + section)
    else:
        st.warning("README.md nicht gefunden. Bitte sicherstellen, dass die Datei im Projekt-Root liegt.")

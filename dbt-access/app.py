"""
dbt Access - Standalone Streamlit App
Data Vault Access View Builder

Aufruf:
  streamlit run streamlit_access.py

Docker:
  docker compose -f docker-compose.access.yml up
"""

import streamlit as st
import streamlit.components.v1 as components
import sys
import os
from pathlib import Path

import yaml

# DV-Parser importieren
import importlib.util
_spec = importlib.util.spec_from_file_location(
    'dv_parser', str(Path(__file__).parent / 'access' / 'lib' / 'dv_parser.py'))
_dv_parser = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_dv_parser)
parse_dv_project = _dv_parser.parse_dv_project
resolve_joins = _dv_parser.resolve_joins
generate_view_sql = _dv_parser.generate_view_sql
get_non_dv_models = _dv_parser.get_non_dv_models

# Page configuration
st.set_page_config(
    page_title="dbt Access - DV View Builder",
    page_icon="🔗",
    layout="wide",
)

# ============================================================
# Helper: Mermaid Rendering
# ============================================================

def render_mermaid(mermaid_code, height=500):
    html = f"""
    <div style="padding: 20px; border-radius: 8px; background: #1a1a2e;">
    <div class="mermaid">
    {mermaid_code}
    </div>
    </div>
    <script src="https://cdn.jsdelivr.net/npm/mermaid@10/dist/mermaid.min.js"></script>
    <script>
    mermaid.initialize({{
        startOnLoad: true,
        theme: 'base',
        themeVariables: {{
            'primaryColor': '#2563eb',
            'primaryTextColor': '#ffffff',
            'primaryBorderColor': '#1d4ed8',
            'lineColor': '#6b7280',
            'secondaryColor': '#059669',
            'tertiaryColor': '#d97706',
            'background': '#1a1a2e',
            'mainBkg': '#1a1a2e',
            'nodeBorder': '#4b5563',
            'clusterBkg': '#1e293b',
            'titleColor': '#e2e8f0',
            'edgeLabelBackground': '#1e293b',
            'fontSize': '14px'
        }}
    }});
    </script>
    """
    components.html(html, height=height, scrolling=True)


def build_dv_mermaid(hubs, links, sats, pits):
    lines = ["graph LR"]
    lines.append("    classDef hub fill:#2563eb,stroke:#1d4ed8,color:#fff,stroke-width:2px")
    lines.append("    classDef sat fill:#059669,stroke:#047857,color:#fff")
    lines.append("    classDef pit fill:#d97706,stroke:#b45309,color:#fff")
    lines.append("    classDef link fill:#7c3aed,stroke:#6d28d9,color:#fff")
    lines.append("    classDef nondv fill:#6b7280,stroke:#4b5563,color:#fff,stroke-dasharray: 5 5")

    for hub_name, hub in sorted(hubs.items()):
        hub_id = hub_name.replace('_', '')
        lines.append(f"    {hub_id}[{hub_name}]:::hub")
        for sat_name in hub.get('satellites', []):
            sat_id = sat_name.replace('_', '')
            lines.append(f"    {sat_id}([{sat_name}]):::sat")
            lines.append(f"    {hub_id} --> {sat_id}")
        if hub.get('pit'):
            pit_id = hub['pit'].replace('_', '')
            lines.append(f"    {pit_id}{{{{{hub['pit']}}}}}:::pit")
            lines.append(f"    {hub_id} -.-> {pit_id}")

    for link_name, link in sorted(links.items()):
        link_hubs = link.get('hubs', [])
        if len(link_hubs) >= 2:
            link_id = link_name.replace('_', '')
            hub1_id = link_hubs[0].replace('_', '')
            hub2_id = link_hubs[1].replace('_', '')
            lines.append(f"    {link_id}{{{{{link_name}}}}}:::link")
            lines.append(f"    {hub1_id} --- {link_id} --- {hub2_id}")

    for sat_name, sat in sorted(sats.items()):
        if sat.get('link'):
            sat_id = sat_name.replace('_', '')
            link = links.get(sat['link'], {})
            link_hubs = link.get('hubs', [])
            if link_hubs:
                hub_id = link_hubs[0].replace('_', '')
                lines.append(f"    {sat_id}([{sat_name}]):::sat")
                lines.append(f"    {hub_id} -.-> {sat_id}")

    return "\n".join(lines)


# ============================================================
# Settings
# ============================================================

SETTINGS_FILE = os.environ.get('ACC_SETTINGS', './access/acc_settings.yml')


@st.cache_data
def load_settings():
    settings = {}
    variables = {}
    try:
        with open(SETTINGS_FILE, 'r', encoding='utf-8-sig') as f:
            settings = yaml.safe_load(f)
    except Exception as e:
        st.error(f"Settings nicht ladbar: {e}")
        return settings, variables

    var_file = settings.get('variables_file', './access/variables.yml')
    try:
        with open(var_file, 'r', encoding='utf-8-sig') as f:
            data = yaml.safe_load(f)
            variables = data.get('variables', {})
    except Exception as e:
        st.error(f"Variables nicht ladbar: {e}")

    return settings, variables


@st.cache_data
def load_dv_graph(dbt_project_path):
    if not dbt_project_path or not os.path.isdir(dbt_project_path):
        return None
    return parse_dv_project(dbt_project_path)


settings, variables = load_settings()
dbt_project = variables.get('dbt_project', '')
dv_graph = load_dv_graph(dbt_project)
target = settings.get('target', 'postgres')
template_dir = settings.get('template_dir', './access/templates/')

# ============================================================
# CSS
# ============================================================

st.markdown("""
<style>
    .main .block-container { padding-top: 1.5rem; }
</style>
""", unsafe_allow_html=True)

# ============================================================
# Sidebar
# ============================================================

with st.sidebar:
    st.markdown("### 🔗 dbt Access")
    st.markdown("Data Vault View Builder")
    st.markdown("---")
    st.markdown(f"**Projekt:** `{os.path.basename(dbt_project)}`")
    st.markdown(f"**Target:** `{target}`")
    st.markdown(f"**dbt Template:** `{variables.get('dbt_template', '-')}`")
    st.markdown(f"**SQL Template:** `{variables.get('sql_template', '-')}`")
    st.markdown("---")
    if st.button("🔄 DV-Graph neu laden"):
        st.cache_data.clear()
        st.rerun()
    st.markdown("---")
    st.markdown("**v0.2.0**")

# ============================================================
# Main
# ============================================================

st.title("🔗 dbt Access — Data Vault View Builder")

if not dv_graph:
    st.error(f"DV-Graph konnte nicht geladen werden. Projekt: `{dbt_project}`")
    st.info("Stelle sicher, dass `dbt compile` und `dbt docs generate` gelaufen sind.")
    st.stop()

hubs = dv_graph.get('hubs', {})
links = dv_graph.get('links', {})
sats = dv_graph.get('satellites', {})
pits = dv_graph.get('pits', {})

col1, col2, col3, col4 = st.columns(4)
col1.metric("Hubs", len(hubs))
col2.metric("Links", len(links))
col3.metric("Satellites", len(sats))
col4.metric("PITs", len(pits))

# ============================================================
# Tabs
# ============================================================

tab_overview, tab_select, tab_graph = st.tabs([
    "📋 Uebersicht", "🎯 Auswahl", "🕸️ Graph"
])

# ---- Tab 1: Uebersicht ----
with tab_overview:
    st.subheader(f"Hubs ({len(hubs)})")
    hub_data = []
    for name, h in sorted(hubs.items()):
        hub_data.append({
            "Name": name, "PK": h.get('pk', ''),
            "NK": h.get('nk', ''),
            "Satellites": ', '.join(h.get('satellites', [])) or '-',
            "Links": ', '.join(h.get('links', [])) or '-',
            "PIT": h.get('pit') or '-',
        })
    if hub_data:
        st.dataframe(hub_data, use_container_width=True, hide_index=True)

    st.subheader(f"Links ({len(links)})")
    link_data = []
    for name, l in sorted(links.items()):
        link_data.append({
            "Name": name, "PK": l.get('pk', ''),
            "FK": ', '.join(l.get('fk', [])),
            "Hubs": ', '.join(l.get('hubs', [])),
        })
    if link_data:
        st.dataframe(link_data, use_container_width=True, hide_index=True)

    st.subheader(f"Satellites ({len(sats)})")
    sat_data = []
    for name, s in sorted(sats.items()):
        parent = s.get('hub') or s.get('link', '?')
        payload = s.get('payload', [])
        payload_str = ', '.join(payload[:5])
        if len(payload) > 5:
            payload_str += f' (+{len(payload) - 5})'
        sat_data.append({
            "Name": name, "PK": s.get('pk', ''),
            "Parent": parent, "Payload": payload_str,
        })
    if sat_data:
        st.dataframe(sat_data, use_container_width=True, hide_index=True)

    if pits:
        st.subheader(f"PITs ({len(pits)})")
        pit_data = []
        for name, p in sorted(pits.items()):
            pit_data.append({
                "Name": name, "PK": p.get('pk', ''),
                "Hub": p.get('hub', '-'),
                "Satellites": ', '.join(p.get('satellites', {}).keys()),
            })
        st.dataframe(pit_data, use_container_width=True, hide_index=True)

# ---- Tab 2: Auswahl + Generierung ----
with tab_select:
    st.subheader("View-Generierung")

    col_left, col_right = st.columns([1, 2])

    with col_left:
        all_sats = sorted(sats.keys())
        selected_sats = st.multiselect(
            "Satellites auswaehlen", options=all_sats, default=[],
            help="Reihenfolge bestimmt den Anker-Hub (FROM) und die JOIN-Reihenfolge"
        )

        non_dv_models = get_non_dv_models(dbt_project) if dbt_project else {}
        if non_dv_models:
            selected_non_dv = st.multiselect(
                "Weitere Tabellen (nicht-DV)",
                options=sorted(non_dv_models.keys()), default=[],
                help="Seeds, Marts oder andere Tabellen ohne AutomateDV"
            )
        else:
            selected_non_dv = []

        st.markdown("---")
        output_mode = st.radio("Output", ["dbt (mit ref())", "SQL (plain)"])
        use_pit = st.checkbox("PIT-Constraints verwenden")

    with col_right:
        if selected_sats:
            involved_hubs = set()
            for sat_name in selected_sats:
                sat = sats.get(sat_name, {})
                if sat.get('hub'):
                    involved_hubs.add(sat['hub'])

            needed_links = []
            for link_name, link in links.items():
                link_hubs = set(link.get('hubs', []))
                if len(link_hubs & involved_hubs) >= 2:
                    needed_links.append(link_name)

            st.markdown("**Ausgewaehlte Satellites:**")
            for sat_name in selected_sats:
                sat = sats.get(sat_name, {})
                parent = sat.get('hub') or sat.get('link', '?')
                st.markdown(f"- **{sat_name}** → {parent} ({len(sat.get('payload', []))} Spalten)")

            if selected_non_dv:
                st.markdown("**Weitere Tabellen:**")
                for tbl in selected_non_dv:
                    info = non_dv_models.get(tbl, {})
                    st.markdown(f"- **{tbl}** ({info.get('resource_type', '')}, {len(info.get('columns', {}))} Spalten)")

            if len(involved_hubs) > 1:
                st.markdown("---")
                st.markdown("**Automatisch ermittelte Links:**")
                if needed_links:
                    for link_name in needed_links:
                        link = links[link_name]
                        st.success(f"🔗 {link_name}: {' ↔ '.join(link.get('hubs', []))}")
                else:
                    st.warning("Kein direkter Link zwischen den beteiligten Hubs gefunden!")

            st.markdown("---")

            if st.button("🚀 View generieren", type="primary"):
                join_plan = resolve_joins(dv_graph, selected_sats, use_pit=use_pit)

                non_dv_dict = None
                if selected_non_dv:
                    non_dv_dict = {n: non_dv_models[n] for n in selected_non_dv if n in non_dv_models}

                mode = 'dbt' if 'dbt' in output_mode else 'sql'
                tmpl_name = variables.get('dbt_template', '') if mode == 'dbt' else variables.get('sql_template', '')

                sql = generate_view_sql(
                    join_plan, output_mode=mode, non_dv_tables=non_dv_dict,
                    template_dir=template_dir, template_name=tmpl_name, target=target
                )

                if join_plan and join_plan.get('missing_constraints'):
                    for mc in join_plan['missing_constraints']:
                        st.warning(f"⚠️ {mc['message']}")

                st.code(sql, language='sql')

                st.download_button(
                    "📥 SQL herunterladen", data=sql,
                    file_name=f"v_access_{'_'.join(s.replace('sat_', '') for s in selected_sats)}.sql",
                    mime="text/sql",
                )

                # Query-Graph
                st.markdown("---")
                st.markdown("**Query-Graph**")
                if join_plan:
                    q_hubs = join_plan.get('involved_hubs', {})
                    q_links = join_plan.get('involved_links', {})
                    q_sats = {s: sats[s] for s in selected_sats if s in sats}
                    q_pits = {}
                    if use_pit:
                        for pit_name, pit in pits.items():
                            if pit.get('hub') in q_hubs:
                                q_pits[pit_name] = pit

                    query_mermaid = build_dv_mermaid(q_hubs, q_links, q_sats, q_pits)

                    if non_dv_dict:
                        extra_lines = query_mermaid.split('\n')
                        extra_lines.append("    classDef nondv fill:#6b7280,stroke:#4b5563,color:#fff,stroke-dasharray: 5 5")
                        for tbl_name in non_dv_dict:
                            tbl_id = tbl_name.replace('_', '')
                            extra_lines.append(f"    {tbl_id}[/{tbl_name}/]:::nondv")
                            first_hub_id = list(q_hubs.keys())[0].replace('_', '') if q_hubs else ''
                            if first_hub_id:
                                extra_lines.append(f"    {first_hub_id} -.-|CROSS JOIN| {tbl_id}")
                        query_mermaid = '\n'.join(extra_lines)

                    render_mermaid(query_mermaid, height=350)
        else:
            st.info("Waehle mindestens einen Satellite aus der Liste links.")

# ---- Tab 3: Graph ----
with tab_graph:
    st.subheader("Data Vault Beziehungsgraph (komplettes Modell)")
    full_mermaid = build_dv_mermaid(hubs, links, sats, pits)
    render_mermaid(full_mermaid, height=500)
    st.markdown("**Legende:**  🟦 Hub  ·  🟩 Satellite  ·  🟧 PIT  ·  🟪 Link")
    with st.expander("Mermaid Code"):
        st.code(full_mermaid, language="mermaid")

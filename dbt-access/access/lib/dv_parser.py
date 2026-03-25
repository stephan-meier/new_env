# -*- coding: utf-8 -*-
"""
Data Vault Parser

Liest die DV-Struktur aus einem dbt-Projekt:
  - manifest.json → Modell-Erkennung (Hubs, Links, Sats, PITs via Macro-Dependencies)
  - catalog.json  → Spalten mit Datentypen
  - Model SQL     → automate_dv Parameter (src_pk, src_fk, src_payload) via Regex

Ergebnis: dv_graph Dictionary mit allen DV-Objekten und ihren Beziehungen.

@author: steph / claude
"""

import json
import os
import re
import yaml
import logging

logger = logging.getLogger('dbt_access')

# AutomateDV Macro-Patterns fuer Erkennung
MACRO_HUB = 'macro.automate_dv.hub'
MACRO_LINK = 'macro.automate_dv.link'
MACRO_SAT = 'macro.automate_dv.sat'
MACRO_PIT = 'macro.automate_dv.pit'


def load_json(filepath):
    """Laedt eine JSON-Datei."""
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"Fehler beim Laden von {filepath}: {e}")
        return None


def parse_sql_param(sql, param_name):
    """Extrahiert einen einzelnen Parameter aus einem automate_dv Macro-Aufruf.

    Unterstuetzt:
      - String: src_pk='customer_hk' → 'customer_hk'
      - Liste:  src_fk=['order_hk', 'customer_hk'] → ['order_hk', 'customer_hk']
      - Mehrzeilige Listen: src_payload=['col1', 'col2',
                                         'col3'] → ['col1', 'col2', 'col3']
    """
    # Versuch 1: String-Parameter (src_pk='value')
    pattern_str = rf"{param_name}\s*=\s*'([^']+)'"
    match = re.search(pattern_str, sql)
    if match:
        return match.group(1)

    # Versuch 2: Listen-Parameter (src_fk=['val1', 'val2', ...])
    # Mehrzeilig: alles zwischen [ und ] sammeln
    pattern_list = rf"{param_name}\s*=\s*\[(.*?)\]"
    match = re.search(pattern_list, sql, re.DOTALL)
    if match:
        items = re.findall(r"'([^']+)'", match.group(1))
        return items

    return None


def parse_pit_yaml(sql):
    """Extrahiert PIT-Metadaten aus dem YAML-Block im SQL.

    PITs verwenden ein anderes Pattern:
      {%- set yaml_metadata -%}
      source_model: 'hub_customer'
      src_pk: 'customer_hk'
      ...
      {%- endset -%}
    """
    yaml_match = re.search(
        r'\{%-?\s*set\s+yaml_metadata\s*-?%\}(.*?)\{%-?\s*endset\s*-?%\}',
        sql, re.DOTALL
    )
    if not yaml_match:
        return None

    try:
        metadata = yaml.safe_load(yaml_match.group(1))
        return metadata
    except yaml.YAMLError as e:
        logger.warning(f"Fehler beim Parsen des PIT YAML: {e}")
        return None


def parse_dv_project(dbt_project_path):
    """Parst ein dbt-Projekt und erstellt den DV-Graphen.

    Args:
        dbt_project_path: Pfad zum dbt-Projekt (mit target/ Verzeichnis)

    Returns:
        dv_graph: Dictionary mit hubs, links, satellites, pits
    """
    manifest_path = os.path.join(dbt_project_path, 'target', 'manifest.json')
    catalog_path = os.path.join(dbt_project_path, 'target', 'catalog.json')

    manifest = load_json(manifest_path)
    catalog = load_json(catalog_path)

    if not manifest:
        logger.error(f"manifest.json nicht gefunden: {manifest_path}")
        return None

    dv_graph = {
        'hubs': {},
        'links': {},
        'satellites': {},
        'pits': {},
        'project_path': dbt_project_path,
    }

    # Catalog-Spalten als Lookup
    catalog_columns = {}
    if catalog and 'nodes' in catalog:
        for node_id, node_data in catalog['nodes'].items():
            if 'columns' in node_data:
                catalog_columns[node_id] = node_data['columns']

    # Manifest Nodes durchgehen
    nodes = manifest.get('nodes', {})

    for node_id, node in nodes.items():
        if node.get('resource_type') != 'model':
            continue

        macros = node.get('depends_on', {}).get('macros', [])
        name = node['name']
        file_path = node.get('original_file_path', '')

        # SQL-Datei lesen fuer Parameter-Extraktion
        sql_path = os.path.join(dbt_project_path, file_path)
        sql_content = ''
        if os.path.exists(sql_path):
            try:
                with open(sql_path, 'r', encoding='utf-8') as f:
                    sql_content = f.read()
            except Exception as e:
                logger.warning(f"SQL-Datei nicht lesbar: {sql_path}: {e}")

        # Spalten aus Catalog
        columns = {}
        if node_id in catalog_columns:
            columns = catalog_columns[node_id]

        # ---- HUB ----
        if MACRO_HUB in macros:
            pk = parse_sql_param(sql_content, 'src_pk')
            nk = parse_sql_param(sql_content, 'src_nk')
            ldts = parse_sql_param(sql_content, 'src_ldts')
            source_model = parse_sql_param(sql_content, 'source_model')

            dv_graph['hubs'][name] = {
                'pk': pk,
                'nk': nk,
                'ldts': ldts,
                'source_model': source_model,
                'satellites': [],       # wird spaeter befuellt
                'links': [],            # wird spaeter befuellt
                'pit': None,            # wird spaeter befuellt
                'columns': columns,
                'tags': node.get('tags', []),
                'description': node.get('description', ''),
            }

        # ---- LINK ----
        elif MACRO_LINK in macros:
            pk = parse_sql_param(sql_content, 'src_pk')
            fk = parse_sql_param(sql_content, 'src_fk')
            ldts = parse_sql_param(sql_content, 'src_ldts')
            source_model = parse_sql_param(sql_content, 'source_model')

            dv_graph['links'][name] = {
                'pk': pk,
                'fk': fk if isinstance(fk, list) else [fk] if fk else [],
                'ldts': ldts,
                'source_model': source_model,
                'hubs': [],             # wird spaeter aufgeloest
                'columns': columns,
                'tags': node.get('tags', []),
                'description': node.get('description', ''),
            }

        # ---- SATELLITE ----
        elif MACRO_SAT in macros:
            pk = parse_sql_param(sql_content, 'src_pk')
            hashdiff = parse_sql_param(sql_content, 'src_hashdiff')
            payload = parse_sql_param(sql_content, 'src_payload')
            eff = parse_sql_param(sql_content, 'src_eff')
            ldts = parse_sql_param(sql_content, 'src_ldts')
            source_model = parse_sql_param(sql_content, 'source_model')

            dv_graph['satellites'][name] = {
                'pk': pk,
                'hashdiff': hashdiff,
                'payload': payload if isinstance(payload, list) else [payload] if payload else [],
                'effective_from': eff,
                'ldts': ldts,
                'source_model': source_model,
                'hub': None,            # wird spaeter aufgeloest
                'columns': columns,
                'tags': node.get('tags', []),
                'description': node.get('description', ''),
            }

        # ---- PIT ----
        elif MACRO_PIT in macros:
            pit_meta = parse_pit_yaml(sql_content)
            if pit_meta:
                source_model = pit_meta.get('source_model', '')
                pk = pit_meta.get('src_pk', '')
                as_of = pit_meta.get('as_of_dates_table', '')
                sats = pit_meta.get('satellites', {})

                dv_graph['pits'][name] = {
                    'source_model': source_model,
                    'pk': pk,
                    'as_of_dates_table': as_of,
                    'satellites': sats,
                    'columns': columns,
                    'tags': node.get('tags', []),
                    'description': node.get('description', ''),
                }

    # ---- Beziehungen aufloesen ----
    _resolve_relationships(dv_graph)

    return dv_graph


def _resolve_relationships(dv_graph):
    """Loest die Beziehungen zwischen Hubs, Links, Satellites und PITs auf.

    Logik:
      - Satellite.pk == Hub.pk → Satellite gehoert zu diesem Hub
      - Link.fk enthaelt Hub.pk → Link verbindet diese Hubs
      - PIT.source_model == Hub.name → PIT gehoert zu diesem Hub
    """
    hubs = dv_graph['hubs']
    links = dv_graph['links']
    satellites = dv_graph['satellites']
    pits = dv_graph['pits']

    # Hub-PK Lookup: pk_value → hub_name
    pk_to_hub = {}
    for hub_name, hub in hubs.items():
        if hub['pk']:
            pk_to_hub[hub['pk']] = hub_name

    # Satellites den Hubs zuordnen (ueber pk)
    for sat_name, sat in satellites.items():
        if sat['pk'] in pk_to_hub:
            hub_name = pk_to_hub[sat['pk']]
            sat['hub'] = hub_name
            hubs[hub_name]['satellites'].append(sat_name)
        else:
            # Satellite am Link (z.B. sat_order_detail mit order_product_hk)
            # → pk zeigt auf einen Link, nicht Hub
            for link_name, link in links.items():
                if sat['pk'] == link['pk']:
                    sat['hub'] = None
                    sat['link'] = link_name
                    break

    # Links den Hubs zuordnen (ueber fk)
    for link_name, link in links.items():
        resolved_hubs = []
        for fk in link['fk']:
            if fk in pk_to_hub:
                resolved_hubs.append(pk_to_hub[fk])
        link['hubs'] = resolved_hubs

        # Links bei den Hubs registrieren
        for hub_name in resolved_hubs:
            if link_name not in hubs[hub_name]['links']:
                hubs[hub_name]['links'].append(link_name)

    # PITs den Hubs zuordnen (ueber source_model)
    for pit_name, pit in pits.items():
        source = pit.get('source_model', '')
        if source in hubs:
            pit['hub'] = source
            hubs[source]['pit'] = pit_name
        else:
            pit['hub'] = None


def print_dv_graph(dv_graph):
    """Gibt den DV-Graphen formatiert auf der Konsole aus."""
    if not dv_graph:
        print("Kein DV-Graph vorhanden.")
        return

    hubs = dv_graph['hubs']
    links = dv_graph['links']
    satellites = dv_graph['satellites']
    pits = dv_graph['pits']

    print(f"\n=== Hubs ({len(hubs)}) ===")
    for name, h in sorted(hubs.items()):
        pit_info = f"pit: {h['pit']}" if h['pit'] else "pit: -"
        sats_info = ', '.join(h['satellites']) if h['satellites'] else '-'
        links_info = ', '.join(h['links']) if h['links'] else '-'
        print(f"  {name:<25} pk: {h['pk']:<20} nk: {h['nk']:<15} "
              f"sats: [{sats_info}]  links: [{links_info}]  {pit_info}")

    print(f"\n=== Links ({len(links)}) ===")
    for name, l in sorted(links.items()):
        fk_info = ', '.join(l['fk']) if l['fk'] else '-'
        hubs_info = ', '.join(l['hubs']) if l['hubs'] else '-'
        print(f"  {name:<25} pk: {l['pk']:<25} fk: [{fk_info}]  hubs: [{hubs_info}]")

    print(f"\n=== Satellites ({len(satellites)}) ===")
    for name, s in sorted(satellites.items()):
        parent = s.get('hub') or s.get('link', '?')
        parent_type = 'hub' if s.get('hub') else 'link'
        payload_preview = ', '.join(s['payload'][:4])
        if len(s['payload']) > 4:
            payload_preview += f', ... (+{len(s["payload"]) - 4})'
        print(f"  {name:<25} pk: {s['pk']:<20} {parent_type}: {parent:<20} "
              f"payload: [{payload_preview}]")

    print(f"\n=== PITs ({len(pits)}) ===")
    for name, p in sorted(pits.items()):
        hub_info = p.get('hub', '-')
        sat_names = ', '.join(p.get('satellites', {}).keys())
        print(f"  {name:<25} pk: {p['pk']:<20} hub: {hub_info:<20} "
              f"satellites: [{sat_names}]")

    # Zusammenfassung
    total = len(hubs) + len(links) + len(satellites) + len(pits)
    print(f"\nTotal: {total} DV-Objekte "
          f"({len(hubs)} Hubs, {len(links)} Links, {len(satellites)} Satellites, {len(pits)} PITs)")


def get_non_dv_models(dbt_project_path):
    """Liefert alle Modelle die NICHT zum Data Vault gehoeren (Seeds, Marts, custom).

    Returns:
        Dict mit model_name → {columns, tags, description, schema, materialized}
    """
    manifest_path = os.path.join(dbt_project_path, 'target', 'manifest.json')
    catalog_path = os.path.join(dbt_project_path, 'target', 'catalog.json')

    manifest = load_json(manifest_path)
    catalog = load_json(catalog_path)
    if not manifest:
        return {}

    dv_macros = {MACRO_HUB, MACRO_LINK, MACRO_SAT, MACRO_PIT}
    non_dv = {}

    # Catalog-Spalten
    catalog_columns = {}
    if catalog and 'nodes' in catalog:
        for node_id, node_data in catalog['nodes'].items():
            if 'columns' in node_data:
                catalog_columns[node_id] = node_data['columns']

    for node_id, node in manifest.get('nodes', {}).items():
        if node.get('resource_type') not in ('model', 'seed'):
            continue
        macros = set(node.get('depends_on', {}).get('macros', []))
        # Staging-Modelle ueberspringen (die gehoeren zum DV-Flow)
        fqn = node.get('fqn', [])
        if 'staging' in fqn:
            continue
        # DV-Modelle ueberspringen
        if macros & dv_macros:
            continue

        name = node['name']
        columns = catalog_columns.get(node_id, {})
        non_dv[name] = {
            'columns': columns,
            'tags': node.get('tags', []),
            'description': node.get('description', ''),
            'schema': node.get('config', {}).get('schema', ''),
            'materialized': node.get('config', {}).get('materialized', ''),
            'resource_type': node.get('resource_type', ''),
        }

    return non_dv


def resolve_joins(dv_graph, selected_sats, use_pit=False):
    """Berechnet den JOIN-Baum fuer ausgewaehlte Satellites.

    Args:
        dv_graph: DV-Graph Dictionary
        selected_sats: Liste von Satellite-Namen
        use_pit: True → PIT-basierte JOINs statt ROW_NUMBER

    Returns:
        join_plan: Dictionary mit CTEs, JOINs und SELECT-Spalten
    """
    hubs = dv_graph['hubs']
    links = dv_graph['links']
    sats = dv_graph['satellites']
    pits = dv_graph['pits']

    # Beteiligte Hubs und Links ermitteln
    involved_hubs = {}   # hub_name → hub_data
    involved_links = {}  # link_name → link_data
    sat_details = []     # Liste der ausgewaehlten Satellites mit Details

    for sat_name in selected_sats:
        sat = sats.get(sat_name)
        if not sat:
            continue
        sat_info = dict(sat)
        sat_info['name'] = sat_name

        if sat.get('hub'):
            hub_name = sat['hub']
            involved_hubs[hub_name] = hubs[hub_name]
            sat_info['parent_type'] = 'hub'
            sat_info['parent_name'] = hub_name
        elif sat.get('link'):
            link_name = sat['link']
            involved_links[link_name] = links[link_name]
            sat_info['parent_type'] = 'link'
            sat_info['parent_name'] = link_name
            # Link-Hubs auch hinzufuegen
            for hub_name in links[link_name].get('hubs', []):
                if hub_name in hubs:
                    involved_hubs[hub_name] = hubs[hub_name]

        sat_details.append(sat_info)

    # Links zwischen den beteiligten Hubs finden
    hub_names = set(involved_hubs.keys())
    for link_name, link in links.items():
        link_hubs = set(link.get('hubs', []))
        if len(link_hubs & hub_names) >= 2 and link_name not in involved_links:
            involved_links[link_name] = link

    # JOIN-Baum aufbauen: erster Hub ist der Anker
    hub_order = list(involved_hubs.keys())
    if not hub_order:
        return None

    anchor_hub = hub_order[0]

    # CTEs erstellen
    ctes = []
    for sat_info in sat_details:
        sat_name = sat_info['name']
        pk = sat_info.get('pk', '')
        ldts = sat_info.get('ldts', 'load_datetime')

        # PIT oder ROW_NUMBER?
        hub_name = sat_info.get('parent_name', '')
        pit_name = involved_hubs.get(hub_name, {}).get('pit') if hub_name in involved_hubs else None

        if use_pit and pit_name and sat_info['parent_type'] == 'hub':
            ctes.append({
                'type': 'sat_pit',
                'name': sat_name,
                'alias': sat_name + '_current',
                'pk': pk,
                'ldts': ldts,
                'pit': pit_name,
            })
        else:
            ctes.append({
                'type': 'sat_rownumber',
                'name': sat_name,
                'alias': sat_name + '_current',
                'pk': pk,
                'ldts': ldts,
            })

    # JOINs erstellen
    joins = []
    joined_hubs = {anchor_hub}
    missing_constraints = []

    # Erster Hub + seine Satellites
    for sat_info in sat_details:
        if sat_info.get('parent_name') == anchor_hub and sat_info['parent_type'] == 'hub':
            joins.append({
                'type': 'hub_sat',
                'hub': anchor_hub,
                'sat': sat_info['name'],
                'pk': sat_info['pk'],
                'join_type': 'INNER JOIN',
            })

    # Weitere Hubs ueber Links verbinden
    for hub_name in hub_order[1:]:
        if hub_name in joined_hubs:
            continue

        # Link finden der diesen Hub mit einem bereits gejointen Hub verbindet
        link_found = None
        for link_name, link in involved_links.items():
            link_hubs = set(link.get('hubs', []))
            if hub_name in link_hubs and len(link_hubs & joined_hubs) > 0:
                link_found = (link_name, link)
                break

        if link_found:
            link_name, link = link_found
            # Link-FK aufloesen: welcher FK zeigt auf den bereits gejointen Hub?
            source_hub = None
            for fk in link['fk']:
                for jh in joined_hubs:
                    if involved_hubs[jh]['pk'] == fk:
                        source_hub = jh
                        break

            joins.append({
                'type': 'link',
                'link': link_name,
                'from_hub': source_hub or list(joined_hubs)[0],
                'to_hub': hub_name,
                'fk': link['fk'],
                'join_type': 'INNER JOIN',
            })
        else:
            missing_constraints.append({
                'hub': hub_name,
                'message': f'Kein Link zwischen {", ".join(joined_hubs)} und {hub_name} gefunden',
            })
            joins.append({
                'type': 'missing',
                'hub': hub_name,
                'join_type': 'LEFT JOIN',
                'message': f'ERROR, MISSING CONSTRAINT - kein Link zwischen '
                           f'{", ".join(joined_hubs)} und {hub_name} gefunden',
            })

        joined_hubs.add(hub_name)

        # Satellites dieses Hubs
        for sat_info in sat_details:
            if sat_info.get('parent_name') == hub_name and sat_info['parent_type'] == 'hub':
                joins.append({
                    'type': 'hub_sat',
                    'hub': hub_name,
                    'sat': sat_info['name'],
                    'pk': sat_info['pk'],
                    'join_type': 'INNER JOIN',
                })

    # Satellites am Link
    for sat_info in sat_details:
        if sat_info['parent_type'] == 'link':
            link_name = sat_info['parent_name']
            joins.append({
                'type': 'link_sat',
                'link': link_name,
                'sat': sat_info['name'],
                'pk': sat_info['pk'],
                'join_type': 'INNER JOIN',
            })

    # SELECT-Spalten zusammenstellen
    select_columns = []
    for hub_name, hub in involved_hubs.items():
        select_columns.append({
            'source': hub_name,
            'column': hub['nk'],
            'comment': f'Hub: {hub_name}',
        })

    for sat_info in sat_details:
        for col in sat_info.get('payload', []):
            select_columns.append({
                'source': sat_info['name'],
                'column': col,
                'comment': f'Satellite: {sat_info["name"]}',
            })

    return {
        'anchor_hub': anchor_hub,
        'involved_hubs': involved_hubs,
        'involved_links': involved_links,
        'ctes': ctes,
        'joins': joins,
        'select_columns': select_columns,
        'missing_constraints': missing_constraints,
        'use_pit': use_pit,
    }


def _prepare_template_data(join_plan, output_mode='dbt', target='postgres',
                           non_dv_tables=None, schema='raw_vault'):
    """Bereitet die Daten fuer das Jinja2-Template auf.

    Returns:
        Dictionary mit allen Template-Variablen
    """
    import datetime

    if not join_plan:
        return None

    anchor = join_plan['anchor_hub']
    anchor_alias = _make_alias(anchor, 'hub')

    # SELECT-Spalten mit Alias anreichern
    select_columns = []
    prev_comment = None
    for col in join_plan['select_columns']:
        source = col['source']
        if source in join_plan['involved_hubs']:
            alias = _make_alias(source, 'hub')
        else:
            alias = _make_alias(source, 'sat')
        select_columns.append({
            'alias': alias,
            'column': col['column'],
            'comment': col['comment'],
            '_prev_comment': prev_comment,
        })
        prev_comment = col['comment']

    # Nicht-DV Spalten
    if non_dv_tables:
        for tbl_name, tbl_info in non_dv_tables.items():
            alias = tbl_name[:3]
            prev_comment = None
            for col_name in tbl_info.get('columns', {}):
                select_columns.append({
                    'alias': alias,
                    'column': col_name,
                    'comment': f'Tabelle: {tbl_name}',
                    '_prev_comment': prev_comment,
                })
                prev_comment = f'Tabelle: {tbl_name}'

    # JOINs mit Aliases anreichern
    enriched_joins = []
    for join in join_plan['joins']:
        j = dict(join)
        if join['type'] == 'hub_sat':
            j['sat_alias'] = _make_alias(join['sat'], 'sat')
            j['hub_alias'] = _make_alias(join['hub'], 'hub')
            j['sat_schema'] = schema
            # PIT?
            pit_cte = None
            for cte in join_plan['ctes']:
                if cte.get('type') == 'sat_pit' and cte['name'] == join['sat']:
                    pit_cte = cte
                    break
            if pit_cte:
                j['pit'] = pit_cte['pit']
                j['pit_schema'] = schema
                j['ldts'] = pit_cte['ldts']
        elif join['type'] == 'link':
            j['link_alias'] = _make_alias(join['link'], 'link')
            j['from_alias'] = _make_alias(join['from_hub'], 'hub')
            j['to_alias'] = _make_alias(join['to_hub'], 'hub')
            j['from_pk'] = join_plan['involved_hubs'][join['from_hub']]['pk']
            j['to_pk'] = join_plan['involved_hubs'][join['to_hub']]['pk']
            j['link_schema'] = schema
            j['to_schema'] = schema
        elif join['type'] == 'link_sat':
            j['sat_alias'] = _make_alias(join['sat'], 'sat')
            j['link_alias'] = _make_alias(join['link'], 'link')
        elif join['type'] == 'missing':
            j['hub_alias'] = _make_alias(join['hub'], 'hub')
            j['hub_schema'] = schema
        enriched_joins.append(j)

    # Nicht-DV Tabellen
    ndv_list = []
    if non_dv_tables:
        for tbl_name, tbl_info in non_dv_tables.items():
            ndv_list.append({
                'name': tbl_name,
                'alias': tbl_name[:3],
                'schema': tbl_info.get('schema', schema),
            })

    # CTE Schema anreichern
    for cte in join_plan['ctes']:
        cte['schema'] = schema

    return {
        'view_name': 'v_access_' + '_'.join(
            s.get('name', '').replace('sat_', '')
            for s in join_plan.get('ctes', []) if s.get('type', '').startswith('sat')
        ),
        'target': target,
        'generated_at': str(datetime.date.today()),
        'satellites': [s.get('name', '') for s in join_plan.get('ctes', [])],
        'ctes': join_plan['ctes'],
        'select_columns': select_columns,
        'joins': enriched_joins,
        'anchor_hub': anchor,
        'anchor_alias': anchor_alias,
        'anchor_schema': schema,
        'schema': schema,
        'non_dv_tables': ndv_list,
        'missing': join_plan.get('missing_constraints', []),
        'use_pit': join_plan.get('use_pit', False),
    }


def generate_view_sql(join_plan, output_mode='dbt', dv_graph=None, non_dv_tables=None,
                      template_dir=None, template_name=None, target='postgres'):
    """Generiert SQL aus dem JOIN-Plan via Jinja2-Template.

    Args:
        join_plan: Ergebnis von resolve_joins()
        output_mode: 'dbt' (mit ref()) oder 'sql' (mit Schema-Prefix)
        dv_graph: DV-Graph (unused, fuer Kompatibilitaet)
        non_dv_tables: Optional, Dict mit nicht-DV Tabellen
        template_dir: Verzeichnis mit .tmpl Dateien
        template_name: Template-Dateiname (aus variables.yml)
        target: 'postgres' oder 'snowflake' (aus acc_settings.yml)

    Returns:
        SQL-String
    """
    if not join_plan:
        return '-- Kein JOIN-Plan vorhanden'

    import jinja2

    # Template-Daten aufbereiten
    data = _prepare_template_data(join_plan, output_mode, target, non_dv_tables)
    if not data:
        return '-- Fehler bei der Datenaufbereitung'

    # Template laden
    tmpl_code = None
    if template_dir and template_name:
        tmpl_path = os.path.join(template_dir, template_name)
        if os.path.exists(tmpl_path):
            try:
                with open(tmpl_path, 'r', encoding='utf-8-sig') as f:
                    tmpl_code = f.read()
            except Exception as e:
                logger.warning(f"Template nicht lesbar: {tmpl_path}: {e}")

    if tmpl_code:
        # Template rendern
        try:
            env = jinja2.Environment()
            template = env.from_string(tmpl_code)
            return template.render(**data)
        except jinja2.TemplateError as e:
            logger.error(f"Template-Fehler: {e}")
            return f'-- Template-Fehler: {e}'

    # Fallback: Inline-Generierung (wenn kein Template vorhanden)
    return _generate_view_inline(join_plan, output_mode, non_dv_tables, target)


def _generate_view_inline(join_plan, output_mode='dbt', non_dv_tables=None, target='postgres'):
    """Inline SQL-Generierung als Fallback (ohne Template)."""
    import datetime

    lines = []
    lines.append('-- dbtAccess Generated View')
    lines.append(f'-- Target: {target}')
    lines.append(f'-- Generated: {datetime.date.today()}')
    lines.append('')

    def ref(model_name):
        if output_mode == 'dbt':
            return "{{ ref('" + model_name + "') }}"
        else:
            return model_name

    # CTEs
    cte_parts = []
    for cte in join_plan['ctes']:
        if cte['type'] == 'sat_rownumber':
            cte_parts.append(
                f"{cte['alias']} AS (\n"
                f"    SELECT *,\n"
                f"        ROW_NUMBER() OVER (PARTITION BY {cte['pk']} "
                f"ORDER BY {cte['ldts']} DESC) AS rn\n"
                f"    FROM {ref(cte['name'])}\n"
                f")"
            )

    if cte_parts:
        lines.append('WITH ' + ',\n\n'.join(cte_parts))
        lines.append('')

    # SELECT
    lines.append('SELECT')
    select_items = []
    last_comment = None
    for col in join_plan['select_columns']:
        comment = col['comment']
        if comment != last_comment:
            select_items.append(f"    -- {comment}")
            last_comment = comment
        source = col['source']
        if source in join_plan['involved_hubs']:
            alias = _make_alias(source, 'hub')
        else:
            alias = _make_alias(source, 'sat')
        select_items.append(f"    {alias}.{col['column']}")

    if non_dv_tables:
        for tbl_name, tbl_info in non_dv_tables.items():
            alias = tbl_name[:3]
            select_items.append(f"    -- Tabelle: {tbl_name}")
            for col_name in tbl_info.get('columns', {}):
                select_items.append(f"    {alias}.{col_name}")

    lines.append(',\n'.join(select_items))

    # FROM
    anchor = join_plan['anchor_hub']
    anchor_alias = _make_alias(anchor, 'hub')
    lines.append(f"FROM {ref(anchor)} {anchor_alias}")

    # JOINs
    for join in join_plan['joins']:
        if join['type'] == 'hub_sat':
            sat_alias = _make_alias(join['sat'], 'sat')
            hub_alias = _make_alias(join['hub'], 'hub')
            pit_cte = None
            for cte in join_plan['ctes']:
                if cte.get('type') == 'sat_pit' and cte['name'] == join['sat']:
                    pit_cte = cte
                    break
            if pit_cte:
                pit_alias = 'pit_' + _make_alias(join['hub'], 'hub')
                lines.append(f"{join['join_type']} {ref(pit_cte['pit'])} {pit_alias}")
                lines.append(f"    ON {hub_alias}.{join['pk']} = {pit_alias}.{join['pk']}")
                lines.append(f"{join['join_type']} {ref(join['sat'])} {sat_alias}")
                lines.append(f"    ON {pit_alias}.{join['pk']} = {sat_alias}.{join['pk']}")
                lines.append(f"    AND {pit_alias}.sat_{join['sat'].replace('sat_', '')}_ldts = {sat_alias}.{pit_cte['ldts']}")
            else:
                lines.append(f"{join['join_type']} {join['sat']}_current {sat_alias}")
                lines.append(f"    ON {hub_alias}.{join['pk']} = {sat_alias}.{join['pk']} AND {sat_alias}.rn = 1")
        elif join['type'] == 'link':
            link_alias = _make_alias(join['link'], 'link')
            from_hub_alias = _make_alias(join['from_hub'], 'hub')
            to_hub_alias = _make_alias(join['to_hub'], 'hub')
            from_hub_pk = join_plan['involved_hubs'][join['from_hub']]['pk']
            to_hub_pk = join_plan['involved_hubs'][join['to_hub']]['pk']
            lines.append(f"{join['join_type']} {ref(join['link'])} {link_alias}")
            lines.append(f"    ON {from_hub_alias}.{from_hub_pk} = {link_alias}.{from_hub_pk}")
            lines.append(f"{join['join_type']} {ref(join['to_hub'])} {to_hub_alias}")
            lines.append(f"    ON {link_alias}.{to_hub_pk} = {to_hub_alias}.{to_hub_pk}")
        elif join['type'] == 'link_sat':
            sat_alias = _make_alias(join['sat'], 'sat')
            link_alias = _make_alias(join['link'], 'link')
            lines.append(f"{join['join_type']} {join['sat']}_current {sat_alias}")
            lines.append(f"    ON {link_alias}.{join['pk']} = {sat_alias}.{join['pk']} AND {sat_alias}.rn = 1")
        elif join['type'] == 'missing':
            hub_alias = _make_alias(join['hub'], 'hub')
            lines.append(f"{join['join_type']} {ref(join['hub'])} {hub_alias}")
            lines.append(f"    ON /* {join['message']} */")

    if non_dv_tables:
        for tbl_name, tbl_info in non_dv_tables.items():
            alias = tbl_name[:3]
            lines.append(f"CROSS JOIN {ref(tbl_name)} {alias}")
            lines.append(f"    /* Moeglicher CROSS JOIN oder ERROR, MISSING CONSTRAINT - JOIN-Bedingung fuer {tbl_name} ergaenzen */")

    lines.append('')
    return '\n'.join(lines)


def _make_alias(name, obj_type):
    """Erstellt einen Alias: Prefix + erste 3 Zeichen jedes Namensteils.

    hub_customer       → h_cus
    hub_order          → h_ord
    sat_customer       → s_cus
    sat_order          → s_ord
    sat_order_detail   → s_ord_det
    lnk_order_customer → l_ord_cus
    lnk_order_product  → l_ord_pro
    """
    parts = name.split('_')
    if obj_type == 'hub' and len(parts) >= 2:
        return 'h_' + '_'.join(p[:3] for p in parts[1:])
    elif obj_type == 'sat' and len(parts) >= 2:
        return 's_' + '_'.join(p[:3] for p in parts[1:])
    elif obj_type == 'link' and len(parts) >= 2:
        return 'l_' + '_'.join(p[:3] for p in parts[1:])
    return name[:4]


# Standalone Test
if __name__ == '__main__':
    import sys
    if len(sys.argv) > 1:
        project_path = sys.argv[1]
    else:
        project_path = '/Users/stephanmeier/Documents/work/new_env/dbt_project'

    graph = parse_dv_project(project_path)
    if graph:
        print_dv_graph(graph)

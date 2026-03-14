{%- set yaml_metadata -%}
source_model: 'hub_order'
src_pk: 'order_hk'
as_of_dates_table: 'as_of_date'
bridge_walk:
  order_customer:
    bridge_link_pk: 'order_customer_hk'
    bridge_start_date: 'load_datetime'
    bridge_end_date: 'load_datetime'
    bridge_load_date: 'load_datetime'
    link_table: 'lnk_order_customer'
    link_pk: 'order_customer_hk'
    link_fk1: 'order_hk'
    link_fk2: 'customer_hk'
    eff_sat_table: 'eff_sat_order_customer'
    eff_sat_pk: 'order_customer_hk'
    eff_sat_end_date: 'effective_from'
    eff_sat_load_date: 'load_datetime'
stage_tables_ldts:
  stg_orders: 'load_datetime'
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.bridge(src_pk=metadata_dict['src_pk'],
                       as_of_dates_table=metadata_dict['as_of_dates_table'],
                       bridge_walk=metadata_dict['bridge_walk'],
                       stage_tables_ldts=metadata_dict['stage_tables_ldts'],
                       source_model=metadata_dict['source_model']) }}

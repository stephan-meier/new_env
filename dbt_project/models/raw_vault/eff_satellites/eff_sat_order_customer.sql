{%- set yaml_metadata -%}
source_model: 'stg_orders'
src_pk: 'order_customer_hk'
src_dfk: 'customer_hk'
src_sfk: 'order_hk'
src_start_date: 'effective_from'
src_end_date: 'effective_from'
src_eff: 'effective_from'
src_ldts: 'load_datetime'
src_source: 'record_source'
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.eff_sat(src_pk=metadata_dict['src_pk'],
                        src_dfk=metadata_dict['src_dfk'],
                        src_sfk=metadata_dict['src_sfk'],
                        src_start_date=metadata_dict['src_start_date'],
                        src_end_date=metadata_dict['src_end_date'],
                        src_eff=metadata_dict['src_eff'],
                        src_ldts=metadata_dict['src_ldts'],
                        src_source=metadata_dict['src_source'],
                        source_model=metadata_dict['source_model']) }}

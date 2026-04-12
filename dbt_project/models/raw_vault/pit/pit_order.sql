-- depends_on: {{ ref('hub_order') }}
-- depends_on: {{ ref('sat_order') }}

{%- set yaml_metadata -%}
source_model: 'hub_order'
src_pk: 'order_hk'
as_of_dates_table: 'as_of_date'
satellites:
  sat_order:
    pk:
      PK: 'order_hk'
    ldts:
      LDTS: 'load_datetime'
stage_tables_ldts:
  stg_orders: 'load_datetime'
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.pit(src_pk=metadata_dict['src_pk'],
                    as_of_dates_table=metadata_dict['as_of_dates_table'],
                    satellites=metadata_dict['satellites'],
                    stage_tables_ldts=metadata_dict['stage_tables_ldts'],
                    source_model=metadata_dict['source_model']) }}

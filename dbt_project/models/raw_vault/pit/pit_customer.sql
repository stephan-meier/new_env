-- depends_on: {{ ref('hub_customer') }}
-- depends_on: {{ ref('sat_customer') }}

{%- set yaml_metadata -%}
source_model: 'hub_customer'
src_pk: 'customer_hk'
as_of_dates_table: 'as_of_date'
satellites:
  sat_customer:
    pk:
      PK: 'customer_hk'
    ldts:
      LDTS: 'load_datetime'
stage_tables_ldts:
  stg_customers: 'load_datetime'
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.pit(src_pk=metadata_dict['src_pk'],
                    as_of_dates_table=metadata_dict['as_of_dates_table'],
                    satellites=metadata_dict['satellites'],
                    stage_tables_ldts=metadata_dict['stage_tables_ldts'],
                    source_model=metadata_dict['source_model']) }}

{%- set yaml_metadata -%}
source_model:
  raw: 'order_details'
derived_columns:
  record_source: '!CSV_LOAD'
  effective_from: 'change_date'
  load_datetime: 'change_date'
hashed_columns:
  order_hk: 'order_id'
  product_hk: 'product_id'
  order_product_hk:
    - 'order_id'
    - 'product_id'
  order_detail_hashdiff:
    is_hashdiff: true
    columns:
      - 'quantity'
      - 'unit_price'
      - 'discount'
      - 'order_detail_status'
      - 'date_allocated'
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.stage(include_source_columns=true,
                      source_model=metadata_dict['source_model'],
                      derived_columns=metadata_dict['derived_columns'],
                      hashed_columns=metadata_dict['hashed_columns'],
                      ranked_columns=none) }}

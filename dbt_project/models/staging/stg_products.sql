{%- set yaml_metadata -%}
source_model:
  raw: 'products'
derived_columns:
  record_source: '!CSV_LOAD'
  effective_from: 'change_date'
  load_datetime: 'change_date'
  product_id: 'id'
hashed_columns:
  product_hk: 'id'
  product_hashdiff:
    is_hashdiff: true
    columns:
      - 'product_code'
      - 'product_name'
      - 'description'
      - 'standard_cost'
      - 'list_price'
      - 'target_level'
      - 'reorder_level'
      - 'minimum_reorder_quantity'
      - 'quantity_per_unit'
      - 'discontinued'
      - 'category'
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.stage(include_source_columns=true,
                      source_model=metadata_dict['source_model'],
                      derived_columns=metadata_dict['derived_columns'],
                      hashed_columns=metadata_dict['hashed_columns'],
                      ranked_columns=none) }}

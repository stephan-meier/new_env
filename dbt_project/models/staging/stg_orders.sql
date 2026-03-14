{%- set yaml_metadata -%}
source_model:
  raw: 'orders'
derived_columns:
  record_source: '!CSV_LOAD'
  effective_from: 'change_date'
  load_datetime: 'change_date'
  order_id: 'id'
  customer_id: 'customer_id'
  employee_id: 'employee_id'
hashed_columns:
  order_hk: 'id'
  customer_hk: 'customer_id'
  employee_hk: 'employee_id'
  order_customer_hk:
    - 'id'
    - 'customer_id'
  order_employee_hk:
    - 'id'
    - 'employee_id'
  order_hashdiff:
    is_hashdiff: true
    columns:
      - 'order_date'
      - 'shipped_date'
      - 'ship_name'
      - 'ship_address1'
      - 'ship_address2'
      - 'ship_city'
      - 'ship_state'
      - 'ship_postal_code'
      - 'ship_country'
      - 'shipping_fee'
      - 'payment_type'
      - 'paid_date'
      - 'order_status'
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.stage(include_source_columns=true,
                      source_model=metadata_dict['source_model'],
                      derived_columns=metadata_dict['derived_columns'],
                      hashed_columns=metadata_dict['hashed_columns'],
                      ranked_columns=none) }}

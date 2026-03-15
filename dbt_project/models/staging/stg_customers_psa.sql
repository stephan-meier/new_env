{{- config(tags=['psa']) -}}

{%- set yaml_metadata -%}
source_model:
  psa: 'v_customers_cur'
derived_columns:
  record_source: '!PSA_NG_GEN'
  effective_from: 'change_date'
  load_datetime: 'change_date'
  customer_id: 'id'
hashed_columns:
  customer_hk: 'id'
  customer_hashdiff:
    is_hashdiff: true
    columns:
      - 'last_name'
      - 'first_name'
      - 'email'
      - 'company'
      - 'phone'
      - 'address1'
      - 'address2'
      - 'city'
      - 'state'
      - 'postal_code'
      - 'country'
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.stage(include_source_columns=true,
                      source_model=metadata_dict['source_model'],
                      derived_columns=metadata_dict['derived_columns'],
                      hashed_columns=metadata_dict['hashed_columns'],
                      ranked_columns=none) }}

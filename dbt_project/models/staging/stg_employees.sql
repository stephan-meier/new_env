{%- set yaml_metadata -%}
source_model:
  raw: 'employees'
derived_columns:
  record_source: '!CSV_LOAD'
  effective_from: 'change_date'
  load_datetime: 'change_date'
  employee_id: 'id'
hashed_columns:
  employee_hk: 'id'
  employee_hashdiff:
    is_hashdiff: true
    columns:
      - 'last_name'
      - 'first_name'
      - 'email'
      - 'avatar'
      - 'job_title'
      - 'department'
      - 'manager_id'
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

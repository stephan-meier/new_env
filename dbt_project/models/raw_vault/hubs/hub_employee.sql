-- depends_on: {{ ref('stg_employees') }}

{{ automate_dv.hub(src_pk='employee_hk',
                    src_nk='employee_id',
                    src_ldts='load_datetime',
                    src_source='record_source',
                    source_model='stg_employees') }}

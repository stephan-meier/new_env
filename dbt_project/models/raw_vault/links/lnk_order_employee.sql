-- depends_on: {{ ref('stg_orders') }}

{{ automate_dv.link(src_pk='order_employee_hk',
                     src_fk=['order_hk', 'employee_hk'],
                     src_ldts='load_datetime',
                     src_source='record_source',
                     source_model='stg_orders') }}

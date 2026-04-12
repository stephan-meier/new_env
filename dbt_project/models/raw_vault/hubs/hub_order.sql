-- depends_on: {{ ref('stg_orders') }}

{{ automate_dv.hub(src_pk='order_hk',
                    src_nk='order_id',
                    src_ldts='load_datetime',
                    src_source='record_source',
                    source_model='stg_orders') }}

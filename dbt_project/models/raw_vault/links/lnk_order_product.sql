-- depends_on: {{ ref('stg_order_details') }}

{{ automate_dv.link(src_pk='order_product_hk',
                     src_fk=['order_hk', 'product_hk'],
                     src_ldts='load_datetime',
                     src_source='record_source',
                     source_model='stg_order_details') }}

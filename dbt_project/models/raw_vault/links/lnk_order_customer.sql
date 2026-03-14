{{ automate_dv.link(src_pk='order_customer_hk',
                     src_fk=['order_hk', 'customer_hk'],
                     src_ldts='load_datetime',
                     src_source='record_source',
                     source_model='stg_orders') }}

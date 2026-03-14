{{ automate_dv.sat(src_pk='order_product_hk',
                    src_hashdiff='order_detail_hashdiff',
                    src_payload=['quantity', 'unit_price', 'discount',
                                 'order_detail_status', 'date_allocated'],
                    src_eff='effective_from',
                    src_ldts='load_datetime',
                    src_source='record_source',
                    source_model='stg_order_details') }}

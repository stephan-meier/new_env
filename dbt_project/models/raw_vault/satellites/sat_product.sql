-- depends_on: {{ ref('stg_products') }}

{{ automate_dv.sat(src_pk='product_hk',
                    src_hashdiff='product_hashdiff',
                    src_payload=['product_code', 'product_name', 'description',
                                 'standard_cost', 'list_price', 'target_level',
                                 'reorder_level', 'minimum_reorder_quantity',
                                 'quantity_per_unit', 'discontinued', 'category'],
                    src_eff='effective_from',
                    src_ldts='load_datetime',
                    src_source='record_source',
                    source_model='stg_products') }}

{{- config(tags=['psa']) -}}

{{ automate_dv.hub(src_pk='customer_hk',
                    src_nk='customer_id',
                    src_ldts='load_datetime',
                    src_source='record_source',
                    source_model='stg_customers_psa') }}

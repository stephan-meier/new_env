{{- config(tags=['psa']) -}}

{{ automate_dv.sat(src_pk='customer_hk',
                    src_hashdiff='customer_hashdiff',
                    src_payload=['last_name', 'first_name', 'email', 'company',
                                 'phone', 'address1', 'address2',
                                 'city', 'state', 'postal_code', 'country'],
                    src_eff='effective_from',
                    src_ldts='load_datetime',
                    src_source='record_source',
                    source_model='stg_customers_psa') }}

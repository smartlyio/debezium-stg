{{ config(
  materialized='incremental',
  incremental_strategy='append',
  unique_key=['maj_id', 'min_id',],
  tags=['snowflake', 'postgres-src'],
) }}

{%- set yaml_fields -%}
- name: '{}::STRING'
- surname: '{}::STRING'
- birthday:
    is_date: true
{%- endset -%}

{{- debezium_stg.to_stg_layer_scd(
  source('fake_source', 'landing_table'),
  fields=fromyaml(yaml_fields),
  incremental=is_incremental(),
  source_engine='postgresql',
  scd_type='scd4',
  pk_fields=[{'maj_id': '{}::INTEGER'}, {'min_id': '{}::STRING'},],
) -}}
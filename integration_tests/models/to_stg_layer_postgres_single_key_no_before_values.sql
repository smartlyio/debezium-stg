{{ config(
  materialized='incremental',
  incremental_strategy='merge',
  unique_key='id',
  tags=['snowflake', 'postgres-src'],
) }}

{%- set yaml_fields -%}
- name: '{}::STRING'
- surname: '{}::STRING'
- birthday:
    is_date: true
{%- endset -%}

{{- debezium_stg.to_stg_layer(
  source('fake_source', 'landing_table'),
  fields=fromyaml(yaml_fields),
  incremental=is_incremental(),
  source_engine='postgresql',
) -}}

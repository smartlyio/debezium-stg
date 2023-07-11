{# /*
  This macro is used for materialization of data, extructed with Debezium CDC
  source kafka connector for Postgres and loaded with Snowflake sync kafka
  connector. The structure of the result model should be similar to the
  structure of the source column in Postgres, enreached with technical,
  deletion-related and optionaly SCD-related fields (not implemented yet). The
  macro is likely to work with sources extructed, using another Debezium
  source kafka connectors (i.e. Cassandra, MongoDB, Oracle, etc...), but it
  does not checked and some composite type encoding could differ.
*/ #}
{% macro to_stg_layer(
  source_ref,
  fields,
  incremental,
  source_engine,
  contains_prev_state=false,
  aliases=none,
  pk_fields=none
) %}
  {{- debezium_stg.ensure_source_engine_supported(source_engine) -}}
  {#{%- if
    model.config.materialization != 'incremental'
    or model.config.incremental_strategy != 'merge'
  -%}
    {{- exceptions.raise_compiler_error(
      (
        'Model "{0}" should be configured with materialization = '
          ~ '"incremental" and incremental_strategy = "merge", got ({1}, {2})'
      ).format(
        model.name,
        model.config.materialization,
        model.config.incremental_strategy,
      ),
    ) -}}
  {%- endif -%}#}

  {%- set pk_fields = pk_fields
    if pk_fields
    else [{'id': '{}::' ~ dbt.type_bigint()}] -%}
  {%- set pk_field_names = [] -%}
  {%- set pk_field_values = [] -%}
  {%- for field in pk_fields -%}
    {%- do pk_field_names.append(
      debezium_stg.extract_column_alias(field),
    ) -%}
    {%- do pk_field_values.append(
      debezium_stg.before_or_after(field, add_alias=false)
    ) -%}
  {%- endfor -%}

  {%- set ts_field = {'ts_ms': dbt_date.from_unixtimestamp(
    epochs='{}::BIGINT',
    format='milliseconds',
  )} -%}

  {%- set aliases = debezium_stg.to_stg_layer_aliases(
    aliases=aliases if aliases is not none else {}
  ) -%}

  {%- set batch_mgmt_fields = (aliases['loaded_at'], aliases['row_id'],) -%}

  {%- if contains_prev_state -%}
    {{- return(debezium_stg.to_stg_layer_with_prev_state(
      source_ref=source_ref,
      fields=fields,
      incremental=incremental,
      pk_fields=pk_fields,
      pk_field_names=pk_field_names,
      pk_field_values=pk_field_values,
      ts_field=ts_field,
      aliases=aliases,
      batch_mgmt_fields=batch_mgmt_fields,
    )) -}}
  {%- endif -%}
  {{- return(debezium_stg.to_stg_layer_without_prev_state(
    source_ref=source_ref,
    fields=fields,
    incremental=incremental,
    pk_fields=pk_fields,
    pk_field_names=pk_field_names,
    pk_field_values=pk_field_values,
    ts_field=ts_field,
    aliases=aliases,
    batch_mgmt_fields=batch_mgmt_fields,
  )) -}}
{%- endmacro -%}

{# /*
  Default values for aliases, used in debezium_stg.to_stg_layer macro.
  The overrides are made becasuse of assumption that the structure of landing
  tables for different warehouse engines is different.

  Arguments:

    - name: aliases
      type: dict
      description: Dict with keys of aliases for per-macro overriding.
*/ #}
{%- macro to_stg_layer_aliases(aliases) -%}
  {{- return(adapter.dispatch('to_stg_layer_aliases', 'debezium_stg')(
    aliases=aliases,
  )) -}}
{%- endmacro -%}


{%- macro default__to_stg_layer_aliases(aliases) -%}
  {{- exceptions.raise_compiler_error(
    'No default implementation for macro `to_stg_layer_aliases` is '
      ~ 'defined',
  ) -}}
{%- endmacro -%}


{%- macro snowflake__to_stg_layer_aliases(aliases) -%}
  {%- set default_aliases = {
    'is_deleted': var('is_deleted__alias'),
    'deleted_at': var('deleted_at__alias'),
    'ts':         var('ts__alias'),
    'row_id':     var('row_id__alias'),
    'loaded_at':  var('loaded_at__alias'),
  } -%}
  {%- do default_aliases.update(aliases) -%}
  {%- do return(default_aliases) -%}
{%- endmacro -%}


{# /*
  Default values for aliases, used in debezium_stg.create_source_table macro.
  The overrides are made becasuse of assumption that the structure of landing
  tables for different warehouse engines is different.

  Arguments:

    - name: aliases
      type: dict
      description: Dict with keys of aliases for per-macro overriding.
*/ #}
{%- macro create_source_table_aliases(aliases) -%}
  {{- return(adapter.dispatch('create_source_table_aliases', 'debezium_stg')(
    aliases
  )) -}}
{%- endmacro -%}


{%- macro default__create_source_table_aliases(aliases) -%}
  {{- exceptions.raise_compiler_error(
    'No default implementation for macro `create_source_table_aliases` is '
      ~ 'defined',
  ) -}}
{%- endmacro -%}


{%- macro snowflake__create_source_table_aliases(aliases) -%}
  {%- set default_aliases = {
    'row_id': var('row_id__alias'),
    'loaded_at': var('loaded_at__alias'),
  } -%}
  {%- do default_aliases.update(aliases) -%}
  {%- do return(default_aliases) -%}
{%- endmacro -%}


{# /*
  Default values for aliases, used in debezium_stg.to_stage_layer_scd macro.
  The overrides are made becasuse of assumption that the structure of landing
  tables for different warehouse engines is different.

  Arguments:

    - name: aliases
      type: dict
      description: Dict with keys of aliases for per-macro overriding.
*/ #}
{%- macro to_stg_layer_scd_aliases(aliases) -%}
  {{- return(adapter.dispatch('to_stg_layer_scd_aliases', 'debezium_stg')(
    aliases=aliases,
  )) -}}
{%- endmacro -%}


{%- macro default__to_stg_layer_scd_aliases(aliases) -%}
  {{- exceptions.raise_compiler_error(
    'No default implementation for macro `to_stg_layer_scd_aliases` is '
      ~ 'defined',
  ) -}}
{%- endmacro -%}


{%- macro snowflake__to_stg_layer_scd_aliases(aliases) -%}
  {%- set default_aliases = {
    'valid_from':     var('valid_from__alias'),
    'valid_to':       var('valid_to__alias'),
    'op':             var('op__alias'),
    'surrogate_key':  var('surrogate_key__alias'),
    'row_id':         var('row_id__alias'),
    'loaded_at':      var('loaded_at__alias'),
  } -%}
  {%- do default_aliases.update(aliases) -%}
  {%- do return(default_aliases) -%}
{%- endmacro -%}

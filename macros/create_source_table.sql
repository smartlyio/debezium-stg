{%- macro create_source_table(
  table_name,
  schema_name=none,
  role_name=none,
  db=none,
  aliases=none,
  dry_run=False
) -%}
  {%- if not execute -%}
    {{ exceptions.raise_compiler_error(
      'create_source_table macro is allowed only for `run-operation` usage',
    ) }}
  {%- endif -%}
  {%- if db is none and target.database is defined -%}
    {%- set db = target.database -%}
  {%- endif -%}
  {%- if schema_name is none -%}
    {%- set schema_name = target.schema -%}
  {%- endif -%}
  {%- if not dry_run %}
    {%- do adapter.create_schema(api.Relation.create(
      database=db,
      schema=schema_name,
    )) -%}
  {%- endif -%}
  {%- set aliases = debezium_stg.create_source_table_aliases(
    aliases=aliases if aliases is not  none else {},
  ) -%}
  {{- return(adapter.dispatch('create_source_table', 'debezium_stg')(
    schema_name=schema_name,
    table_name=table_name,
    role_name=role_name,
    db=db,
    aliases=aliases,
    dry_run=dry_run,
  )) -}}
{%- endmacro -%}

{# /*
  Implementation of the logic of macro debezium_stg.to_stg_layer for the case,
  when we have a previous state.
*/ #}
{%- macro to_stg_layer_with_prev_state(
  source_ref,
  fields,
  incremental,
  pk_fields,
  pk_field_names,
  pk_field_values,
  ts_field,
  aliases,
  batch_mgmt_fields
) -%}
  {{- return(adapter.dispatch('to_stg_layer_with_prev_state', 'debezium_stg')(
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


{# /*
  The default implementation of debezium_stg.to_stg_layer_with_prev_state is
  not defined.
*/ #}
{%- macro default__to_stg_layer_with_prev_state(
  source_ref,
  fields,
  incremental,
  pk_fields,
  pk_field_names,
  pk_field_values,
  ts_field,
  aliases,
  batch_mgmt_fields
) -%}
  {{- exceptions.raise_compiler_error(
    'Default implementation for macro '
      ~ '"debezium_stg.to_stg_layer_with_prev_state" is not defined',
  ) -}}
{%- endmacro -%}


{# /*
  The version of to_stg_layer macro for the case when we could
  rely on values in "before" part of the **record_content**. That assumption
  makes it more simple.
*/ #}
{%- macro snowflake__to_stg_layer_with_prev_state(
  source_ref,
  fields,
  incremental,
  pk_fields,
  pk_field_names,
  pk_field_values,
  ts_field,
  aliases,
  batch_mgmt_fields
) -%}
  WITH
    {% if incremental %}
      {{- debezium_stg.lower_bound_cte(aliases=aliases) }},
    {%- endif %}

    result AS (
      SELECT

        {# /* Materialize values */ #}
        {% for field in pk_fields -%}
          {{ debezium_stg.before_or_after(field=field) }},
        {%- endfor %}
        {% for field in fields -%}
          {{ debezium_stg.before_or_after(field=field) }},
        {%- endfor %}

        {# /* Matrialize fields, related to soft deletion */ #}
        {{ debezium_stg.a_deletion_event() }} AS {{ aliases['is_deleted'] }},
        iff(
          {{- debezium_stg.a_deletion_event() }},
          {{- debezium_stg.source_field(
            field=ts_field,
            add_alias=false,
          ) }},
          NULL
        ) AS {{ aliases['deleted_at'] }},

        {# /*
          Materialize filds, related to incremental materialization (stuff for
          ordering and batching).
        */ #}
        {{ debezium_stg.source_field(
            field=ts_field,
            column_alias=aliases['ts'],
        ) }},

        {{ debezium_stg.iterator_format_join(
          itr=batch_mgmt_fields,
          format_str='src.{0}')
        }}

      FROM {{ source_ref }} src{% if incremental %}, lower_bound lb{% endif %}
      WHERE
        src.record_content <> parse_json('{}')
        {% if incremental -%}
          AND (
            {{- debezium_stg.batch_ordering_comparison(
              fields=batch_mgmt_fields,
            ) -}}
          )
        {%- endif %}
      {{ debezium_stg._snowflake__qualify_part(
        field_names=pk_field_values,
        aliases=aliases
      ) }}
    )

  SELECT
    {%- for field in pk_field_names %}
      {{ field }},
    {%- endfor -%}
    {%- for field in fields -%}
      {{- debezium_stg.extract_column_alias(field=field) }},
    {%- endfor -%}

    {{- aliases['is_deleted'] -}},
    {{- aliases['deleted_at'] -}},

    {{ debezium_stg.iterator_format_join(
      itr=batch_mgmt_fields,
      format_str='{0}')
    }}

  FROM result
{%- endmacro -%}

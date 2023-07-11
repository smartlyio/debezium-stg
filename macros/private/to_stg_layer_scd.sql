{# /*
  Generate model for SCD4 (see.
  https://en.wikipedia.org/wiki/Slowly_changing_dimension#Type_4:_add_history_table
  ).
*/ #}
{%- macro to_stg_layer_scd4(
  source_ref,
  fields,
  pk_field_aliases,
  batch_mgmt_fields,
  incremental,
  ctes_only,
  aliases
) -%}
  {{- return(adapter.dispatch('to_stg_layer_scd4', 'debezium_stg')(
    source_ref=source_ref,
    fields=fields,
    pk_field_aliases=pk_field_aliases,
    batch_mgmt_fields=batch_mgmt_fields,
    incremental=incremental,
    ctes_only=ctes_only,
    aliases=aliases,
  )) -}}
{%- endmacro -%}


{%- macro default__to_stg_layer_scd4(
  source_ref,
  fields,
  pk_field_aliases,
  batch_mgmt_fields,
  incremental,
  ctes_only,
  aliases
) -%}
  {{- exceptions.raise_compiler_error(
    'No default implementation for macro `to_stg_layer_scd4` is defined',
  ) -}}
{%- endmacro -%}


{%- macro snowflake__to_stg_layer_scd4(
  source_ref,
  fields,
  pk_field_aliases,
  batch_mgmt_fields,
  incremental,
  ctes_only,
  aliases
) -%}
  {%- set ts_field = {'ts_ms': dbt_date.from_unixtimestamp(
    epochs='{}::BIGINT',
    format='milliseconds',
  )} -%}

  {% if incremental %}
    {% if not ctes_only %}WITH{% endif %}
    {{ debezium_stg.lower_bound_cte(aliases=aliases) -}}
    {%- if ctes_only %},{% endif %}
  {%- endif %}

  {% if ctes_only %}result AS ({% endif %}
  SELECT
    {# /* Materialize values */ #}
    {% for field in fields -%}
      {{ debezium_stg.before_or_after(field=field) }},
    {%- endfor %}

    {# /* setup valid_from */ #}
    {{ debezium_stg.source_field(
      field=ts_field,
      column_alias=aliases['valid_from'],
    ) }},

    src.record_content:op::STRING AS {{ aliases['op'] }},

    {# /* Evaluate unique_row_id */ #}
    {{ dbt_utils.generate_surrogate_key(pk_field_aliases) }}
     AS {{ aliases['surrogate_key'] }},

    {# /*
      Materialize fields, related to incremental materialization (stuff for
      ordering and batching).
    */ #}
    {{ debezium_stg.iterator_format_join(
      itr=batch_mgmt_fields,
      format_str='src.{0}',
    ) }}

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
  {# /* Closing bracket */ #}
  {% if ctes_only %}){% endif %}
{%- endmacro -%}


{# /*
  Just shortcut to common code block, which appears several times in the code.
  Only for internal usage. Used only in this file.
*/ #}
{%- macro _snowflake__select_all_fields_except_valid_to(
  source_cte,
  field_aliases,
  aliases
) -%}
  SELECT
    {% for field in field_aliases %}{{ field }},{% endfor %}

    {{ aliases['valid_from'] }},
    {{ aliases['surrogate_key'] }},
    {{ aliases['op'] }},

    {{ aliases['loaded_at'] }},
    {{ aliases['row_id'] }}

  FROM {{ source_cte }}
{%- endmacro -%}


{%- macro to_stg_layer_scd4_to_scd2(
  field_aliases,
  incremental,
  aliases
) -%}
  {%- set hander_macro = adapter.dispatch(
    'to_stg_layer_scd4_to_scd2',
    'debezium_stg',
  ) -%}
  {%- set scd4 = caller() -%}
  {%- call hander_macro(
    field_aliases=field_aliases,
    incremental=incremental,
    aliases=aliases,
  ) -%}
    {{- scd4 -}}
  {%- endcall -%}
{%- endmacro -%}


{%- macro default__to_stg_layer_scd4_to_scd2(
  field_aliases,
  incremental,
  aliases
) -%}
  {%- set unused = caller() -%}
  {{- exceptions.raise_compiler_error(
    'No default implementation for macro `to_stg_layer_scd4_to_scd2` is '
      ~ 'defined',
  ) -}}
{%- endmacro -%}


{# /*
  This macro is suitable for materializing `snowflake_valid_to` field from
  scd4 CTEs. It also queries the destination model for rows, that should be
  updated.
*/ #}
{%- macro snowflake__to_stg_layer_scd4_to_scd2(
  field_aliases,
  incremental,
  aliases
) -%}
  WITH
    {{ caller() }},

    {# /*
      Unioning with already materialized models, that should be updated.
      Here we have an assumption, that the order of the events would be
      kept for each primary key. That's why we potentially need to
      update only some the rows with `snowflake_valid_to` NULL values.
      The assumption should be valid becuase Debezium sets the source
      tables' primary key as a partition key for the related topics.
    */ #}
    {% if incremental -%}
      rows_to_update AS (
        SELECT
          {% for field in field_aliases %}
            {{ field }},
          {% endfor %}

          {{ aliases['valid_from'] }},
          {{ aliases['surrogate_key'] }},
          {# /*
            Here it's just important that it's not `deletion_snowflake_op` and
            not `NULL` (to prevent unexpected comparison results).
          */ #}
          '' AS {{ aliases['op'] }},

          {{ aliases['loaded_at'] }},
          {{ aliases['row_id'] }}

        FROM {{ this }}
        WHERE
          {{ aliases['surrogate_key'] }} IN (
            SELECT DISTINCT {{ aliases['surrogate_key'] }} FROM result
          )
          AND {{ aliases['valid_to'] }} IS NULL
      ),

      union_result AS (
        (
          {{- debezium_stg._snowflake__select_all_fields_except_valid_to(
            source_cte='result',
            field_aliases=field_aliases,
            aliases=aliases,
          ) -}}
        ) UNION ALL (
          {{- debezium_stg._snowflake__select_all_fields_except_valid_to(
            source_cte='rows_to_update',
            field_aliases=field_aliases,
            aliases=aliases,
          ) -}}
        )
      )
    {% else %}
      union_result AS (
        {{- debezium_stg._snowflake__select_all_fields_except_valid_to(
          source_cte='result',
          field_aliases=field_aliases,
          aliases=aliases
        ) -}}
      )
    {%- endif %},

    with_valid_to AS (
      SELECT
        {% for field in field_aliases %}scd4.{{ field }},{% endfor %}

        scd4.{{ aliases['valid_from'] }},
        scd4.{{ aliases['surrogate_key'] }},
        scd4.{{ aliases['op'] }},

        LEAD(scd4.{{ aliases['valid_from'] }}) OVER (
          PARTITION BY scd4.{{ aliases['surrogate_key'] }}
          ORDER BY scd4.{{ aliases['valid_from'] }}
        ) AS {{ aliases['valid_to'] }},

        scd4.{{ aliases['loaded_at'] }},
        scd4.{{ aliases['row_id'] }}

      FROM union_result scd4
    )

  SELECT
    {% for field in field_aliases %}{{ field }},{% endfor %}
    
    {{ aliases['valid_from'] }},
    {{ aliases['valid_to'] }},
    {{ aliases['surrogate_key'] }},

    {{ aliases['loaded_at'] }},
    {{ aliases['row_id'] }}

  FROM with_valid_to
  WHERE {{ aliases['op'] }} <> {{ debezium_stg.deletion_op() }}
{%- endmacro -%}

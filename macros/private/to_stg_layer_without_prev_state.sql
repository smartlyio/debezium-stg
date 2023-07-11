{# /*
  Implementation of the logic of macro debezium_stg.to_stg_layer for the case,
  when we have no previous state.
*/ #}
{%- macro to_stg_layer_without_prev_state(
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
  {{- return(adapter.dispatch('to_stg_layer_without_prev_state', 'debezium_stg')(
    source_ref=source_ref,
    fields=fields,
    incremental=incremental,
    pk_fields=pk_fields,
    pk_field_names=pk_field_names,
    pk_field_values=pk_field_values,
    ts_field=ts_field,
    aliases=aliases,
    batch_mgmt_fields=batch_mgmt_fields
  )) -}}
{%- endmacro -%}


{# /*
  The default implementation of debezium_stg.to_stg_layer_without_prev_state
  is not defined.
*/ #}
{%- macro default__to_stg_layer_without_prev_state(
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
      ~ '"debezium_stg.to_stg_layer_without_prev_state" is not defined',
  ) -}}
{%- endmacro -%}


{# /*
  The version of debezium_stg.to_stg_layer macro for the case when we could
  not rely on values in "before" part of the **record_content** (only on
  indices). For this case we should determin a value of the deleted entity
  from the previous event with the same primary key.
*/ #}
{%- macro snowflake__to_stg_layer_without_prev_state(
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
    {% if incremental -%}
      {{- debezium_stg.lower_bound_cte(
        aliases=aliases,
      ) -}},
    {%- endif %}

    last_not_deleted_records AS (
      SELECT

        {# /* Materialize values */ #}
        {% for field in pk_fields %}
            {{ debezium_stg.after_field(field=field) }},
        {% endfor %}
        {% for field in fields %}
            {{ debezium_stg.after_field(field=field) }},
        {% endfor %}

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
        AND {{ debezium_stg.an_upsert_event() }}
        {% if incremental -%}
          AND (
            {{- debezium_stg.batch_ordering_comparison(
              fields=batch_mgmt_fields,
            ) -}}
          )
        {%- endif %}
      {{ debezium_stg._snowflake__qualify_part(
        field_names=pk_field_values,
        aliases=aliases,
      ) }}
    ),

    last_deleted_records AS (
      SELECT

        {# /* Materialize primary keys */ #}
        {% for field in pk_fields %}
          {{ debezium_stg.before_field(field=field) }},
        {% endfor %}

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
        {{ debezium_stg.a_deletion_event() }}
        {% if incremental -%}
          AND (
            {{- debezium_stg.batch_ordering_comparison(
              fields=batch_mgmt_fields,
            ) -}}
          )
        {%- endif %}
      {{ debezium_stg._snowflake__qualify_part(
        field_names=pk_field_values,
        aliases=aliases,
      ) }}
    ),

    deleted_entities AS (
      SELECT

        {# /* Materialize values */ #}
        {% for field in pk_field_names %}
          src_d.{{ field }},
        {% endfor %}
        {% for field in fields %}
          {{ debezium_stg.coalesce_with_dst(
            use_coalesce=incremental,
            field_name=debezium_stg.extract_column_alias(field=field),
            source_table_alias='src_nd',
          ) }},
        {% endfor %}

        {# /* Matrialize fields, related to soft deletion */ #}
        TRUE AS {{ aliases['is_deleted'] }},
        src_d.{{ aliases['ts'] }} AS {{ aliases['deleted_at'] }},

        {# /*
          Materialize filds, related to incremental materialization (stuff for
          ordering and batching).
        */ #}
        {{ debezium_stg.iterator_format_join(
          itr=batch_mgmt_fields,
          format_str='src_d.{0}')
        }}

      FROM last_deleted_records src_d
      LEFT JOIN last_not_deleted_records src_nd ON
        {{ debezium_stg.iterator_format_join(
          itr=pk_field_names,
          format_str='src_nd.{0} = src_d.{0}',
          delimiter=' AND ',
        ) }}
      {% if incremental %}
        LEFT JOIN {{ this }} dst ON
          {{ debezium_stg.iterator_format_join(
            itr=pk_field_names,
            format_str='dst.{0} = src_d.{0}',
            delimiter=' AND ',
          ) }}
      {% endif %}
      WHERE
        src_nd.{{ pk_field_names[0] }} is NULL
        OR src_nd.{{ aliases['ts'] }} < src_d.{{ aliases['ts'] }}
    ),

    changed_entities AS (
      SELECT

        {# /* Materialize values */ #}
        {% for field in pk_fields %}
          src_nd.{{ debezium_stg.extract_column_alias(field=field) }},
        {% endfor %}
        {% for field in fields %}
          src_nd.{{ debezium_stg.extract_column_alias(field=field) }},
        {% endfor %}

        {# /* Matrialize fields, related to soft deletion */ #}
        FALSE AS {{ aliases['is_deleted'] }},
        NULL::TIMESTAMP_NTZ(9) AS {{ aliases['deleted_at'] }},

        {# /*
          Materialize filds, related to incremental materialization (stuff for
          ordering and batching).
        */ #}
        {{ debezium_stg.iterator_format_join(
          itr=batch_mgmt_fields,
          format_str='src_nd.{0}'
        ) }}

      FROM last_not_deleted_records src_nd
      LEFT JOIN last_deleted_records src_d ON
        {{ debezium_stg.iterator_format_join(
          itr=pk_field_names,
          format_str='src_d.{0} = src_nd.{0}',
          delimiter=' AND ',
        ) }}
      WHERE
        src_d.{{ pk_field_names[0] }} IS NULL OR
        src_d.{{ aliases['ts'] }} < src_nd.{{ aliases['ts'] }}
    )

  (SELECT * FROM deleted_entities) UNION ALL (SELECT * FROM changed_entities)
{%- endmacro -%}

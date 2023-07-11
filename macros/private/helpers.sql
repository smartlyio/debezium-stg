{# /*
  These macros are internal usage only and should not be used outside of the
  package.
*/ #}

{# /*
  Generates database name prefix (with ".") for connecters where applicable.

  Arguments:

    - name: db
      type: string or None
      description: database name
*/ #}
{%- macro if_db(db) -%}
  {{- return(adapter.dispatch('if_db', 'debezium_stg')(db=db)) -}}
{%- endmacro -%}


{%- macro default__if_db(db) -%}
{%- endmacro -%}


{%- macro snowflake__if_db(db) -%}
  {%- if db is not none %}{{ db }}.{% endif -%}
{%- endmacro -%}


{# /*
  Renders field with table alias if given

  Arguments:

    - name: field
      type: string
      description: String to render (probably, with alias)

    - name: table_alias
      type: string or None
      description: Table alias to use. If None or empty string, table alias
        will be omited.

  Examples:

    debezium_stg.if_table_alias('foo')

    debezium_stg.if_table_alias('foo', none)

    debezium_stg.if_table_alias('foo', '')

  renders as `foo`

    debezium_stg.if_table_alias('foo', 'foo_table')

  renders as `foo_table.foo`
*/ #}
{%- macro if_table_alias(field, table_alias=none) -%}
  {% if table_alias %}{{ table_alias }}.{% endif %}{{ field }}
{%- endmacro -%}


{# /*
  Renders `field` with `format_str`, if present. Otherwise returns it "as is".

  Arguments:

    - name: field
      type: string
      description: Field name to be rentered.

    - name: format_str
      type: string or None
      description: Format string, accepting `.format` method

  Examples:

    debezium_stg._field_if_format('foo', none) renders "foo"

    debezium_stg._field_if_format('foo', '') renders "foo"

    debezium_stg._field_if_format('foo', 'COALESCE({}, \'no value\'') renders
      "COALESCE(foo, 'no value')"

    debezium_stg._field_if_format('foo', 'MAX({0}) AS max_{0}') renders
      "MAX(foo) AS max_foo"
*/ #}
{%- macro field_if_format(field, format_str) -%}
  {%- if format_str %}{% do return(format_str.format(field)) %}{% endif -%}
  {{- field -}}
{%- endmacro -%}


{# /*
  Returns `alias`, if present, otherwise `field`. Usefull to inshort other
  macros in case of ` AS ` parts.

  Arguments:

    - name: field
      type: string
      description: field name

    - name: alias
      type: string or None
      description: alias
*/ #}
{%- macro field_if_alias(field, alias=none) -%}
  {%- if alias %}{{ alias }}{% else %}{{ field }}{% endif -%}
{%- endmacro -%}


{# /*
  Evaluates `delimiter.join(itr.map(lambda i: format_str.format((i,))))`.

  Arguments:

    - name: itr
      type: iteratable
      description: stringth to iterate through

    - name: format_str
      type: string
      description: format to apply for each item of itr. Not support `None`s
        because if you want to pass None, just use `delimiter.join(itr)`

    - name: delimiter
      type: string
      description: the delimiter, used to join rendered items
*/ #}
{%- macro iterator_format_join(itr, format_str, delimiter=', ') -%}
  {%- for item in itr -%}
    {{ format_str.format(item) }}
    {%- if not loop.last %}{{ delimiter }}{% endif -%}
  {%- endfor -%}
{%- endmacro -%}


{# /*
  Extract "op" field from the source table.

  Arguments:

    - name: source_alias
      type: string
      description: Source table alias.
*/ #}
{%- macro extract_op(source_alias='src') -%}
  {{- return(adapter.dispatch('extract_op', 'debezium_stg')(
    source_alias=source_alias,
  )) -}}
{%- endmacro -%}


{%- macro default__extract_op(source_alias) -%}
  {{- exceptions.raise_compiler_error(
    'No default implementation for macro `extract_op` is defined',
  ) -}}
{%- endmacro -%}


{%- macro snowflake__extract_op(source_alias) -%}
  {{- return(debezium_stg.if_table_alias(
    field='record_content:op::STRING',
    table_alias=source_alias,
  )) -}}
{%- endmacro -%}


{# /* Returns platform-specific deletion string literal */ #}
{%- macro deletion_op() -%}
  {{- return(dbt.string_literal('d')) -}}
{%- endmacro -%}


{# /*
  Expression that is true, where cdc event is not a deletion event.

  Arguments:

    - name: source_alias
      type: string
      description: Source table alias.
*/ #}
{%- macro an_upsert_event(source_alias=src) -%}
  {{- return(
    debezium_stg.extract_op(source_alias) ~ ' <> '
      ~ debezium_stg.deletion_op()
  ) -}}
{%- endmacro -%}


{# /*
  Expression that is true, where cdc event is a deletion event.

  Arguments:

    - name: source_alias
      type: string
      description: Source table alias.
*/ #}
{%- macro a_deletion_event(source_alias=src) -%}
  {{- return(
    debezium_stg.extract_op(source_alias) ~ ' = '
      ~ debezium_stg.deletion_op()
  ) -}}
{%- endmacro -%}


{# /*
  Extract the column name from field definition

  Arguments:

    - name: field
      type: string or dict
      description: just a column name or a dict with the only key, which is a
        column name.

  Examples:

    debezium_stg.extract_column_alias('foo') renders as "foo"

    debezium_stg.extract_column_alias({'foo': 'bar',}) renders as "foo"
*/ #}
{%- macro extract_column_alias(field) -%}
  {%- if field is string %}{% do return(field) %}{% endif -%}

  {%- if (field | length) != 1 -%}
    {{- exceptions.raise_compiler_error(
      'field dict should contain only one key',
    ) -}}
  {%- endif -%}

  {%- do return((field.keys() | list)[0]) -%}
{%- endmacro -%}


{# /*
  Extracts the field form somehow materialized jsons, sent by debezium, (see
  https://debezium.io/documentation/reference/2.3/connectors/postgresql.html#postgresql-create-events

  i.e.

    {
      {
        "before": null,
        "after": {
          "id": 1,
          "first_name": "Anne",
          "last_name": "Kretchmar",
          "email": "annek@noanswer.org"
      },
      "source": {
        "version": "2.3.2.Final",
        "connector": "postgresql",
        "name": "PostgreSQL_server",
        "ts_ms": 1559033904863,
        "snapshot": true,
        "db": "postgres",
        "sequence": "[\"24023119\",\"24023128\"]",
        "schema": "public",
        "table": "customers",
        "txId": 555,
        "lsn": 24023128,
        "xmin": null
      },
      "op": "c",
      "ts_ms": 1559033904863
    }

  then debezium_stg._build_field_path(
    'last_name',
    'after',
    'src',
    '',
    false,
  )

  renders `src.record_content:after:last_name`

  Arguments:

    - name: field_name
      type: string
      description: The name of the field in JSON

    - name: state
      type: enum('after', 'before', 'source')
      description: The "place", where to find field_name

    - name: table_alias
      type: string
      description: the alias of the source table to query.

    - name: column_alias
      type: string or None
      description: an alias of the result. If None or blank string --
        `field_name` would be taken. All this things takes an effect only if
        `add_alias` is True, otherwise no "AS" part would be added

    - name: add_alias
      type: boolean
      description: If the alias (aka "AS ...") should be added or not

    - name: format_str
      type: string
      description: Format string to render with the field like "{}::STRING".
*/ #}
{%- macro build_field_path(
  field_name,
  state,
  table_alias,
  column_alias,
  add_alias,
  format_str=none
) -%}
  {{- return(adapter.dispatch('build_field_path', 'debezium_stg')(
    field_name=field_name,
    state=state,
    table_alias=table_alias,
    column_alias=column_alias,
    add_alias=add_alias,
    format_str=format_str,
  )) -}}
{%- endmacro -%}


{%- macro default__build_field_path(
  field_name,
  state,
  table_alias,
  column_alias,
  add_alias,
  format_str
) -%}
  {{- exceptions.raize_compiler_error(
    'No default implementation for macro `build_field_path` is defined',
  ) -}}
{%- endmacro -%}


{%- macro snowflake__build_field_path(
  field_name,
  state,
  table_alias,
  column_alias,
  add_alias,
  format_str
) -%}
  {%- if state not in ('after', 'before', 'source') -%}
    {{- exceptions.raize_compiler_error(
      'Unavailable state "' ~ state ~ '". Avaliable states are" "after", '
        ~ '"before", or "source"',
    ) -}}
  {%- endif -%}

  {{- debezium_stg.field_if_format(
    debezium_stg.if_table_alias(
      field='record_content:' ~ state ~ ':' ~ field_name,
      table_alias=table_alias,
    ),
    format_str,
  ) -}}
  {%- if add_alias %}
    AS {{ debezium_stg.field_if_alias(
      field=field_name,
      alias=column_alias,
    ) -}}
  {%- endif -%}
{%- endmacro -%}


{# /*
  Returns format string with platform-specific expression for turning number
  of days from Epoch to timestamp.
*/ #}
{%- macro date_extraction_format() -%}
  {{- return(adapter.dispatch('date_extraction_format', 'debezium_stg')()) -}}
{%- endmacro -%}


{%- macro default__date_extraction_format() -%}
  {{- exceptions.raize_compiler_error(
    'No default implementation for macro `date_extraction_format` is defined',
  ) -}}
{%- endmacro -%}


{%- macro snowflake__date_extraction_format() -%}
  {{- return('TO_TIMESTAMP_NTZ({0}::BIGINT * 86400, 0)') -}}
{%- endmacro -%}


{# /*
  Extracts given field from given part of raw table.

  Arguments:

    - name: field
      type: string or dict[string, string or dict[string boolean]
      description: The field to extract.

    - name: source_table_alias
      type: string
      description: the alias of table to extract from (in case there would be
        multiple candidates).

    - name: column_alias
      type: string or none
      description: an alias for the result. If none -- field value would be
        taken. Has any scense only if `add_alias` argument is true.

    - name: add_alias
      type: boolean
      description: If an alias should be added to the expression. If used in
        another expression, should be false.
*/ #}
{%- macro field(
  field,
  state,
  source_table_alias,
  column_alias,
  add_alias
) -%}
  {%- if field is string -%}
    {%- do return(debezium_stg.build_field_path(
      field_name=field,
      state=state,
      table_alias=source_table_alias,
      column_alias=column_alias,
      add_alias=add_alias,
    )) -%}
  {%- endif -%}
  {%- if field | length != 1 -%}
    {{- exceptions.raise_compiler_error(
      'field dict should contain only one key',
    ) -}}
  {%- endif -%}
  {%- for field_name, field_value in field.items() -%}
    {%- if field_value is string -%}
      {%- do return(debezium_stg.build_field_path(
        field_name=field_name,
        state=state,
        table_alias=source_table_alias,
        column_alias=column_alias,
        add_alias=add_alias,
        format_str=field_value,
      )) -%}
    {%- elif 'is_date' in field_value and field_value.is_date -%}
      {%- do return(debezium_stg.build_field_path(
        field_name=field_name,
        state=state,
        table_alias=source_table_alias,
        column_alias=column_alias,
        add_alias=add_alias,
        format_str=debezium_stg.date_extraction_format(),
      )) -%}
    {%- else -%}
      {{- exceptions.raise_compiler_error('unsupported field format') -}}
    {%- endif -%}
  {%- endfor -%}
{%- endmacro -%}


{# /*
  Extracts given field from "after" part of raw table.

  Arguments:

    - name: field
      type: string or dict[string, string or dict[string boolean]
      description: The field to extract.

    - name: source_table_alias
      type: string
      description: the alias of table to extract from (in case there would be
        multiple candidates).

    - name: column_alias
      type: string or none
      description: an alias for the result. If none -- field value would be
        taken. Has any scense only if `add_alias` argument is true.

    - name: add_alias
      type: boolean
      description: If an alias should be added to the expression. If used in
        another expression, should be false.
*/ #}
{%- macro after_field(
  field,
  source_table_alias='src',
  column_alias=none,
  add_alias=true
) -%}
  {{- debezium_stg.field(
    field=field,
    state='after',
    source_table_alias=source_table_alias,
    column_alias=column_alias,
    add_alias=add_alias,
  ) -}}
{%- endmacro -%}


{# /*
  Extracts given field from "before" part of raw table.

  Arguments:

    - name: field
      type: string or dict[string, string or dict[string boolean]
      description: The field to extract.

    - name: source_table_alias
      type: string
      description: the alias of table to extract from (in case there would be
        multiple candidates).

    - name: column_alias
      type: string or none
      description: an alias for the result. If none -- field value would be
        taken. Has any scense only if `add_alias` argument is true.

    - name: add_alias
      type: boolean
      description: If an alias should be added to the expression. If used in
        another expression, should be false.
*/ #}
{%- macro before_field(
  field,
  source_table_alias='src',
  column_alias=none,
  add_alias=true
) -%}
  {{- debezium_stg.field(
    field=field,
    state='before',
    source_table_alias=source_table_alias,
    column_alias=column_alias,
    add_alias=add_alias,
  ) -}}
{%- endmacro -%}


{# /*
  Extracts given field from "source" part of raw table.

  Arguments:

    - name: field
      type: string or dict[string, string or dict[string boolean]
      description: The field to extract.

    - name: source_table_alias
      type: string
      description: the alias of table to extract from (in case there would be
        multiple candidates).

    - name: column_alias
      type: string or none
      description: an alias for the result. If none -- field value would be
        taken. Has any scense only if `add_alias` argument is true.

    - name: add_alias
      type: boolean
      description: If an alias should be added to the expression. If used in
        another expression, should be false.
*/ #}
{%- macro source_field(
  field,
  source_table_alias='src',
  column_alias=none,
  add_alias=true
) -%}
  {{- debezium_stg.field(
    field=field,
    state='source',
    source_table_alias=source_table_alias,
    column_alias=column_alias,
    add_alias=add_alias,
  ) -}}
{%- endmacro -%}


{# /*
  Generates statement that takes the value either from `before` or form
  `after` fields, depending on the operation type.

  Arguments:

    - name: field
      type: string or dict[string, string or dict[string boolean]
      description: The field to extract.

    - name: source_table_alias
      type: string
      description: the alias of table to extract from (in case there would be
        multiple candidates).

    - name: column_alias
      type: string or none
      description: an alias for the result. If none -- field value would be
        taken. Has any scense only if `add_alias` argument is true.

    - name: add_alias
      type: boolean
      description: If an alias should be added to the expression. If used in
        another expression, should be false.
*/ #}
{%- macro before_or_after(
  field,
  source_table_alias='src',
  column_alias=none,
  add_alias=true
) -%}
  {{- return(adapter.dispatch('before_or_after', 'debezium_stg')(
    field=field,
    source_table_alias=source_table_alias,
    column_alias=column_alias,
    add_alias=add_alias,
  )) -}}
{%- endmacro -%}


{%- macro default__before_or_after(
  field,
  source_table_alias,
  column_alias,
  add_alias
) -%}
  {{- exceptions.raize_compiler_error(
    'No default implementation for macro `before_or_after` is defined',
  ) -}}
{%- endmacro -%}


{%- macro snowflake__before_or_after(
  field,
  source_table_alias,
  column_alias,
  add_alias
) -%}
  iff(
    {{- debezium_stg.a_deletion_event(source_alias=source_table_alias) }},
    {{- debezium_stg.before_field(
      field=field,
      source_table_alias=source_table_alias,
      add_alias=false,
    ) }},
    {{- debezium_stg.after_field(
      field=field,
      source_table_alias=source_table_alias,
      add_alias=false,
    ) -}}
  ) {% if add_alias %}AS {{ debezium_stg.field_if_alias(
    field=debezium_stg.extract_column_alias(field),
    alias=column_alias,
  ) }}{% endif -%}
{%- endmacro -%}


{# /*
    Macro to get only last change withing the primary key fields expression,
    which consists of field aliases, passed as an argument.

    This is a helper macro which is used only in `to_stg_layer` macro
    implementations.
*/ #}
{%- macro _snowflake__qualify_part(field_names, aliases) -%}
    QUALIFY row_number() OVER (
      PARTITION BY {{ field_names | join(', ') }}
      ORDER BY
        {{ aliases['ts'] }} DESC,
        {{ aliases['row_id'] }} DESC
    ) = 1
{%- endmacro -%}


{# /*
  Generates either `COALESCE(source_table_field, model_table_field)` or just
  `source_table_field`.

  Arguments:

    - name: use_coalesce
      type: boolean
      description: Wether `COALESCE` should be generated or just
        `source_table_field`.

    - name: field_name
      type: string
      description: Column name in source table.

    - name: source_table_alias
      type: string
      description: Source table alias.

    - name: dst_table_alias
      type: string
      description: Destination table alias.

    - name: dst_field_alias
      type: string or None
      description: Column name in the description table. It also should be
        used as an alias for the expression. If None or empty string --
        field_name is taken.
*/ #}
{%- macro coalesce_with_dst(
  use_coalesce,
  field_name,
  source_table_alias='src',
  dst_table_alias='dst',
  dst_field_alias=none
) -%}
  {%- set dst_field_alias =
    dst_field_alias if dst_field_aliasi else field_name -%}

  {%- if use_coalesce -%}
    COALESCE(
      {{- debezium_stg.if_table_alias(
        field=field_name,
        table_alias=source_table_alias,
      ) }},
      {{- debezium_stg.if_table_alias(
        field=dst_field_alias,
        table_alias=dst_table_alias,
      ) -}}
    ) AS {{ dst_field_alias -}}
  {%- else -%}
    {{- debezium_stg.if_table_alias(
      field=field_name,
      table_alias=source_table_alias
    ) }} AS {{ dst_field_alias -}}
  {%- endif -%}
{%- endmacro -%}


{# /*
  Ensures source engine is supported

  Arguments:

    - name: source_engine
      type: string
      description: Source engine name to ensure.
*/ #}
{%- macro ensure_source_engine_supported(source_engine) -%}
  {%- set supported_source_engines = ('postgresql',) -%}
  {%- if source_engine not in supported_source_engines -%}
    {{- exceptions.raise_compiler_error(
      'Source engine \'{0}\' is not supported. Supported source engines are {1}'.format(
        source_engine,
        supported_source_engines,
      )
    ) -}}
  {%- endif -%}
{%- endmacro -%}


{# /*
*/ #}
{%- macro lower_bound_cte(aliases) -%}
  {{- return(adapter.dispatch('lower_bound_cte', 'debezium_stg')(
    aliases=aliases,
  )) -}}
{%- endmacro -%}


{%- macro snowflake__lower_bound_cte(aliases) -%}
  lower_bound AS (
    SELECT
      COALESCE(
        MAX({{ aliases['loaded_at'] }}),
        '01/01/0000 00:00:00'::TIMESTAMP_NTZ
      ) AS max_{{ aliases['loaded_at'] }},
      COALESCE(MAX({{ aliases['row_id'] }}), 0) AS max_{{ aliases['row_id'] }}
    FROM {{ this }}
  )
{%- endmacro -%}


{# /*
  Generates multicolumn batch ordering comparison.
  Lat's say, we have columns `loaded_at` and `row_number` in the source table
  `source_table`, which are materialized in some journal of processed entities
  when the entities are processed. Then, we could have somewhere (let's say in
  CTE `processed_entities` max values of the fields (`max_loaded_at` and
  `max_row_number`). Then, to filter out only unprocessed entities we need to
  add predicate like:

    src.loaded_at > processed_entities.loaded_at
    OR (
      src.loaded_at = processed_entities.max_loaded_at
      AND src.row_number > processed_entities.max_row_number
    )

  Usage of such constrations lets us efficiently utilize partitioning
  mechanisms of the warehouse engines to encrease performance and decrease the
  costs.

  Arguments:

    - name: fields
      type: list[string]
      description: List of the fields to compare.

    - name: source_table_alias
      type: string
      description: Alias of the table (view, cte, etc.) from which the new
        batch
        should be taken.

    - name: processed_table_alias
      type: string
      description: Alias of the table (view, cte, etc.) with the maximum vlues
        of batching fields of allready processed entities.
*/ #}
{%- macro batch_ordering_comparison(
  fields,
  source_table_alias='src',
  processed_table_alias='lb'
) -%}
  {%- set conditions = [] -%}
  {%- for field in fields -%}
    {%- set subconditions = [] %}
    {%- for previous_field in fields[:loop.index0] -%}
      {%- do subconditions.append('{0} = {1}'.format(
        debezium_stg.if_table_alias(
          field=previous_field,
          table_alias=source_table_alias,
        ),
        debezium_stg.if_table_alias(
          field='max_{}'.format(previous_field),
          table_alias=processed_table_alias,
        ),
      )) -%}
    {%- endfor -%}
    {%- do subconditions.append('{0} > {1}'.format(
      debezium_stg.if_table_alias(
        field=field,
        table_alias=source_table_alias,
      ),
      debezium_stg.if_table_alias(
        field='max_{}'.format(field),
        table_alias=processed_table_alias,
      ),
    )) -%}
    {%- do conditions.append(' AND '.join(subconditions)) -%}
  {%- endfor -%}
  {{- return('(' ~ ') OR ('.join(conditions) ~ ')') -}}
{%- endmacro -%}

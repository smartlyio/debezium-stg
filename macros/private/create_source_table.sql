{%- macro default__create_source_table(
  schema_name,
  table_name,
  role_name,
  db,
  aliases,
  dry_run
) -%}
  {{- exceptions.raise_compiler_error(
    'No default implementation for macro `create_source_table` is defined',
  ) -}}
{%- endmacro -%}


{%- macro snowflake__create_source_table(
  schema_name,
  table_name,
  role_name,
  db,
  aliases,
  dry_run
) -%}
  {%- set sql -%}
    CREATE TABLE {{ debezium_stg.if_db(db) }}{{ schema_name }}.{{ table_name }} IF NOT EXISTS (
      {{ aliases['row_id'] }} INTEGER AUTOINCREMENT,
      record_content VARIANT,
      record_metadata VARIANT,
      {{ aliases['loaded_at'] }} TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
    );
  {%- endset -%}

  {%- if role_name is not none -%}
    {%- set sql -%}
      {{- sql -}}
      GRANT CREATE PIPE, CREATE STAGE, USAGE ON SCHEMA {{ debezium_stg.if_db(db) -}}
        {{ schema_name }} TO ROLE {{ role_name }};
      GRANT OWNERSHIP ON TABLE {{ debezium_stg.if_db(db) }}{{ schema_name }}.{{ table_name }}
        TO ROLE {{ role_name }};
    {%- endset -%}
  {%- endif -%}

  {%- set yml_schema = {
    'version': 2,
    'sources': [
      {
        'name': 'YOU_SHOULD_FILL_IT_YOURSELF',
        'schema': schema_name,
        'database': db,
        'tables': [
          {
            'name': 'YOU_SHOULD_FILL_IT_YOURSELF',
            'description': 'YOU_SHOULD_FILL_IT_YOURSELF',
            'identifier': table_name,
            'columns': [
              {
                'name': aliases['row_id'],
                'description': 'Autoincremented row id. Suitable for batch ' ~
                  'uploads into ODS layer.',
                'data_type': 'INTEGER',
                'tests': ['not_null, unique',],
              },
              {
                'name': 'record_content',
                'description': 'Data of CDC events.',
                'data_type': 'VARIANT',
                'tests': ['not_null',],
              },
              {
                'name': 'record_metadata',
                'description': 'Data about the Kafka message (like ' ~
                  'partition, partition key, offset, etc).',
                'data_type': 'VARIANT',
                'tests': ['not_null',],
              },
              {
                'name': aliases['loaded_at'],
                'description': 'Timestamp when the record was loaded from ' ~
                  'a snowpipe.',
                'data_type': 'TIMESTAMP_NTZ',
                'tests': ['not_null'],
              },
            ],
          },
        ],
      },
    ],
  } -%}

  {{- log(toyaml(yml_schema)) -}}

  {%- if not dry_run -%}
    {{- run_query(sql) -}}
  {%- else -%}
  {%- endif -%}
{%- endmacro -%}


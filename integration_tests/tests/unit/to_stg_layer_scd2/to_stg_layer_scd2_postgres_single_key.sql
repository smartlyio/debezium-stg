{{ config(
  tags=['unit-test', 'snowflake']
) }}

{%- call dbt_unit_testing.test(
  'to_stg_layer_scd2_postgres_single_key',
  'Properly stage initial data | simple insert',
) -%}
  {%- call dbt_unit_testing.mock_source('fake_source', 'landing_table') -%}
    SELECT
      1 AS __rolling_row_id__,
      -- birthday: "1970-02-03T00:00:00Z"
      parse_json('{
        "after": {
          "id": 1,
          "name": "John",
          "surname": "Gault",
          "birthday": 33
        },
        "before": null,
        "op": "r",
        "source": {
          "connector": "mock-postgresql",
          "db": "mock_db",
          "lsn": 1,
          "name": "mock-integration-mock-service",
          "schema": "public",
          "sequence": "[null,\\"1\\"]",
          "snapshot": "false",
          "table": "mock_table",
          "ts_ms": 1674658578883,
          "txId": 3,
          "version": "1.9.7.Final",
          "xmin": null
        },
        "transaction": null,
        "ts_ms": 1674658578893
      }')::VARIANT AS record_content,
      parse_json('{
        "CreateTime": 1674658579293,
        "key": "Struct{id=1}",
        "offset": 100,
        "partition": 0,
        "schema_id": 3,
        "topic": "mock-integration-mock-service.public.mock_table"
      }')::VARIANT AS record_metadata,
      '2023-01-31T14:29:09.051Z'::TIMESTAMP_NTZ AS __loaded_at__
  {%- endcall -%}
  {%- call dbt_unit_testing.expect() -%}
    SELECT
      1 AS id,
      'John' AS name,
      'Gault' AS surname,
      '1970-02-03T00:00:00.000Z'::TIMESTAMP_NTZ AS birthday,
      '2023-01-25T14:56:18.883Z'::TIMESTAMP_NTZ AS __valid_from__,
      NULL::TIMESTAMP_NTZ AS __valid_to__,
      'c4ca4238a0b923820dcc509a6f75849b' AS __inner_key__,
      1 AS __rolling_row_id__,
      '2023-01-31T14:29:09.051Z'::TIMESTAMP_NTZ AS __loaded_at__
  {%- endcall -%}
{%- endcall -%}

UNION ALL

{%- call dbt_unit_testing.test(
  'to_stg_layer_scd2_postgres_single_key',
  'Properly stage initial data | insert with update',
) -%}
  {%- call dbt_unit_testing.mock_source('fake_source', 'landing_table') -%}
    {%- call snowflake_source_template() -%}
      (
        1,
        -- birthday: "1970-02-05T00:00:00Z"
        '{
          "after": {
            "id": 2,
            "name": "Stephen",
            "surname": "King",
            "birthday": 35
          },
          "before": null,
          "op": "r",
          "source": {
            "connector": "mock-postgresql",
            "db": "mock_db",
            "lsn": 1,
            "name": "mock-integration-mock-service",
            "schema": "public",
            "sequence": "[null,\\"1\\"]",
            "snapshot": "false",
            "table": "mock_table",
            "ts_ms": 1674658578883,
            "txId": 3,
            "version": "1.9.7.Final",
            "xmin": null
          },
          "transaction": null,
          "ts_ms": 1674658578953
        }',
        '{
          "CreateTime": 1674658579293,
          "key": "Struct{id=2}",
          "offset": 101,
          "partition": 0,
          "schema_id": 3,
          "topic": "mock-integration-mock-service.public.mock_table"
        }',
        '2023-01-31T14:29:09.051Z'
      ),
      (
        2,
        -- birthday: "1970-03-17T00:00:00Z"
        '{
          "after": {
            "id": 2,
            "name": "Stanislav",
            "surname": "Lem",
            "birthday": 75
          },
          "before": null,
          "op": "u",
          "source": {
            "connector": "mock-postgresql",
            "db": "mock_db",
            "lsn": 1,
            "name": "mock-integration-mock-service",
            "schema": "public",
            "sequence": "[null,\\"1\\"]",
            "snapshot": "false",
            "table": "mock_table",
            "ts_ms": 1674658589363,
            "txId": 338532261,
            "version": "1.9.7.Final",
            "xmin": null
          },
          "transaction": null,
          "ts_ms": 1674658578953
        }',
        '{
          "CreateTime": 1674658579716,
          "key": "Struct{id=2}",
          "offset": 114,
          "partition": 0,
          "schema_id": 3,
          "topic": "mock-integration-mock-service.public.mock_table"
        }',
        '2023-01-31T14:29:09.051Z'
      )
    {%- endcall -%}
  {%- endcall -%}
  {%- call dbt_unit_testing.expect(options={'input_format': 'csv'}) -%}
    {{ scd2_simple_key_header_csv() }}
    2,'Stephen'  ,'King','1970-02-05T00:00:00.000Z','2023-01-25T14:56:18.883Z','2023-01-25T14:56:29.363Z','c81e728d9d4c2f636f067f89cc14862c',1,'2023-01-31T14:29:09.051Z'
    2,'Stanislav','Lem' ,'1970-03-17T00:00:00.000Z','2023-01-25T14:56:29.363Z',NULL                      ,'c81e728d9d4c2f636f067f89cc14862c',2,'2023-01-31T14:29:09.051Z'
  {%- endcall -%}
{%- endcall -%}

UNION ALL

{%- call dbt_unit_testing.test(
  'to_stg_layer_scd2_postgres_single_key',
  'Properly stage initial data | insert with delete',
) -%}
  {%- call dbt_unit_testing.mock_source('fake_source', 'landing_table') -%}
    {%- call snowflake_source_template() -%}
      (
        1,
        -- birthday: "1970-03-17T00:00:00Z"
        '{
          "after": {
            "id": 3,
            "name": "Claude",
            "surname": "Monet",
            "birthday": 75
          },
          "before": null,
          "op": "r",
          "source": {
            "connector": "mock-postgresql",
            "db": "mock_db",
            "lsn": 1,
            "name": "mock-integration-mock-service",
            "schema": "public",
            "sequence": "[null,\\"1\\"]",
            "snapshot": "false",
            "table": "mock_table",
            "ts_ms": 1674658578883,
            "txId": 3,
            "version": "1.9.7.Final",
            "xmin": null
          },
          "transaction": null,
          "ts_ms": 1674658579053
        }',
        '{
          "CreateTime": 1674658579294,
          "key": "Struct{id=3}",
          "offset": 102,
          "partition": 0,
          "schema_id": 3,
          "topic": "mock-integration-mock-service.public.mock_table"
        }',
        '2023-01-31T14:29:09.051Z'
      ),
      (
        2,
        '{
          "after": null,
          "before": {
            "id": 3,
            "name": "",
            "surname": "",
            "birthday": 0
          },
          "op": "d",
          "source": {
            "connector": "mock-postgresql",
            "db": "mock_db",
            "lsn": 1,
            "name": "mock-integration-mock-service",
            "schema": "public",
            "sequence": "[null,\\"1\\"]",
            "snapshot": "false",
            "table": "mock_table",
            "ts_ms": 1674658589463,
            "txId": 338532361,
            "version": "1.9.7.Final",
            "xmin": null
          },
          "transaction": null,
          "ts_ms": 1674658579053
        }',
        '{
          "CreateTime": 1674658579816,
          "key": "Struct{id=3}",
          "offset": 115,
          "partition": 0,
          "schema_id": 3,
          "topic": "mock-integration-mock-service.public.mock_table"
        }',
        '2023-01-31T14:29:09.051Z'
      ),
      (
        3,
        '{}',
        '{
          "CreateTime": 1674658579816,
          "key": "Struct{id=3}",
          "offset": 116,
          "partition": 0,
          "schema_id": 0,
          "topic": "mock-integration-mock-service.public.mock_table"
        }',
        '2023-01-31T14:29:09.051Z'
      )
    {%- endcall -%}
  {%- endcall -%}
  {%- call dbt_unit_testing.expect() -%}
    SELECT
      3 AS id,
      'Claude' AS name,
      'Monet' AS surname,
      '1970-03-17T00:00:00Z'::TIMESTAMP_NTZ AS birthday,
      '2023-01-25 14:56:18.883Z'::TIMESTAMP_NTZ AS __valid_from__,
      '2023-01-25T14:56:29.463Z'::TIMESTAMP_NTZ AS __valid_to__,
      'eccbc87e4b5ce2fe28308fd9f2a7baf3' AS __inner_key__,
      1 AS __rolling_row_id__,
      '2023-01-31T14:29:09.051Z'::TIMESTAMP_NTZ AS __loaded_at__
  {%- endcall -%}
{%- endcall -%}

UNION ALL

{%- call dbt_unit_testing.test(
  'to_stg_layer_scd2_postgres_single_key',
  'Properly stage initial data | update unknown entity',
) -%}
  {%- call dbt_unit_testing.mock_source('fake_source', 'landing_table') -%}
    SELECT
      1 AS __rolling_row_id__,
      parse_json('{
        "after": {
          "id": 7,
          "name": "Pablo",
          "surname": "Picasso",
          "birthday": 313
        },
        "before": null,
        "op": "u",
        "source": {
          "connector": "mock-postgresql",
          "db": "mock_db",
          "lsn": 37559974761568,
          "name": "mock-integration-mock-service",
          "schema": "public",
          "sequence": "[null,\\"37559974761568\\"]",
          "snapshot": "false",
          "table": "mock_table",
          "ts_ms": 1674658589063,
          "txId": 338532061,
          "version": "1.9.7.Final",
          "xmin": null
        },
        "transaction": null,
        "ts_ms": 1674658579153
      }')::VARIANT AS record_content,
      parse_json('{
        "CreateTime": 1674658579416,
        "key": "Struct{id=7}",
        "offset": 108,
        "partition": 0,
        "schema_id": 3,
        "topic": "mock-integration-mock-service.public.mock_table"
      }')::VARIANT AS record_metadata,
      '2023-01-31T14:29:09.051Z'::TIMESTAMP_NTZ AS __loaded_at__
  {%- endcall -%}
  {%- call dbt_unit_testing.expect() -%}
    SELECT
      7 AS id,
      'Pablo' AS name,
      'Picasso' AS surname,
      '1970-11-10T00:00:00Z'::TIMESTAMP_NTZ AS birthday,
      '2023-01-25 14:56:29.063Z'::TIMESTAMP_NTZ AS __valid_from__,
      NULL::TIMESTAMP_NTZ AS __valid_to__,
      '8f14e45fceea167a5a36dedd4bea2543' AS __inner_key__,
      1 AS __rolling_row_id__,
      '2023-01-31T14:29:09.051Z'::TIMESTAMP_NTZ AS __loaded_at__
  {%- endcall -%}
{%- endcall -%}

UNION ALL

{%- call dbt_unit_testing.test(
  'to_stg_layer_scd2_postgres_single_key',
  'Properly stage initial data | delete unknown entity',
) -%}
  {%- call dbt_unit_testing.mock_source('fake_source', 'landing_table') -%}
    {%- call snowflake_source_template() -%}
      (
        1,
        '{
          "after": null,
          "before": {
            "id": 8,
            "name": "",
            "surname": "",
            "birthday": 0
          },
          "op": "d",
          "source": {
            "connector": "mock-postgresql",
            "db": "mock_db",
            "lsn": 37559974761568,
            "name": "mock-integration-mock-service",
            "schema": "public",
            "sequence": "[null,\\"37559974761568\\"]",
            "snapshot": "false",
            "table": "mock_table",
            "ts_ms": 1674658589063,
            "txId": 338532061,
            "version": "1.9.7.Final",
            "xmin": null
          },
          "transaction": null,
          "ts_ms": 1674658579153
        }',
        '{
          "CreateTime": 1674658579516,
          "key": "Struct{id=8}",
          "offset": 109,
          "partition": 0,
          "schema_id": 3,
          "topic": "mock-integration-mock-service.public.mock_table"
        }',
        '2023-01-31T14:29:09.051Z'
      ),
      (
        2,
        '{}',
        '{
          "CreateTime": 1674658579516,
          "key": "Struct{id=8}",
          "offset": 110,
          "partition": 0,
          "schema_id": 0,
          "topic": "mock-integration-mock-service.public.mock_table"
        }',
        '2023-01-31T14:29:09.051Z'
      )
    {%- endcall -%}
  {%- endcall -%}
  {%- call dbt_unit_testing.expect() -%}
    {# /* selecting empty set */ #}
    SELECT * FROM (
      SELECT
        7 AS id,
        'Pablo' AS name,
        'Picasso' AS surname,
        '1970-11-10T00:00:00Z'::TIMESTAMP_NTZ AS birthday,
        '2023-01-25 14:56:29.063Z'::TIMESTAMP_NTZ AS __valid_from__,
        NULL::TIMESTAMP_NTZ AS __valid_to__,
        '8f14e45fceea167a5a36dedd4bea2543' AS __inner_key__,
        1 AS __rolling_row_id__,
        '2023-01-31T14:29:09.051Z'::TIMESTAMP_NTZ AS __loaded_at__
    ) t WHERE id <> 7
  {%- endcall -%}
{%- endcall -%}

UNION ALL

{%- call dbt_unit_testing.test(
  'to_stg_layer_scd2_postgres_single_key',
  'Properly stage initial data | restore deleted entity'
) -%}
  {%- call dbt_unit_testing.mock_source('fake_source', 'landing_table') -%}
    {%- call snowflake_source_template() -%}
      (
        1,
        '{
          "after": null,
          "before": {
            "id": 13,
            "name": "",
            "surname": "",
            "birthday": 0
          },
          "op": "d",
          "source": {
            "connector": "mock-postgresql",
            "db": "mock_db",
            "lsn": 37559974761568,
            "name": "mock-integration-mock-service",
            "schema": "public",
            "sequence": "[null,\\"37559974761568\\"]",
            "snapshot": "false",
            "table": "mock_table",
            "ts_ms": 1674658589163,
            "txId": 338532061,
            "version": "1.9.7.Final",
            "xmin": null
          },
          "transaction": null,
          "ts_ms": 1674658579153
        }',
        '{
          "CreateTime": 1674658579516,
          "key": "Struct{id=13}",
          "offset": 111,
          "partition": 0,
          "schema_id": 3,
          "topic": "mock-integration-mock-service.public.mock_table"
        }',
        '2023-01-31T14:29:09.051Z'
      ),
      (
        2,
        '{}',
        '{
          "CreateTime": 1674658579516,
          "key": "Struct{id=13}",
          "offset": 112,
          "partition": 0,
          "schema_id": 3,
          "topic": "mock-integration-mock-service.public.mock_table"
        }',
        '2023-01-31T14:29:09.051Z'
      ),
      (
        3,
        '{
          "after": {
            "id": 13,
            "name": "Rene",
            "surname": "Magritte",
            "birthday": 123
          },
          "before": null,
          "op": "c",
          "source": {
            "connector": "mock-postgresql",
            "db": "mock_db",
            "lsn": 37559974761568,
            "name": "mock-integration-mock-service",
            "schema": "public",
            "sequence": "[null,\\"37559974761568\\"]",
            "snapshot": "false",
            "table": "mock_table",
            "ts_ms": 1674658589263,
            "txId": 338532161,
            "version": "1.9.7.Final",
            "xmin": null
          },
          "transaction": null,
          "ts_ms": 1674658579153
        }',
        '{
          "CreateTime": 1674658579616,
          "key": "Struct{id=13}",
          "offset": 113,
          "partition": 0,
          "schema_id": 0,
          "topic": "mock-integration-mock-service.public.mock_table"
        }',
        '2023-01-31T14:29:09.051Z'
      )
    {%- endcall -%}
  {%- endcall -%}
  {%- call dbt_unit_testing.expect() -%}
    SELECT
      13 AS id,
      'Rene' AS name,
      'Magritte' AS surname,
      '1970-05-04T00:00:00Z'::TIMESTAMP_NTZ AS birthday,
      '2023-01-25 14:56:29.263Z'::TIMESTAMP_NTZ AS __valid_from__,
      NULL::TIMESTAMP_NTZ AS __valid_to__,
      'c51ce410c124a10e0db5e4b97fc2af39' AS __inner_key__,
      3 AS __rolling_row_id__,
      '2023-01-31T14:29:09.051Z'::TIMESTAMP_NTZ AS __loaded_at__
  {%- endcall -%}
{%- endcall -%}

UNION ALL

-- INCREMENTAL CASES

{%- call dbt_unit_testing.test(
  'to_stg_layer_scd2_postgres_single_key',
  'Properly stage incremental data | simple insert',
  options={"run_as_incremental": "True"},
) -%}
  {%- call dbt_unit_testing.mock_source('fake_source', 'landing_table') -%}
    {%- call snowflake_source_template() -%}
      {{ postgres_single_key_no_before_values_processed_entities_source() }},
      (
        13,
        '{
          "after": {
            "id": 14,
            "name": "Keith",
            "surname": "Haring",
            "birthday": 11543
          },
          "before": null,
          "op": "u",
          "source": {
            "connector": "mock-postgresql",
            "db": "mock_db",
            "lsn": 37559974761568,
            "name": "mock-integration-mock-service",
            "schema": "public",
            "sequence": "[null,\\"37559974761568\\"]",
            "snapshot": "false",
            "table": "mock_table",
            "ts_ms": 1675427348296,
            "txId": 338531961,
            "version": "1.9.7.Final",
            "xmin": null
          },
          "transaction": null,
          "ts_ms": 1675427348396
        }',
        '{
          "CreateTime": 1675427349096,
          "key": "Struct{id=14}",
          "offset": 126,
          "partition": 0,
          "schema_id": 3,
          "topic": "mock-integration-mock-service.public.mock_table"
        }',
        '2023-02-03T14:29:09.051Z'
      )
    {%- endcall -%}
  {%- endcall -%}
  {%- call dbt_unit_testing.mock_ref(
    'to_stg_layer_scd2_postgres_single_key',
    options={'input_format': 'csv'},
  ) -%}
    {{ scd2_simple_key_processed_entities_model_csv() }}
  {%- endcall -%}
  {%- call dbt_unit_testing.expect() %}
    SELECT
      14 AS id,
      'Keith' AS name,
      'Haring' AS surname,
      '2001-08-09T00:00:00.000Z'::TIMESTAMP_NTZ AS birthday,
      '2023-02-03T12:29:08.296Z'::TIMESTAMP_NTZ AS __valid_from__,
      NULL::TIMESTAMP_NTZ AS __valid_to__,
      'aab3238922bcc25a6f606eb525ffdc56' AS __inner_key__,
      13 AS __rolling_row_id__,
      '2023-02-03T14:29:09.051Z'::TIMESTAMP_NTZ AS __loaded_at__
  {% endcall -%}
{%- endcall -%}

UNION ALL

{%- call dbt_unit_testing.test(
  'to_stg_layer_scd2_postgres_single_key',
  'Properly stage incremental data | update existing entity',
  options={"run_as_incremental": "True"},
) -%}
  {%- call dbt_unit_testing.mock_source('fake_source', 'landing_table') -%}
    {%- call snowflake_source_template() -%}
      {{ postgres_single_key_no_before_values_processed_entities_source() }},
      (
        13,
        '{
          "after": {
            "id": 5,
            "name": "Gustav",
            "surname": "Klimt",
            "birthday": 11554
          },
          "before": null,
          "op": "u",
          "source": {
            "connector": "mock-postgresql",
            "db": "mock_db",
            "lsn": 37559974761568,
            "name": "mock-integration-mock-service",
            "schema": "public",
            "sequence": "[null,\\"37559974761568\\"]",
            "snapshot": "false",
            "table": "mock_table",
            "ts_ms": 1675427348096,
            "txId": 338531961,
            "version": "1.9.7.Final",
            "xmin": null
          },
          "transaction": null,
          "ts_ms": 1675427348196
        }',
        '{
          "CreateTime": 1675427349096,
          "key": "Struct{id=5}",
          "offset": 117,
          "partition": 0,
          "schema_id": 3,
          "topic": "mock-integration-mock-service.public.mock_table"
        }',
        '2023-02-03T14:29:09.051Z'
      )
    {%- endcall -%}
  {%- endcall -%}
  {%- call dbt_unit_testing.mock_ref(
    'to_stg_layer_scd2_postgres_single_key',
    options={'input_format': 'csv'},
  ) -%}
    {{ scd2_simple_key_processed_entities_model_csv() }}
  {%- endcall -%}
  {%- call dbt_unit_testing.expect(options={'input_format': 'csv'}) %}
    {{ scd2_simple_key_header_csv() }}
      5 ,'Gustav','Klimt','2001-07-30T00:00:00.000Z','2023-01-25T14:56:19.883Z','2023-02-03T12:29:08.096Z','e4da3b7fbbce2345d7772b0674a318d5',3 ,'2023-01-31T14:29:09.051Z'
      5 ,'Gustav','Klimt','2001-08-20T00:00:00.000Z','2023-02-03T12:29:08.096Z',NULL                      ,'e4da3b7fbbce2345d7772b0674a318d5',13,'2023-02-03T14:29:09.051Z'
  {% endcall -%}
{%- endcall -%}

UNION ALL

{%- call dbt_unit_testing.test(
  'to_stg_layer_scd2_postgres_single_key',
  'Properly stage incremental data | delete existing entity',
  options={"run_as_incremental": "True"},
) -%}
  {%- call dbt_unit_testing.mock_source('fake_source', 'landing_table') -%}
    {%- call snowflake_source_template() -%}
      {{ postgres_single_key_no_before_values_processed_entities_source() }},
      (
        13,
        '{
          "after": null,
          "before": {
            "id": 7,
            "name": "",
            "surname": "",
            "birthday": 0
          },
          "op": "d",
          "source": {
            "connector": "mock-postgresql",
            "db": "mock_db",
            "lsn": 1,
            "name": "mock-integration-mock-service",
            "schema": "public",
            "sequence": "[null,\\"1\\"]",
            "snapshot": "false",
            "table": "mock_table",
            "ts_ms": 1675427348396,
            "txId": 338532361,
            "version": "1.9.7.Final",
            "xmin": null
          },
          "transaction": null,
          "ts_ms": 1675427348496
        }',
        '{
          "CreateTime": 1675427349096,
          "key": "Struct{id=7}",
          "offset": 120,
          "partition": 0,
          "schema_id": 3,
          "topic": "mock-integration-mock-service.public.mock_table"
        }',
        '2023-02-03T14:29:09.051Z'
      ),
      (
        14,
        '{}',
        '{
          "CreateTime": 1675427349096,
          "key": "Struct{id=7}",
          "offset": 121,
          "partition": 0,
          "schema_id": 0,
          "topic": "mock-integration-mock-service.public.mock_table"
        }',
        '2023-02-03T14:29:09.051Z'
      )
    {%- endcall -%}
  {%- endcall -%}
  {%- call dbt_unit_testing.mock_ref(
    'to_stg_layer_scd2_postgres_single_key',
    options={'input_format': 'csv'},
  ) -%}
    {{ scd2_simple_key_processed_entities_model_csv() }}
  {%- endcall -%}
  {%- call dbt_unit_testing.expect() %}
    SELECT
      7 AS id,
      'Pablo' AS name,
      'Picasso' AS surname,
      '2001-11-10T00:00:00.000Z'::TIMESTAMP_NTZ AS birthday,
      '2023-01-25T14:56:29.063Z'::TIMESTAMP_NTZ AS __valid_from__,
      '2023-02-03T12:29:08.396Z'::TIMESTAMP_NTZ AS __valid_to__,
      '8f14e45fceea167a5a36dedd4bea2543' AS __inner_key__,
      4 AS __rolling_row_id__,
      '2023-01-31T14:29:09.051Z'::TIMESTAMP_NTZ AS __loaded_at__
  {%- endcall -%}
{%- endcall -%}

UNION ALL

{%- call dbt_unit_testing.test(
  'to_stg_layer_scd2_postgres_single_key',
  'Properly stage incremental data | restore deleted entity',
  options={"run_as_incremental": "True"}
) -%}
  {%- call dbt_unit_testing.mock_source('fake_source', 'landing_table') -%}
    {%- call snowflake_source_template() -%}
      {{ postgres_single_key_no_before_values_processed_entities_source() }},
      (
        13,
        '{
          "after": {
            "id": 9,
            "name": "Gustav",
            "surname": "Klimt",
            "birthday": 11543
          },
          "before": null,
          "op": "c",
          "source": {
            "connector": "mock-postgresql",
            "db": "mock_db",
            "lsn": 37559974761568,
            "name": "mock-integration-mock-service",
            "schema": "public",
            "sequence": "[null,\\"37559974761568\\"]",
            "snapshot": "false",
            "table": "mock_table",
            "ts_ms": 1675427348196,
            "txId": 338531961,
            "version": "1.9.7.Final",
            "xmin": null
          },
          "transaction": null,
          "ts_ms": 1675427348296
        }',
        '{
          "CreateTime": 1675427349096,
          "key": "Struct{id=9}",
          "offset": 118,
          "partition": 0,
          "schema_id": 3,
          "topic": "mock-integration-mock-service.public.mock_table"
        }',
        '2023-02-03T14:29:09.051Z'
      )
    {%- endcall -%}
  {%- endcall -%}
  {%- call dbt_unit_testing.mock_ref(
    'to_stg_layer_scd2_postgres_single_key',
    options={'input_format': 'csv'},
  ) -%}
    {{ scd2_simple_key_processed_entities_model_csv() }}
  {%- endcall -%}
  {%- call dbt_unit_testing.expect() %}
    SELECT
      9 AS id,
      'Gustav' AS name,
      'Klimt' AS surname,
      '2001-08-09T00:00:00.000Z'::TIMESTAMP_NTZ AS birthday,
      '2023-02-03T12:29:08.196Z'::TIMESTAMP_NTZ AS __valid_from__,
      NULL::TIMESTAMP_NTZ AS __valid_to__,
      '45c48cce2e2d7fbdea1afc51c7c6ad26' AS __inner_key__,
      13 AS __rolling_row_id__,
      '2023-02-03T14:29:09.051Z'::TIMESTAMP_NTZ AS __loaded_at__
  {%- endcall -%}
{%- endcall -%}

{%- macro snowflake_source_template() -%}
SELECT
  __rolling_row_id__::NUMBER AS __rolling_row_id__,
  parse_json(record_content)::VARIANT AS record_content,
  parse_json(record_metadata)::VARIANT AS record_metadata,
  __loaded_at__::TIMESTAMP_NTZ AS __loaded_at__
FROM (
  VALUES
    {{ caller() }}
) src (__rolling_row_id__, record_content, record_metadata, __loaded_at__)
{%- endmacro -%}


{%- macro postgres_single_key_no_before_values_processed_entities_source() -%}
  (
    1,
    -- birthday: "2001-07-30T00:00:00.000Z"
    '{
      "after": {
      "id": 4,
      "name": "Vincent",
      "surname": "van Gogh",
      "birthday": 11533
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
        "ts_ms": 1674658578883,
        "txid": 338531960,
        "version": "1.9.7.final",
        "xmin": null
      },
      "transaction": null,
      "ts_ms": 1674658579053
    }',
    '{
      "createtime": 1674658579394,
      "key": "Struct{id=4}",
      "offset": 103,
      "partition": 0,
      "schema_id": 3,
      "topic": "mock-integration-mock-service.public.mock_table"
    }',
    '2023-01-31T14:29:09.051Z'
  ),
  -- next two entities are evolution of the same one.
  (
    2,
    -- birthday: "2001-07-30T00:00:00.000Z"
    '{
      "after": {
        "id": 5,
        "name": "Pierre-Auguste",
        "surname": "Renoir",
        "birthday": 11533
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
        "ts_ms": 1674658578883,
        "txid": 338531960,
        "version": "1.9.7.final",
        "xmin": null
      },
      "transaction": null,
      "ts_ms": 1674658579153
    }',
    '{
      "createtime": 1674658579395,
      "key": "Struct{id=5}",
      "offset": 104,
      "partition": 0,
      "schema_id": 3,
      "topic": "mock-integration-mock-service.public.mock_table"
    }',
    '2023-01-31T14:29:09.051Z'
  ),
  (
    3,
    -- birthday: "2001-07-30T00:00:00.000Z"
    '{
      "after": {
        "id": 5,
        "name": "Gustav",
        "surname": "Klimt",
        "birthday": 11533
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
        "ts_ms": 1674658588983,
        "txid": 338531961,
        "version": "1.9.7.final",
        "xmin": null
      },
      "transaction": null,
      "ts_ms": 1674658579153
    }',
    '{
      "createtime": 1674658579396,
      "key": "Struct{id=5}",
      "offset": 105,
      "partition": 0,
      "schema_id": 3,
      "topic": "mock-integration-mock-service.public.mock_table"
    }',
    '2023-01-31T14:29:09.051Z'
  ),
  -- update "unknown" entity
  (
    4,
    -- birthday: "2001-11-10T00:00:00.000Z"
    '{
      "after": {
        "id": 7,
        "name": "Pablo",
        "surname": "Picasso",
        "birthday": 11636
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
        "txid": 338532061,
        "version": "1.9.7.final",
        "xmin": null
      },
      "transaction": null,
      "ts_ms": 1674658579153
    }',
    '{
      "createtime": 1674658579416,
      "key": "Struct{id=7}",
      "offset": 108,
      "partition": 0,
      "schema_id": 3,
      "topic": "mock-integration-mock-service.public.mock_table"
    }',
    '2023-01-31T14:29:09.051Z'
  ),
  -- delete "unknown" entity
  (
    5,
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
        "txid": 338532061,
        "version": "1.9.7.final",
        "xmin": null
      },
      "transaction": null,
      "ts_ms": 1674658579153
    }',
    '{
      "createtime": 1674658579516,
      "key": "Struct{id=8}",
      "offset": 119,
      "partition": 0,
      "schema_id": 3,
      "topic": "mock-integration-mock-service.public.mock_table"
    }',
    '2023-01-31T14:29:09.051Z'
  ),
  -- some anomaly, appeared after deletion events
  (
    6,
    '{}',
    '{
      "createtime": 1674658579516,
      "key": "Struct{id=8}",
      "offset": 110,
      "partition": 0,
      "schema_id": 0,
      "topic": "mock-integration-mock-service.public.mock_table"
    }',
    '2023-01-31T14:29:09.051Z'
  ),
  -- next two entities are creation and deletion of the same entity.
  (
    7,
    -- birthday: "2001-05-04T00:00:00.000Z"
    '{
      "after": {
        "id": 9,
        "name": "Rene",
        "surname": "Magritte",
        "birthday": 11446
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
        "ts_ms": 1674658589163,
        "txid": 338532061,
        "version": "1.9.7.final",
        "xmin": null
      },
      "transaction": null,
      "ts_ms": 1674658579153
    }',
    '{
      "createtime": 1674658579516,
      "key": "Struct{id=9}",
      "offset": 111,
      "partition": 0,
      "schema_id": 3,
      "topic": "mock-integration-mock-service.public.mock_table"
    }',
    '2023-01-31T14:29:09.051Z'
  ),
  (
    8,
    '{
      "after": null,
      "before": {
        "id": 9,
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
        "ts_ms": 1674658589263,
        "txid": 338532161,
        "version": "1.9.7.final",
        "xmin": null
      },
      "transaction": null,
      "ts_ms": 1674658579153
    }',
    '{
      "createtime": 1674658579616,
      "key": "Struct{id=9}",
      "offset": 112,
      "partition": 0,
      "schema_id": 3,
      "topic": "mock-integration-mock-service.public.mock_table"
    }',
    '2023-01-31T14:29:09.051Z'
  ),
  -- some anomaly, appeared after deletion events
  (
    9,
    '{}',
    '{
      "createtime": 1674658579616,
      "key": "Struct{id=9}",
      "offset": 113,
      "partition": 0,
      "schema_id": 0,
      "topic": "mock-integration-mock-service.public.mock_table"
    }',
    '2023-01-31T14:29:09.051Z'
  ),
  -- deletion and restoration.
  (
    10,
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
        "txid": 338532061,
        "version": "1.9.7.final",
        "xmin": null
      },
      "transaction": null,
      "ts_ms": 1674658579153
    }',
    '{
      "createtime": 1674658579516,
      "key": "Struct{id=13}",
      "offset": 111,
      "partition": 0,
      "schema_id": 3,
      "topic": "mock-integration-mock-service.public.mock_table"
    }',
    '2023-01-31T14:29:09.051Z'
  ),
  (
    11,
    '{}',
    '{
      "createtime": 1674658579516,
      "key": "Struct{id=13}",
      "offset": 112,
      "partition": 0,
      "schema_id": 3,
      "topic": "mock-integration-mock-service.public.mock_table"
    }',
    '2023-01-31T14:29:09.051Z'
  ),
  (
    12,
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
        "txid": 338532161,
        "version": "1.9.7.final",
        "xmin": null
      },
      "transaction": null,
      "ts_ms": 1674658579153
    }',
    '{
      "createtime": 1674658579616,
      "key": "Struct{id=13}",
      "offset": 113,
      "partition": 0,
      "schema_id": 0,
      "topic": "mock-integration-mock-service.public.mock_table"
    }',
    '2023-01-31T14:29:09.051Z'
  )
{%- endmacro -%}


{%- macro postgres_single_key_with_before_values_processed_entities_source() -%}
  (
    1,
    -- birthday: "2001-07-30T00:00:00.000Z"
    '{
      "after": {
        "id": 4,
        "name": "Vincent",
        "surname": "van Gogh",
        "birthday": 11533
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
        "ts_ms": 1674658578883,
        "txid": 338531960,
        "version": "1.9.7.final",
        "xmin": null
      },
      "transaction": null,
      "ts_ms": 1674658579053
    }',
    '{
      "createtime": 1674658579394,
      "key": "Struct{id=4}",
      "offset": 103,
      "partition": 0,
      "schema_id": 3,
      "topic": "mock-integration-mock-service.public.mock_table"
    }',
    '2023-01-31T14:29:09.051Z'
  ),
  -- next two entities are evolution of the same one.
  (
    2,
    -- birthday: "2001-07-30T00:00:00.000Z"
    '{
      "after": {
        "id": 5,
        "name": "Pierre-Auguste",
        "surname": "Renoir",
        "birthday": 11533
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
        "ts_ms": 1674658578883,
        "txid": 338531960,
        "version": "1.9.7.final",
        "xmin": null
      },
      "transaction": null,
      "ts_ms": 1674658579153
    }',
    '{
      "createtime": 1674658579395,
      "key": "Struct{id=5}",
      "offset": 104,
      "partition": 0,
      "schema_id": 3,
      "topic": "mock-integration-mock-service.public.mock_table"
    }',
    '2023-01-31T14:29:09.051Z'
  ),
  (
    3,
    -- birthday: "2001-07-30T00:00:00.000Z"
    '{
      "after": {
        "id": 5,
        "name": "Gustav",
        "surname": "Klimt",
        "birthday": 11533
      },
      "before": {
        "id": 5,
        "name": "Pierre-Auguste",
        "surname": "Renoir",
        "birthday": 11533
      },
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
        "ts_ms": 1674658588983,
        "txid": 338531961,
        "version": "1.9.7.final",
        "xmin": null
      },
      "transaction": null,
      "ts_ms": 1674658579153
    }',
    '{
      "createtime": 1674658579396,
      "key": "Struct{id=5}",
      "offset": 105,
      "partition": 0,
      "schema_id": 3,
      "topic": "mock-integration-mock-service.public.mock_table"
    }',
    '2023-01-31T14:29:09.051Z'
  ),
  -- update "unknown" entity
  (
    4,
    -- birthday: "2001-11-10t00:00"
    '{
      "after": {
        "id": 7,
        "name": "Pablo",
        "surname": "Picasso",
        "birthday": 11636
      },
      "before": {
        "id": 7,
        "name": "Joan",
        "surname": "Miro",
        "birthday": 313
      },
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
        "txid": 338532061,
        "version": "1.9.7.final",
        "xmin": null
      },
      "transaction": null,
      "ts_ms": 1674658579153
    }',
    '{
      "createtime": 1674658579416,
      "key": "Struct{id=7}",
      "offset": 108,
      "partition": 0,
      "schema_id": 3,
      "topic": "mock-integration-mock-service.public.mock_table"
    }',
    '2023-01-31T14:29:09.051Z'
  ),
  -- delete "unknown" entity
  (
    5,
    '{
      "after": null,
      "before": {
        "id": 8,
        "name": "Diego",
        "surname": "Velazquez",
        "birthday": 626
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
        "txid": 338532061,
        "version": "1.9.7.final",
        "xmin": null
      },
      "transaction": null,
      "ts_ms": 1674658579153
    }',
    '{
      "createtime": 1674658579516,
      "key": "Struct{id=8}",
      "offset": 109,
      "partition": 0,
      "schema_id": 3,
      "topic": "mock-integration-mock-service.public.mock_table"
    }',
    '2023-01-31T14:29:09.051Z'
  ),
  -- some anomaly, appeared after deletion events
  (
    6,
    '{}',
    '{
      "createtime": 1674658579516,
      "key": "Struct{id=8}",
      "offset": 110,
      "partition": 0,
      "schema_id": 0,
      "topic": "mock-integration-mock-service.public.mock_table"
    }',
    '2023-01-31T14:29:09.051Z'
  ),
  -- next two entities are creation and deletion of the same entity.
  (
    7,
    -- birthday: "2001-05-04T00:00:00.000Z"
    '{
      "after": {
        "id": 9,
        "name": "Rene",
        "surname": "Magritte",
        "birthday": 11446
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
        "ts_ms": 1674658589163,
        "txid": 338532061,
        "version": "1.9.7.final",
        "xmin": null
      },
      "transaction": null,
      "ts_ms": 1674658579153
    }',
    '{
      "createtime": 1674658579516,
      "key": "Struct{id=9}",
      "offset": 111,
      "partition": 0,
      "schema_id": 3,
      "topic": "mock-integration-mock-service.public.mock_table"
    }',
    '2023-01-31T14:29:09.051Z'
  ),
  (
    8,
    '{
      "after": null,
      "before": {
        "id": 9,
        "name": "Rene",
        "surname": "Magritte",
        "birthday": 11446
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
        "ts_ms": 1674658589263,
        "txid": 338532161,
        "version": "1.9.7.final",
        "xmin": null
      },
      "transaction": null,
      "ts_ms": 1674658579153
    }',
    '{
      "createtime": 1674658579616,
      "key": "Struct{id=9}",
      "offset": 112,
      "partition": 0,
      "schema_id": 3,
      "topic": "mock-integration-mock-service.public.mock_table"
    }',
    '2023-01-31T14:29:09.051Z'
  ),
  -- some anomaly, appeared after deletion events
  (
    9,
    '{}',
    '{
      "createtime": 1674658579616,
      "key": "Struct{id=9}",
      "offset": 113,
      "partition": 0,
      "schema_id": 0,
      "topic": "mock-integration-mock-service.public.mock_table"
    }',
    '2023-01-31T14:29:09.051Z'
  ),
  -- deletion and restoration.
  (
    10,
    '{
      "after": null,
      "before": {
        "id": 13,
        "name": "Stephen",
        "surname": "King",
        "birthday": 35
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
        "txid": 338532061,
        "version": "1.9.7.final",
        "xmin": null
      },
      "transaction": null,
      "ts_ms": 1674658579153
    }',
    '{
      "createtime": 1674658579516,
      "key": "Struct{id=13}",
      "offset": 111,
      "partition": 0,
      "schema_id": 3,
      "topic": "mock-integration-mock-service.public.mock_table"
    }',
    '2023-01-31T14:29:09.051Z'
  ),
  (
    11,
    '{}',
    '{
      "createtime": 1674658579516,
      "key": "Struct{id=13}",
      "offset": 112,
      "partition": 0,
      "schema_id": 3,
      "topic": "mock-integration-mock-service.public.mock_table"
    }',
    '2023-01-31T14:29:09.051Z'
  ),
  (
    12,
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
        "txid": 338532161,
        "version": "1.9.7.final",
        "xmin": null
      },
      "transaction": null,
      "ts_ms": 1674658579153
    }',
    '{
      "createtime": 1674658579616,
      "key": "Struct{id=13}",
      "offset": 113,
      "partition": 0,
      "schema_id": 0,
      "topic": "mock-integration-mock-service.public.mock_table"
    }',
    '2023-01-31T14:29:09.051Z'
  )
{%- endmacro -%}


{%- macro postgres_single_key_no_before_values_processed_entities_model_csv() -%}
id::integer,name::string,surname::string,birthday::timestamp_ntz,is_deleted::boolean,deleted_at::timestamp_ntz,__rolling_row_id__::integer,__loaded_at__::timestamp_ntz
4 ,'Vincent','van Gogh','2001-07-30T00:00:00.000Z',false,null                      ,1 ,'2023-01-31T14:29:09.051Z'
5 ,'Gustav' ,'Klimt'   ,'2001-07-30T00:00:00.000Z',false,null                      ,3 ,'2023-01-31T14:29:09.051Z'
7 ,'Pablo'  ,'Picasso' ,'2001-11-10T00:00:00.000Z',false,null                      ,4 ,'2023-01-31T14:29:09.051Z'
8 ,null     ,null      ,null                      ,true ,'2023-01-25T14:56:29.063Z',5 ,'2023-01-31T14:29:09.051Z'
9 ,'Rene'   ,'Magritte','2001-05-04T00:00:00.000Z',true ,'2023-01-25T14:56:29.063Z',8 ,'2023-01-31T14:29:09.051Z'
13,'Rene'   ,'Magritte','1970-05-04T00:00:00.000Z',false,null                      ,12,'2023-01-31T14:29:09.051Z'
{%- endmacro -%}


{%- macro postgres_single_key_with_before_values_processed_entities_model_csv() -%}
id::integer,name::string,surname::string,birthday::timestamp_ntz,is_deleted::boolean,deleted_at::timestamp_ntz,__rolling_row_id__::integer,__loaded_at__::timestamp_ntz
4 ,'Vincent','van Gogh' ,'2001-07-30T00:00:00.000Z',false,null                      ,1 ,'2023-01-31T14:29:09.051Z'
5 ,'Gustav' ,'Klimt'    ,'2001-07-30T00:00:00.000Z',false,null                      ,3 ,'2023-01-31T14:29:09.051Z'
7 ,'Pablo'  ,'Picasso'  ,'2001-11-10T00:00:00.000Z',false,null                      ,4 ,'2023-01-31T14:29:09.051Z'
8 ,'Diego'  ,'Velazquez','1971-09-19T00:00:00.000Z',true ,'2023-01-25T14:56:29.063Z',5 ,'2023-01-31T14:29:09.051Z'
9 ,'Rene'   ,'Magritte' ,'2001-05-04t00:00:00.000Z',true ,'2023-01-25T14:56:29.063Z',8 ,'2023-01-31T14:29:09.051Z'
13,'Rene'   ,'Magritte' ,'1970-05-04t00:00:00.000Z',false,null                      ,12,'2023-01-31T14:29:09.051Z'
{%- endmacro -%}


{%- macro postgres_compound_key_no_before_values_processed_entities_source() -%}
  (
    1,
    -- birthday: "2001-07-30T00:00:00.000Z"
    '{
      "after": {
        "maj_id": 4,
        "min_id": "04e1594b",
        "name": "Vincent",
        "surname": "van Gogh",
        "birthday": 11533
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
        "ts_ms": 1674658578883,
        "txid": 338531960,
        "version": "1.9.7.final",
        "xmin": null
      },
      "transaction": null,
      "ts_ms": 1674658579053
    }',
    '{
      "createtime": 1674658579394,
      "key": "Struct{id=4,min_id=04e1594b}",
      "offset": 103,
      "partition": 0,
      "schema_id": 3,
      "topic": "mock-integration-mock-service.public.mock_table"
    }',
    '2023-01-31T14:29:09.051Z'
  ),
  -- next two entities are evolution of the same one.
  (
    2,
    -- birthday: "2001-07-30T00:00:00.000Z"
    '{
      "after": {
        "maj_id": 5,
        "min_id": "7b09b7c7",
        "name": "Pierre-Auguste",
        "surname": "Renoir",
        "birthday": 11533
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
        "ts_ms": 1674658578883,
        "txid": 338531960,
        "version": "1.9.7.final",
        "xmin": null
      },
      "transaction": null,
      "ts_ms": 1674658579153
    }',
    '{
      "createtime": 1674658579395,
      "key": "Struct{maj_id=5,min_id=7b09b7c7}",
      "offset": 104,
      "partition": 0,
      "schema_id": 3,
      "topic": "mock-integration-mock-service.public.mock_table"
    }',
    '2023-01-31T14:29:09.051Z'
  ),
  (
    3,
    -- birthday: "2001-07-30T00:00:00.000Z"
    '{
      "after": {
        "maj_id": 5,
        "min_id": "7b09b7c7",
        "name": "Gustav",
        "surname": "Klimt",
        "birthday": 11533
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
        "ts_ms": 1674658588983,
        "txid": 338531961,
        "version": "1.9.7.final",
        "xmin": null
      },
      "transaction": null,
      "ts_ms": 1674658579153
    }',
    '{
      "createtime": 1674658579396,
      "key": "Struct{maj_id=5,min_id=7b09b7c7}",
      "offset": 105,
      "partition": 0,
      "schema_id": 3,
      "topic": "mock-integration-mock-service.public.mock_table"
    }',
    '2023-01-31T14:29:09.051Z'
  ),
  -- update "unknown" entity
  (
    4,
    -- birthday: "2001-11-10T00:00:00.000Z"
    '{
      "after": {
        "maj_id": 7,
        "min_id": "e58c8366",
        "name": "Pablo",
        "surname": "Picasso",
        "birthday": 11636
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
        "txid": 338532061,
        "version": "1.9.7.final",
        "xmin": null
      },
      "transaction": null,
      "ts_ms": 1674658579153
    }',
    '{
      "createtime": 1674658579416,
      "key": "Struct{maj_id=7,min_id=e58c8366}",
      "offset": 108,
      "partition": 0,
      "schema_id": 3,
      "topic": "mock-integration-mock-service.public.mock_table"
    }',
    '2023-01-31T14:29:09.051Z'
  ),
  -- delete "unknown" entity
  (
    5,
    '{
      "after": null,
      "before": {
        "maj_id": 8,
        "min_id": "15aa2814",
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
        "txid": 338532061,
        "version": "1.9.7.final",
        "xmin": null
      },
      "transaction": null,
      "ts_ms": 1674658579153
    }',
    '{
      "createtime": 1674658579516,
      "key": "Struct{maj_id=8,min_id=15aa2814}",
      "offset": 109,
      "partition": 0,
      "schema_id": 3,
      "topic": "mock-integration-mock-service.public.mock_table"
    }',
    '2023-01-31T14:29:09.051Z'
  ),
  -- some anomaly, appeared after deletion events
  (
    6,
    '{}',
    '{
      "createtime": 1674658579516,
      "key": "Struct{maj_id=8,min_id=15aa2814}",
      "offset": 110,
      "partition": 0,
      "schema_id": 0,
      "topic": "mock-integration-mock-service.public.mock_table"
    }',
    '2023-01-31T14:29:09.051Z'
  ),
  -- next two entities are creation and deletion of the same entity.
  (
    7,
    -- birthday: "2001-05-04T00:00:00.000Z"
    '{
      "after": {
        "maj_id": 9,
        "min_id": "6114584e",
        "name": "Rene",
        "surname": "Magritte",
        "birthday": 11446
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
        "ts_ms": 1674658589163,
        "txid": 338532061,
        "version": "1.9.7.final",
        "xmin": null
      },
      "transaction": null,
      "ts_ms": 1674658579153
    }',
    '{
      "createtime": 1674658579516,
      "key": "Struct{maj_id=9,min_id=6114584e}",
      "offset": 111,
      "partition": 0,
      "schema_id": 3,
      "topic": "mock-integration-mock-service.public.mock_table"
    }',
    '2023-01-31T14:29:09.051Z'
  ),
  (
    8,
    '{
      "after": null,
      "before": {
        "maj_id": 9,
        "min_id": "6114584e",
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
        "ts_ms": 1674658589263,
        "txid": 338532161,
        "version": "1.9.7.final",
        "xmin": null
      },
      "transaction": null,
      "ts_ms": 1674658579153
    }',
    '{
      "createtime": 1674658579616,
      "key": "Struct{maj_id=9,min_id=6114584e}",
      "offset": 112,
      "partition": 0,
      "schema_id": 3,
      "topic": "mock-integration-mock-service.public.mock_table"
    }',
    '2023-01-31T14:29:09.051Z'
  ),
  -- some anomaly, appeared after deletion events
  (
    9,
    '{}',
    '{
      "createtime": 1674658579616,
      "key": "Struct{maj_id=9,min_id=6114584e}",
      "offset": 113,
      "partition": 0,
      "schema_id": 0,
      "topic": "mock-integration-mock-service.public.mock_table"
    }',
    '2023-01-31T14:29:09.051Z'
  ),
  -- deletion and restoration.
  (
    10,
    '{
      "after": null,
      "before": {
        "maj_id": 13,
        "min_id": "1a5b396b",
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
        "txid": 338532061,
        "version": "1.9.7.final",
        "xmin": null
      },
      "transaction": null,
      "ts_ms": 1674658579153
    }',
    '{
      "createtime": 1674658579516,
      "key": "Struct{maj_id=13,min_id=1a5b396b}",
      "offset": 111,
      "partition": 0,
      "schema_id": 3,
      "topic": "mock-integration-mock-service.public.mock_table"
    }',
    '2023-01-31T14:29:09.051Z'
  ),
  (
    11,
    '{}',
    '{
      "createtime": 1674658579516,
      "key": "Struct{maj_id=13,min_id=1a5b396b}",
      "offset": 112,
      "partition": 0,
      "schema_id": 3,
      "topic": "mock-integration-mock-service.public.mock_table"
    }',
    '2023-01-31T14:29:09.051Z'
  ),
  (
    12,
    '{
      "after": {
        "maj_id": 13,
        "min_id": "1a5b396b",
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
        "txid": 338532161,
        "version": "1.9.7.final",
        "xmin": null
      },
      "transaction": null,
      "ts_ms": 1674658579153
    }',
    '{
      "createtime": 1674658579616,
      "key": "Struct{maj_id=13,min_id=1a5b396b}",
      "offset": 113,
      "partition": 0,
      "schema_id": 0,
      "topic": "mock-integration-mock-service.public.mock_table"
    }',
    '2023-01-31T14:29:09.051Z'
  )
{%- endmacro -%}


{%- macro postgres_compound_key_with_before_values_processed_entities_source() -%}
  (
    1,
    -- birthday: "2001-07-30T00:00:00.000Z"
    '{
      "after": {
        "maj_id": 4,
        "min_id": "04e1594b",
        "name": "Vincent",
        "surname": "van Gogh",
        "birthday": 11533
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
        "ts_ms": 1674658578883,
        "txid": 338531960,
        "version": "1.9.7.final",
        "xmin": null
      },
      "transaction": null,
      "ts_ms": 1674658579053
    }',
    '{
      "createtime": 1674658579394,
      "key": "Struct{maj_id=4,min_id=04e1594b}",
      "offset": 103,
      "partition": 0,
      "schema_id": 3,
      "topic": "mock-integration-mock-service.public.mock_table"
    }',
    '2023-01-31T14:29:09.051Z'
  ),
  -- next two entities are evolution of the same one.
  (
    2,
    -- birthday: "2001-07-30T00:00:00.000Z"
    '{
      "after": {
        "maj_id": 5,
        "min_id": "7b09b7c7",
        "name": "Pierre-Auguste",
        "surname": "Renoir",
        "birthday": 11533
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
        "ts_ms": 1674658578883,
        "txid": 338531960,
        "version": "1.9.7.final",
        "xmin": null
      },
      "transaction": null,
      "ts_ms": 1674658579153
    }',
    '{
      "createtime": 1674658579395,
      "key": "Struct{maj_id=5,min_id=7b09b7c7}",
      "offset": 104,
      "partition": 0,
      "schema_id": 3,
      "topic": "mock-integration-mock-service.public.mock_table"
    }',
    '2023-01-31T14:29:09.051Z'
  ),
  (
    3,
    -- birthday: "2001-07-30T00:00:00.000Z"
    '{
      "after": {
        "maj_id": 5,
        "min_id": "7b09b7c7",
        "name": "Gustav",
        "surname": "Klimt",
        "birthday": 11533
      },
      "before": {
        "maj_id": 5,
        "name": "Pierre-Auguste",
        "surname": "Renoir",
        "birthday": 11533
      },
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
        "ts_ms": 1674658588983,
        "txid": 338531961,
        "version": "1.9.7.final",
        "xmin": null
      },
      "transaction": null,
      "ts_ms": 1674658579153
    }',
    '{
      "createtime": 1674658579396,
      "key": "Struct{maj_id=5,min_id=7b09b7c7}",
      "offset": 105,
      "partition": 0,
      "schema_id": 3,
      "topic": "mock-integration-mock-service.public.mock_table"
    }',
    '2023-01-31T14:29:09.051Z'
  ),
  -- update "unknown" entity
  (
    4,
    -- birthday: "2001-11-10T00:00:00.000Z"
    '{
      "after": {
        "maj_id": 7,
        "min_id": "e58c8366",
        "name": "Pablo",
        "surname": "Picasso",
        "birthday": 11636
      },
      "before": {
        "maj_id": 7,
        "name": "Joan",
        "surname": "Miro",
        "birthday": 313
      },
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
        "txid": 338532061,
        "version": "1.9.7.final",
        "xmin": null
      },
      "transaction": null,
      "ts_ms": 1674658579153
    }',
    '{
      "createtime": 1674658579416,
      "key": "Struct{maj_id=7,min_id=e58c8366}",
      "offset": 108,
      "partition": 0,
      "schema_id": 3,
      "topic": "mock-integration-mock-service.public.mock_table"
    }',
    '2023-01-31T14:29:09.051Z'
  ),
  -- delete "unknown" entity
  (
    5,
    '{
      "after": null,
      "before": {
        "maj_id": 8,
        "min_id": "15aa2814",
        "name": "Diego",
        "surname": "Velazquez",
        "birthday": 626
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
        "txid": 338532061,
        "version": "1.9.7.final",
        "xmin": null
      },
      "transaction": null,
      "ts_ms": 1674658579153
    }',
    '{
      "createtime": 1674658579516,
      "key": "Struct{maj_id=8,min_id=15aa2814}",
      "offset": 109,
      "partition": 0,
      "schema_id": 3,
      "topic": "mock-integration-mock-service.public.mock_table"
    }',
    '2023-01-31T14:29:09.051Z'
  ),
  -- some anomaly, appeared after deletion events
  (
    6,
    '{}',
    '{
      "createtime": 1674658579516,
      "key": "Struct{maj_id=8,min_id=15aa2814}",
      "offset": 110,
      "partition": 0,
      "schema_id": 0,
      "topic": "mock-integration-mock-service.public.mock_table"
    }',
    '2023-01-31T14:29:09.051Z'
  ),
  -- next two entities are creation and deletion of the same entity.
  (
    7,
    -- birthday: "2001-05-04T00:00:00.000Z"
    '{
      "after": {
        "maj_id": 9,
        "min_id": "6114584e",
        "name": "Rene",
        "surname": "Magritte",
        "birthday": 11446
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
        "ts_ms": 1674658589163,
        "txid": 338532061,
        "version": "1.9.7.final",
        "xmin": null
      },
      "transaction": null,
      "ts_ms": 1674658579153
    }',
    '{
      "createtime": 1674658579516,
      "key": "Struct{maj_id=9,min_id=6114584e}",
      "offset": 111,
      "partition": 0,
      "schema_id": 3,
      "topic": "mock-integration-mock-service.public.mock_table"
    }',
    '2023-01-31T14:29:09.051Z'
  ),
  (
    8,
    '{
      "after": null,
      "before": {
        "maj_id": 9,
        "min_id": "6114584e",
        "name": "Rene",
        "surname": "Magritte",
        "birthday": 11446
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
        "ts_ms": 1674658589263,
        "txid": 338532161,
        "version": "1.9.7.final",
        "xmin": null
      },
      "transaction": null,
      "ts_ms": 1674658579153
    }',
    '{
      "createtime": 1674658579616,
      "key": "Struct{maj_id=9,min_id=6114584e}",
      "offset": 112,
      "partition": 0,
      "schema_id": 3,
      "topic": "mock-integration-mock-service.public.mock_table"
    }',
    '2023-01-31T14:29:09.051Z'
  ),
  -- some anomaly, appeared after deletion events
  (
    9,
    '{}',
    '{
      "createtime": 1674658579616,
      "key": "Struct{maj_id=9,min_id=6114584e}",
      "offset": 113,
      "partition": 0,
      "schema_id": 0,
      "topic": "mock-integration-mock-service.public.mock_table"
    }',
    '2023-01-31T14:29:09.051Z'
  ),
  -- deletion and restoration.
  (
    10,
    '{
      "after": null,
      "before": {
        "maj_id": 13,
        "min_id": "1a5b396b",
        "name": "Stephen",
        "surname": "King",
        "birthday": 35
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
        "txid": 338532061,
        "version": "1.9.7.final",
        "xmin": null
      },
      "transaction": null,
      "ts_ms": 1674658579153
    }',
    '{
      "createtime": 1674658579516,
      "key": "Struct{maj_id=13,min_id=1a5b396b}",
      "offset": 111,
      "partition": 0,
      "schema_id": 3,
      "topic": "mock-integration-mock-service.public.mock_table"
    }',
    '2023-01-31T14:29:09.051Z'
  ),
  (
    11,
    '{}',
    '{
      "createtime": 1674658579516,
      "key": "Struct{maj_id=13,min_id=1a5b396b}",
      "offset": 112,
      "partition": 0,
      "schema_id": 3,
      "topic": "mock-integration-mock-service.public.mock_table"
    }',
    '2023-01-31T14:29:09.051Z'
  ),
  (
    12,
    '{
      "after": {
        "maj_id": 13,
        "min_id": "1a5b396b",
        "name": "Mene",
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
        "txid": 338532161,
        "version": "1.9.7.final",
        "xmin": null
      },
      "transaction": null,
      "ts_ms": 1674658579153
    }',
    '{
      "createtime": 1674658579616,
      "key": "Struct{maj_id=13,min_id=1a5b396b}",
      "offset": 113,
      "partition": 0,
      "schema_id": 0,
      "topic": "mock-integration-mock-service.public.mock_table"
    }',
    '2023-01-31T14:29:09.051Z'
  )
{%- endmacro -%}


{%- macro postgres_compound_key_no_before_values_processed_entities_model_csv() -%}
  maj_id::integer,min_id::string,name::string,surname::string,birthday::timestamp_ntz,is_deleted::boolean,deleted_at::timestamp_ntz,__rolling_row_id__::integer,__loaded_at__::timestamp_ntz
  4 ,'04e1594b','Vincent','van Gogh','2001-07-30T00:00:00.000Z',false,null                      ,1 ,'2023-01-31T14:29:09.051Z'
  5 ,'7b09b7c7','Gustav' ,'Klimt'   ,'2001-07-30T00:00:00.000Z',false,null                      ,3 ,'2023-01-31T14:29:09.051Z'
  7 ,'e58c8366','Pablo'  ,'Picasso' ,'2001-11-10T00:00:00.000Z',false,null                      ,4 ,'2023-01-31T14:29:09.051Z'
  8 ,'15aa2814',null     ,null      ,null                      ,true ,'2023-01-25T14:56:29.063Z',5 ,'2023-01-31T14:29:09.051Z'
  9 ,'6114584e','Rene'   ,'Magritte','2001-05-04T00:00:00.000Z',true ,'2023-01-25T14:56:29.263Z',8 ,'2023-01-31T14:29:09.051Z'
  13,'1a5b396b','Rene'   ,'Magritte','1970-05-04T00:00:00.000Z',false,null                      ,12,'2023-01-31T14:29:09.051Z'
{%- endmacro -%}


{%- macro postgres_compound_key_with_before_values_processed_entities_model_csv() -%}
  maj_id::integer,min_id::string,name::string,surname::string,birthday::timestamp_ntz,is_deleted::boolean,deleted_at::timestamp_ntz,__rolling_row_id__::integer,__loaded_at__::timestamp_ntz
  4 ,'04e1594b','Vincent','van Gogh' ,'2001-07-30T00:00:00.000Z',FALSE,NULL                      ,1 ,'2023-01-31T14:29:09.051Z'
  5 ,'7b09b7c7','Gustav' ,'Klimt'    ,'2001-07-30T00:00:00.000Z',FALSE,NULL                      ,3 ,'2023-01-31T14:29:09.051Z'
  7 ,'e58c8366','Pablo'  ,'Picasso'  ,'2001-11-10T00:00:00.000Z',FALSE,NULL                      ,4 ,'2023-01-31T14:29:09.051Z'
  8 ,'15aa2814','Diego'  ,'Velazquez','1971-09-19T00:00:00.000Z',TRUE ,'2023-01-25T14:56:29.063Z',5 ,'2023-01-31T14:29:09.051Z'
  9 ,'6114584e','Rene'   ,'Magritte' ,'2001-05-04T00:00:00.000Z',TRUE ,'2023-01-25T14:56:29.263Z',8 ,'2023-01-31T14:29:09.051Z'
  13,'1a5b396b','Rene'   ,'Magritte' ,'1970-05-04T00:00:00.000Z',FALSE,NULL                      ,12,'2023-01-31T14:29:09.051Z'
{%- endmacro -%}


{%- macro scd2_simple_key_header_csv() -%}
  id::integer,name::string,surname::string,birthday::timestamp_ntz,__valid_from__::timestamp_ntz,__valid_to__::timestamp_ntz,__inner_key__::string,__rolling_row_id__::number,__loaded_at__::timestamp_ntz
{%- endmacro -%}


{%- macro scd2_simple_key_processed_entities_model_csv() -%}
  {{- scd2_simple_key_header_csv() }}
  4 ,'Vinsent'       ,'van Gogh','2001-07-30T00:00:00.000Z','2023-01-25T14:56:18.883Z',NULL                      ,'a87ff679a2f3e71d9181a67b7542122c',1 ,'2023-01-31T14:29:09.051Z'
  5 ,'Pierre-Auguste','Renoir'  ,'2001-07-30T00:00:00.000Z','2023-01-25T14:56:18.883Z','2023-01-25T14:56:19.883Z','e4da3b7fbbce2345d7772b0674a318d5',2 ,'2023-01-31T14:29:09.051Z'
  5 ,'Gustav'        ,'Klimt'   ,'2001-07-30T00:00:00.000Z','2023-01-25T14:56:19.883Z',NULL                      ,'e4da3b7fbbce2345d7772b0674a318d5',3 ,'2023-01-31T14:29:09.051Z'
  7 ,'Pablo'         ,'Picasso' ,'2001-11-10T00:00:00.000Z','2023-01-25T14:56:29.063Z',NULL                      ,'8f14e45fceea167a5a36dedd4bea2543',4 ,'2023-01-31T14:29:09.051Z'
  9 ,'Rene'          ,'Magritte','2001-05-04T00:00:00.000Z','2023-01-25T14:56:29.163Z','2023-01-25T14:56:29.263Z','45c48cce2e2d7fbdea1afc51c7c6ad26',7 ,'2023-01-31T14:29:09.051Z'
  13,'Rene'          ,'Magritte','1970-05-04T00:00:00.000Z','2023-01-25T14:56:29.263Z',NULL                      ,'c51ce410c124a10e0db5e4b97fc2af39',12,'2023-01-31T14:29:09.051Z'
{%- endmacro -%}


{%- macro scd2_compound_key_header_csv() -%}
  maj_id::integer,min_id::string,name::string,surname::string,birthday::timestamp_ntz,__valid_from__::timestamp_ntz,__valid_to__::timestamp_ntz,__inner_key__::string,__rolling_row_id__::number,__loaded_at__::timestamp_ntz
{%- endmacro -%}


{%- macro scd2_compound_key_processed_entities_model_csv() -%}
  {{- scd2_compound_key_header_csv() }}
  4 ,'04e1594b','Vincent'       ,'van Gogh' ,'2001-07-30T00:00:00.000Z','2023-01-25T14:56:18.883Z',NULL                      ,'12f460329753ed920adec3bb36884380',1 ,'2023-01-31T14:29:09.051Z'
  5 ,'7b09b7c7','Pierre-Auguste','Renoir'   ,'2001-07-30T00:00:00.000Z','2023-01-25T14:56:18.883Z','2023-01-25T14:56:18.883Z','2fb83d5eb811ce22d6f020e4f4f146cb',2 ,'2023-01-31T14:29:09.051Z'
  5 ,'7b09b7c7','Gustav'        ,'Klimt'    ,'2001-07-30T00:00:00.000Z','2023-01-25T14:56:18.883Z',NULL                      ,'2fb83d5eb811ce22d6f020e4f4f146cb',3 ,'2023-01-31T14:29:09.051Z'
  7 ,'e58c8366','Pablo'         ,'Picasso'  ,'2001-11-10T00:00:00.000Z','2023-01-25T14:56:19.883Z',NULL                      ,'2bc8d719e59c3f760aabcf0bc0dd6bc7',4 ,'2023-01-31T14:29:09.051Z'
  9 ,'6114584e','Rene'          ,'Magritte' ,'2001-05-04T00:00:00.000Z','2023-01-25T14:56:29.163Z','2023-01-25T14:56:29.263Z','d0e14c02bd7ea42355bb0e8d73707994',8 ,'2023-01-31T14:29:09.051Z'
  13,'1a5b396b','Rene'          ,'Magritte' ,'1970-05-04T00:00:00.000Z','2023-01-25T14:56:29.263Z',NULL                      ,'b9ec5cbf954b5ae91ec20db52f6f23c4',12,'2023-01-31T14:29:09.051Z'
{%- endmacro -%}


{%- macro scd4_simple_key_header_csv() -%}
  id::integer,name::string,surname::string,birthday::timestamp_ntz,__valid_from__::timestamp_ntz,__op__::string,__inner_key__::string,__rolling_row_id__::number,__loaded_at__::timestamp_ntz
{%- endmacro -%}


{%- macro scd4_simple_key_processed_entities_model_csv() -%}
  {{- scd4_simple_key_header_csv() }}
  4 ,'Vinsent'       ,'van Gogh','2001-07-30T00:00:00.000Z','2023-01-25T14:56:18.883Z','c','a87ff679a2f3e71d9181a67b7542122c',1 ,'2023-01-31T14:29:09.051Z'
  5 ,'Pierre-Auguste','Renoir'  ,'2001-07-30T00:00:00.000Z','2023-01-25T14:56:18.883Z','c','e4da3b7fbbce2345d7772b0674a318d5',2 ,'2023-01-31T14:29:09.051Z'
  5 ,'Gustav'        ,'Klimt'   ,'2001-07-30T00:00:00.000Z','2023-01-25T14:56:19.883Z','u','e4da3b7fbbce2345d7772b0674a318d5',3 ,'2023-01-31T14:29:09.051Z'
  7 ,'Pablo'         ,'Picasso' ,'2001-11-10T00:00:00.000Z','2023-01-25T14:56:29.063Z','u','8f14e45fceea167a5a36dedd4bea2543',4 ,'2023-01-31T14:29:09.051Z'
  8,''               ,''        ,'1970-01-01T00:00:00.000Z','2023-01-25T14:56:29.063Z','d','c9f0f895fb98ab9159f51fd0297e236d',5 ,'2023-01-31T14:29:09.051Z'
  9 ,'Rene'          ,'Magritte','2001-05-04T00:00:00.000Z','2023-01-25T14:56:29.163Z','c','45c48cce2e2d7fbdea1afc51c7c6ad26',7 ,'2023-01-31T14:29:09.051Z'
  9 ,''              ,''        ,'1970-01-01T00:00:00.000Z','2023-01-25T14:56:29.263Z','d','45c48cce2e2d7fbdea1afc51c7c6ad26',8 ,'2023-01-31T14:29:09.051Z'
  13,''              ,''        ,'1970-01-01T00:00:00.000Z','2023-01-25T14:56:29.163Z','d','c51ce410c124a10e0db5e4b97fc2af39',10,'2023-01-31T14:29:09.051Z'
  13,'Rene'          ,'Magritte','1970-05-04T00:00:00.000Z','2023-01-25T14:56:29.263Z','c','c51ce410c124a10e0db5e4b97fc2af39',12,'2023-01-31T14:29:09.051Z'
{%- endmacro -%}


{%- macro scd4_compound_key_header_csv() -%}
  maj_id::integer,min_id::string,name::string,surname::string,birthday::timestamp_ntz,__valid_from__::timestamp_ntz,__op__::string,__inner_key__::string,__rolling_row_id__::number,__loaded_at__::timestamp_ntz
{%- endmacro -%}


{%- macro scd4_compound_key_processed_entities_model_csv() -%}
  {{- scd4_compound_key_header_csv() }}
  4 ,'04e1594b','Vincent'       ,'van Gogh' ,'2001-07-30T00:00:00.000Z','2023-01-25T14:56:18.883Z','c','12f460329753ed920adec3bb36884380',1 ,'2023-01-31T14:29:09.051Z'
  5 ,'7b09b7c7','Pierre-Auguste','Renoir'   ,'2001-07-30T00:00:00.000Z','2023-01-25T14:56:18.883Z','c','2fb83d5eb811ce22d6f020e4f4f146cb',2 ,'2023-01-31T14:29:09.051Z'
  5 ,'7b09b7c7','Gustav'        ,'Klimt'    ,'2001-07-30T00:00:00.000Z','2023-01-25T14:56:18.883Z','u','2fb83d5eb811ce22d6f020e4f4f146cb',3 ,'2023-01-31T14:29:09.051Z'
  7 ,'e58c8366','Pablo'         ,'Picasso'  ,'2001-11-10T00:00:00.000Z','2023-01-25T14:56:19.883Z','u','2bc8d719e59c3f760aabcf0bc0dd6bc7',4 ,'2023-01-31T14:29:09.051Z'
  8 ,'15aa2814',''              ,''         ,'1970-01-01T00:00:00.000Z','2023-01-25T14:56:29.063Z','d','da77b4b771b7708b241e6bab1df2f9e4',5 ,'2023-01-31T14:29:09.051Z'
  9 ,'6114584e','Rene'          ,'Magritte' ,'2001-05-04T00:00:00.000Z','2023-01-25T14:56:29.163Z','c','d0e14c02bd7ea42355bb0e8d73707994',8 ,'2023-01-31T14:29:09.051Z'
  9 ,'6114584e',''              ,''         ,'1970-01-01T00:00:00.000Z','2023-01-25T14:56:29.163Z','d','d0e14c02bd7ea42355bb0e8d73707994',8 ,'2023-01-31T14:29:09.051Z'
  13,'1a5b396b',''              ,''         ,'1970-01-01T00:00:00.000Z','2023-01-25T14:56:29.163Z','d','b9ec5cbf954b5ae91ec20db52f6f23c4',10,'2023-01-31T14:29:09.051Z'
  13,'1a5b396b','Rene'          ,'Magritte' ,'1970-05-04T00:00:00.000Z','2023-01-25T14:56:29.263Z','c','b9ec5cbf954b5ae91ec20db52f6f23c4',12,'2023-01-31T14:29:09.051Z'
{%- endmacro -%}

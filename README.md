# Debezium operational data store

Set of tools to materialize raw data, provided by
[Debezium CDC](https://debezium.io/) and landed with
[Kafka-Connect](https://cwiki.apache.org/confluence/display/KAFKA/Ecosystem#Ecosystem-KafkaConnect).

## How to use it.

### 1. Generate Raw landing table.

In this package there is an operation, which helps to generate landing tables
for debezium sources.

Operation `create_source_table`

Generates the source table in database and prints the YAML for it to the
output.

Arguments:

- `table_name`
  **Required**
  Type: `string`.
  A name of a table to create.
- `schema_name`
  *Optional*
  Type: `string` or `None`.
  Default `None`. A name of a schema to create the table in. If empty, the
  schema would be taken from target. If the schema does not exist, the
  operation attempts to create it.
- `role_name`
  *Optional*
  Type: `string` or `None`.
  Default `None`. A name of a role to grant required permissions. If `None` --
  no permissions will be grant.
- `db`
  *Optional*
  Type: `string` or `None`.
  Default `None`. A name of a database to create the table and the schema. If
  `None` -- the database from a profile is used. Taking effect only for
  platforms, supporting multi-database adapters.
- `aliases`
  *Optional*
  Type: `Dict[string,string]` or `None`.
  Default `None`. Default technical column aliases. Takes into the account
  keys `row_id` and `loaded_at`. The values are names for the related columns.
  If `None` or the keys are missed -- values are taken from vars
  `row_id__alias` and `loaded_at__alias`. The default values for those are
  `rolling_row_id` and `snowpipe_loaded_at`.
- `dry_run`
  *Optional*
  Type: `boolean`
  Default `False`. If it should really apply the queries to the database or
  just prit it.

Example:

```bash
$ dbt run-operation create_source_table \
    --args '{db: raw_layer, schema_name: kafka_connect, table_name: my_lending_table}'
```

### 2. Build model.

#### 2.1 Use only actual data.

To stage the table with an actual state the model should be defined with the
following macro:

Macro `to_stg_layer`

Generates the model internals to stage a source with actual states.

Arguments:

- `source_ref`
  **Required**
  Type: `Source`
  The Source definition.
- `fields`
  **Required**
  Type: `List[string | Dict[string,string] | Dict[string,Dict[string,boolean]]`.
  List of the fields **not including the Primary Key field**. Each field could
  be defined with a name (as a string), with single-key dict, containing the
  field name as the key and an expresssion to cast it to some type in the python
  format string format as the, or with dict `{"is_date": true}` as value.
- `incremental`
  **Required**
  Type: `boolean`.
  The value of `is_incremental()` macro.
- `source_engine`
  **Required**
  Type: `string`
  The type of source database engine. Currently supports only `postgresql`
  value.
- `contains_prev_state`
  *Optional*
  Type: `boolean`
  Default `False`. If the source contains previous values. For more information see
  [Debezium documentation](https://debezium.io/documentation/reference/2.0/connectors/postgresql.html#postgresql-replica-identity).
- `aliases`
  *Optional*
  Type: `Dict[string,string]` or `None`.
  Default `None`. Default technical column aliases. Takes into the account
  keys `is_deleted`, `deleted_at`, `ts`, `row_id` and `loaded_at`. The values
  are names for the related columns. If `None` or the keys are missed --
  values are taken from vars `is_deleted__alias`, `deleted_at__alias`,
  `ts__alias`, `row_id__alias` and `loaded_at__alias`. The default values for
  those are `is_deleted`, deleted_at`, `ts_ms `rolling_row_id` and `snowpipe_loaded_at`.
```sql

```

#### 2.2 Use historical data (Slow changing dimentions)

## Supported stack

### 1. Supported debezium connectors:

- [io.debezium.connector.postgresql.PostgresConnector](https://debezium.io/documentation/reference/2.3/connectors/postgresql.html)

### 2. Supported warehouse synk connectors:

- [Snowflake](https://docs.snowflake.com/en/user-guide/kafka-connector)

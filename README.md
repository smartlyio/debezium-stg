# Debezium operational data store

Set of tools to materialize raw data, provided by
[Debezium CDC](https://debezium.io/) and landed with
[Kafka-Connect](https://cwiki.apache.org/confluence/display/KAFKA/Ecosystem#Ecosystem-KafkaConnect).

## How to use it.

### 1. Generate Raw landing table.

In this package there is an operation, which helps to generate landing tables for
deezeup sources.

```bash
$ dbt run-operation create_source_table \
    --args '{db: raw_layer, schema_name: kafka_connect, table_name: my_lending_table}'
```

### 2. Build model.

#### 2.1 Use only actual data.

##### Note: Combining last actual data with dbt snapshotting

#### 2.2 Use historical data (Slow changing dimentions)

## Supported stack

### 1. Supported debezium connectors:

- [io.debezium.connector.postgresql.PostgresConnector](https://debezium.io/documentation/reference/2.3/connectors/postgresql.html)

### 2. Supported warehouse synk connectors:

- [Snowflake](https://docs.snowflake.com/en/user-guide/kafka-connector)

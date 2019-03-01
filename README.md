# Kafka adapter for PostgreSQL/TimescaleDB

1) You can use [prometheus-kafka-adapter](https://github.com/Telefonica/prometheus-kafka-adapter) to send your Prometheus metrics to Kafka
2) Then you can use this adapter to consume the metrics and add them to your PostgreSQL/Timescale cluster.

If you have not yet prepared your PostgreSQL/Timescale environment please take a look at [prometheus-postgresql-adapter](https://github.com/timescale/prometheus-postgresql-adapter) repository.

# Configuration

kafka-timescale-adapter consumes metrics from Kafka and sends them to PostgreSQL/Timescale. The adapter can be configured with the following environment variables:

- `LISTEN_ADDR`: Listen address for the adapter, defaults to `:9528`
- `TELEMETRY_PATH`: Endpoint for the metrics, defaults to `/metrics`
- `BATCH_SIZE`: Number of metrics consumed from Kafka and sent to PostgreSQL/Timescale at a time, defaults to `10000`
- `LOG_LEVEL`: Log level, defaults to `info`
- `WHITELIST_FILE`: The path of the whitelist file listing regular expressions. Only metrics matching the expressions will be sent to PostgreSQL/Timescale. Defaults to `/etc/prometheus/kafka-timescale-adapter.whitelist.regex`

- `KAFKA_BROKER_LIST`: Comma separated Kafka endpoints, defaults to `localhost:9092`
- `KAFKA_TOPIC`: Kafka topic for the metrics, defaults to `metrics`
- `KAFKA_GROUP_ID`: Consumer group id, defaults to `metrics_consumers`

- `PG_HOST`: PostgreSQL/Timescale hostname, defaults to `localhost`
- `PG_PORT`: PostgreSQL/Timescale port, defaults to `5432`
- `PG_DATABASE`: Database name, defaults to `prometheus`
- `PG_USERNAME`: Database username, defaults to `prometheus`
- `PG_PASSWORD`: Database user password, `prometheus`
- `PG_TABLE`: Database table for the metrics, defaults to `metrics`
- `PG_WRITE_TIMEOUT`: Timeout to insert metrics to the database, defaults to `30s`
- `PG_WRITE_RETRY`: The adapter will retry insert if there's a failure, defaults to `3`
- `PG_MAX_OPEN_CONNS`: Maximum open connections to the database, defaults to `10`
- `PG_MAX_IDLE_CONNS`: Maximum number of idle connections to the database, defaults to `2`
- `PG_MAX_CONN_LIFETIME`: Maximum lifetime of connections to the database, defaults to `1h`

- `PG_NORMALIZE`: Refer to [storage formats](https://github.com/timescale/pg_prometheus#storage-formats), defaults to `true`
- `PG_USE_TIMESCALEDB`: Use TimescaleDB extension, defaults to `true`
- `PG_CHUNK_INTERVAL`: The size of a time-partition chunk in TimescaleDB, defaults to `12h`

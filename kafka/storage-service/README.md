# Kafka Storage Service

Java / Spring Boot. Subscribes to the readings topic and persists messages to PostgreSQL.

Runs as a **batch listener**: each Kafka poll (up to `max.poll.records`) is written in a single JDBC batch (the spec's 500-message batching optimization), and offsets are committed only after a successful insert (`ack-mode: BATCH`), giving at-least-once storage. Rows are written with `broker_type = 'kafka'` and `delivery_mode` taken from the producer's `acks` header. On startup it declares the topic with multiple partitions so partitioning/consumer-lag can be studied.

## Configuration (environment variables)

| Variable | Default | Description |
| --- | --- | --- |
| `KAFKA_BOOTSTRAP` | `kafka:9092` | Kafka bootstrap servers |
| `KAFKA_TOPIC` | `iot.readings` | Topic to consume |
| `KAFKA_GROUP_ID` | `kafka-storage` | Consumer group |
| `KAFKA_PARTITIONS` | `3` | Partitions created for the topic |
| `KAFKA_MAX_POLL_RECORDS` | `500` | Max records per poll = batch size |
| `KAFKA_CONCURRENCY` | `1` | Concurrent listener threads (<= partitions) |
| `KAFKA_AUTO_OFFSET_RESET` | `earliest` | Offset reset policy |
| `POSTGRES_HOST` / `POSTGRES_PORT` | `postgres` / `5432` | Database host/port |
| `POSTGRES_DB` / `POSTGRES_USER` / `POSTGRES_PASSWORD` | `iotdb` / `iotuser` / `iotpass` | Database credentials |

## Run

```bash
docker compose up -d postgres kafka kafka-storage-service
docker compose logs kafka-storage-service --tail=50
```

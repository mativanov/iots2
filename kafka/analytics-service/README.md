# Kafka Analytics Service

Java / Spring Boot. Subscribes to the readings stream and runs a fixed (tumbling) window aggregation.

Every `WINDOW_SECONDS` (default 10) it computes the average temperature over the messages received in that window and logs a summary; when the average exceeds `ALERT_THRESHOLD` it logs a critical `ALERT` line. It uses its own consumer group, separate from storage, so it gets its own copy of every message (publish/subscribe fan-out). Mirrors the MQTT analytics service.

## Configuration (environment variables)

| Variable | Default | Description |
| --- | --- | --- |
| `KAFKA_BOOTSTRAP` | `kafka:9092` | Kafka bootstrap servers |
| `KAFKA_TOPIC` | `iot.readings` | Topic to consume |
| `KAFKA_GROUP_ID` | `kafka-analytics` | Consumer group (distinct from storage) |
| `KAFKA_AUTO_OFFSET_RESET` | `latest` | Offset reset policy (live data) |
| `ALERT_THRESHOLD` | `50` | Average-temperature alert threshold |
| `WINDOW_SECONDS` | `10` | Tumbling window length |

## Run

```bash
docker compose up -d postgres kafka kafka-analytics-service
docker compose logs kafka-analytics-service --tail=50
```

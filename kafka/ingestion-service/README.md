# Kafka Ingestion Service

Java / Spring Boot. Simulates IoT devices: loads the shared CSV dataset, generates sensor readings, and produces them to a Kafka topic, then exits (run as a one-shot job, like the MQTT ingestion service).

Each record is keyed by `deviceId` (so a device's readings keep to one partition) and carries the producer's `acks` value as a message header, which the storage service turns into `delivery_mode`. The JSON payload is identical to the MQTT side (`messageId`, `deviceId`, `temperature`, `humidity`, `createdAt`).

## Configuration (environment variables)

| Variable | Default | Description |
| --- | --- | --- |
| `KAFKA_BOOTSTRAP` | `kafka:9092` | Kafka bootstrap servers |
| `KAFKA_TOPIC` | `iot.readings` | Target topic |
| `KAFKA_ACKS` | `all` | Producer acknowledgements: `0`, `1`, or `all` |
| `TOTAL_MESSAGES` | `100` | Number of messages to publish |
| `MESSAGES_PER_SECOND` | `10` | Publish rate |
| `DEVICE_COUNT` | `10` | Number of distinct simulated devices |
| `DATASET_PATH` | `/app/sample-data/Smart_Farming_Crop_Yield_2024.csv` | CSV dataset path |

## Run

```bash
docker compose run --rm kafka-ingestion-service
docker compose run --rm -e KAFKA_ACKS=1 -e TOTAL_MESSAGES=1000 -e MESSAGES_PER_SECOND=50 kafka-ingestion-service
```

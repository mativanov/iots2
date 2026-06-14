# IoT Event-Driven Microservices: Comparative Evaluation of MQTT and Kafka

This repository contains the foundation for a university project comparing MQTT and Kafka as event brokers for IoT sensor-reading workloads.

At this stage, the project includes infrastructure, shared contracts, sample-data tooling, a working MQTT flow (Node.js), and a working Kafka flow (Java / Spring Boot). The two broker pipelines deliberately use different technologies, as required by the project specification. Benchmark automation, dashboards, and the final report are still to come.

## Objective

The project will later evaluate how MQTT and Kafka behave in an event-driven microservices architecture for IoT data. The planned comparison can include throughput, latency, delivery behavior, operational complexity, and storage/analytics integration.

## High-Level Architecture

```text
CSV dataset
   |
   v
Shared event generator
   |
   +--> MQTT broker --> MQTT ingestion/storage/analytics services
   |
   +--> Kafka broker --> Kafka ingestion/storage/analytics services (future)
                               |
                               v
                         PostgreSQL
```

PostgreSQL is included as the shared persistence layer. Eclipse Mosquitto provides the MQTT broker. Kafka runs in KRaft mode without ZooKeeper, but Kafka application logic is intentionally left for a later phase.

## Folder Structure

```text
iot-event-brokers/
+-- docker-compose.yml
+-- README.md
+-- .gitignore
+-- shared/
|   +-- contracts/
|   |   +-- sensor-reading.schema.json
|   +-- event-generator/
|   |   +-- package.json
|   |   +-- README.md
|   |   +-- scripts/
|   |   |   +-- preview-events.js
|   |   +-- src/
|   |       +-- sensorReading.js
|   |       +-- csvDatasetLoader.js
|   |       +-- eventGenerator.js
|   |       +-- index.js
|   +-- sample-data/
|   |   +-- README.md
|   |   +-- Smart_Farming_Crop_Yield_2024.csv
|   +-- docs/
|       +-- architecture.md
+-- database/
|   +-- init.sql
+-- mqtt/
|   +-- ingestion-service/
|   |   +-- Dockerfile
|   |   +-- package.json
|   |   +-- README.md
|   |   +-- src/
|   |       +-- index.js
|   +-- storage-service/
|   |   +-- Dockerfile
|   |   +-- package.json
|   |   +-- README.md
|   |   +-- src/
|   |       +-- index.js
|   +-- analytics-service/
|   |   +-- Dockerfile
|   |   +-- package.json
|   |   +-- README.md
|   |   +-- src/
|   |       +-- index.js
|   +-- mosquitto.conf
+-- kafka/
|   +-- ingestion-service/
|   |   +-- README.md
|   +-- storage-service/
|   |   +-- README.md
|   +-- analytics-service/
|       +-- README.md
+-- benchmarks/
    +-- scripts/
    |   +-- README.md
    +-- results/
        +-- README.md
```

## Docker Services

| Service | Image | Purpose | Host Port |
| --- | --- | --- | --- |
| `postgres` | `postgres:16-alpine` | Stores sensor readings | `5432` |
| `mosquitto` | `eclipse-mosquitto:2` | MQTT broker | `1883` |
| `kafka` | `apache/kafka:3.7.0` | Kafka broker in KRaft mode | `9092` |
| `mqtt-ingestion-service` | local Node.js build | Publishes generated SensorReading events to MQTT | none |
| `mqtt-storage-service` | local Node.js build | Stores MQTT events in PostgreSQL | none |
| `mqtt-analytics-service` | local Node.js build | Logs 10-second tumbling-window temperature analytics | none |

The MQTT topic used by the implemented services is:

```text
iot/readings
```

The Kafka topic used by the implemented Kafka services is:

```text
iot.readings
```

The storage service declares this topic with multiple partitions (default 3) on startup so partitioning and consumer-lag behavior can be exercised. Auto-creation is also enabled as a fallback.

## Startup Instructions

Start all infrastructure services:

```bash
docker compose up -d
```

Check service status:

```bash
docker compose ps
```

Stop and remove the running containers:

```bash
docker compose down
```

To remove local container data as well:

```bash
docker compose down -v
```

## MQTT Flow

Start PostgreSQL, Mosquitto, MQTT storage, and MQTT analytics:

```bash
docker compose up -d postgres mosquitto mqtt-storage-service mqtt-analytics-service
```

Run the MQTT ingestion service once:

```bash
docker compose run --rm mqtt-ingestion-service
```

The ingestion service publishes `TOTAL_MESSAGES` events and exits. Storage and analytics keep running.

Useful ingestion controls:

```bash
docker compose run --rm -e TOTAL_MESSAGES=1000 -e MESSAGES_PER_SECOND=50 -e DEVICE_COUNT=100 mqtt-ingestion-service
```

## MQTT Verification

Start the MQTT stack:

```bash
docker compose up -d postgres mosquitto mqtt-storage-service mqtt-analytics-service
```

Run one ingestion pass with the default settings:

```bash
docker compose run --rm mqtt-ingestion-service
```

Run QoS 0, QoS 1, and QoS 2 tests:

```bash
docker compose run --rm -e MQTT_QOS=0 -e TOTAL_MESSAGES=100 mqtt-ingestion-service
docker compose run --rm -e MQTT_QOS=1 -e TOTAL_MESSAGES=100 mqtt-ingestion-service
docker compose run --rm -e MQTT_QOS=2 -e TOTAL_MESSAGES=100 mqtt-ingestion-service
```

The MQTT storage and analytics services subscribe with QoS 2 by default in Docker Compose so the received message QoS can match each ingestion run. If you explicitly override subscriber QoS with `MQTT_QOS`, the effective received QoS is the lower value between publisher and subscriber.

Check PostgreSQL counts grouped by delivery mode:

```bash
docker compose exec postgres psql -U iotuser -d iotdb -c "SELECT broker_type, delivery_mode, COUNT(*) FROM sensor_readings GROUP BY broker_type, delivery_mode ORDER BY broker_type, delivery_mode;"
```

Inspect recent MQTT storage and analytics logs:

```bash
docker compose logs mqtt-storage-service --tail=50
docker compose logs mqtt-analytics-service --tail=50
```

Run the Windows smoke-test script:

```powershell
.\benchmarks\scripts\mqtt-smoke-test.ps1
```

If PowerShell script execution is disabled on your machine, run:

```powershell
powershell -ExecutionPolicy Bypass -File .\benchmarks\scripts\mqtt-smoke-test.ps1
```

The MQTT vertical slice is working when:

- `mqtt-storage-service` logs successful batch inserts.
- `mqtt-analytics-service` logs window summaries with message count and average temperature.
- PostgreSQL shows `broker_type = mqtt` rows grouped under `qos-0`, `qos-1`, and `qos-2` after the three QoS ingestion runs.

## Kafka Flow

The Kafka services are written in Java with Spring Boot (a different technology stack from the Node.js MQTT services, per the specification) and produce the exact same JSON message format, so they share the `sensor_readings` table and the `broker_type = 'kafka'` label.

Start PostgreSQL, Kafka, Kafka storage, and Kafka analytics. Start storage first so the topic is created with multiple partitions before any data arrives:

```bash
docker compose up -d postgres kafka kafka-storage-service kafka-analytics-service
```

Run the Kafka ingestion service once (publishes `TOTAL_MESSAGES` events, then exits):

```bash
docker compose run --rm kafka-ingestion-service
```

Useful ingestion controls (mirror the MQTT ones):

```bash
docker compose run --rm -e TOTAL_MESSAGES=1000 -e MESSAGES_PER_SECOND=50 -e DEVICE_COUNT=100 kafka-ingestion-service
```

### Kafka Verification (acks levels)

The producer's acknowledgement level is set with `KAFKA_ACKS` and is carried as a message header, so the storage service records it as `delivery_mode` (`acks-0` / `acks-1` / `acks-all`):

```bash
docker compose run --rm -e KAFKA_ACKS=0   -e TOTAL_MESSAGES=100 kafka-ingestion-service
docker compose run --rm -e KAFKA_ACKS=1   -e TOTAL_MESSAGES=100 kafka-ingestion-service
docker compose run --rm -e KAFKA_ACKS=all -e TOTAL_MESSAGES=100 kafka-ingestion-service
```

Check PostgreSQL counts grouped by broker and delivery mode (shows both MQTT and Kafka rows side by side):

```bash
docker compose exec postgres psql -U iotuser -d iotdb -c "SELECT broker_type, delivery_mode, COUNT(*) FROM sensor_readings GROUP BY broker_type, delivery_mode ORDER BY broker_type, delivery_mode;"
```

Inspect recent Kafka storage and analytics logs:

```bash
docker compose logs kafka-storage-service --tail=50
docker compose logs kafka-analytics-service --tail=50
```

Inspect consumer-group lag and partition assignment:

```bash
docker compose exec kafka kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group kafka-storage
```

The Kafka vertical slice is working when:

- `kafka-storage-service` logs successful batch inserts.
- `kafka-analytics-service` logs window summaries with message count and average temperature (and `ALERT` lines when the average exceeds the threshold).
- PostgreSQL shows `broker_type = kafka` rows grouped under `acks-0`, `acks-1`, and `acks-all` after the three acks ingestion runs.

## Verification

Verify PostgreSQL is running:

```bash
docker compose exec postgres pg_isready -U iotuser -d iotdb
```

Verify the `sensor_readings` table exists:

```bash
docker compose exec postgres psql -U iotuser -d iotdb -c "\d sensor_readings"
```

Verify Mosquitto is running:

```bash
docker compose logs mosquitto
```

You should see Mosquitto listening on port `1883`.

Verify Kafka is running:

```bash
docker compose exec kafka kafka-topics.sh --bootstrap-server localhost:9092 --list
```

This command should complete successfully. It may return an empty list because no topics are required for the foundation stage.

Verify MQTT rows were inserted:

```bash
docker compose exec postgres psql -U iotuser -d iotdb -c "SELECT broker_type, delivery_mode, COUNT(*) FROM sensor_readings GROUP BY broker_type, delivery_mode ORDER BY broker_type, delivery_mode;"
```

View MQTT service logs:

```bash
docker compose logs mqtt-storage-service
docker compose logs mqtt-analytics-service
```

## Current Scope

Implemented now:

- Docker Compose infrastructure for PostgreSQL, Mosquitto, and Kafka.
- Database schema initialization.
- Shared JSON contract for sensor readings.
- Reusable CSV dataset loader and SensorReading event generator.
- MQTT ingestion, storage, and analytics services.
- MQTT smoke-test script for Windows.
- Sample-data transformation documentation.
- Benchmark folders for future scripts and results.

Intentionally not implemented yet:

- Kafka producers or consumers.
- APIs or dashboards.
- Benchmark execution scripts.

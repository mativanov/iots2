# IoT Event-Driven Microservices: Comparative Evaluation of MQTT and Kafka

This repository contains the foundation for a university project comparing MQTT and Kafka as event brokers for IoT sensor-reading workloads.

At this stage, the project includes infrastructure, shared contracts, sample-data tooling, and a working MQTT flow. Kafka application services, REST APIs, dashboards, and benchmark automation are not implemented yet.

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

The Kafka topic planned for later application logic is:

```text
iot-readings
```

The topic is not required by the current skeleton, but Kafka is configured with topic auto-creation enabled for future experiments.

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

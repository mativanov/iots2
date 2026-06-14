# IoT Event-Driven Microservices: Comparative Evaluation of MQTT and Kafka

This repository contains the foundation for a university project comparing MQTT and Kafka as event brokers for IoT sensor-reading workloads.

At this stage, the project intentionally includes only infrastructure, contracts, sample-data documentation, and service folders. MQTT and Kafka producers, consumers, APIs, analytics logic, and storage logic are not implemented yet.

## Objective

The project will later evaluate how MQTT and Kafka behave in an event-driven microservices architecture for IoT data. The planned comparison can include throughput, latency, delivery behavior, operational complexity, and storage/analytics integration.

## High-Level Architecture

```text
CSV dataset
   |
   v
Event generation layer (future)
   |
   +--> MQTT broker --> MQTT ingestion/storage/analytics services (future)
   |
   +--> Kafka broker --> Kafka ingestion/storage/analytics services (future)
                               |
                               v
                         PostgreSQL
```

PostgreSQL is included as the shared persistence layer. Eclipse Mosquitto provides the MQTT broker. Kafka runs in KRaft mode without ZooKeeper.

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
|   |   +-- README.md
|   +-- storage-service/
|   |   +-- README.md
|   +-- analytics-service/
|   |   +-- README.md
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

## Current Scope

Implemented now:

- Docker Compose infrastructure for PostgreSQL, Mosquitto, and Kafka.
- Database schema initialization.
- Shared JSON contract for sensor readings.
- Reusable CSV dataset loader and SensorReading event generator.
- Sample-data transformation documentation.
- Empty service folders with README placeholders.
- Benchmark folders for future scripts and results.

Intentionally not implemented yet:

- MQTT producers or consumers.
- Kafka producers or consumers.
- Business logic for ingestion, storage, or analytics.
- APIs or dashboards.
- Benchmark execution scripts.

# Architecture Notes

This project is organized around two parallel broker paths:

- MQTT path: Eclipse Mosquitto plus future MQTT ingestion, storage, and analytics services.
- Kafka path: Kafka in KRaft mode plus future Kafka ingestion, storage, and analytics services.

Both paths will eventually process the same logical sensor-reading event contract so their behavior can be compared under similar workloads.

PostgreSQL is shared infrastructure for persisted readings and benchmark-related inspection. The table created in `database/init.sql` includes broker metadata fields so future storage services can record whether a reading arrived through MQTT or Kafka.

Application services are intentionally represented only as folders at this stage.

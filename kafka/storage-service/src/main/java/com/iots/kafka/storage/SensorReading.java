package com.iots.kafka.storage;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * Incoming reading payload. Mirrors the JSON emitted by both the Kafka and MQTT
 * ingestion services. Unknown properties are ignored so the schema can evolve
 * without breaking storage.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record SensorReading(
        String messageId,
        String deviceId,
        Double temperature,
        Double humidity,
        String createdAt
) {
}

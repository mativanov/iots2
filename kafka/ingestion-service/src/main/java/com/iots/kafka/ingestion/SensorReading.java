package com.iots.kafka.ingestion;

/**
 * Wire format for a single sensor reading.
 *
 * <p>Field names match the JSON produced by the Node.js MQTT side
 * (messageId, deviceId, temperature, humidity, createdAt) so both brokers feed
 * the same downstream consumers and the same PostgreSQL table.</p>
 */
public record SensorReading(
        String messageId,
        String deviceId,
        double temperature,
        double humidity,
        String createdAt
) {
}

package com.iots.kafka.storage;

import java.sql.Timestamp;

/** A reading validated and normalized for insertion into PostgreSQL. */
public record StoredReading(
        String messageId,
        String deviceId,
        double temperature,
        double humidity,
        Timestamp createdAt,
        String deliveryMode
) {
}

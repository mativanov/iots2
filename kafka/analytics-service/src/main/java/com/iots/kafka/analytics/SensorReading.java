package com.iots.kafka.analytics;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/** Incoming reading payload; only temperature is needed for the window stats. */
@JsonIgnoreProperties(ignoreUnknown = true)
public record SensorReading(
        String messageId,
        String deviceId,
        Double temperature,
        Double humidity,
        String createdAt
) {
}

package com.iots.kafka.ingestion;

/**
 * A normalized row from the IoT CSV dataset (device id, temperature, humidity,
 * source timestamp). Mirrors the normalized shape produced by the shared
 * Node.js csvDatasetLoader so both broker pipelines use the same data.
 */
public record DatasetRecord(
        String deviceId,
        double temperature,
        double humidity,
        String createdAt
) {
}

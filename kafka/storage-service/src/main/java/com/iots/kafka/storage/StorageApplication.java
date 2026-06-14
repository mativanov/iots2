package com.iots.kafka.storage;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Kafka Storage Service.
 *
 * <p>Subscribes to the readings topic and persists messages to PostgreSQL using
 * batched inserts (the spec's 500-message batching optimization). Counterpart of
 * the Node.js MQTT storage service; writes to the same {@code sensor_readings}
 * table with {@code broker_type = 'kafka'}.</p>
 */
@SpringBootApplication
public class StorageApplication {

    public static void main(String[] args) {
        SpringApplication.run(StorageApplication.class, args);
    }
}

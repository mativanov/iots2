package com.iots.kafka.storage;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

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

    /**
     * Plain Jackson mapper for deserializing readings. Declared explicitly because
     * these services use the core spring-boot-starter (not the web starter), so
     * the JSON auto-configuration that would otherwise provide this bean is absent.
     */
    @Bean
    public ObjectMapper objectMapper() {
        return new ObjectMapper();
    }
}

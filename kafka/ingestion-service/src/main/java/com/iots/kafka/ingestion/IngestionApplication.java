package com.iots.kafka.ingestion;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

/**
 * Kafka Ingestion Service.
 *
 * <p>Simulates IoT devices: loads the shared CSV dataset, generates sensor
 * readings and produces them to a Kafka topic. This is the Kafka counterpart of
 * the Node.js MQTT ingestion service and intentionally keeps the same JSON
 * message format so the storage/analytics services and the PostgreSQL schema
 * stay broker-agnostic.</p>
 */
@SpringBootApplication
public class IngestionApplication {

    public static void main(String[] args) {
        SpringApplication.run(IngestionApplication.class, args);
    }

    /**
     * Plain Jackson mapper for serializing readings. Declared explicitly because
     * these services use the core spring-boot-starter (not the web starter), so
     * the JSON auto-configuration that would otherwise provide this bean is absent.
     */
    @Bean
    public ObjectMapper objectMapper() {
        return new ObjectMapper();
    }
}

package com.iots.kafka.ingestion;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

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
}

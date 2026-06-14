package com.iots.kafka.analytics;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Kafka Analytics Service.
 *
 * <p>Subscribes to the readings stream and maintains a fixed 10-second tumbling
 * window. Every window it computes the average temperature and logs a critical
 * ALERT if the average exceeds the configured threshold. Counterpart of the
 * Node.js MQTT analytics service.</p>
 */
@SpringBootApplication
@EnableScheduling
public class AnalyticsApplication {

    public static void main(String[] args) {
        SpringApplication.run(AnalyticsApplication.class, args);
    }
}

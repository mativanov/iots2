package com.iots.kafka.analytics;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Fixed (tumbling) window aggregation over the readings stream.
 *
 * <p>Incoming messages accumulate temperature sum/count; every
 * {@code WINDOW_SECONDS} the window is flushed: it logs the average temperature
 * and a critical ALERT when the average exceeds {@code ALERT_THRESHOLD}. Matches
 * the MQTT analytics service's behavior and log shape.</p>
 */
@Component
public class TumblingWindowAnalytics {

    private static final Logger log = LoggerFactory.getLogger(TumblingWindowAnalytics.class);

    private final ObjectMapper objectMapper;

    @Value("${analytics.alert-threshold}")
    private double alertThreshold;

    @Value("${analytics.window-seconds}")
    private int windowSeconds;

    // Guarded by 'lock'.
    private final Object lock = new Object();
    private long windowStart = System.currentTimeMillis();
    private long messageCount = 0;
    private double temperatureSum = 0.0;
    private long skippedMessages = 0;

    public TumblingWindowAnalytics(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @KafkaListener(
            topics = "${analytics.topic}",
            groupId = "${spring.kafka.consumer.group-id}")
    public void consume(String payload) {
        try {
            SensorReading event = objectMapper.readValue(payload, SensorReading.class);
            Double temperature = event.temperature();
            if (temperature == null || !Double.isFinite(temperature)) {
                synchronized (lock) {
                    skippedMessages++;
                }
                return;
            }
            synchronized (lock) {
                messageCount++;
                temperatureSum += temperature;
            }
        } catch (Exception e) {
            synchronized (lock) {
                skippedMessages++;
            }
            log.warn("Analytics skipped malformed message: {}", e.getMessage());
        }
    }

    @Scheduled(fixedRateString = "#{${analytics.window-seconds} * 1000}")
    public void flushWindow() {
        long count;
        double sum;
        long start;
        synchronized (lock) {
            count = messageCount;
            sum = temperatureSum;
            start = windowStart;
            messageCount = 0;
            temperatureSum = 0.0;
            windowStart = System.currentTimeMillis();
        }

        double average = count > 0 ? sum / count : 0.0;
        boolean alert = count > 0 && average > alertThreshold;

        if (alert) {
            log.warn("ALERT Kafka average temperature exceeded threshold avg={} threshold={}",
                    round(average), alertThreshold);
        }

        log.info("Kafka analytics window windowStart={} windowEnd={} messageCount={} averageTemperature={} alert={}",
                Instant.ofEpochMilli(start), Instant.ofEpochMilli(System.currentTimeMillis()),
                count, round(average), alert);
    }

    private static double round(double value) {
        return Math.round(value * 100.0) / 100.0;
    }
}

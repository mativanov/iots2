package com.iots.kafka.storage;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Consumes batches of readings and persists them to PostgreSQL.
 *
 * <p>Runs in batch listener mode: each Kafka poll is handed over as a list,
 * validated, and written in a single JDBC batch. Offsets are committed only
 * after a successful write (ack-mode BATCH); if the insert throws, the batch is
 * redelivered, giving at-least-once storage.</p>
 */
@Component
public class KafkaStorageConsumer {

    private static final Logger log = LoggerFactory.getLogger(KafkaStorageConsumer.class);

    private final SensorReadingRepository repository;
    private final ObjectMapper objectMapper;

    private long insertedTotal = 0;
    private long skippedTotal = 0;

    public KafkaStorageConsumer(SensorReadingRepository repository, ObjectMapper objectMapper) {
        this.repository = repository;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(
            topics = "${storage.topic}",
            groupId = "${spring.kafka.consumer.group-id}")
    public void consume(List<ConsumerRecord<String, String>> records) {
        List<StoredReading> batch = new ArrayList<>(records.size());

        for (ConsumerRecord<String, String> record : records) {
            StoredReading reading = toStoredReading(record);
            if (reading != null) {
                batch.add(reading);
            } else {
                skippedTotal++;
            }
        }

        // Throws on failure -> offsets not committed -> batch redelivered.
        repository.saveBatch(batch);
        insertedTotal += batch.size();

        log.info("Kafka storage batch inserted batchSize={} insertedTotal={} skippedTotal={}",
                batch.size(), insertedTotal, skippedTotal);
    }

    private StoredReading toStoredReading(ConsumerRecord<String, String> record) {
        try {
            SensorReading event = objectMapper.readValue(record.value(), SensorReading.class);

            if (event.messageId() == null || event.deviceId() == null
                    || event.temperature() == null || !Double.isFinite(event.temperature())
                    || event.humidity() == null || !Double.isFinite(event.humidity())
                    || event.createdAt() == null) {
                return null;
            }

            Timestamp createdAt;
            try {
                createdAt = Timestamp.from(Instant.parse(event.createdAt()));
            } catch (Exception e) {
                return null;
            }

            return new StoredReading(
                    event.messageId(),
                    event.deviceId(),
                    event.temperature(),
                    event.humidity(),
                    createdAt,
                    deliveryMode(record));
        } catch (Exception e) {
            log.warn("Skipped malformed message: {}", e.getMessage());
            return null;
        }
    }

    /** Derives delivery_mode from the producer's acks header (acks-0 / acks-1 / acks-all). */
    private String deliveryMode(ConsumerRecord<String, String> record) {
        Header header = record.headers().lastHeader("acks");
        if (header == null || header.value() == null) {
            return "acks-unknown";
        }
        return "acks-" + new String(header.value(), StandardCharsets.UTF_8);
    }
}

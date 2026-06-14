package com.iots.kafka.storage;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

/**
 * Batch writer for sensor readings. Uses a single JDBC batch per Kafka poll so
 * the database is not the bottleneck during high-intensity scenarios (A and C).
 */
@Repository
public class SensorReadingRepository {

    private static final String INSERT_SQL = """
            INSERT INTO sensor_readings (
                id, message_id, device_id, temperature, humidity,
                created_at, broker_type, delivery_mode, received_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """;

    private final JdbcTemplate jdbcTemplate;

    public SensorReadingRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public void saveBatch(List<StoredReading> readings) {
        if (readings.isEmpty()) {
            return;
        }

        Timestamp receivedAt = Timestamp.from(Instant.now());

        jdbcTemplate.batchUpdate(INSERT_SQL, readings, readings.size(), (ps, reading) -> {
            ps.setObject(1, UUID.randomUUID());
            ps.setString(2, reading.messageId());
            ps.setString(3, reading.deviceId());
            ps.setDouble(4, reading.temperature());
            ps.setDouble(5, reading.humidity());
            ps.setTimestamp(6, reading.createdAt());
            ps.setString(7, "kafka");
            ps.setString(8, reading.deliveryMode());
            ps.setTimestamp(9, receivedAt);
        });
    }
}

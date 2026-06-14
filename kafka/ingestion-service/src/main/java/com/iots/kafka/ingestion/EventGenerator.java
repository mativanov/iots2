package com.iots.kafka.ingestion;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

/**
 * Generates sensor reading events from normalized dataset records.
 *
 * <p>Port of the shared Node.js EventGenerator: optionally restricts the dataset
 * to the first {@code deviceCount} unique devices, then emits
 * {@code totalMessages} readings by cycling through the records, attaching a
 * fresh messageId to each.</p>
 */
public class EventGenerator {

    private final List<DatasetRecord> records;

    public EventGenerator(List<DatasetRecord> records) {
        if (records == null || records.isEmpty()) {
            throw new IllegalArgumentException("EventGenerator requires at least one dataset record.");
        }
        this.records = records;
    }

    public List<SensorReading> generateEvents(int totalMessages, int deviceCount) {
        if (totalMessages < 0) {
            throw new IllegalArgumentException("totalMessages must be a non-negative integer.");
        }

        List<DatasetRecord> pool = filterByDeviceCount(deviceCount);
        if (pool.isEmpty()) {
            throw new IllegalStateException("No dataset records available for the requested controls.");
        }

        List<SensorReading> events = new ArrayList<>(totalMessages);
        for (int i = 0; i < totalMessages; i++) {
            DatasetRecord r = pool.get(i % pool.size());
            events.add(new SensorReading(
                    UUID.randomUUID().toString(),
                    r.deviceId(),
                    r.temperature(),
                    r.humidity(),
                    r.createdAt()
            ));
        }
        return events;
    }

    private List<DatasetRecord> filterByDeviceCount(int deviceCount) {
        if (deviceCount <= 0) {
            return records;
        }

        Set<String> selected = new LinkedHashSet<>();
        for (DatasetRecord r : records) {
            if (selected.size() >= deviceCount) {
                break;
            }
            selected.add(r.deviceId());
        }

        List<DatasetRecord> filtered = new ArrayList<>();
        for (DatasetRecord r : records) {
            if (selected.contains(r.deviceId())) {
                filtered.add(r);
            }
        }
        return filtered;
    }
}

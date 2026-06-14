package com.iots.kafka.ingestion;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.List;

/**
 * Loads and normalizes the IoT CSV dataset.
 *
 * <p>Port of the shared Node.js {@code csvDatasetLoader.js}: it requires the same
 * columns ({@code sensor_id}, {@code timestamp}, {@code temperature_C},
 * {@code humidity_%}) and produces the same normalized records, so the Kafka
 * pipeline replays exactly the same data the MQTT pipeline does.</p>
 */
public final class CsvDatasetLoader {

    private static final List<String> REQUIRED_FIELDS =
            List.of("sensor_id", "timestamp", "temperature_C", "humidity_%");

    private CsvDatasetLoader() {
    }

    public static List<DatasetRecord> load(String filePath) {
        final String content;
        try {
            content = Files.readString(Path.of(filePath));
        } catch (IOException e) {
            throw new IllegalStateException("Unable to read dataset at " + filePath, e);
        }

        List<List<String>> rows = parseCsv(content);
        if (rows.isEmpty()) {
            return List.of();
        }

        List<String> headers = new ArrayList<>();
        for (String header : rows.get(0)) {
            headers.add(header.trim());
        }
        validateHeaders(headers);

        List<DatasetRecord> records = new ArrayList<>();
        for (int i = 1; i < rows.size(); i++) {
            DatasetRecord record = normalizeRow(headers, rows.get(i));
            if (record != null) {
                records.add(record);
            }
        }
        return records;
    }

    private static void validateHeaders(List<String> headers) {
        List<String> missing = new ArrayList<>();
        for (String field : REQUIRED_FIELDS) {
            if (!headers.contains(field)) {
                missing.add(field);
            }
        }
        if (!missing.isEmpty()) {
            throw new IllegalStateException("CSV is missing required fields: " + String.join(", ", missing));
        }
    }

    private static DatasetRecord normalizeRow(List<String> headers, List<String> row) {
        String deviceId = cell(headers, row, "sensor_id");
        Double temperature = toNumber(cell(headers, row, "temperature_C"));
        Double humidity = toNumber(cell(headers, row, "humidity_%"));
        String createdAt = normalizeTimestamp(cell(headers, row, "timestamp"));

        if (deviceId == null || deviceId.isEmpty()
                || temperature == null || humidity == null || createdAt == null) {
            return null;
        }
        return new DatasetRecord(deviceId, temperature, humidity, createdAt);
    }

    private static String cell(List<String> headers, List<String> row, String column) {
        int index = headers.indexOf(column);
        if (index < 0 || index >= row.size()) {
            return "";
        }
        return row.get(index).trim();
    }

    private static Double toNumber(String value) {
        if (value == null || value.isEmpty()) {
            return null;
        }
        try {
            double parsed = Double.parseDouble(value);
            return Double.isFinite(parsed) ? parsed : null;
        } catch (NumberFormatException e) {
            return null;
        }
    }

    /** Normalize to an ISO-8601 UTC instant string; supports date-only and full timestamps. */
    private static String normalizeTimestamp(String value) {
        if (value == null || value.isEmpty()) {
            return null;
        }
        String trimmed = value.trim();
        try {
            return OffsetDateTime.parse(trimmed).toInstant().toString();
        } catch (DateTimeParseException ignored) {
            // fall through
        }
        try {
            return LocalDate.parse(trimmed).atStartOfDay().toInstant(ZoneOffset.UTC).toString();
        } catch (DateTimeParseException ignored) {
            return null;
        }
    }

    /** Minimal RFC-4180-ish CSV parser supporting quoted fields and escaped quotes. */
    static List<List<String>> parseCsv(String content) {
        List<List<String>> rows = new ArrayList<>();
        List<String> row = new ArrayList<>();
        StringBuilder field = new StringBuilder();
        boolean inQuotes = false;

        for (int i = 0; i < content.length(); i++) {
            char c = content.charAt(i);
            char next = (i + 1 < content.length()) ? content.charAt(i + 1) : '\0';

            if (c == '"') {
                if (inQuotes && next == '"') {
                    field.append('"');
                    i++;
                } else {
                    inQuotes = !inQuotes;
                }
                continue;
            }

            if (c == ',' && !inQuotes) {
                row.add(field.toString());
                field.setLength(0);
                continue;
            }

            if ((c == '\n' || c == '\r') && !inQuotes) {
                if (c == '\r' && next == '\n') {
                    i++;
                }
                row.add(field.toString());
                field.setLength(0);
                if (rowHasContent(row)) {
                    rows.add(row);
                }
                row = new ArrayList<>();
                continue;
            }

            field.append(c);
        }

        if (field.length() > 0 || !row.isEmpty()) {
            row.add(field.toString());
            if (rowHasContent(row)) {
                rows.add(row);
            }
        }
        return rows;
    }

    private static boolean rowHasContent(List<String> row) {
        for (String value : row) {
            if (!value.trim().isEmpty()) {
                return true;
            }
        }
        return false;
    }
}

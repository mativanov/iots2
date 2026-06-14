package com.iots.kafka.ingestion;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/**
 * Drives one ingestion run: load dataset -> generate events -> publish to Kafka
 * at a fixed rate, then report a summary and exit (compose runs this with
 * restart: "no", like the MQTT ingestion job).
 *
 * <p>Each record is keyed by deviceId so a device's readings always land on the
 * same partition (per-device ordering). The producer's acks level is carried as
 * a message header so the storage service can label delivery_mode in PostgreSQL,
 * since acks is otherwise a producer-only setting with no consumer-visible trace.</p>
 */
@Component
public class KafkaIngestionRunner implements CommandLineRunner {

    private static final Logger log = LoggerFactory.getLogger(KafkaIngestionRunner.class);

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final ConfigurableApplicationContext context;

    @Value("${ingestion.topic}")
    private String topic;

    @Value("${ingestion.acks}")
    private String acks;

    @Value("${ingestion.total-messages}")
    private int totalMessages;

    @Value("${ingestion.messages-per-second}")
    private int messagesPerSecond;

    @Value("${ingestion.device-count}")
    private int deviceCount;

    @Value("${ingestion.dataset-path}")
    private String datasetPath;

    public KafkaIngestionRunner(KafkaTemplate<String, String> kafkaTemplate,
                                ObjectMapper objectMapper,
                                ConfigurableApplicationContext context) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
        this.context = context;
    }

    @Override
    public void run(String... args) throws Exception {
        if (messagesPerSecond < 1) {
            throw new IllegalArgumentException("MESSAGES_PER_SECOND must be >= 1.");
        }

        List<DatasetRecord> records = CsvDatasetLoader.load(datasetPath);
        EventGenerator generator = new EventGenerator(records);
        List<SensorReading> events = generator.generateEvents(totalMessages, deviceCount);

        long intervalMs = 1000L / messagesPerSecond;
        byte[] acksHeader = acks.getBytes(StandardCharsets.UTF_8);
        long startedAt = System.currentTimeMillis();
        int published = 0;

        log.info("Kafka ingestion starting topic={} acks={} totalMessages={} messagesPerSecond={} "
                        + "deviceCount={} datasetPath={} normalizedRecords={}",
                topic, acks, totalMessages, messagesPerSecond, deviceCount, datasetPath, records.size());

        try {
            for (SensorReading event : events) {
                long sendStart = System.currentTimeMillis();
                String value = objectMapper.writeValueAsString(event);

                ProducerRecord<String, String> record =
                        new ProducerRecord<>(topic, event.deviceId(), value);
                record.headers().add(new RecordHeader("acks", acksHeader));

                // Block per send so a low acks level (0/1) vs acks=all is reflected
                // in the run duration and throughput, which is the point of the benchmark.
                kafkaTemplate.send(record).get();
                published++;

                long elapsed = System.currentTimeMillis() - sendStart;
                long remaining = intervalMs - elapsed;
                if (remaining > 0 && published < totalMessages) {
                    Thread.sleep(remaining);
                }
            }
            kafkaTemplate.flush();
        } finally {
            long durationMs = System.currentTimeMillis() - startedAt;
            double seconds = durationMs / 1000.0;
            double rate = seconds > 0 ? published / seconds : published;
            log.info("Kafka ingestion summary attempted={} published={} acks={} durationMs={} approxMsgPerSec={}",
                    totalMessages, published, acks, durationMs, Math.round(rate * 100.0) / 100.0);
        }

        int code = SpringApplication.exit(context, () -> 0);
        System.exit(code);
    }
}

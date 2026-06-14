package com.iots.kafka.storage;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * Declares the readings topic with multiple partitions so partitioning and
 * consumer-lag behavior can be exercised in the benchmarks. Spring's KafkaAdmin
 * creates it on startup if it does not already exist.
 */
@Configuration
public class KafkaTopicConfig {

    @Bean
    public NewTopic readingsTopic(@Value("${storage.topic}") String topic,
                                  @Value("${storage.partitions}") int partitions) {
        return TopicBuilder.name(topic)
                .partitions(partitions)
                .replicas(1)
                .build();
    }
}

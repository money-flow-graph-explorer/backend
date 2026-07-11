package com.money_flow_graph_explorer.backend.monitor;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * Ensures the "transactions" topic exists at startup.
 * KafkaAdmin (auto-configured by spring-kafka) will create it if missing.
 */
@Configuration
public class KafkaConfig {

    @Bean
    public NewTopic transactionsTopic() {
        return TopicBuilder.name("transactions")
                .partitions(1)
                .replicas(1)
                .build();
    }
}

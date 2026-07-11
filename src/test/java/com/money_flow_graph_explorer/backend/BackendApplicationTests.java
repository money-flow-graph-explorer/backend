package com.money_flow_graph_explorer.backend;

import com.money_flow_graph_explorer.backend.account.AccountRepository;
import com.money_flow_graph_explorer.backend.monitor.TransactionEvent;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.data.neo4j.autoconfigure.DataNeo4jAutoConfiguration;
import org.springframework.boot.data.neo4j.autoconfigure.DataNeo4jReactiveRepositoriesAutoConfiguration;
import org.springframework.boot.data.neo4j.autoconfigure.DataNeo4jRepositoriesAutoConfiguration;
import org.springframework.boot.neo4j.autoconfigure.Neo4jAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.data.neo4j.core.Neo4jClient;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.boot.kafka.autoconfigure.KafkaAutoConfiguration;

import static org.mockito.Mockito.mock;

@SpringBootTest
@EnableAutoConfiguration(exclude = {
        Neo4jAutoConfiguration.class,
        DataNeo4jAutoConfiguration.class,
        DataNeo4jRepositoriesAutoConfiguration.class,
        DataNeo4jReactiveRepositoriesAutoConfiguration.class,
        KafkaAutoConfiguration.class
})
class BackendApplicationTests {

    @TestConfiguration
    static class MockNeo4jConfig {

        @Bean
        public Neo4jClient neo4jClient() {
            return mock(Neo4jClient.class);
        }

        @Bean
        public AccountRepository accountRepository() {
            return mock(AccountRepository.class);
        }

        @SuppressWarnings("unchecked")
        @Bean
        public KafkaTemplate<String, TransactionEvent> kafkaTemplate() {
            return mock(KafkaTemplate.class);
        }
    }

    @Test
    void contextLoads() {
    }
}

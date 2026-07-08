package com.money_flow_graph_explorer.backend.stats;

import lombok.RequiredArgsConstructor;
import org.springframework.data.neo4j.core.Neo4jClient;
import org.springframework.stereotype.Service;

import java.util.Map;

@Service
@RequiredArgsConstructor
public class StatsService {

    private final Neo4jClient neo4jClient;

    public StatsDto getStats() {
        long totalAccounts = toLong(neo4jClient.query(
                "MATCH (a:Account) RETURN count(a) AS c")
                .fetch().one().orElse(Map.of()).get("c"));

        long totalTransactions = toLong(neo4jClient.query(
                "MATCH ()-[t:TRANSFER]->() RETURN count(t) AS c")
                .fetch().one().orElse(Map.of()).get("c"));

        long fraudAccounts = toLong(neo4jClient.query(
                "MATCH (a:Account {isFraud: true}) RETURN count(a) AS c")
                .fetch().one().orElse(Map.of()).get("c"));

        long circularAlerts = toLong(neo4jClient.query(
                "MATCH (al:Alert {alertType: 'cycle'}) RETURN count(al) AS c")
                .fetch().one().orElse(Map.of()).get("c"));

        long fanInAlerts = toLong(neo4jClient.query(
                "MATCH (al:Alert {alertType: 'fan_in'}) RETURN count(al) AS c")
                .fetch().one().orElse(Map.of()).get("c"));

        return StatsDto.builder()
                .totalAccounts(totalAccounts)
                .totalTransactions(totalTransactions)
                .fraudAccounts(fraudAccounts)
                .circularAlerts(circularAlerts)
                .fanInAlerts(fanInAlerts)
                .build();
    }

    private Long toLong(Object v) {
        if (v == null) return 0L;
        if (v instanceof Long l) return l;
        if (v instanceof Number n) return n.longValue();
        return 0L;
    }
}

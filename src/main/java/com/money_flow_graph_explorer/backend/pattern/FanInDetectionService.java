package com.money_flow_graph_explorer.backend.pattern;

import lombok.RequiredArgsConstructor;
import org.springframework.data.neo4j.core.Neo4jClient;
import org.springframework.stereotype.Service;

import java.util.Map;

@Service
@RequiredArgsConstructor
public class FanInDetectionService {

    private final Neo4jClient neo4jClient;

    public FanInPatternResponse detect(Integer accountId) {
        String query = """
                MATCH (b:Account)-[t:TRANSFER]->(a:Account {accountId: $accountId})
                RETURN count(DISTINCT b) AS senderCount, coalesce(sum(t.amount), 0.0) AS totalAmount
                """;

        Map<String, Object> row = neo4jClient.query(query)
                .bind(accountId).to("accountId")
                .fetch().one()
                .orElse(Map.of());

        long senderCount = toLong(row.get("senderCount"));
        double totalAmount = toDouble(row.get("totalAmount"));

        return FanInPatternResponse.builder()
                .patternType("FAN_IN")
                .detected(senderCount > 1)
                .targetAccountId(accountId)
                .senderCount(senderCount)
                .totalAmount(totalAmount)
                .build();
    }

    private Long toLong(Object v) {
        if (v == null) return 0L;
        if (v instanceof Long l) return l;
        if (v instanceof Number n) return n.longValue();
        return 0L;
    }

    private Double toDouble(Object v) {
        if (v == null) return 0.0;
        if (v instanceof Double d) return d;
        if (v instanceof Number n) return n.doubleValue();
        return 0.0;
    }
}

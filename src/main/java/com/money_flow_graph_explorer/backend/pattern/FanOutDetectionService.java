package com.money_flow_graph_explorer.backend.pattern;

import lombok.RequiredArgsConstructor;
import org.springframework.data.neo4j.core.Neo4jClient;
import org.springframework.stereotype.Service;

import java.util.Map;

@Service
@RequiredArgsConstructor
public class FanOutDetectionService {

    private final Neo4jClient neo4jClient;

    public FanOutPatternResponse detect(Integer accountId) {
        String query = """
                MATCH (a:Account {accountId: $accountId})-[t:TRANSFER]->(b:Account)
                RETURN count(DISTINCT b) AS receiverCount, coalesce(sum(t.amount), 0.0) AS totalAmount
                """;

        Map<String, Object> row = neo4jClient.query(query)
                .bind(accountId).to("accountId")
                .fetch().one()
                .orElse(Map.of());

        long receiverCount = toLong(row.get("receiverCount"));
        double totalAmount = toDouble(row.get("totalAmount"));

        return FanOutPatternResponse.builder()
                .patternType("FAN_OUT")
                .detected(receiverCount > 1)
                .sourceAccountId(accountId)
                .receiverCount(receiverCount)
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

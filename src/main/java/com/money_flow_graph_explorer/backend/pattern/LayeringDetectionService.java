package com.money_flow_graph_explorer.backend.pattern;

import lombok.RequiredArgsConstructor;
import org.springframework.data.neo4j.core.Neo4jClient;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Service
@RequiredArgsConstructor
public class LayeringDetectionService {

    private final Neo4jClient neo4jClient;

    public LayeringPatternResponse detect(Integer accountId, int minDepth) {
        if (minDepth < 2) minDepth = 3;
        int maxDepth = minDepth + 2; // search up to minDepth+2 hops

        // Find simple directed paths (no repeated nodes) starting from accountId with at
        // least minDepth hops. Neo4j forbids a bound parameter in a variable-length range,
        // so the validated bounds are inlined — safe from injection.
        String query = String.format("""
                MATCH path = (start:Account {accountId: $accountId})-[:TRANSFER*%d..%d]->(end:Account)
                WHERE ALL(n IN nodes(path) WHERE size([x IN nodes(path) WHERE x = n]) = 1)
                  AND start <> end
                WITH path,
                     [n IN nodes(path) | n.accountId] AS accts,
                     length(path) AS depth,
                     reduce(s = 0.0, r IN relationships(path) | s + r.amount) AS total
                RETURN accts, depth, total
                LIMIT 20
                """, minDepth, maxDepth);
        // NOTE: no ORDER BY — ordering would force Neo4j to materialise every path in the
        // (potentially huge) variable-length expansion before applying LIMIT, blowing the
        // transaction-memory budget on dense nodes. Without it, LIMIT streams and short-circuits.

        List<Map<String, Object>> rows;
        try {
            rows = new ArrayList<>(neo4jClient.query(query)
                    .bind(accountId).to("accountId")
                    .fetch().all());
        } catch (Exception e) {
            rows = List.of();
        }

        List<LayeringPatternResponse.LayeringPath> paths = rows.stream()
                .map(r -> LayeringPatternResponse.LayeringPath.builder()
                        .accounts(toIntList(r.get("accts")))
                        .depth(toInt(r.get("depth")))
                        .totalAmount(toDouble(r.get("total")))
                        .build())
                .toList();

        return LayeringPatternResponse.builder()
                .patternType("LAYERING")
                .detected(!paths.isEmpty())
                .paths(paths)
                .build();
    }

    @SuppressWarnings("unchecked")
    private List<Integer> toIntList(Object v) {
        if (v == null) return List.of();
        if (v instanceof List<?> list) {
            return list.stream().map(e -> {
                if (e instanceof Long l) return l.intValue();
                if (e instanceof Integer i) return i;
                if (e instanceof Number n) return n.intValue();
                return 0;
            }).toList();
        }
        return List.of();
    }

    private Integer toInt(Object v) {
        if (v == null) return 0;
        if (v instanceof Long l) return l.intValue();
        if (v instanceof Integer i) return i;
        if (v instanceof Number n) return n.intValue();
        return 0;
    }

    private Double toDouble(Object v) {
        if (v == null) return 0.0;
        if (v instanceof Double d) return d;
        if (v instanceof Number n) return n.doubleValue();
        return 0.0;
    }
}

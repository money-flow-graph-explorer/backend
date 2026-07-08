package com.money_flow_graph_explorer.backend.pattern;

import lombok.RequiredArgsConstructor;
import org.springframework.data.neo4j.core.Neo4jClient;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Service
@RequiredArgsConstructor
public class CircularDetectionService {

    private final Neo4jClient neo4jClient;

    public CircularPatternResponse detect(Integer accountId, int depth) {
        if (depth < 2 || depth > 6) depth = 4;

        // Find cycles: paths starting and ending at the given account.
        // Neo4j forbids a bound parameter in a variable-length range, so the validated
        // depth (2..6) is inlined — safe from injection.
        String query = String.format("""
                MATCH path = (start:Account {accountId: $accountId})-[:TRANSFER*2..%d]->(start)
                WITH path,
                     [n IN nodes(path) | n.accountId] AS accts,
                     [r IN relationships(path) | r.txId] AS txIds,
                     reduce(s = 0.0, r IN relationships(path) | s + r.amount) AS total
                RETURN accts, txIds, total
                LIMIT 20
                """, depth);

        List<Map<String, Object>> rows;
        try {
            rows = new ArrayList<>(neo4jClient.query(query)
                    .bind(accountId).to("accountId")
                    .fetch().all());
        } catch (Exception e) {
            rows = List.of();
        }

        List<CircularPatternResponse.CircularPath> paths = rows.stream()
                .map(r -> CircularPatternResponse.CircularPath.builder()
                        .accounts(toIntList(r.get("accts")))
                        .transactionIds(toLongList(r.get("txIds")))
                        .totalAmount(toDouble(r.get("total")))
                        .build())
                .toList();

        return CircularPatternResponse.builder()
                .patternType("CIRCULAR_TRANSACTION")
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

    @SuppressWarnings("unchecked")
    private List<Long> toLongList(Object v) {
        if (v == null) return List.of();
        if (v instanceof List<?> list) {
            return list.stream().map(e -> {
                if (e instanceof Long l) return l;
                if (e instanceof Number n) return n.longValue();
                return 0L;
            }).toList();
        }
        return List.of();
    }

    private Double toDouble(Object v) {
        if (v == null) return 0.0;
        if (v instanceof Double d) return d;
        if (v instanceof Number n) return n.doubleValue();
        return 0.0;
    }
}

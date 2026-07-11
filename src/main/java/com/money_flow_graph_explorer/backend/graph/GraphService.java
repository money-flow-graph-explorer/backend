package com.money_flow_graph_explorer.backend.graph;

import com.money_flow_graph_explorer.backend.graph.dto.GraphEdgeDto;
import com.money_flow_graph_explorer.backend.graph.dto.GraphNodeDto;
import com.money_flow_graph_explorer.backend.graph.dto.GraphResponse;
import lombok.RequiredArgsConstructor;
import org.springframework.data.neo4j.core.Neo4jClient;
import org.springframework.stereotype.Service;

import java.util.*;

@Service
@RequiredArgsConstructor
public class GraphService {

    private static final int NODE_LIMIT = 300;
    private static final int EDGE_LIMIT = 600;

    private final Neo4jClient neo4jClient;

    public GraphResponse getGraph(Integer accountId, int depth) {
        if (depth < 1 || depth > 5) {
            throw new IllegalArgumentException("depth must be between 1 and 5");
        }

        // Neo4j does not allow a bound parameter in a variable-length range (*1..$depth),
        // so the validated depth (1..5) is inlined into the Cypher pattern — safe from injection.
        // No ORDER BY: ordering forces Neo4j to materialise every path before LIMIT can short-circuit,
        // which blows the transaction-memory budget on dense nodes. Without it, LIMIT streams.

        String outgoingQuery = String.format("""
                MATCH (center:Account {accountId: $accountId})
                MATCH path=(center)-[:TRANSFER*1..%d]->(:Account)
                UNWIND relationships(path) AS r
                WITH DISTINCT r
                MATCH (s:Account)-[r]->(t:Account)
                RETURN r.txId AS txId, s.accountId AS fromAccountId, t.accountId AS toAccountId,
                       r.amount AS amount, r.timestamp AS timestamp, r.isFraud AS isFraud
                LIMIT $edgeLimit
                """, depth);

        String incomingQuery = String.format("""
                MATCH (center:Account {accountId: $accountId})
                MATCH path=(:Account)-[:TRANSFER*1..%d]->(center)
                UNWIND relationships(path) AS r
                WITH DISTINCT r
                MATCH (s:Account)-[r]->(t:Account)
                RETURN r.txId AS txId, s.accountId AS fromAccountId, t.accountId AS toAccountId,
                       r.amount AS amount, r.timestamp AS timestamp, r.isFraud AS isFraud
                LIMIT $edgeLimit
                """, depth);

        // Run both directed queries and merge, de-duplicating by txId.
        // We fetch EDGE_LIMIT+1 from each so we can detect truncation after merging.
        Map<Long, Map<String, Object>> edgesByTxId = new LinkedHashMap<>();

        for (String query : List.of(outgoingQuery, incomingQuery)) {
            List<Map<String, Object>> rows;
            try {
                rows = new ArrayList<>(neo4jClient.query(query)
                        .bind(accountId).to("accountId")
                        .bind(EDGE_LIMIT + 1).to("edgeLimit")
                        .fetch().all());
            } catch (Exception e) {
                rows = List.of();
            }
            for (Map<String, Object> row : rows) {
                Long txId = toLong(row.get("txId"));
                edgesByTxId.putIfAbsent(txId, row);
                // Stop collecting early once we have enough to detect truncation
                if (edgesByTxId.size() > EDGE_LIMIT) break;
            }
            if (edgesByTxId.size() > EDGE_LIMIT) break;
        }

        boolean truncatedEdges = edgesByTxId.size() > EDGE_LIMIT;
        List<Map<String, Object>> edgeRows = new ArrayList<>(edgesByTxId.values());
        if (truncatedEdges) edgeRows = edgeRows.subList(0, EDGE_LIMIT);

        // Collect the node IDs referenced by the kept edges, plus always include the center.
        Set<Integer> nodeIdSet = new LinkedHashSet<>();
        nodeIdSet.add(accountId);
        for (Map<String, Object> row : edgeRows) {
            Integer fromId = toInt(row.get("fromAccountId"));
            Integer toId   = toInt(row.get("toAccountId"));
            if (fromId != null) nodeIdSet.add(fromId);
            if (toId   != null) nodeIdSet.add(toId);
        }

        boolean truncatedNodes = nodeIdSet.size() > NODE_LIMIT;
        List<Integer> nodeIds = new ArrayList<>(nodeIdSet);
        if (truncatedNodes) nodeIds = nodeIds.subList(0, NODE_LIMIT);
        Set<Integer> allowedNodeIds = new HashSet<>(nodeIds);

        // Fetch isFraud for the capped node set in one lookup.
        String nodeQuery = """
                MATCH (a:Account) WHERE a.accountId IN $ids
                RETURN a.accountId AS accountId, a.isFraud AS isFraud
                """;

        Map<Integer, Boolean> fraudByNodeId = new HashMap<>();
        try {
            neo4jClient.query(nodeQuery)
                    .bind(nodeIds).to("ids")
                    .fetch().all()
                    .forEach(row -> {
                        Integer nId = toInt(row.get("accountId"));
                        if (nId != null) {
                            fraudByNodeId.put(nId, (Boolean) row.getOrDefault("isFraud", false));
                        }
                    });
        } catch (Exception ignored) {
            // fraudByNodeId stays empty; nodes will default to isFraud=false below
        }

        // Build node DTOs — center is always present even with no edges.
        List<GraphNodeDto> graphNodes = new ArrayList<>();
        for (Integer nId : nodeIds) {
            graphNodes.add(GraphNodeDto.builder()
                    .id(String.valueOf(nId))
                    .label(String.valueOf(nId))
                    .type("ACCOUNT")
                    .isFraud(fraudByNodeId.getOrDefault(nId, false))
                    .build());
        }

        // Build edge DTOs, dropping any whose endpoints fell outside the capped node set.
        List<GraphEdgeDto> graphEdges = new ArrayList<>();
        for (Map<String, Object> row : edgeRows) {
            Integer fromId = toInt(row.get("fromAccountId"));
            Integer toId   = toInt(row.get("toAccountId"));
            if (fromId == null || toId == null) continue;
            if (!allowedNodeIds.contains(fromId) || !allowedNodeIds.contains(toId)) continue;
            graphEdges.add(GraphEdgeDto.builder()
                    .id(String.valueOf(toLong(row.get("txId"))))
                    .source(String.valueOf(fromId))
                    .target(String.valueOf(toId))
                    .amount(toDouble(row.get("amount")))
                    .timestamp(toInt(row.get("timestamp")))
                    .suspicious((Boolean) row.getOrDefault("isFraud", false))
                    .build());
        }

        return GraphResponse.builder()
                .nodes(graphNodes)
                .edges(graphEdges)
                .truncated(truncatedEdges || truncatedNodes)
                .build();
    }

    private Integer toInt(Object v) {
        if (v == null) return null;
        if (v instanceof Integer i) return i;
        if (v instanceof Long l) return l.intValue();
        if (v instanceof Number n) return n.intValue();
        return null;
    }

    private Long toLong(Object v) {
        if (v == null) return 0L;
        if (v instanceof Long l) return l;
        if (v instanceof Number n) return n.longValue();
        return 0L;
    }

    private Double toDouble(Object v) {
        if (v == null) return null;
        if (v instanceof Double d) return d;
        if (v instanceof Number n) return n.doubleValue();
        return null;
    }
}

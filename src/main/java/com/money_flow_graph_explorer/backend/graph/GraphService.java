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
        // so the validated depth (1..5) is inlined into the pattern — safe from injection.
        String nodesQuery = String.format("""
                MATCH (center:Account {accountId: $accountId})
                OPTIONAL MATCH (center)-[:TRANSFER*1..%d]-(neighbor:Account)
                WITH collect(DISTINCT center) + collect(DISTINCT neighbor) AS allNodes
                UNWIND allNodes AS n
                WITH DISTINCT n
                RETURN n.accountId AS accountId, n.isFraud AS isFraud
                LIMIT $nodeLimit
                """, depth);

        // Fetch nodes
        List<Map<String, Object>> nodeRows;
        try {
            nodeRows = new ArrayList<>(neo4jClient.query(nodesQuery)
                    .bind(accountId).to("accountId")
                    .bind(NODE_LIMIT + 1).to("nodeLimit")
                    .fetch().all());
        } catch (Exception e) {
            nodeRows = List.of();
        }

        boolean truncatedNodes = nodeRows.size() > NODE_LIMIT;
        if (truncatedNodes) nodeRows = nodeRows.subList(0, NODE_LIMIT);

        // Collect node IDs that are in the capped set
        Set<Integer> allowedNodeIds = new HashSet<>();
        List<GraphNodeDto> graphNodes = new ArrayList<>();
        for (Map<String, Object> row : nodeRows) {
            Integer nId = toInt(row.get("accountId"));
            if (nId != null) {
                allowedNodeIds.add(nId);
                graphNodes.add(GraphNodeDto.builder()
                        .id(String.valueOf(nId))
                        .label(String.valueOf(nId))
                        .type("ACCOUNT")
                        .isFraud((Boolean) row.getOrDefault("isFraud", false))
                        .build());
            }
        }

        // Edges are all TRANSFER relationships whose endpoints are both in the discovered
        // node set. This avoids re-enumerating variable-length paths (cheap: nodeIds is small).
        String edgesQuery = """
                MATCH (s:Account)-[r:TRANSFER]->(t:Account)
                WHERE s.accountId IN $nodeIds AND t.accountId IN $nodeIds
                RETURN r.txId AS txId, s.accountId AS fromAccountId, t.accountId AS toAccountId,
                       r.amount AS amount, r.timestamp AS timestamp, r.isFraud AS isFraud
                LIMIT $edgeLimit
                """;

        // Fetch edges
        List<Map<String, Object>> edgeRows;
        if (allowedNodeIds.isEmpty()) {
            edgeRows = List.of();
        } else {
            try {
                edgeRows = new ArrayList<>(neo4jClient.query(edgesQuery)
                        .bind(new ArrayList<>(allowedNodeIds)).to("nodeIds")
                        .bind(EDGE_LIMIT + 1).to("edgeLimit")
                        .fetch().all());
            } catch (Exception e) {
                edgeRows = List.of();
            }
        }

        boolean truncatedEdges = edgeRows.size() > EDGE_LIMIT;
        if (truncatedEdges) edgeRows = edgeRows.subList(0, EDGE_LIMIT);

        List<GraphEdgeDto> graphEdges = new ArrayList<>();
        for (Map<String, Object> row : edgeRows) {
            Integer fromId = toInt(row.get("fromAccountId"));
            Integer toId = toInt(row.get("toAccountId"));
            // Only include edges between nodes in the capped set
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
                .truncated(truncatedNodes || truncatedEdges)
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

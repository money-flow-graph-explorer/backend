package com.money_flow_graph_explorer.backend.alert;

import com.money_flow_graph_explorer.backend.alert.dto.AlertDetailDto;
import com.money_flow_graph_explorer.backend.alert.dto.AlertPageResponse;
import com.money_flow_graph_explorer.backend.alert.dto.AlertSummaryDto;
import com.money_flow_graph_explorer.backend.graph.dto.GraphEdgeDto;
import com.money_flow_graph_explorer.backend.graph.dto.GraphNodeDto;
import com.money_flow_graph_explorer.backend.graph.dto.GraphResponse;
import lombok.RequiredArgsConstructor;
import org.springframework.data.neo4j.core.Neo4jClient;
import org.springframework.stereotype.Service;

import java.util.*;

@Service
@RequiredArgsConstructor
public class AlertService {

    private final Neo4jClient neo4jClient;

    private static String toPatternType(String alertType) {
        if (alertType == null) return "UNKNOWN";
        return switch (alertType.toLowerCase()) {
            case "cycle" -> "CIRCULAR_TRANSACTION";
            case "fan_in" -> "FAN_IN";
            default -> alertType.toUpperCase();
        };
    }

    private static String toCypherAlertType(String patternType) {
        if (patternType == null) return null;
        return switch (patternType.toUpperCase()) {
            case "CIRCULAR_TRANSACTION" -> "cycle";
            case "FAN_IN" -> "fan_in";
            default -> null;
        };
    }

    public AlertPageResponse listAlerts(String patternType, int page, int size) {
        String alertTypeFilter = patternType != null ? toCypherAlertType(patternType) : null;
        int skip = page * size;

        long total;
        List<Map<String, Object>> rows;

        if (alertTypeFilter != null) {
            total = toLong(neo4jClient.query("""
                            MATCH (al:Alert {alertType: $alertType})
                            RETURN count(al) AS total
                            """)
                    .bind(alertTypeFilter).to("alertType")
                    .fetch().one().orElse(Map.of()).get("total"));

            rows = new ArrayList<>(neo4jClient.query("""
                            MATCH (al:Alert {alertType: $alertType})
                            CALL {
                              WITH al
                              MATCH ()-[t:TRANSFER {alertId: al.alertId}]->()
                              RETURN count(t) AS txCount, coalesce(sum(t.amount), 0.0) AS totalAmount
                            }
                            RETURN al.alertId AS alertId, al.alertType AS alertType, txCount, totalAmount
                            ORDER BY alertId
                            SKIP $skip LIMIT $size
                            """)
                    .bind(alertTypeFilter).to("alertType")
                    .bind(skip).to("skip")
                    .bind(size).to("size")
                    .fetch().all());
        } else {
            total = toLong(neo4jClient.query("""
                            MATCH (al:Alert)
                            RETURN count(al) AS total
                            """)
                    .fetch().one().orElse(Map.of()).get("total"));

            rows = new ArrayList<>(neo4jClient.query("""
                            MATCH (al:Alert)
                            CALL {
                              WITH al
                              MATCH ()-[t:TRANSFER {alertId: al.alertId}]->()
                              RETURN count(t) AS txCount, coalesce(sum(t.amount), 0.0) AS totalAmount
                            }
                            RETURN al.alertId AS alertId, al.alertType AS alertType, txCount, totalAmount
                            ORDER BY alertId
                            SKIP $skip LIMIT $size
                            """)
                    .bind(skip).to("skip")
                    .bind(size).to("size")
                    .fetch().all());
        }

        List<AlertSummaryDto> dtos = rows.stream()
                .map(r -> AlertSummaryDto.builder()
                        .alertId(toInt(r.get("alertId")))
                        .patternType(toPatternType((String) r.get("alertType")))
                        .txCount(toLong(r.get("txCount")))
                        .totalAmount(toDouble(r.get("totalAmount")))
                        .build())
                .toList();

        return AlertPageResponse.builder()
                .alerts(dtos)
                .page(page)
                .size(size)
                .totalElements(total)
                .build();
    }

    public AlertDetailDto getAlertDetail(Integer alertId) {
        var alertRow = neo4jClient.query("""
                MATCH (al:Alert {alertId: $alertId})
                RETURN al.alertId AS alertId, al.alertType AS alertType
                """)
                .bind(alertId).to("alertId")
                .fetch().one()
                .orElseThrow(() -> new NoSuchElementException("Alert not found: " + alertId));

        String alertType = (String) alertRow.get("alertType");
        String patternType = toPatternType(alertType);

        List<Map<String, Object>> txRows = new ArrayList<>(neo4jClient.query("""
                MATCH (s:Account)-[t:TRANSFER {alertId: $alertId}]->(d:Account)
                RETURN t.txId AS txId, s.accountId AS fromAccountId, d.accountId AS toAccountId,
                       t.amount AS amount, t.timestamp AS timestamp, t.isFraud AS isFraud
                """)
                .bind(alertId).to("alertId")
                .fetch().all());

        List<Integer> relatedTxIds = txRows.stream()
                .map(r -> toInt(r.get("txId")))
                .filter(Objects::nonNull)
                .distinct()
                .toList();

        Set<Integer> accountSet = new LinkedHashSet<>();
        for (Map<String, Object> r : txRows) {
            Integer from = toInt(r.get("fromAccountId"));
            Integer to = toInt(r.get("toAccountId"));
            if (from != null) accountSet.add(from);
            if (to != null) accountSet.add(to);
        }
        List<Integer> relatedAccounts = new ArrayList<>(accountSet);

        List<GraphNodeDto> graphNodes = relatedAccounts.stream()
                .map(id -> GraphNodeDto.builder()
                        .id(String.valueOf(id))
                        .label(String.valueOf(id))
                        .type("ACCOUNT")
                        .isFraud(false)
                        .build())
                .toList();

        List<GraphEdgeDto> graphEdges = txRows.stream()
                .map(r -> GraphEdgeDto.builder()
                        .id(String.valueOf(toLong(r.get("txId"))))
                        .source(String.valueOf(toInt(r.get("fromAccountId"))))
                        .target(String.valueOf(toInt(r.get("toAccountId"))))
                        .amount(toDouble(r.get("amount")))
                        .timestamp(toInt(r.get("timestamp")))
                        .suspicious((Boolean) r.getOrDefault("isFraud", true))
                        .build())
                .toList();

        String description = switch (patternType) {
            case "CIRCULAR_TRANSACTION" -> "Cycle typology across " + relatedAccounts.size() + " accounts.";
            case "FAN_IN" -> "Fan-in typology: " + relatedAccounts.size() + " accounts converging.";
            default -> "Alert pattern detected.";
        };

        return AlertDetailDto.builder()
                .alertId(alertId)
                .patternType(patternType)
                .description(description)
                .relatedAccounts(relatedAccounts)
                .relatedTransactions(relatedTxIds)
                .graph(GraphResponse.builder()
                        .nodes(graphNodes)
                        .edges(graphEdges)
                        .truncated(false)
                        .build())
                .build();
    }

    private Long toLong(Object v) {
        if (v == null) return 0L;
        if (v instanceof Long l) return l;
        if (v instanceof Number n) return n.longValue();
        return 0L;
    }

    private Integer toInt(Object v) {
        if (v == null) return null;
        if (v instanceof Integer i) return i;
        if (v instanceof Long l) return l.intValue();
        if (v instanceof Number n) return n.intValue();
        return null;
    }

    private Double toDouble(Object v) {
        if (v == null) return 0.0;
        if (v instanceof Double d) return d;
        if (v instanceof Number n) return n.doubleValue();
        return 0.0;
    }
}

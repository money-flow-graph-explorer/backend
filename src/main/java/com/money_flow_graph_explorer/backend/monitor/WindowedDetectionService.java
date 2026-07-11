package com.money_flow_graph_explorer.backend.monitor;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.neo4j.core.Neo4jClient;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

/**
 * Windowed AML detection.
 *
 * For each new edge (from) → (to) at timestamp T with window [T - windowSteps, T]:
 *
 *   1. FAN_IN  — count distinct senders into (to) within the window.
 *                Fires if count >= fanInMinSenders.
 *
 *   2. CYCLE   — check whether a directed path  to -[:TRANSFER*1..(L-1)]→ from
 *                exists using ONLY edges in the window.
 *                A path found means the NEW edge closes a cycle.
 *
 * Key constraints (hard-won from prior bugs):
 *   - cycleMaxHops (L) is inlined into the Cypher pattern string; Neo4j forbids
 *     a bound parameter in a variable-length range.
 *   - NO ORDER BY before LIMIT 1 on a path query — ORDER BY forces full
 *     materialisation of all paths which causes OOM on large graphs.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class WindowedDetectionService {

    private final Neo4jClient neo4jClient;
    private final MonitorProperties props;

    // ---------------------------------------------------------------

    public DetectionResult detect(TransactionEvent event) {
        int from = event.getFrom();
        int to   = event.getTo();
        int T    = event.getTimestamp();
        int lo   = T - props.getWindowSteps();

        // --- 1. cycle check (run first: more specific) ---------------
        DetectionResult cycleResult = detectCycle(from, to, T, lo);
        if (cycleResult.isLaundering()) return cycleResult;

        // --- 2. fan-in check -----------------------------------------
        DetectionResult fanInResult = detectFanIn(to, T, lo, from, event.getTxId());
        if (fanInResult.isLaundering()) return fanInResult;

        return DetectionResult.builder()
                .laundering(false)
                .patternType(null)
                .accounts(List.of())
                .txIds(List.of())
                .fraudTxIds(List.of())
                .build();
    }

    /** Amount cap for suspicious edges; when disabled (&le;0) use +inf so the filter is a no-op. */
    private double effectiveMaxAmount() {
        double max = props.getMaxSuspiciousAmount();
        return max > 0 ? max : Double.MAX_VALUE;
    }

    // ---------------------------------------------------------------
    // Cycle detection
    // ---------------------------------------------------------------

    private DetectionResult detectCycle(int from, int to, int T, int lo) {
        // L = cycleMaxHops; max relationship hops in the intermediate path = L-1
        // (the new edge itself is the L-th hop closing the ring).
        // L-1 is inlined to satisfy Neo4j variable-length range constraint.
        int maxHops = Math.max(1, props.getCycleMaxHops() - 1);

        String query = String.format("""
                MATCH path = (startNode:Account {accountId: $to})-[:TRANSFER*1..%d]->(endNode:Account {accountId: $from})
                WHERE ALL(rel IN relationships(path) WHERE rel.timestamp >= $lo AND rel.timestamp <= $T AND rel.amount <= $maxAmt)
                RETURN [n IN nodes(path) | n.accountId] AS accts,
                       [r IN relationships(path) | r.txId] AS txIds,
                       [r IN relationships(path) | r.alertId] AS alertIds
                LIMIT 1
                """, maxHops);

        try {
            var result = neo4jClient.query(query)
                    .bind(to).to("to")
                    .bind(from).to("from")
                    .bind(lo).to("lo")
                    .bind(T).to("T")
                    .bind(effectiveMaxAmount()).to("maxAmt")
                    .fetch().one();

            if (result.isPresent()) {
                Map<String, Object> row = result.get();
                List<Integer> accts    = toIntList(row.get("accts"));
                List<Long>    txIds    = toLongList(row.get("txIds"));
                List<Long>    alertIds = toLongList(row.get("alertIds"));

                List<Long> fraudTxIds = buildFraudTxIds(txIds, alertIds);

                // Prepend 'from' to complete the ring: from → [path] → from
                List<Integer> ringAccts = new ArrayList<>();
                ringAccts.add(from);
                ringAccts.addAll(accts);

                return DetectionResult.builder()
                        .laundering(true)
                        .patternType("CIRCULAR_TRANSACTION")
                        .accounts(ringAccts)
                        .txIds(txIds)
                        .fraudTxIds(fraudTxIds)
                        .build();
            }
        } catch (Exception e) {
            log.debug("Cycle query error (tx {} -> {}): {}", from, to, e.getMessage());
        }

        return DetectionResult.builder()
                .laundering(false).patternType(null)
                .accounts(List.of()).txIds(List.of()).fraudTxIds(List.of())
                .build();
    }

    // ---------------------------------------------------------------
    // Fan-in detection
    // ---------------------------------------------------------------

    private DetectionResult detectFanIn(int to, int T, int lo, int from, long txId) {
        String query = """
                MATCH (b:Account)-[r:TRANSFER]->(a:Account {accountId: $to})
                WHERE r.timestamp >= $lo AND r.timestamp <= $T AND r.amount <= $maxAmt
                RETURN count(DISTINCT b) AS senders,
                       collect(DISTINCT b.accountId) AS senderAccts,
                       collect(r.txId) AS txIds,
                       collect(r.alertId) AS alertIds
                """;

        try {
            var result = neo4jClient.query(query)
                    .bind(to).to("to")
                    .bind(lo).to("lo")
                    .bind(T).to("T")
                    .bind(effectiveMaxAmount()).to("maxAmt")
                    .fetch().one();

            if (result.isPresent()) {
                Map<String, Object> row = result.get();
                long senders = toLong(row.get("senders"));
                if (senders >= props.getFanInMinSenders()) {
                    List<Integer> accts    = new ArrayList<>(toIntList(row.get("senderAccts")));
                    if (!accts.contains(to)) accts.add(to);

                    List<Long> txIds    = toLongList(row.get("txIds"));
                    List<Long> alertIds = toLongList(row.get("alertIds"));

                    List<Long> fraudTxIds = buildFraudTxIds(txIds, alertIds);

                    return DetectionResult.builder()
                            .laundering(true)
                            .patternType("FAN_IN")
                            .accounts(accts)
                            .txIds(txIds)
                            .fraudTxIds(fraudTxIds)
                            .build();
                }
            }
        } catch (Exception e) {
            log.debug("Fan-in query error (to={}): {}", to, e.getMessage());
        }

        return DetectionResult.builder()
                .laundering(false).patternType(null)
                .accounts(List.of()).txIds(List.of()).fraudTxIds(List.of())
                .build();
    }

    // ---------------------------------------------------------------
    // Helpers
    // ---------------------------------------------------------------

    /**
     * Returns the subset of txIds whose corresponding alertId != -1 (i.e., ground-truth fraud).
     * Both lists are assumed to be parallel (same index = same edge).
     */
    private List<Long> buildFraudTxIds(List<Long> txIds, List<Long> alertIds) {
        List<Long> fraud = new ArrayList<>();
        int len = Math.min(txIds.size(), alertIds.size());
        for (int i = 0; i < len; i++) {
            if (alertIds.get(i) != -1L) {
                fraud.add(txIds.get(i));
            }
        }
        return fraud;
    }

    // ---------------------------------------------------------------
    // Type helpers (mirror pattern package conventions)
    // ---------------------------------------------------------------

    @SuppressWarnings("unchecked")
    private List<Integer> toIntList(Object v) {
        if (v == null) return List.of();
        if (v instanceof List<?> list) {
            return list.stream().map(e -> {
                if (e instanceof Long l)   return l.intValue();
                if (e instanceof Integer i) return i;
                if (e instanceof Number n)  return n.intValue();
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
                if (e instanceof Long l)  return l;
                if (e instanceof Number n) return n.longValue();
                return 0L;
            }).toList();
        }
        return List.of();
    }

    private long toLong(Object v) {
        if (v == null) return 0L;
        if (v instanceof Long l)  return l;
        if (v instanceof Number n) return n.longValue();
        return 0L;
    }
}

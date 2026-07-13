package com.money_flow_graph_explorer.backend.monitor;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.neo4j.core.Neo4jClient;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Extracts ML features from a (TransactionEvent, DetectionResult) pair for
 * training-data collection and future re-scoring.
 *
 * Feature order is STABLE — consumer code must not re-order.
 * Label-leaking fields (isFraud, alertId) are intentionally excluded.
 *
 * Graph queries use Neo4jClient directly (same client as WindowedDetectionService).
 * All graph I/O is guarded so a failed lookup produces safe zero-valued features
 * rather than propagating an exception to the Kafka listener.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class FeatureExtractor {

    private final Neo4jClient neo4jClient;
    private final MonitorProperties props;

    // Column name constants — referenced by TrainingDataWriter for the CSV header.
    static final String[] FEATURE_NAMES = {
            "pattern_is_fanin",
            "num_accounts",
            "num_txids",
            "amt_mean",
            "amt_std",
            "amt_min",
            "amt_max",
            "amt_cv",
            "trigger_amount",
            "ts_span",
            "target_in_deg",
            "target_out_deg",
            "target_resend"
    };

    /**
     * Extracts all features. Returns a {@link LinkedHashMap} to preserve insertion order.
     * Never throws; on graph errors the affected features default to 0.0.
     */
    public Map<String, Double> extract(TransactionEvent event, DetectionResult result) {
        Map<String, Double> features = new LinkedHashMap<>();

        // ---- 1. Pattern flag ----
        features.put("pattern_is_fanin",
                "FAN_IN".equals(result.getPatternType()) ? 1.0 : 0.0);

        // ---- 2. Structural counts ----
        features.put("num_accounts", (double) safeSize(result.getAccounts()));
        features.put("num_txids",    (double) safeSize(result.getTxIds()));

        // ---- 3. Amount stats over involved edges ----
        AmountStats amtStats = fetchAmountStats(result.getTxIds());
        features.put("amt_mean", amtStats.mean);
        features.put("amt_std",  amtStats.std);
        features.put("amt_min",  amtStats.min);
        features.put("amt_max",  amtStats.max);
        features.put("amt_cv",   amtStats.cv);
        features.put("ts_span",  amtStats.tsSpan);

        // ---- 4. Trigger edge ----
        features.put("trigger_amount", event.getAmount());

        // ---- 5. Target-node degree features ----
        int    to  = event.getTo();
        int    T   = event.getTimestamp();
        int    lo  = T - props.getWindowSteps();

        features.put("target_in_deg",   fetchInDeg(to, lo, T));
        features.put("target_out_deg",  fetchOutDeg(to, lo, T));
        features.put("target_resend",   fetchResend(to, lo, T));

        return features;
    }

    // ---------------------------------------------------------------
    // Graph helpers
    // ---------------------------------------------------------------

    /**
     * Fetches amounts and timestamps of involved edges in one query, then computes
     * mean, population std, min, max, cv, and ts_span.
     */
    private AmountStats fetchAmountStats(List<Long> txIds) {
        if (txIds == null || txIds.isEmpty()) {
            return AmountStats.ZERO;
        }

        String query = """
                MATCH ()-[r:TRANSFER]->()
                WHERE r.txId IN $txIds
                RETURN r.amount AS amount, r.timestamp AS ts
                """;

        try {
            List<Map<String, Object>> rows = new ArrayList<>(
                    neo4jClient.query(query)
                            .bind(txIds).to("txIds")
                            .fetch().all()
            );

            if (rows.isEmpty()) return AmountStats.ZERO;

            double min = Double.MAX_VALUE;
            double max = -Double.MAX_VALUE;
            double sum = 0.0;
            long   tsMin = Long.MAX_VALUE;
            long   tsMax = Long.MIN_VALUE;
            int    n = rows.size();

            double[] amounts = new double[n];
            for (int i = 0; i < n; i++) {
                Map<String, Object> row = rows.get(i);
                double amt = toDouble(row.get("amount"));
                long   ts  = toLong(row.get("ts"));
                amounts[i] = amt;
                sum += amt;
                if (amt < min) min = amt;
                if (amt > max) max = amt;
                if (ts < tsMin) tsMin = ts;
                if (ts > tsMax) tsMax = ts;
            }

            double mean = sum / n;

            double varSum = 0.0;
            for (double a : amounts) {
                double diff = a - mean;
                varSum += diff * diff;
            }
            double std = Math.sqrt(varSum / n);   // population std
            double cv  = (mean == 0.0) ? 0.0 : std / mean;

            long tsSpan = (tsMin == Long.MAX_VALUE) ? 0L : Math.max(0L, tsMax - tsMin);

            return new AmountStats(mean, std, min, max, cv, (double) tsSpan);

        } catch (Exception e) {
            log.debug("FeatureExtractor: amount stats query failed for txIds={}: {}", txIds, e.getMessage());
            return AmountStats.ZERO;
        }
    }

    /** Distinct senders into {@code to} within [lo, T]. */
    private double fetchInDeg(int to, int lo, int T) {
        String query = """
                MATCH (b)-[r:TRANSFER]->(a:Account {accountId: $to})
                WHERE r.timestamp >= $lo AND r.timestamp <= $T
                RETURN count(DISTINCT b) AS cnt
                """;
        try {
            return neo4jClient.query(query)
                    .bind(to).to("to")
                    .bind(lo).to("lo")
                    .bind(T).to("T")
                    .fetch().one()
                    .map(row -> (double) toLong(row.get("cnt")))
                    .orElse(0.0);
        } catch (Exception e) {
            log.debug("FeatureExtractor: in-deg query failed for to={}: {}", to, e.getMessage());
            return 0.0;
        }
    }

    /** Distinct receivers from {@code to} within [lo, T]. */
    private double fetchOutDeg(int to, int lo, int T) {
        String query = """
                MATCH (a:Account {accountId: $to})-[r:TRANSFER]->(b)
                WHERE r.timestamp >= $lo AND r.timestamp <= $T
                RETURN count(DISTINCT b) AS cnt
                """;
        try {
            return neo4jClient.query(query)
                    .bind(to).to("to")
                    .bind(lo).to("lo")
                    .bind(T).to("T")
                    .fetch().one()
                    .map(row -> (double) toLong(row.get("cnt")))
                    .orElse(0.0);
        } catch (Exception e) {
            log.debug("FeatureExtractor: out-deg query failed for to={}: {}", to, e.getMessage());
            return 0.0;
        }
    }

    /**
     * Returns 1.0 if {@code to} has ANY outgoing TRANSFER in [lo, T], 0.0 otherwise.
     * "Receives then forwards" mule signal.
     */
    private double fetchResend(int to, int lo, int T) {
        String query = """
                MATCH (a:Account {accountId: $to})-[r:TRANSFER]->()
                WHERE r.timestamp >= $lo AND r.timestamp <= $T
                RETURN count(r) > 0 AS hasResend
                LIMIT 1
                """;
        try {
            return neo4jClient.query(query)
                    .bind(to).to("to")
                    .bind(lo).to("lo")
                    .bind(T).to("T")
                    .fetch().one()
                    .map(row -> {
                        Object v = row.get("hasResend");
                        if (v instanceof Boolean b) return b ? 1.0 : 0.0;
                        // Some Neo4j drivers return a Long (0 or 1) for boolean aggregates
                        if (v instanceof Number n) return n.longValue() > 0 ? 1.0 : 0.0;
                        return 0.0;
                    })
                    .orElse(0.0);
        } catch (Exception e) {
            log.debug("FeatureExtractor: resend query failed for to={}: {}", to, e.getMessage());
            return 0.0;
        }
    }

    // ---------------------------------------------------------------
    // Type helpers
    // ---------------------------------------------------------------

    private int safeSize(List<?> list) {
        return (list == null) ? 0 : list.size();
    }

    private double toDouble(Object v) {
        if (v == null) return 0.0;
        if (v instanceof Double d)  return d;
        if (v instanceof Number n)  return n.doubleValue();
        return 0.0;
    }

    private long toLong(Object v) {
        if (v == null) return 0L;
        if (v instanceof Long l)   return l;
        if (v instanceof Number n) return n.longValue();
        return 0L;
    }

    // ---------------------------------------------------------------
    // Value object for amount stats
    // ---------------------------------------------------------------

    private record AmountStats(double mean, double std, double min, double max,
                               double cv, double tsSpan) {
        static final AmountStats ZERO = new AmountStats(0, 0, 0, 0, 0, 0);
    }
}

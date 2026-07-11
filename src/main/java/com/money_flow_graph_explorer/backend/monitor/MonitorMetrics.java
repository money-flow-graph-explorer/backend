package com.money_flow_graph_explorer.backend.monitor;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Thread-safe, in-memory metrics counters for the AML monitoring session.
 *
 * Coverage-based evaluation semantics:
 *   - An alert is a TP if it involves >= 1 ground-truth fraud edge (fraudTxIds non-empty), else FP.
 *   - recall  = coveredFraudTxIds.size() / streamedFraudTxIds.size()
 *   - fn      = streamedFraudTxIds.size() - coveredFraudTxIds.size()  (derived, not stored)
 *   - precision = tp / (tp + fp)
 *
 * Exported as-is via GET /api/monitor/metrics and as the "metrics" SSE event.
 */
@Data
public class MonitorMetrics {

    // --- cumulative counters ---
    private final AtomicLong processed    = new AtomicLong(0);
    private final AtomicLong alertsRaised = new AtomicLong(0);
    private final AtomicLong tp           = new AtomicLong(0);
    private final AtomicLong fp           = new AtomicLong(0);

    // Coverage sets — thread-safe via ConcurrentHashMap key sets
    /** All fraud txIds that have been streamed through the consumer so far. */
    private final Set<Long> streamedFraudTxIds = ConcurrentHashMap.newKeySet();
    /** Fraud txIds that are covered by at least one TP alert. */
    private final Set<Long> coveredFraudTxIds  = ConcurrentHashMap.newKeySet();

    // per-type sub-counters
    private final TypeCounters circular = new TypeCounters();
    private final TypeCounters fanIn    = new TypeCounters();

    // ---------------------------------------------------------------
    // Snapshot for JSON serialisation (returns plain longs + doubles)
    // ---------------------------------------------------------------
    public Snapshot snapshot() {
        long tpV  = tp.get();
        long fpV  = fp.get();
        long streamedCount = streamedFraudTxIds.size();
        long coveredCount  = coveredFraudTxIds.size();

        double precision = (tpV + fpV) > 0 ? (double) tpV / (tpV + fpV) : 0.0;
        double recall    = streamedCount > 0 ? (double) coveredCount / streamedCount : 0.0;
        long   fnV       = Math.max(0L, streamedCount - coveredCount);

        return new Snapshot(
                processed.get(),
                alertsRaised.get(),
                tpV, fpV, fnV,
                precision, recall,
                Map.of(
                        "CIRCULAR_TRANSACTION", circular.snapshot(),
                        "FAN_IN",               fanIn.snapshot()
                )
        );
    }

    /** Reset all counters and coverage sets (called on /reset). */
    public void reset() {
        processed.set(0);
        alertsRaised.set(0);
        tp.set(0);
        fp.set(0);
        streamedFraudTxIds.clear();
        coveredFraudTxIds.clear();
        circular.reset();
        fanIn.reset();
    }

    // ---------------------------------------------------------------

    public record Snapshot(
            long processed,
            long alertsRaised,
            long tp,
            long fp,
            long fn,
            double precision,
            double recall,
            Map<String, TypeCounters.Snapshot> byType
    ) {}

    @Data
    public static class TypeCounters {
        private final AtomicLong tp       = new AtomicLong(0);
        private final AtomicLong fp       = new AtomicLong(0);
        private final AtomicLong detected = new AtomicLong(0);

        public void reset() {
            tp.set(0); fp.set(0); detected.set(0);
        }

        public Snapshot snapshot() {
            return new Snapshot(tp.get(), fp.get(), detected.get());
        }

        public record Snapshot(long tp, long fp, long detected) {}
    }
}

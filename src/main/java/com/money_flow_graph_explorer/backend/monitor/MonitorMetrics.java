package com.money_flow_graph_explorer.backend.monitor;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Thread-safe, in-memory metrics counters for the AML monitoring session.
 * Exported as-is via GET /api/monitor/metrics and as the "metrics" SSE event.
 */
@Data
public class MonitorMetrics {

    // --- cumulative counters (atomic for concurrent producer/consumer) ---
    private final AtomicLong processed    = new AtomicLong(0);
    private final AtomicLong alertsRaised = new AtomicLong(0);
    private final AtomicLong tp           = new AtomicLong(0);
    private final AtomicLong fp           = new AtomicLong(0);
    private final AtomicLong fn           = new AtomicLong(0);
    private final AtomicLong tn           = new AtomicLong(0);

    // per-type sub-counters
    private final TypeCounters circular = new TypeCounters();
    private final TypeCounters fanIn    = new TypeCounters();

    // ---------------------------------------------------------------
    // Snapshot for JSON serialisation (returns plain longs + doubles)
    // ---------------------------------------------------------------
    public Snapshot snapshot() {
        long tpV  = tp.get();
        long fpV  = fp.get();
        long fnV  = fn.get();

        double precision = (tpV + fpV) > 0 ? (double) tpV / (tpV + fpV) : 0.0;
        double recall    = (tpV + fnV) > 0 ? (double) tpV / (tpV + fnV) : 0.0;

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

    /** Reset all counters (called on /reset). */
    public void reset() {
        processed.set(0);
        alertsRaised.set(0);
        tp.set(0); fp.set(0); fn.set(0); tn.set(0);
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

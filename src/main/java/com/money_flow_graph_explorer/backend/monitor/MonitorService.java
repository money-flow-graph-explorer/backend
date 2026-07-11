package com.money_flow_graph_explorer.backend.monitor;

import tools.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Central hub for SSE emitters and in-memory metrics.
 * Thread-safe: emitters stored in CopyOnWriteArrayList;
 * broadcast() prunes dead emitters.
 *
 * Coverage-based evaluation:
 *   - recordStreamedTx()   — called once per consumed transaction to register ground-truth.
 *   - recordDetection()    — called when detection fires; classifies alert as TP or FP
 *                             based on whether any involved edge is ground-truth fraud.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class MonitorService {

    private final ObjectMapper objectMapper;

    private final CopyOnWriteArrayList<SseEmitter> emitters = new CopyOnWriteArrayList<>();
    private final MonitorMetrics metrics = new MonitorMetrics();

    // ---------------------------------------------------------------
    // SSE emitter registry
    // ---------------------------------------------------------------

    public SseEmitter createEmitter() {
        SseEmitter emitter = new SseEmitter(Long.MAX_VALUE);
        emitters.add(emitter);
        emitter.onCompletion(() -> emitters.remove(emitter));
        emitter.onTimeout(()    -> emitters.remove(emitter));
        emitter.onError(e      -> emitters.remove(emitter));
        return emitter;
    }

    // ---------------------------------------------------------------
    // Broadcast helpers
    // ---------------------------------------------------------------

    /**
     * Sends a named SSE event to every registered emitter.
     * Dead emitters are pruned silently.
     */
    public void broadcast(String eventName, Object payload) {
        String json;
        try {
            json = objectMapper.writeValueAsString(payload);
        } catch (Exception e) {
            log.warn("Failed to serialise SSE payload for event '{}': {}", eventName, e.getMessage());
            return;
        }

        List<SseEmitter> dead = new ArrayList<>();
        for (SseEmitter emitter : emitters) {
            try {
                emitter.send(SseEmitter.event()
                        .name(eventName)
                        .data(json));
            } catch (Exception e) {
                dead.add(emitter);
            }
        }
        emitters.removeAll(dead);
    }

    // ---------------------------------------------------------------
    // Metrics
    // ---------------------------------------------------------------

    public MonitorMetrics getMetrics() {
        return metrics;
    }

    public MonitorMetrics.Snapshot getMetricsSnapshot() {
        return metrics.snapshot();
    }

    /**
     * Called once per consumed transaction (regardless of detection).
     * Increments the processed counter and, if the event is ground-truth fraud,
     * registers its txId in {@code streamedFraudTxIds}.
     *
     * @param event the transaction event just consumed from Kafka
     */
    public void recordStreamedTx(TransactionEvent event) {
        metrics.getProcessed().incrementAndGet();
        if (event.getAlertId() != -1) {
            metrics.getStreamedFraudTxIds().add(event.getTxId());
        }
    }

    /**
     * Called when a detection alert fires.
     * An alert is a <b>true positive</b> if {@code result.getFraudTxIds()} is non-empty
     * (at least one involved edge is ground-truth fraud), else <b>false positive</b>.
     * All covered fraud txIds are added to {@code coveredFraudTxIds} for recall tracking.
     *
     * @param result      the DetectionResult that triggered the alert
     * @param patternType the pattern type string (already available on result, passed explicitly for clarity)
     * @return true if this alert was a true positive
     */
    public boolean recordDetection(DetectionResult result, String patternType) {
        boolean isTP = !result.getFraudTxIds().isEmpty();

        metrics.getAlertsRaised().incrementAndGet();

        if (isTP) {
            metrics.getTp().incrementAndGet();
            metrics.getCoveredFraudTxIds().addAll(result.getFraudTxIds());
        } else {
            metrics.getFp().incrementAndGet();
        }

        if ("CIRCULAR_TRANSACTION".equals(patternType)) {
            metrics.getCircular().getDetected().incrementAndGet();
            if (isTP) metrics.getCircular().getTp().incrementAndGet();
            else       metrics.getCircular().getFp().incrementAndGet();
        } else if ("FAN_IN".equals(patternType)) {
            metrics.getFanIn().getDetected().incrementAndGet();
            if (isTP) metrics.getFanIn().getTp().incrementAndGet();
            else       metrics.getFanIn().getFp().incrementAndGet();
        }

        return isTP;
    }

    /** Clears all metrics counters and coverage sets (called from /reset). */
    public void resetMetrics() {
        metrics.reset();
    }

    /** Total number of active SSE clients (for diagnostics). */
    public int activeEmitters() {
        return emitters.size();
    }
}

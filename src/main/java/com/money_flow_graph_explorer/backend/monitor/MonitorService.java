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
     * Record one processed transaction and update TP/FP/FN/TN.
     *
     * @param predicted true = detection fired
     * @param truth     true = ground-truth fraud (alertId != -1)
     * @param patternType the pattern that fired, or null
     */
    public void recordEvaluation(boolean predicted, boolean truth, String patternType) {
        metrics.getProcessed().incrementAndGet();

        if (predicted && truth)       { metrics.getTp().incrementAndGet(); }
        else if (predicted)           { metrics.getFp().incrementAndGet(); }
        else if (truth)               { metrics.getFn().incrementAndGet(); }
        else                          { metrics.getTn().incrementAndGet(); }

        if (predicted) {
            metrics.getAlertsRaised().incrementAndGet();
            if ("CIRCULAR_TRANSACTION".equals(patternType)) {
                metrics.getCircular().getDetected().incrementAndGet();
                if (truth) metrics.getCircular().getTp().incrementAndGet();
                else       metrics.getCircular().getFp().incrementAndGet();
            } else if ("FAN_IN".equals(patternType)) {
                metrics.getFanIn().getDetected().incrementAndGet();
                if (truth) metrics.getFanIn().getTp().incrementAndGet();
                else       metrics.getFanIn().getFp().incrementAndGet();
            }
        }
    }

    /** Clears metrics counters (called from /reset). */
    public void resetMetrics() {
        metrics.reset();
    }

    /** Total number of active SSE clients (for diagnostics). */
    public int activeEmitters() {
        return emitters.size();
    }
}

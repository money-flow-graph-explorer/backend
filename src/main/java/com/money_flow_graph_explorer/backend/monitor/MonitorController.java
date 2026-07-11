package com.money_flow_graph_explorer.backend.monitor;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.neo4j.core.Neo4jClient;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.util.Map;

/**
 * REST + SSE controller for the real-time AML monitoring pipeline.
 *
 * Endpoints:
 *   POST /api/monitor/start   — begin CSV replay (query params: rate, limit, maxStep)
 *   POST /api/monitor/stop    — stop replay
 *   POST /api/monitor/reset   — delete TRANSFER edges, clear metrics
 *   GET  /api/monitor/metrics — current MonitorMetrics snapshot
 *   GET  /api/monitor/stream  — SSE stream (events: transaction, alert, metrics)
 */
@Slf4j
@RestController
@RequestMapping("/api/monitor")
@RequiredArgsConstructor
public class MonitorController {

    private final ProducerService producerService;
    private final MonitorService  monitorService;
    private final Neo4jClient     neo4jClient;

    // ---------------------------------------------------------------
    // POST /start
    // ---------------------------------------------------------------

    @PostMapping("/start")
    public Map<String, Object> start(
            @RequestParam(defaultValue = "10")   int rate,
            @RequestParam(defaultValue = "5000") int limit,
            @RequestParam(required = false, defaultValue = "0") int maxStep
    ) {
        producerService.start(rate, limit, maxStep);
        return Map.of(
                "status",  "started",
                "rate",    rate,
                "limit",   limit,
                "maxStep", maxStep
        );
    }

    // ---------------------------------------------------------------
    // POST /stop
    // ---------------------------------------------------------------

    @PostMapping("/stop")
    public Map<String, Object> stop() {
        producerService.stop();
        return Map.of("status", "stopped");
    }

    // ---------------------------------------------------------------
    // POST /reset
    // ---------------------------------------------------------------

    /**
     * Deletes all TRANSFER relationships (keeps Account + Alert nodes).
     * Also resets in-memory metrics.
     */
    @PostMapping("/reset")
    public Map<String, Object> reset() {
        // Stop producer first so no new edges are written during deletion
        producerService.stop();

        long transfersDeleted = 0;
        long accounts = 0;

        // Delete TRANSFER edges in bounded batches with plain Cypher. A single DELETE of the
        // full ~1.3M relationships exceeds Neo4j's transaction-memory limit, and APOC is not
        // installed in the community image. Neo4jClient can't run `CALL {} IN TRANSACTIONS`
        // (it opens a managed tx), so we loop, each iteration deleting a bounded chunk.
        try {
            long deleted;
            do {
                var res = neo4jClient.query("""
                        MATCH ()-[r:TRANSFER]->()
                        WITH r LIMIT 20000
                        DELETE r
                        RETURN count(*) AS cnt
                        """)
                        .fetch().one();
                deleted = (res.isPresent() && res.get().get("cnt") instanceof Number n) ? n.longValue() : 0;
                transfersDeleted += deleted;
            } while (deleted > 0);
        } catch (Exception e) {
            log.warn("Transfer deletion error: {}", e.getMessage());
        }

        try {
            var accResult = neo4jClient.query("""
                    MATCH (a:Account)
                    RETURN count(a) AS cnt
                    """)
                    .fetch().one();
            if (accResult.isPresent()) {
                Object cnt = accResult.get().get("cnt");
                if (cnt instanceof Number n) accounts = n.longValue();
            }
        } catch (Exception e) {
            log.warn("Account count error: {}", e.getMessage());
        }

        monitorService.resetMetrics();

        return Map.of(
                "transfersDeleted", transfersDeleted,
                "accounts",         accounts
        );
    }

    // ---------------------------------------------------------------
    // GET /metrics
    // ---------------------------------------------------------------

    @GetMapping("/metrics")
    public MonitorMetrics.Snapshot metrics() {
        return monitorService.getMetricsSnapshot();
    }

    // ---------------------------------------------------------------
    // GET /stream  (SSE)
    // ---------------------------------------------------------------

    @GetMapping(path = "/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public SseEmitter stream() {
        SseEmitter emitter = monitorService.createEmitter();
        log.debug("New SSE client connected (total={})", monitorService.activeEmitters());
        return emitter;
    }
}

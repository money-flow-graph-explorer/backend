package com.money_flow_graph_explorer.backend.monitor;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.neo4j.core.Neo4jClient;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * REST + SSE controller for the real-time AML monitoring pipeline.
 *
 * Endpoints:
 *   POST /api/monitor/start   — begin CSV replay (query params: rate, limit, maxStep)
 *   POST /api/monitor/stop    — stop replay
 *   POST /api/monitor/reset   — delete TRANSFER edges, clear metrics
 *   GET  /api/monitor/metrics — current MonitorMetrics snapshot
 *   GET  /api/monitor/stream  — SSE stream (events: transaction, alert, metrics)
 *   GET  /api/monitor/missed  — currently-uncovered ground-truth-fraud transactions (the fn set)
 *   GET  /api/monitor/settings — current detection thresholds (windowSteps, fanInMinSenders, ...)
 *   PUT  /api/monitor/settings — update detection thresholds; applied to the very next event,
 *                                even mid-replay — see {@link MonitorProperties}
 */
@Slf4j
@RestController
@RequestMapping("/api/monitor")
@RequiredArgsConstructor
public class MonitorController {

    private final ProducerService producerService;
    private final MonitorService  monitorService;
    private final Neo4jClient     neo4jClient;
    private final MonitorProperties props;

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

    // ---------------------------------------------------------------
    // GET/PUT /settings — live-tunable detection thresholds
    // ---------------------------------------------------------------

    /**
     * {@link WindowedDetectionService} reads these fields fresh on every event (never
     * caches them), so an update here takes effect starting with the very next
     * transaction processed — no restart, and a replay already in progress picks up
     * the new thresholds mid-stream.
     */
    @GetMapping("/settings")
    public MonitorSettingsDto getSettings() {
        return MonitorSettingsDto.from(props);
    }

    /** Fields omitted (null) in the request body are left unchanged. */
    @PutMapping("/settings")
    public MonitorSettingsDto updateSettings(@RequestBody MonitorSettingsDto req) {
        if (req.windowSteps() != null) {
            if (req.windowSteps() < 1) throw new IllegalArgumentException("windowSteps must be >= 1");
            props.setWindowSteps(req.windowSteps());
        }
        if (req.fanInMinSenders() != null) {
            if (req.fanInMinSenders() < 2) throw new IllegalArgumentException("fanInMinSenders must be >= 2");
            props.setFanInMinSenders(req.fanInMinSenders());
        }
        if (req.cycleMaxHops() != null) {
            if (req.cycleMaxHops() < 2) throw new IllegalArgumentException("cycleMaxHops must be >= 2");
            props.setCycleMaxHops(req.cycleMaxHops());
        }
        if (req.maxSuspiciousAmount() != null) {
            if (req.maxSuspiciousAmount() < 0) throw new IllegalArgumentException("maxSuspiciousAmount must be >= 0");
            props.setMaxSuspiciousAmount(req.maxSuspiciousAmount());
        }
        if (req.amountEqualityTolerance() != null) {
            if (req.amountEqualityTolerance() < 0) throw new IllegalArgumentException("amountEqualityTolerance must be >= 0");
            props.setAmountEqualityTolerance(req.amountEqualityTolerance());
        }
        MonitorSettingsDto updated = MonitorSettingsDto.from(props);
        log.info("Monitor detection settings updated: {}", updated);
        return updated;
    }

    public record MonitorSettingsDto(
            Integer windowSteps,
            Integer fanInMinSenders,
            Integer cycleMaxHops,
            Double maxSuspiciousAmount,
            Double amountEqualityTolerance
    ) {
        static MonitorSettingsDto from(MonitorProperties props) {
            return new MonitorSettingsDto(
                    props.getWindowSteps(),
                    props.getFanInMinSenders(),
                    props.getCycleMaxHops(),
                    props.getMaxSuspiciousAmount(),
                    props.getAmountEqualityTolerance()
            );
        }
    }

    // ---------------------------------------------------------------
    // GET /missed  — the actual FN case list backing the `fn` metric
    // ---------------------------------------------------------------

    /**
     * Returns the currently-uncovered ground-truth-fraud transactions (the exact set the
     * {@code fn} metric counts). Unlike the SSE stream, this reflects full session history
     * regardless of when the client connected — a client that joins mid-session or
     * reconnects after a drop never receives earlier "transaction" broadcasts, so its
     * local buffers can under-count relative to this endpoint.
     */
    @GetMapping("/missed")
    public List<MissedTransactionDto> missed() {
        Set<Long> txIds = monitorService.getMissedFraudTxIds();
        if (txIds.isEmpty()) return List.of();

        String query = """
                MATCH (s:Account)-[r:TRANSFER]->(t:Account)
                WHERE r.txId IN $txIds
                RETURN r.txId AS txId, s.accountId AS fromId, t.accountId AS toId,
                       r.amount AS amount, r.timestamp AS timestamp,
                       r.isFraud AS isFraud, r.alertId AS alertId
                """;

        List<MissedTransactionDto> result = new ArrayList<>();
        try {
            for (Map<String, Object> row : neo4jClient.query(query).bind(txIds).to("txIds").fetch().all()) {
                result.add(new MissedTransactionDto(
                        toLong(row.get("txId")),
                        toInt(row.get("fromId")),
                        toInt(row.get("toId")),
                        toDouble(row.get("amount")),
                        toInt(row.get("timestamp")),
                        row.get("isFraud") instanceof Boolean b && b,
                        toInt(row.get("alertId"))
                ));
            }
        } catch (Exception e) {
            log.warn("Missed-transaction lookup failed: {}", e.getMessage());
            return List.of();
        }
        result.sort((a, b) -> Long.compare(b.txId(), a.txId()));
        return result;
    }

    /** Mirrors the frontend's MonitorTransaction shape; laundering is always false (uncovered by definition). */
    public record MissedTransactionDto(long txId, int from, int to, double amount, int timestamp,
                                        boolean isFraud, int alertId) {}

    private long toLong(Object v) {
        return (v instanceof Number n) ? n.longValue() : 0L;
    }

    private int toInt(Object v) {
        return (v instanceof Number n) ? n.intValue() : 0;
    }

    private double toDouble(Object v) {
        return (v instanceof Number n) ? n.doubleValue() : 0.0;
    }
}

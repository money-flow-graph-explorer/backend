package com.money_flow_graph_explorer.backend.monitor;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

/**
 * Kafka consumer that processes each transaction event:
 *   1. Runs windowed fan_in + cycle detection on the Neo4j graph.
 *   2. Evaluates result against ground-truth labels.
 *   3. Updates metrics + broadcasts SSE events to the frontend.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class DetectionConsumer {

    private final WindowedDetectionService detectionService;
    private final MonitorService monitorService;

    @KafkaListener(topics = "transactions", groupId = "aml-monitor")
    public void consume(TransactionEvent event) {
        try {
            process(event);
        } catch (Exception e) {
            log.error("Error processing transaction {}: {}", event.getTxId(), e.getMessage(), e);
        }
    }

    private void process(TransactionEvent event) {
        // ----- 1. Detection -----
        DetectionResult result = detectionService.detect(event);

        // ----- 2. Ground-truth evaluation -----
        boolean predicted = result.isLaundering();
        boolean truth     = event.getAlertId() != -1;   // alertId == -1 means normal

        monitorService.recordEvaluation(predicted, truth, result.getPatternType());

        // ----- 3. Broadcast "transaction" SSE event -----
        Map<String, Object> txPayload = new HashMap<>();
        txPayload.put("txId",        event.getTxId());
        txPayload.put("from",        event.getFrom());
        txPayload.put("to",          event.getTo());
        txPayload.put("amount",      event.getAmount());
        txPayload.put("timestamp",   event.getTimestamp());
        txPayload.put("isFraud",     event.isFraud());
        txPayload.put("alertId",     event.getAlertId());
        txPayload.put("laundering",  predicted);
        txPayload.put("patternType", result.getPatternType());
        monitorService.broadcast("transaction", txPayload);

        // ----- 4. Broadcast "alert" SSE event (laundering only) -----
        if (predicted) {
            boolean correct = truth; // TP if truth positive, FP if truth negative
            Map<String, Object> alertPayload = new HashMap<>();
            alertPayload.put("patternType", result.getPatternType());
            alertPayload.put("accounts",    result.getAccounts());
            alertPayload.put("txIds",       result.getTxIds());
            alertPayload.put("correct",     correct);
            alertPayload.put("timestamp",   event.getTimestamp());
            monitorService.broadcast("alert", alertPayload);
        }

        // ----- 5. Broadcast "metrics" SSE event -----
        monitorService.broadcast("metrics", monitorService.getMetricsSnapshot());
    }
}

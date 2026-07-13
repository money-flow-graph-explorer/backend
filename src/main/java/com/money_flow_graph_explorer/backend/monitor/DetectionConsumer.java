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
 *   2. Optionally re-scores rule candidates through the XGBoost model service
 *      (gated by {@code monitor.model.enabled}).  A rule candidate that the model
 *      scores below {@code monitor.model.threshold} is suppressed — no detection is
 *      recorded and no alert is emitted — reducing false positives at the cost of
 *      some recall.  When the model is disabled the rule decision is used directly.
 *   3. Records ground-truth coverage (every tx) and alert classification (confirmed
 *      detections only).
 *   4. Updates metrics + broadcasts SSE events to the frontend.
 *   5. (Optional) When {@code monitor.model.collectTrainingData} is true, appends a
 *      feature row to the training-data CSV for every rule candidate that fires.
 *      Collect mode does NOT alter detection behavior or metric recording.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class DetectionConsumer {

    private final WindowedDetectionService detectionService;
    private final MonitorService           monitorService;
    private final MonitorProperties        props;
    private final FeatureExtractor         featureExtractor;
    private final TrainingDataWriter       trainingDataWriter;
    private final ModelClient              modelClient;

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

        // ----- 2. Coverage-based ground-truth evaluation -----
        // Always record the streamed transaction regardless of detection outcome.
        monitorService.recordStreamedTx(event);

        // ----- 3. Training-data collect mode (rule candidates only, additive, config-gated) -----
        // Logs ALL rule candidates with their rule label BEFORE any model gating.
        // Intentionally not gated on the model decision so the collected rows reflect
        // unfiltered rule output and can be used to train / evaluate the model itself.
        if (result.isLaundering() && props.getModel().isCollectTrainingData()) {
            collectTrainingRow(event, result);
        }

        // ----- 4. Re-score filter -----
        boolean ruleFired = result.isLaundering();
        double  modelScore = -1.0;
        boolean confirmed;

        if (ruleFired && props.getModel().isEnabled()) {
            modelScore = modelClient.predict(featureExtractor.extract(event, result));
            confirmed  = modelScore >= props.getModel().getThreshold();
            if (!confirmed) {
                log.debug("ModelClient suppressed rule candidate tx={} score={} threshold={}",
                        event.getTxId(), modelScore, props.getModel().getThreshold());
            }
        } else {
            // Model disabled — rule decision is authoritative.
            confirmed = ruleFired;
        }

        // ----- 5. Broadcast "transaction" SSE event -----
        Map<String, Object> txPayload = new HashMap<>();
        txPayload.put("txId",        event.getTxId());
        txPayload.put("from",        event.getFrom());
        txPayload.put("to",          event.getTo());
        txPayload.put("amount",      event.getAmount());
        txPayload.put("timestamp",   event.getTimestamp());
        txPayload.put("isFraud",     event.isFraud());
        txPayload.put("alertId",     event.getAlertId());
        txPayload.put("laundering",  confirmed);
        txPayload.put("patternType", confirmed ? result.getPatternType() : null);
        monitorService.broadcast("transaction", txPayload);

        // ----- 6. Record detection + broadcast "alert" SSE event (confirmed only) -----
        if (confirmed) {
            boolean correct = monitorService.recordDetection(result, result.getPatternType());

            Map<String, Object> alertPayload = new HashMap<>();
            alertPayload.put("patternType", result.getPatternType());
            alertPayload.put("accounts",    result.getAccounts());
            alertPayload.put("txIds",       result.getTxIds());
            alertPayload.put("correct",     correct);
            alertPayload.put("timestamp",   event.getTimestamp());
            alertPayload.put("modelScore",  modelScore);
            monitorService.broadcast("alert", alertPayload);
        }

        // ----- 7. Broadcast "metrics" SSE event -----
        monitorService.broadcast("metrics", monitorService.getMetricsSnapshot());
    }

    /**
     * Extracts features and appends a CSV row to the training-data file.
     * Errors are caught and logged; they must never interrupt the Kafka listener.
     */
    private void collectTrainingRow(TransactionEvent event, DetectionResult result) {
        try {
            Map<String, Double> features = featureExtractor.extract(event, result);
            trainingDataWriter.append(event, result, features);
        } catch (Exception e) {
            log.error("collect-mode: failed to write training row for tx {}: {}",
                    event.getTxId(), e.getMessage(), e);
        }
    }
}

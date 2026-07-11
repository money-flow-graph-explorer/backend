package com.money_flow_graph_explorer.backend.monitor;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Kafka message payload for a single transaction being replayed.
 * isFraud / alertId are ground-truth labels — only used for evaluation,
 * never for detection logic.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class TransactionEvent {

    private long txId;
    private int from;
    private int to;
    private double amount;
    private int timestamp;
    /** Ground-truth fraud flag — evaluation only. */
    private boolean isFraud;
    /** Ground-truth alert id (-1 = not fraud) — evaluation only. */
    private int alertId;
}

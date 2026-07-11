package com.money_flow_graph_explorer.backend.monitor;

import lombok.Builder;
import lombok.Value;

import java.util.List;

/**
 * Outcome of windowed detection for a single transaction.
 */
@Value
@Builder
public class DetectionResult {

    boolean laundering;

    /** "CIRCULAR_TRANSACTION", "FAN_IN", or null */
    String patternType;

    List<Integer> accounts;
    List<Long>    txIds;
}

package com.money_flow_graph_explorer.backend.monitor;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Data
@Component
@ConfigurationProperties(prefix = "monitor")
public class MonitorProperties {

    /** Sliding window width (in timestamp steps). */
    private int windowSteps = 7;

    /** Minimum distinct senders to trigger fan-in alert. */
    private int fanInMinSenders = 3;

    /** Maximum hop count for cycle detection. */
    private int cycleMaxHops = 6;

    /**
     * Only edges with amount &le; this are considered suspicious (laundering in this dataset
     * is structured into tiny amounts). 0 disables the filter.
     */
    private double maxSuspiciousAmount = 0;

    /**
     * Maximum absolute spread (max − min) among the amounts in a candidate pattern
     * for them to be considered "near-equal". Applies to both cycle and fan-in detectors.
     */
    private double amountEqualityTolerance = 1.0;

    /** Path to the transactions CSV (relative to working directory). */
    private String csvPath = "../data/transactions.csv";
}

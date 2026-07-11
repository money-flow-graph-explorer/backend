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

    /** Path to the transactions CSV (relative to working directory). */
    private String csvPath = "../data/transactions.csv";
}

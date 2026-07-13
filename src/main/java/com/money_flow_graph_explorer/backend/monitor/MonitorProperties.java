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

    /** XGBoost re-score model config (step 2 wiring not yet implemented). */
    private Model model = new Model();

    @Data
    public static class Model {

        /** Enable model re-scoring (not yet wired — reserved for step 2). */
        private boolean enabled = false;

        /** Base URL of the model inference service. */
        private String url = "http://localhost:8000";

        /** Score threshold above which an alert is confirmed by the model. */
        private double threshold = 0.5;

        /**
         * When true, FeatureExtractor is invoked on every rule candidate and the
         * resulting feature row is appended to {@link #trainingDataPath}.
         * Does NOT suppress or alter detection behaviour.
         */
        private boolean collectTrainingData = false;

        /** Path to the training-data CSV (relative to the service working directory). */
        private String trainingDataPath = "../data/training_candidates.csv";
    }
}

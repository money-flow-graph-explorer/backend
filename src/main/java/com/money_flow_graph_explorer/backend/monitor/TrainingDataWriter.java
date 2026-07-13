package com.money_flow_graph_explorer.backend.monitor;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

/**
 * Thread-safe CSV writer for ML training-data collection.
 *
 * A single {@link PrintWriter} (in append mode) is opened lazily on the first
 * write call so that the bean loads cleanly in the test context — no file I/O
 * occurs unless collect mode is actually used.
 *
 * The writer itself is guarded by {@code synchronized} because the Kafka
 * listener may be called concurrently by multiple consumer threads.
 *
 * Column order (stable, matches {@link FeatureExtractor#FEATURE_NAMES}):
 * <pre>
 *   ts, label,
 *   pattern_is_fanin, num_accounts, num_txids,
 *   amt_mean, amt_std, amt_min, amt_max, amt_cv,
 *   trigger_amount, ts_span,
 *   target_in_deg, target_out_deg, target_resend
 * </pre>
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class TrainingDataWriter {

    static final String HEADER;

    static {
        StringBuilder sb = new StringBuilder("ts,label");
        for (String name : FeatureExtractor.FEATURE_NAMES) {
            sb.append(',').append(name);
        }
        HEADER = sb.toString();
    }

    private final MonitorProperties props;

    /** Lazily opened; guarded by the instance monitor. */
    private PrintWriter writer;

    /**
     * Appends one CSV row to the training-data file.
     *
     * @param event   the triggering transaction (provides {@code ts})
     * @param result  the detection result (provides {@code label})
     * @param features the ordered feature map from {@link FeatureExtractor#extract}
     */
    public synchronized void append(TransactionEvent event,
                                    DetectionResult result,
                                    Map<String, Double> features) {
        try {
            ensureWriter();

            int     ts    = event.getTimestamp();
            int     label = result.getFraudTxIds().isEmpty() ? 0 : 1;

            StringBuilder row = new StringBuilder();
            row.append(ts).append(',').append(label);
            for (String name : FeatureExtractor.FEATURE_NAMES) {
                row.append(',').append(features.getOrDefault(name, 0.0));
            }

            writer.println(row);

        } catch (IOException e) {
            log.error("TrainingDataWriter: failed to append CSV row: {}", e.getMessage(), e);
        }
    }

    /**
     * Opens (or re-opens after an error) the {@link PrintWriter} in append mode.
     * Writes the header row if the file is new or empty.
     * Must be called inside a {@code synchronized} block.
     */
    private void ensureWriter() throws IOException {
        if (writer != null) return;

        String pathStr = props.getModel().getTrainingDataPath();
        Path   path    = Path.of(pathStr);

        // Create parent directories if needed
        if (path.getParent() != null) {
            Files.createDirectories(path.getParent());
        }

        boolean needsHeader = !Files.exists(path) || Files.size(path) == 0;

        writer = new PrintWriter(new BufferedWriter(new FileWriter(path.toFile(), true)));

        if (needsHeader) {
            writer.println(HEADER);
        }

        log.info("TrainingDataWriter: opened {} (header={})", path.toAbsolutePath(), needsHeader);
    }
}

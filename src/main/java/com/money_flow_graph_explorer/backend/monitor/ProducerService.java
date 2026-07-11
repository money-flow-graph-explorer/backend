package com.money_flow_graph_explorer.backend.monitor;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.neo4j.core.Neo4jClient;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Background producer that replays transactions.csv row-by-row.
 *
 * For each row it:
 *   1. MERGEs (sender)-[:TRANSFER {props}]->(receiver) in Neo4j (commit).
 *   2. Publishes the TransactionEvent to Kafka topic "transactions".
 *   3. Sleeps `rate` ms.
 *
 * Guards against double-start via the `running` flag.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class ProducerService {

    private static final String TOPIC = "transactions";

    private final Neo4jClient neo4jClient;
    private final KafkaTemplate<String, TransactionEvent> kafkaTemplate;
    private final MonitorProperties props;

    private final ExecutorService executor = Executors.newSingleThreadExecutor(r -> {
        Thread t = new Thread(r, "monitor-producer");
        t.setDaemon(true);
        return t;
    });

    private final AtomicBoolean running = new AtomicBoolean(false);
    private volatile Future<?> currentTask;

    // ---------------------------------------------------------------

    /**
     * Start the replay loop.
     *
     * @param rateMs   milliseconds to sleep between sends (default 10)
     * @param limit    max number of rows to process (default 5000)
     * @param maxStep  optional; if > 0 only rows with timestamp <= maxStep are included
     */
    public synchronized void start(int rateMs, int limit, int maxStep) {
        if (running.get()) {
            log.info("Producer already running — ignoring duplicate start");
            return;
        }
        running.set(true);
        currentTask = executor.submit(() -> runLoop(rateMs, limit, maxStep));
    }

    public synchronized void stop() {
        running.set(false);
        if (currentTask != null) {
            currentTask.cancel(true);
            currentTask = null;
        }
    }

    // ---------------------------------------------------------------

    private void runLoop(int rateMs, int limit, int maxStep) {
        log.info("Monitor producer starting: rateMs={}, limit={}, maxStep={}", rateMs, limit, maxStep);
        try {
            List<CsvRow> rows = loadCsv(limit, maxStep);
            log.info("Loaded {} rows from {}", rows.size(), props.getCsvPath());

            for (CsvRow row : rows) {
                if (!running.get() || Thread.currentThread().isInterrupted()) break;

                // 1. Persist edge to Neo4j
                persistTransfer(row);

                // 2. Publish to Kafka
                TransactionEvent event = TransactionEvent.builder()
                        .txId(row.txId)
                        .from(row.from)
                        .to(row.to)
                        .amount(row.amount)
                        .timestamp(row.timestamp)
                        .isFraud(row.isFraud)
                        .alertId(row.alertId)
                        .build();

                kafkaTemplate.send(TOPIC, String.valueOf(row.txId), event);

                // 3. Rate limiting
                if (rateMs > 0) {
                    Thread.sleep(rateMs);
                }
            }
            log.info("Monitor producer finished.");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.info("Monitor producer interrupted.");
        } catch (Exception e) {
            log.error("Monitor producer error: {}", e.getMessage(), e);
        } finally {
            running.set(false);
        }
    }

    // ---------------------------------------------------------------
    // Neo4j persistence
    // ---------------------------------------------------------------

    private void persistTransfer(CsvRow row) {
        try {
            neo4jClient.query("""
                    MERGE (s:Account {accountId: $from})
                    MERGE (t:Account {accountId: $to})
                    MERGE (s)-[r:TRANSFER {txId: $txId}]->(t)
                    ON CREATE SET r.amount    = $amount,
                                  r.timestamp = $timestamp,
                                  r.isFraud   = $isFraud,
                                  r.alertId   = $alertId
                    """)
                    .bind(row.from).to("from")
                    .bind(row.to).to("to")
                    .bind((long) row.txId).to("txId")
                    .bind(row.amount).to("amount")
                    .bind(row.timestamp).to("timestamp")
                    .bind(row.isFraud).to("isFraud")
                    .bind(row.alertId).to("alertId")
                    .run();
        } catch (Exception e) {
            log.warn("Neo4j MERGE failed for txId={}: {}", row.txId, e.getMessage());
        }
    }

    // ---------------------------------------------------------------
    // CSV loading
    // ---------------------------------------------------------------

    private List<CsvRow> loadCsv(int limit, int maxStep) throws Exception {
        List<CsvRow> rows = new ArrayList<>();
        String path = props.getCsvPath();

        try (BufferedReader br = new BufferedReader(new FileReader(path))) {
            String header = br.readLine(); // skip header
            if (header == null) return rows;

            String line;
            while ((line = br.readLine()) != null) {
                CsvRow row = parseLine(line);
                if (row == null) continue;
                if (maxStep > 0 && row.timestamp > maxStep) continue;
                rows.add(row);
            }
        }

        // Sort by timestamp ascending (chronological replay)
        rows.sort(Comparator.comparingInt(r -> r.timestamp));

        // Apply limit
        if (limit > 0 && rows.size() > limit) {
            rows = rows.subList(0, limit);
        }

        return rows;
    }

    /**
     * Parse CSV line.
     * Column order: TX_ID,SENDER_ACCOUNT_ID,RECEIVER_ACCOUNT_ID,TX_TYPE,TX_AMOUNT,TIMESTAMP,IS_FRAUD,ALERT_ID
     */
    private CsvRow parseLine(String line) {
        try {
            String[] parts = line.split(",", -1);
            if (parts.length < 8) return null;

            long   txId      = Long.parseLong(parts[0].trim());
            int    from      = Integer.parseInt(parts[1].trim());
            int    to        = Integer.parseInt(parts[2].trim());
            // parts[3] = TX_TYPE (ignored — all TRANSFER in this dataset)
            double amount    = Double.parseDouble(parts[4].trim());
            int    timestamp = Integer.parseInt(parts[5].trim());
            boolean isFraud  = Boolean.parseBoolean(parts[6].trim()) ||
                               "1".equals(parts[6].trim()) ||
                               "true".equalsIgnoreCase(parts[6].trim());
            int    alertId   = Integer.parseInt(parts[7].trim());

            return new CsvRow(txId, from, to, amount, timestamp, isFraud, alertId);
        } catch (NumberFormatException e) {
            return null; // skip malformed lines
        }
    }

    public boolean isRunning() {
        return running.get();
    }

    // ---------------------------------------------------------------

    private record CsvRow(long txId, int from, int to, double amount,
                          int timestamp, boolean isFraud, int alertId) {}
}

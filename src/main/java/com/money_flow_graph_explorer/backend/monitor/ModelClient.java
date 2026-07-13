package com.money_flow_graph_explorer.backend.monitor;

import lombok.extern.slf4j.Slf4j;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;

import java.util.Map;

/**
 * HTTP client for the XGBoost model inference service.
 *
 * <p>Behaviour:
 * <ul>
 *   <li>Sends {@code POST {url}/predict} with body {@code {"features": {...}}} and parses
 *       {@code {"probability": <double>}} from the response.</li>
 *   <li>Uses a 1-second connect + 1-second read timeout so a slow or absent model service
 *       does not stall the Kafka consumer thread for long.</li>
 *   <li><b>Fail-open</b>: any error (timeout, connection refused, unexpected JSON shape,
 *       parse failure) is caught, logged at DEBUG level, and returns {@code 1.0} — keeping
 *       the rule candidate in the alert pipeline rather than silently suppressing it
 *       when the model is unavailable.</li>
 * </ul>
 *
 * <p>The bean is constructed eagerly at startup but never calls the network during
 * construction, so the application context loads fine whether or not the model service
 * is running.
 */
@Slf4j
@Component
public class ModelClient {

    private static final int TIMEOUT_MS = 1_000;

    private final RestClient restClient;

    public ModelClient(MonitorProperties props) {
        SimpleClientHttpRequestFactory factory = new SimpleClientHttpRequestFactory();
        factory.setConnectTimeout(TIMEOUT_MS);
        factory.setReadTimeout(TIMEOUT_MS);

        this.restClient = RestClient.builder()
                .requestFactory(factory)
                .baseUrl(props.getModel().getUrl())
                .build();
    }

    /**
     * Calls the model inference service and returns the predicted probability that a
     * transaction is laundering.
     *
     * @param features feature map produced by {@link FeatureExtractor#extract}
     * @return probability in [0.0, 1.0], or {@code 1.0} on any error (fail-open)
     */
    public double predict(Map<String, Double> features) {
        try {
            PredictRequest body = new PredictRequest(features);

            PredictResponse response = restClient.post()
                    .uri("/predict")
                    .body(body)
                    .retrieve()
                    .body(PredictResponse.class);

            if (response == null) {
                log.debug("ModelClient: null response from model service; failing open");
                return 1.0;
            }
            return response.probability();

        } catch (Exception e) {
            log.debug("ModelClient: prediction failed ({}); failing open with score=1.0",
                    e.getMessage());
            return 1.0;
        }
    }

    // ---------------------------------------------------------------
    // Request / Response value objects
    // ---------------------------------------------------------------

    /** POST body: {"features": {name: value, ...}} */
    private record PredictRequest(Map<String, Double> features) {}

    /** Response body: {"probability": <double>} */
    private record PredictResponse(double probability) {}
}

package com.money_flow_graph_explorer.backend.pattern;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class CircularPatternResponse {
    private String patternType;
    private Boolean detected;
    private List<CircularPath> paths;

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class CircularPath {
        private List<Integer> accounts;
        private List<Long> transactionIds;
        private Double totalAmount;
    }
}

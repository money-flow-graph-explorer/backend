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
public class LayeringPatternResponse {
    private String patternType;
    private Boolean detected;
    private List<LayeringPath> paths;

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class LayeringPath {
        private List<Integer> accounts;
        private Integer depth;
        private Double totalAmount;
    }
}

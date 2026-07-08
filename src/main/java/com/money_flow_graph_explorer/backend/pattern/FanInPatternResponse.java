package com.money_flow_graph_explorer.backend.pattern;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class FanInPatternResponse {
    private String patternType;
    private Boolean detected;
    private Integer targetAccountId;
    private Long senderCount;
    private Double totalAmount;
}

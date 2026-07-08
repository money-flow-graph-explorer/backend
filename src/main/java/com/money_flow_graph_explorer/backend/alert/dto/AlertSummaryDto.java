package com.money_flow_graph_explorer.backend.alert.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class AlertSummaryDto {
    private Integer alertId;
    private String patternType;
    private Long txCount;
    private Double totalAmount;
}

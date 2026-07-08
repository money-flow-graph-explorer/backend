package com.money_flow_graph_explorer.backend.alert.dto;

import com.money_flow_graph_explorer.backend.graph.dto.GraphResponse;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class AlertDetailDto {
    private Integer alertId;
    private String patternType;
    private String description;
    private List<Integer> relatedAccounts;
    private List<Integer> relatedTransactions;
    private GraphResponse graph;
}

package com.money_flow_graph_explorer.backend.graph.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class GraphEdgeDto {
    private String id;
    private String source;
    private String target;
    private Double amount;
    private Integer timestamp;
    private Boolean suspicious;
}

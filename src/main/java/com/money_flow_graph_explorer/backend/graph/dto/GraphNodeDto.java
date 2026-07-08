package com.money_flow_graph_explorer.backend.graph.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class GraphNodeDto {
    private String id;
    private String label;
    private String type;
    private Boolean isFraud;
}

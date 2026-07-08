package com.money_flow_graph_explorer.backend.graph.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class GraphResponse {
    private List<GraphNodeDto> nodes;
    private List<GraphEdgeDto> edges;
    private Boolean truncated;
}

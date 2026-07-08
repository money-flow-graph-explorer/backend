package com.money_flow_graph_explorer.backend.alert.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class AlertPageResponse {
    private List<AlertSummaryDto> alerts;
    private int page;
    private int size;
    private long totalElements;
}

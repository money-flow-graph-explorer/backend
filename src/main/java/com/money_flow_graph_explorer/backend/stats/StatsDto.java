package com.money_flow_graph_explorer.backend.stats;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class StatsDto {
    private Long totalAccounts;
    private Long totalTransactions;
    private Long fraudAccounts;
    private Long circularAlerts;
    private Long fanInAlerts;
}

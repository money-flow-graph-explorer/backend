package com.money_flow_graph_explorer.backend.account.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class AccountSearchResponse {
    private List<AccountSummaryDto> accounts;
}

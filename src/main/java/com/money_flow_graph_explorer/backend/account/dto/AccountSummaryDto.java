package com.money_flow_graph_explorer.backend.account.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class AccountSummaryDto {
    private Integer accountId;
    private String accountType;
    private String country;
    private Boolean isFraud;
    private Double initBalance;
}

package com.money_flow_graph_explorer.backend.account.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class AccountDetailDto {
    private Integer accountId;
    private String customerId;
    private String accountType;
    private String country;
    private Boolean isFraud;
    private Double initBalance;
    private Integer txBehaviorId;
    private Long incomingCount;
    private Long outgoingCount;
    private Double totalIncomingAmount;
    private Double totalOutgoingAmount;
}

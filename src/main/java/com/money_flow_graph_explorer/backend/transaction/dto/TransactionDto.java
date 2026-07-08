package com.money_flow_graph_explorer.backend.transaction.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TransactionDto {
    private Long transactionId;
    private Integer fromAccountId;
    private Integer toAccountId;
    private Double amount;
    private Integer timestamp;
    private Boolean isFraud;
    private Integer alertId;
}

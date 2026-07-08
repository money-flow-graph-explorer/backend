package com.money_flow_graph_explorer.backend.transaction.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TransactionPageResponse {
    private List<TransactionDto> transactions;
    private int page;
    private int size;
    private long totalElements;
}

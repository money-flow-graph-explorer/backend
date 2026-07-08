package com.money_flow_graph_explorer.backend.account;

import com.money_flow_graph_explorer.backend.account.dto.AccountDetailDto;
import com.money_flow_graph_explorer.backend.account.dto.AccountSearchResponse;
import com.money_flow_graph_explorer.backend.account.dto.AccountSummaryDto;
import com.money_flow_graph_explorer.backend.transaction.dto.TransactionDto;
import com.money_flow_graph_explorer.backend.transaction.dto.TransactionPageResponse;
import lombok.RequiredArgsConstructor;
import org.springframework.data.neo4j.core.Neo4jClient;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

@Service
@RequiredArgsConstructor
public class AccountService {

    private final AccountRepository accountRepository;
    private final Neo4jClient neo4jClient;

    public AccountSearchResponse search(String keyword) {
        List<AccountNode> nodes = accountRepository.searchByKeyword(keyword);
        List<AccountSummaryDto> dtos = nodes.stream()
                .map(n -> AccountSummaryDto.builder()
                        .accountId(n.getAccountId())
                        .accountType(n.getAccountType())
                        .country(n.getCountry())
                        .isFraud(n.getIsFraud())
                        .initBalance(n.getInitBalance())
                        .build())
                .toList();
        return AccountSearchResponse.builder().accounts(dtos).build();
    }

    public AccountDetailDto getDetail(Integer accountId) {
        AccountNode node = accountRepository.findByAccountId(accountId)
                .orElseThrow(() -> new NoSuchElementException("Account not found: " + accountId));

        // incoming stats
        var inStats = neo4jClient.query("""
                MATCH (src:Account)-[t:TRANSFER]->(a:Account {accountId: $accountId})
                RETURN count(t) AS incomingCount, coalesce(sum(t.amount), 0.0) AS totalIncomingAmount
                """)
                .bind(accountId).to("accountId")
                .fetch().one().orElse(Map.of());

        // outgoing stats
        var outStats = neo4jClient.query("""
                MATCH (a:Account {accountId: $accountId})-[t:TRANSFER]->(dst:Account)
                RETURN count(t) AS outgoingCount, coalesce(sum(t.amount), 0.0) AS totalOutgoingAmount
                """)
                .bind(accountId).to("accountId")
                .fetch().one().orElse(Map.of());

        return AccountDetailDto.builder()
                .accountId(node.getAccountId())
                .customerId(node.getCustomerId())
                .accountType(node.getAccountType())
                .country(node.getCountry())
                .isFraud(node.getIsFraud())
                .initBalance(node.getInitBalance())
                .txBehaviorId(node.getTxBehaviorId())
                .incomingCount(toLong(inStats.get("incomingCount")))
                .totalIncomingAmount(toDouble(inStats.get("totalIncomingAmount")))
                .outgoingCount(toLong(outStats.get("outgoingCount")))
                .totalOutgoingAmount(toDouble(outStats.get("totalOutgoingAmount")))
                .build();
    }

    public TransactionPageResponse getTransactions(Integer accountId, String direction, int page, int size) {
        // Verify account exists
        accountRepository.findByAccountId(accountId)
                .orElseThrow(() -> new NoSuchElementException("Account not found: " + accountId));

        direction = direction == null ? "ALL" : direction.toUpperCase();
        int skip = page * size;

        String matchClause;
        String countQuery;
        String dataQuery;

        switch (direction) {
            case "OUT" -> {
                dataQuery = """
                        MATCH (a:Account {accountId: $accountId})-[t:TRANSFER]->(b:Account)
                        RETURN t.txId AS txId, a.accountId AS fromAccountId, b.accountId AS toAccountId,
                               t.amount AS amount, t.timestamp AS timestamp, t.isFraud AS isFraud, t.alertId AS alertId
                        ORDER BY t.timestamp DESC
                        SKIP $skip LIMIT $size
                        """;
                countQuery = """
                        MATCH (a:Account {accountId: $accountId})-[t:TRANSFER]->(b:Account)
                        RETURN count(t) AS total
                        """;
            }
            case "IN" -> {
                dataQuery = """
                        MATCH (b:Account)-[t:TRANSFER]->(a:Account {accountId: $accountId})
                        RETURN t.txId AS txId, b.accountId AS fromAccountId, a.accountId AS toAccountId,
                               t.amount AS amount, t.timestamp AS timestamp, t.isFraud AS isFraud, t.alertId AS alertId
                        ORDER BY t.timestamp DESC
                        SKIP $skip LIMIT $size
                        """;
                countQuery = """
                        MATCH (b:Account)-[t:TRANSFER]->(a:Account {accountId: $accountId})
                        RETURN count(t) AS total
                        """;
            }
            default -> {
                dataQuery = """
                        MATCH (a:Account {accountId: $accountId})
                        CALL {
                          WITH a
                          MATCH (a)-[t:TRANSFER]->(b:Account)
                          RETURN t, a AS src, b AS dst
                          UNION ALL
                          WITH a
                          MATCH (b:Account)-[t:TRANSFER]->(a)
                          RETURN t, b AS src, a AS dst
                        }
                        RETURN t.txId AS txId, src.accountId AS fromAccountId, dst.accountId AS toAccountId,
                               t.amount AS amount, t.timestamp AS timestamp, t.isFraud AS isFraud, t.alertId AS alertId
                        ORDER BY timestamp DESC
                        SKIP $skip LIMIT $size
                        """;
                countQuery = """
                        MATCH (a:Account {accountId: $accountId})
                        CALL {
                          WITH a
                          MATCH (a)-[t:TRANSFER]->(b:Account)
                          RETURN count(t) AS c
                          UNION ALL
                          WITH a
                          MATCH (b:Account)-[t:TRANSFER]->(a)
                          RETURN count(t) AS c
                        }
                        RETURN sum(c) AS total
                        """;
            }
        }

        List<Map<String, Object>> rows = new ArrayList<>(
                neo4jClient.query(dataQuery)
                        .bind(accountId).to("accountId")
                        .bind(skip).to("skip")
                        .bind(size).to("size")
                        .fetch().all()
        );

        var countResult = neo4jClient.query(countQuery)
                .bind(accountId).to("accountId")
                .fetch().one().orElse(Map.of());
        long total = toLong(countResult.get("total"));

        List<TransactionDto> txDtos = rows.stream()
                .map(r -> TransactionDto.builder()
                        .transactionId(toLong(r.get("txId")))
                        .fromAccountId(toInt(r.get("fromAccountId")))
                        .toAccountId(toInt(r.get("toAccountId")))
                        .amount(toDouble(r.get("amount")))
                        .timestamp(toInt(r.get("timestamp")))
                        .isFraud((Boolean) r.getOrDefault("isFraud", false))
                        .alertId(toInt(r.get("alertId")))
                        .build())
                .toList();

        return TransactionPageResponse.builder()
                .transactions(txDtos)
                .page(page)
                .size(size)
                .totalElements(total)
                .build();
    }

    // --- helpers ---
    private Long toLong(Object v) {
        if (v == null) return 0L;
        if (v instanceof Long l) return l;
        if (v instanceof Integer i) return i.longValue();
        if (v instanceof Number n) return n.longValue();
        return 0L;
    }

    private Integer toInt(Object v) {
        if (v == null) return null;
        if (v instanceof Integer i) return i;
        if (v instanceof Long l) return l.intValue();
        if (v instanceof Number n) return n.intValue();
        return null;
    }

    private Double toDouble(Object v) {
        if (v == null) return 0.0;
        if (v instanceof Double d) return d;
        if (v instanceof Number n) return n.doubleValue();
        return 0.0;
    }
}

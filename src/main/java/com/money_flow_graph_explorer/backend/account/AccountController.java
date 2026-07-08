package com.money_flow_graph_explorer.backend.account;

import com.money_flow_graph_explorer.backend.account.dto.AccountDetailDto;
import com.money_flow_graph_explorer.backend.account.dto.AccountSearchResponse;
import com.money_flow_graph_explorer.backend.transaction.dto.TransactionPageResponse;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/accounts")
@RequiredArgsConstructor
public class AccountController {

    private final AccountService accountService;

    @GetMapping
    public ResponseEntity<AccountSearchResponse> search(
            @RequestParam(defaultValue = "") String keyword) {
        return ResponseEntity.ok(accountService.search(keyword));
    }

    @GetMapping("/{accountId}")
    public ResponseEntity<AccountDetailDto> getDetail(
            @PathVariable Integer accountId) {
        return ResponseEntity.ok(accountService.getDetail(accountId));
    }

    @GetMapping("/{accountId}/transactions")
    public ResponseEntity<TransactionPageResponse> getTransactions(
            @PathVariable Integer accountId,
            @RequestParam(defaultValue = "ALL") String direction,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "20") int size) {
        return ResponseEntity.ok(accountService.getTransactions(accountId, direction, page, size));
    }
}

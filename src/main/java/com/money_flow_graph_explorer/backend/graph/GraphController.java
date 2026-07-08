package com.money_flow_graph_explorer.backend.graph;

import com.money_flow_graph_explorer.backend.graph.dto.GraphResponse;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/accounts")
@RequiredArgsConstructor
public class GraphController {

    private final GraphService graphService;

    @GetMapping("/{accountId}/graph")
    public ResponseEntity<GraphResponse> getGraph(
            @PathVariable Integer accountId,
            @RequestParam(defaultValue = "2") int depth) {
        return ResponseEntity.ok(graphService.getGraph(accountId, depth));
    }
}

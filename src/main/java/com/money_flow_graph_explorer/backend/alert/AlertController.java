package com.money_flow_graph_explorer.backend.alert;

import com.money_flow_graph_explorer.backend.alert.dto.AlertDetailDto;
import com.money_flow_graph_explorer.backend.alert.dto.AlertPageResponse;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/alerts")
@RequiredArgsConstructor
public class AlertController {

    private final AlertService alertService;

    @GetMapping
    public ResponseEntity<AlertPageResponse> listAlerts(
            @RequestParam(required = false) String patternType,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "20") int size) {
        return ResponseEntity.ok(alertService.listAlerts(patternType, page, size));
    }

    @GetMapping("/{alertId}")
    public ResponseEntity<AlertDetailDto> getAlertDetail(
            @PathVariable Integer alertId) {
        return ResponseEntity.ok(alertService.getAlertDetail(alertId));
    }
}

package com.money_flow_graph_explorer.backend.pattern;

import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/patterns")
@RequiredArgsConstructor
public class PatternController {

    private final CircularDetectionService circularDetectionService;
    private final LayeringDetectionService layeringDetectionService;
    private final FanOutDetectionService fanOutDetectionService;
    private final FanInDetectionService fanInDetectionService;

    @GetMapping("/circular")
    public ResponseEntity<CircularPatternResponse> detectCircular(
            @RequestParam Integer accountId,
            @RequestParam(defaultValue = "4") int depth) {
        return ResponseEntity.ok(circularDetectionService.detect(accountId, depth));
    }

    @GetMapping("/layering")
    public ResponseEntity<LayeringPatternResponse> detectLayering(
            @RequestParam Integer accountId,
            @RequestParam(defaultValue = "3") int minDepth) {
        return ResponseEntity.ok(layeringDetectionService.detect(accountId, minDepth));
    }

    @GetMapping("/fan-out")
    public ResponseEntity<FanOutPatternResponse> detectFanOut(
            @RequestParam Integer accountId) {
        return ResponseEntity.ok(fanOutDetectionService.detect(accountId));
    }

    @GetMapping("/fan-in")
    public ResponseEntity<FanInPatternResponse> detectFanIn(
            @RequestParam Integer accountId) {
        return ResponseEntity.ok(fanInDetectionService.detect(accountId));
    }
}

package com.riskforge.api_gateway.controller;

import java.util.Map;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
class GatewayFallbackController {

    @RequestMapping("/fallback/ingestion")
    ResponseEntity<Map<String, String>> ingestionUnavailable() {
        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                .body(Map.of(
                        "code", "INGESTION_UNAVAILABLE",
                        "message", "Transaction intake is temporarily unavailable. Please retry later."
                ));
    }
}

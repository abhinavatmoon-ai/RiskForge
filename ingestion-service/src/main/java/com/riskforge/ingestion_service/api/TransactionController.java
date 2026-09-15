package com.riskforge.ingestion_service.api;

import com.riskforge.ingestion_service.application.TransactionIngestionService;
import jakarta.validation.Valid;
import java.net.URI;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1/transactions")
public class TransactionController {
    private final TransactionIngestionService ingestionService;
    public TransactionController(TransactionIngestionService ingestionService) { this.ingestionService = ingestionService; }

    @PostMapping
    public ResponseEntity<TransactionAcceptedResponse> submit(@RequestHeader("Idempotency-Key") String idempotencyKey,
            @RequestHeader(value = "X-Auth-User-Id", required = false) String authenticatedUserId,
            @Valid @RequestBody TransactionRequest request) {
        TransactionAcceptedResponse response = ingestionService.accept(request, idempotencyKey, authenticatedUserId);
        return ResponseEntity.accepted().location(URI.create("/api/v1/transactions/" + response.transactionId())).body(response);
    }
}

package com.riskforge.ingestion_service.api;

import java.time.Instant;
import java.util.UUID;

public record TransactionAcceptedResponse(UUID transactionId, String status, Instant receivedAt) { }

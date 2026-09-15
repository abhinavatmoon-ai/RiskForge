package com.riskforge.ingestion_service.application;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

public record TransactionReceivedEvent(UUID eventId, String eventType, Instant occurredAt, UUID correlationId,
        String producer, Payload payload) {
    public record Payload(UUID transactionId, String accountId, String cardId, BigDecimal amount, String currency,
            String merchantId, BigDecimal latitude, BigDecimal longitude, Instant clientTimestamp, Instant receivedAt) { }
}

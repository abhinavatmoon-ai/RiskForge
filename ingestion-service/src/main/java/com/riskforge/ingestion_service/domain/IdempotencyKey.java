package com.riskforge.ingestion_service.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "idempotency_keys")
public class IdempotencyKey {
    @Id @Column(name = "idempotency_key", length = 128) private String key;
    @Column(nullable = false, length = 64) private String requestHash;
    @Column(nullable = false) private UUID transactionId;
    @Column(nullable = false) private Instant createdAt;
    protected IdempotencyKey() { }
    public IdempotencyKey(String key, String requestHash, UUID transactionId, Instant createdAt) {
        this.key = key; this.requestHash = requestHash; this.transactionId = transactionId; this.createdAt = createdAt;
    }
    public String getRequestHash() { return requestHash; }
    public UUID getTransactionId() { return transactionId; }
}

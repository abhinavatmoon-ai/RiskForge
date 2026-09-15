package com.riskforge.ingestion_service.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "transactions")
public class Transaction {
    @Id private UUID id;
    @Column(nullable = false, length = 64) private String accountId;
    @Column(nullable = false, length = 64) private String cardId;
    @Column(nullable = false, precision = 17, scale = 2) private BigDecimal amount;
    @Column(nullable = false, length = 3) private String currency;
    @Column(nullable = false, length = 64) private String merchantId;
    @Column(nullable = false, precision = 9, scale = 6) private BigDecimal latitude;
    @Column(nullable = false, precision = 9, scale = 6) private BigDecimal longitude;
    @Column(nullable = false) private Instant clientTimestamp;
    @Column(nullable = false) private Instant receivedAt;
    @Column(length = 128) private String authenticatedUserId;
    @Enumerated(EnumType.STRING) @Column(nullable = false, length = 16) private TransactionStatus status;

    protected Transaction() { }
    public Transaction(UUID id, String accountId, String cardId, BigDecimal amount, String currency, String merchantId,
            BigDecimal latitude, BigDecimal longitude, Instant clientTimestamp, Instant receivedAt, String authenticatedUserId) {
        this.id = id; this.accountId = accountId; this.cardId = cardId; this.amount = amount; this.currency = currency;
        this.merchantId = merchantId; this.latitude = latitude; this.longitude = longitude; this.clientTimestamp = clientTimestamp;
        this.receivedAt = receivedAt; this.authenticatedUserId = authenticatedUserId; this.status = TransactionStatus.PENDING;
    }
    public UUID getId() { return id; }
    public String getAccountId() { return accountId; }
    public Instant getReceivedAt() { return receivedAt; }
}

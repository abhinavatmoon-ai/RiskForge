package com.riskforge.ingestion_service.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Lob;
import jakarta.persistence.Table;
import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "outbox_events")
public class OutboxEvent {
    @Id private UUID id;
    @Column(nullable = false) private UUID aggregateId;
    @Column(nullable = false, length = 128) private String topic;
    @Column(nullable = false, length = 64) private String messageKey;
    @Lob @Column(nullable = false) private String payload;
    @Column(nullable = false) private Instant createdAt;
    private Instant publishedAt;
    protected OutboxEvent() { }
    public OutboxEvent(UUID id, UUID aggregateId, String topic, String messageKey, String payload, Instant createdAt) {
        this.id = id; this.aggregateId = aggregateId; this.topic = topic; this.messageKey = messageKey; this.payload = payload; this.createdAt = createdAt;
    }
    public UUID getId() { return id; }
    public String getTopic() { return topic; }
    public String getMessageKey() { return messageKey; }
    public String getPayload() { return payload; }
    public void markPublished(Instant publishedAt) { this.publishedAt = publishedAt; }
}

package com.riskforge.ingestion_service.application;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.riskforge.ingestion_service.api.TransactionAcceptedResponse;
import com.riskforge.ingestion_service.api.TransactionRequest;
import com.riskforge.ingestion_service.domain.IdempotencyKey;
import com.riskforge.ingestion_service.domain.OutboxEvent;
import com.riskforge.ingestion_service.domain.Transaction;
import com.riskforge.ingestion_service.infrastructure.IdempotencyKeyRepository;
import com.riskforge.ingestion_service.infrastructure.OutboxEventRepository;
import com.riskforge.ingestion_service.infrastructure.TransactionRepository;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.HexFormat;
import java.util.UUID;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class TransactionIngestionService {
    private final TransactionRepository transactionRepository;
    private final IdempotencyKeyRepository idempotencyKeyRepository;
    private final OutboxEventRepository outboxEventRepository;
    private final ObjectMapper objectMapper;
    private final String rawTransactionsTopic;

    public TransactionIngestionService(TransactionRepository transactionRepository, IdempotencyKeyRepository idempotencyKeyRepository,
            OutboxEventRepository outboxEventRepository, ObjectMapper objectMapper,
            @Value("${riskforge.kafka.raw-transactions-topic}") String rawTransactionsTopic) {
        this.transactionRepository = transactionRepository; this.idempotencyKeyRepository = idempotencyKeyRepository;
        this.outboxEventRepository = outboxEventRepository; this.objectMapper = objectMapper; this.rawTransactionsTopic = rawTransactionsTopic;
    }

    @Transactional
    public TransactionAcceptedResponse accept(TransactionRequest request, String idempotencyKey, String authenticatedUserId) {
        if (idempotencyKey == null || idempotencyKey.isBlank() || idempotencyKey.length() > 128) {
            throw new IllegalArgumentException("Idempotency-Key must be between 1 and 128 characters");
        }
        String requestHash = hash(request);
        IdempotencyKey existing = idempotencyKeyRepository.findById(idempotencyKey).orElse(null);
        if (existing != null) {
            if (!existing.getRequestHash().equals(requestHash)) throw new IllegalArgumentException("Idempotency-Key was already used for a different request");
            Transaction transaction = transactionRepository.findById(existing.getTransactionId()).orElseThrow();
            return new TransactionAcceptedResponse(transaction.getId(), "PENDING", transaction.getReceivedAt());
        }

        UUID transactionId = UUID.randomUUID();
        Instant receivedAt = Instant.now();
        Transaction transaction = new Transaction(transactionId, request.accountId(), request.cardId(), request.amount(), request.currency(),
                request.merchantId(), request.latitude(), request.longitude(), request.timestamp(), receivedAt, authenticatedUserId);
        transactionRepository.save(transaction);
        TransactionReceivedEvent event = new TransactionReceivedEvent(UUID.randomUUID(), "riskforge.transaction.received.v1", receivedAt,
                transactionId, "ingestion-service", new TransactionReceivedEvent.Payload(transactionId, request.accountId(), request.cardId(),
                request.amount(), request.currency(), request.merchantId(), request.latitude(), request.longitude(), request.timestamp(), receivedAt));
        outboxEventRepository.save(new OutboxEvent(event.eventId(), transactionId, rawTransactionsTopic, request.accountId(), json(event), receivedAt));
        idempotencyKeyRepository.save(new IdempotencyKey(idempotencyKey, requestHash, transactionId, receivedAt));
        return new TransactionAcceptedResponse(transactionId, "PENDING", receivedAt);
    }

    private String json(TransactionReceivedEvent event) {
        try { return objectMapper.writeValueAsString(event); }
        catch (JsonProcessingException exception) { throw new IllegalStateException("Could not serialize transaction event", exception); }
    }
    private String hash(TransactionRequest request) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256")
                    .digest((request.accountId() + "|" + request.cardId() + "|" + request.amount() + "|" + request.currency() + "|" + request.merchantId()
                            + "|" + request.latitude() + "|" + request.longitude() + "|" + request.timestamp()).getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException exception) { throw new IllegalStateException(exception); }
    }
}

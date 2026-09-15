package com.riskforge.ingestion_service.infrastructure;

import com.riskforge.ingestion_service.domain.OutboxEvent;
import java.time.Instant;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Component
public class OutboxPublisher {
    private final OutboxEventRepository outboxEventRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;
    public OutboxPublisher(OutboxEventRepository outboxEventRepository, KafkaTemplate<String, String> kafkaTemplate) {
        this.outboxEventRepository = outboxEventRepository; this.kafkaTemplate = kafkaTemplate;
    }

    @Scheduled(fixedDelayString = "${riskforge.outbox.publish-delay-ms}")
    @Transactional
    public void publishPendingEvents() {
        for (OutboxEvent event : outboxEventRepository.findTop100ByPublishedAtIsNullOrderByCreatedAtAsc()) {
            try {
                kafkaTemplate.send(event.getTopic(), event.getMessageKey(), event.getPayload()).get();
                event.markPublished(Instant.now());
            } catch (Exception exception) {
                return; // Preserve ordering; retry this record on the next scheduled run.
            }
        }
    }
}

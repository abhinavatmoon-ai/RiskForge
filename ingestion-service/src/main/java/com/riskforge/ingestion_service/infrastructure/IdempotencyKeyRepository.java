package com.riskforge.ingestion_service.infrastructure;
import com.riskforge.ingestion_service.domain.IdempotencyKey;
import org.springframework.data.jpa.repository.JpaRepository;
public interface IdempotencyKeyRepository extends JpaRepository<IdempotencyKey, String> { }

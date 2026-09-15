package com.riskforge.ingestion_service.infrastructure;
import com.riskforge.ingestion_service.domain.Transaction;
import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;
public interface TransactionRepository extends JpaRepository<Transaction, UUID> { }

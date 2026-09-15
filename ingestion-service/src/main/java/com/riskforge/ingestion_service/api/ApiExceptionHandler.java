package com.riskforge.ingestion_service.api;

import java.time.Instant;
import java.util.Map;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

@RestControllerAdvice
class ApiExceptionHandler {
    @ExceptionHandler(MethodArgumentNotValidException.class)
    ResponseEntity<Map<String, Object>> validationError(MethodArgumentNotValidException exception) {
        Map<String, String> fields = exception.getBindingResult().getFieldErrors().stream().collect(java.util.stream.Collectors.toMap(
                error -> error.getField(), error -> error.getDefaultMessage() == null ? "invalid value" : error.getDefaultMessage(), (first, ignored) -> first));
        return ResponseEntity.badRequest().body(Map.of("timestamp", Instant.now(), "errors", fields));
    }
    @ExceptionHandler(IllegalArgumentException.class)
    ResponseEntity<Map<String, Object>> badRequest(IllegalArgumentException exception) {
        return ResponseEntity.badRequest().body(Map.of("timestamp", Instant.now(), "message", exception.getMessage()));
    }
}

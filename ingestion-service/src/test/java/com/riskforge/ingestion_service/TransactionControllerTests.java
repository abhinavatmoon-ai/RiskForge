package com.riskforge.ingestion_service;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

@SpringBootTest
@AutoConfigureMockMvc
class TransactionControllerTests {
    @Autowired private MockMvc mockMvc;

    @Test
    void acceptsValidTransaction() throws Exception {
        mockMvc.perform(post("/api/v1/transactions")
                        .header("Idempotency-Key", "request-1")
                        .header("X-Auth-User-Id", "user-1001")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(validRequest()))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.status").value("PENDING"))
                .andExpect(jsonPath("$.transactionId").exists());
    }

    @Test
    void sameKeyAndRequestReturnsSameTransaction() throws Exception {
        String request = validRequest();
        String first = mockMvc.perform(post("/api/v1/transactions").header("Idempotency-Key", "request-2")
                        .contentType(MediaType.APPLICATION_JSON).content(request)).andReturn().getResponse().getContentAsString();
        String second = mockMvc.perform(post("/api/v1/transactions").header("Idempotency-Key", "request-2")
                        .contentType(MediaType.APPLICATION_JSON).content(request)).andReturn().getResponse().getContentAsString();
        org.junit.jupiter.api.Assertions.assertEquals(transactionId(first), transactionId(second));
    }

    @Test
    void rejectsInvalidAmount() throws Exception {
        mockMvc.perform(post("/api/v1/transactions").header("Idempotency-Key", "request-3")
                        .contentType(MediaType.APPLICATION_JSON).content(validRequest().replace("12500.00", "0")))
                .andExpect(status().isBadRequest());
    }

    private String transactionId(String response) { return response.replaceAll(".*\\\"transactionId\\\":\\\"([^\\\"]+)\\\".*", "$1"); }
    private String validRequest() {
        return "{\"accountId\":\"ACC-10001\",\"cardId\":\"CARD-90001\",\"amount\":12500.00,\"currency\":\"INR\",\"merchantId\":\"MERCHANT-100\",\"latitude\":19.0760,\"longitude\":72.8777,\"timestamp\":\"2026-08-26T10:30:00Z\"}";
    }
}

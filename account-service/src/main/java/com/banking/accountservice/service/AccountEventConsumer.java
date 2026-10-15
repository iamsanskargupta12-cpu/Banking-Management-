package com.banking.accountservice.service;

import com.banking.accountservice.repository.AccountRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.Map;

@Service
@Slf4j
@RequiredArgsConstructor
public class AccountEventConsumer {
    public final AccountService accountService;

    /**
     *
     * Consumes transaction.completed events from kafka
     * Credit reciever amount
     * @param payload
     */
    @KafkaListener(topics = "transaction.completed")
    public void consumeTransactionCompleted
            (@Payload Map<String, Object> payload) {

        try {
            String recieverAccount = (String) payload.get("recieverAccountNumber");
            BigDecimal amount = new BigDecimal(payload.get("amount").toString());

            log.info("Crediting amount : {}  to Account : {}", amount, recieverAccount);

            accountService.creditBalance(recieverAccount, amount);
        }
        catch (Exception e) {
            log.info("Error while crediting amount : {} ", e.getMessage());
        }
    }

    /**
     * Consume fraud.detected from kafka
     * Blocks the flagged account
     * @param payload
     */
    @KafkaListener(topics = "fraud.detected")
    public void consumeFraudDetected
        (@Payload Map<String, Object> payload) {

        try {
            String accountNumber = (String) payload.get("accountNumber");
            log.info("Fraud detected - blocking account : {}", accountNumber);
            accountService.blockAccount(accountNumber);
        }
        catch (Exception e) {
             log.info("Error while blocking account : {}", e.getMessage());
        }
    }
}

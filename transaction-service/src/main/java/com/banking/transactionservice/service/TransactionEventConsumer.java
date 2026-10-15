package com.banking.transactionservice.service;

import com.banking.transactionservice.entity.Transaction;
import com.banking.transactionservice.entity.TransactionStatus;
import com.banking.transactionservice.repository.TransactionRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Service
@Slf4j
@RequiredArgsConstructor
public class TransactionEventConsumer {

    private final TransactionRepository transactionRepository;
    private final TransactionService transactionService;

    private final RedisTemplate<String, String> redisTemplate;
    private static long OTP_EXPIRY_MINUTES =  5;

    private final KafkaTemplate<String, Object> kafkaTemplate;

    private static final String TRANSACTION_OTP_GENERATED_TOPIC = "transaction.otp.generated";

    /**
     *
     * Consume verification.required
     * Generate OTP
     * @param payload
     */
    @KafkaListener(topics = "verification.required")
    public void consumerVerificationRequired(
            @Payload Map<String, Object> payload
    ){

        try {
            String transactionId = (String)payload.get("transactionId");
            String accountNumber = (String)payload.get("accountNumber");
            String reason = (String)payload.get("reason");


            log.info("Verification required , transactionId - {}, rason - {}",  transactionId, reason);


            Transaction transaction =  transactionRepository.findById(Long.valueOf(transactionId))
                    .orElseThrow(() -> new RuntimeException("Transaction not Found " + transactionId));


            if(transaction.getStatus() != TransactionStatus.PROCESSING){
                 log.warn("Transaction {}  not processing", transactionId);
                 return;
            }

            //Generate 6Digit OTP;
            String otp = String.format("%06d", (int)(Math.random() * 900000) + 100000);

            //Store OTP in Redis, OTP expires in 5 minutes.

            String otpkey = "verification:otp" + transactionId ;


            redisTemplate.opsForValue().set(otpkey, otp , OTP_EXPIRY_MINUTES, TimeUnit.MINUTES);

            //Update Status

             transaction.setStatus(TransactionStatus.PENDING_VERIFICATION);
             transactionRepository.save(transaction);

             log.info("OTP generated for Transaction : {}, and expieres in : {} minutes", transactionId, OTP_EXPIRY_MINUTES);

             //Notify User.
            Map<String, Object> otpEvent = new HashMap<>();
            otpEvent.put("transactionId", transactionId);
            otpEvent.put("accountNumber", accountNumber);
            otpEvent.put("reason", reason );
            otpEvent.put("amount", payload.get("amount"));
            otpEvent.put("otp", otp);


            kafkaTemplate.send(TRANSACTION_OTP_GENERATED_TOPIC, transactionId,otpEvent);
        }
        catch(Exception e){
            log.error("Error handling Verification required: {}", e.getMessage());
        }
    }
    @KafkaListener(topics = "fraud.check.clean")
    public void consumeFraudCheckCleanResult(
            @Payload Map<String, Object> payload){
        try {
            String transactionId = (String)payload.get("transactionId");

            transactionService.processCleanResult(transactionId);
        }
        catch (Exception e){
            log.error("Error Processing fraud check result :  {}", e.getMessage());
        }
    }
}

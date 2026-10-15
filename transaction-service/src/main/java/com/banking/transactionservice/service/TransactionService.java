package com.banking.transactionservice.service;

import com.banking.transactionservice.client.AccountServiceClient;
import com.banking.transactionservice.dto.TransactionResponse;
import com.banking.transactionservice.dto.TransferRequest;
import com.banking.transactionservice.entity.Transaction;
import com.banking.transactionservice.entity.TransactionStatus;
import com.banking.transactionservice.entity.TransactionType;
import com.banking.transactionservice.event.TransactionCompletedEvent;
import com.banking.transactionservice.event.TransactionInitiatedEvent;
import com.banking.transactionservice.repository.TransactionRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.RedisSystemException;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.lang.reflect.MalformedParameterizedTypeException;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

@Service
@Slf4j
@RequiredArgsConstructor
public class TransactionService {

    private final TransactionRepository transactionRepository;
    private final AccountServiceClient accountServiceClient;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final RedisTemplate<String, String> redisTemplate;

    private static final String TRANSACTION_INITIATED_TOPIC = "transaction.initiated";
    private static final String TRANSACTION_COMPLETED_TOPIC = "transaction.completed";
    private static final String TRANSACTION_REFUNDED_TOPIC = "transaction.refunded";
    private static final String FRAUD_DETECTED_TOPIC = "fraud.detected";


    /**
     * SAGA step 1 : initiate transfer
     * Deducts from sender via feign
     * Saves transaction as PROCESSING
     * publish event to kafka for fraud check
     * Returns.
     * @param request
     * @return
     */
    public TransactionResponse transfer(TransferRequest request) {
        log.info("SAGA starts : Transfer from {} -> {}, amount : {}", request.getSenderAccountNumber(), request.getRecieverAccountNumber(), request.getAmount());

        //SAGA step 1  : Deduct from Sender

        accountServiceClient.deductBalance(request.getSenderAccountNumber(), request.getAmount());

        Transaction transaction = new Transaction();
        transaction.setSenderAccountNumber(request.getSenderAccountNumber());
        transaction.setReceiverAccountNumber(request.getRecieverAccountNumber());
        transaction.setAmount(request.getAmount());
        transaction.setType(TransactionType.TRANSFER);
        transaction.setStatus(TransactionStatus.PROCESSING);
        transaction.setDescription(request.getDescription());
        transaction.setReferenceNumber(UUID.randomUUID().toString());

        Transaction savedTransaction = transactionRepository.save(transaction);

        log.info("Transction saved as PROCESSING TransactionId : {}",  savedTransaction.getId());

//        SAGA step2 : publish for fraud check.
        TransactionInitiatedEvent event = new TransactionInitiatedEvent(
                savedTransaction.getId(),
                savedTransaction.getSenderAccountNumber(),
                savedTransaction.getReceiverAccountNumber(),
                savedTransaction.getAmount(),
                savedTransaction.getDescription()
        );


        kafkaTemplate.send(TRANSACTION_INITIATED_TOPIC, savedTransaction.getId(), event);

        log.info("SAGA sep 2 - TransactionInitiatedEvent Published : {} ", savedTransaction.getId());

        return mapToResponse(savedTransaction);

    }

    public TransactionResponse getTransaction(String transactionId) {
        return mapToResponse(transactionRepository
                .findById(Long.valueOf(transactionId))
                .orElseThrow(()-> new RuntimeException("Transaction not found" +  transactionId))
        );
    }

    public List<TransactionResponse> getTransactionHistory(String accountNumber) {
        return transactionRepository
                .findBySenderAccountNumberOrderByCreatedAtDesc(accountNumber)
                .stream()
                .map(this::mapToResponse)
                .collect(Collectors.toList());
    }


    public TransactionResponse verifyOTP(String transactionId, String otp){
        log.info("OTP verification for transaction {}", transactionId);

        Transaction transaction = transactionRepository.findById(Long.valueOf(transactionId))
                .orElseThrow(()-> new RuntimeException("Transaction not found" +  transactionId));

        String otpKey = "verification:otp" + transactionId;

        String storedOtp = redisTemplate.opsForValue().get(otpKey);

        if(storedOtp == null){
            //OTP Expired
            log.warn("OTP verification for transaction {}", transactionId);

            compensateTransaction(transaction, "OTP expired, transaction cancelled and amount refunded");

            return mapToResponse(transaction);

        }

        if(!storedOtp.equals(otp)){
            //BLOCK Account REFUNDED

            log.warn("Wrong OTP, - blocking account and refunding {}", transactionId);
            redisTemplate .delete(otpKey);
            blockAccountAndCompensate(transaction, "Wrong OTP entered - transaction cancelled" + " Account blocked for secuirity");

            return mapToResponse(transaction);
        }

        //OTP Correct - complete transaction

        log.info("OTP verified, transaction Complete : {}" , transactionId);

        redisTemplate.delete(otpKey);
        completeTransaction(transaction);
        return mapToResponse(transaction);

    }

    private void completeTransaction(Transaction transaction) {
        transaction.setStatus(TransactionStatus.COMPLETED);
        transaction.setCompletedAt(LocalDateTime.now());
        transactionRepository.save(transaction);

        TransactionCompletedEvent completedEvent = new TransactionCompletedEvent(
                transaction.getId(),
                transaction.getSenderAccountNumber(),
                transaction.getReceiverAccountNumber(),
                transaction.getAmount(),
                transaction.getDescription()

        );

        kafkaTemplate.send(TRANSACTION_COMPLETED_TOPIC, transaction.getId(), completedEvent);

        log.info("SAGA COMPLETE - TRANSACTION {} completed", transaction.getId());
    }

    private void blockAccountAndCompensate(Transaction transaction, String reason) {
        //Publish  fraud.detected -> Account service  will be blocked

        Map<String, Object> fraudEvent = new  HashMap<>();

        fraudEvent.put("transactionId", transaction.getId());
        fraudEvent.put("accountNumber", transaction.getSenderAccountNumber());
        fraudEvent.put("reason", reason);

        kafkaTemplate.send(FRAUD_DETECTED_TOPIC, transaction.getSenderAccountNumber(), fraudEvent);

        log.warn("fraud.detected published - account {} will blocked, Kindly contact the bank",  transaction.getSenderAccountNumber());

        //SAGA COMPENSATOIN
        compensateTransaction(transaction,reason );
    }


    private void compensateTransaction( Transaction transaction, String reason){
        log.warn("SAGA compensation - refunding  : {}, amount : {}", transaction.getSenderAccountNumber(), transaction.getAmount());

        //CREDIT MONEY BACK TO SENDER SYNCHRONOUSLY

        accountServiceClient.creditBalance(transaction.getSenderAccountNumber(), transaction.getAmount());

        transaction.setStatus(TransactionStatus.FAILED);
        transaction.setFailureReason(reason +  "SAGA compensation executed, amount refunded at " + LocalDateTime.now());

        transactionRepository.save(transaction);

        //PUBLISH REFUND EVENT - Notification Service will alert user

        Map<String, Object> refundEvent = new HashMap<>();

        refundEvent.put("transactionId", transaction.getId());
        refundEvent.put("amount", transaction.getAmount());
        refundEvent.put("accountNumber",  transaction.getSenderAccountNumber());
        refundEvent.put("reason", reason);

        kafkaTemplate.send(TRANSACTION_REFUNDED_TOPIC ,transaction.getId(),  refundEvent);

        log.info("SAGA compensation completed - refunded :{}  to :{}",
                transaction.getAmount(), transaction.getSenderAccountNumber());


    }



    private TransactionResponse mapToResponse(Transaction savedTransaction) {
        TransactionResponse transactionResponse = new TransactionResponse();
        transactionResponse.setId(savedTransaction.getId());
        transactionResponse.setDescription(savedTransaction.getDescription());
        transactionResponse.setAmount(savedTransaction.getAmount());
        transactionResponse.setSenderAccountNumber(savedTransaction.getSenderAccountNumber());
        transactionResponse.setReceiverAccountNumber(savedTransaction.getReceiverAccountNumber());
        transactionResponse.setStatus(savedTransaction.getStatus());
        transactionResponse.setReferenceNumber(savedTransaction.getReferenceNumber());
        transactionResponse.setFailureReason(savedTransaction.getFailureReason());
        transactionResponse.setType(savedTransaction.getType());
        transactionResponse.setCreatedAt(savedTransaction.getCreatedAt());
        transactionResponse.setCompletedAt(savedTransaction.getCompletedAt());

        return transactionResponse;
    }

    public void processCleanResult(String transactionId) {
        Transaction transaction = transactionRepository.findById(Long.valueOf(transactionId))
                .orElseThrow(()-> new RuntimeException("Transaction not found" +  transactionId));

        if(transaction.getStatus() != TransactionStatus.PROCESSING){
            log.warn("Transaction {}  not processing", transactionId);
            return;
        }

        completeTransaction(transaction);
    }
}

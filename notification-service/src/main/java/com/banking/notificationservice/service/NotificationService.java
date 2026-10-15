package com.banking.notificationservice.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import java.util.Map;

@Service
@Slf4j
public class NotificationService {

    @KafkaListener(topics  = "transaction.otp.generated")
    public void consumeOtpGenerated(
            @Payload Map<String, Object> payload
    ){
        try{
            String accountNumber = (String) payload.get("accountNumber");

            String otp = (String) payload.get("otp");
            String amount = payload.get("amount").toString();

            String reason = (String) payload.get("reason");

             sendAlert(accountNumber,
                     "TRANSACTION VERIFICATION REQUIRED",
                     String.format(
                             "Suspicious activity detected on your account. " +
                                     "Reason is %s. " +
                                     "A transaction of %s is pending verification. " +
                                     "Your OTP is %s and is valid for 5 minutes. " +
                                     "If this isn't you - ignore this message.",
                             reason, amount, otp
                     )
             );
        }
        catch(Exception e){
            log.error("Error sending OTP verification: {} ", e.getMessage());
        }
    }

    @KafkaListener(topics =  "transaction.completed")
    public void consumeTransactoinCompleted(
            @Payload Map<String, Object> payload){

        try{
            String senderAccountNumber = (String) payload.get("senderAccountNumber");
            String receiverAccountNumber = (String) payload.get("receiverAccountNumber");
            String amount = payload.get("amount").toString() ;

            //DEBIT ALERT
             sendAlert(senderAccountNumber, "DEBIT ALERT", String.format("%s debited from account %s", amount, senderAccountNumber));

            //CREDIT ALERT
            sendAlert(receiverAccountNumber, "CREDIT ALERT", String.format("%s credited to account %s", amount, receiverAccountNumber));
        }
        catch (Exception e) {
            log.error("Error sending transaction notification : {} ", e.getMessage());
        }

    }

    @KafkaListener(topics = "fraud.detected")
    public void consumeFraudDetected(
            @Payload Map<String, Object> payload
    ){
        try{
            String accountNumber = (String) payload.get("accountNumber");
            String reason =  (String) payload.get("reason");
            sendAlert(accountNumber, "SUSPICIOUS ACTIVITY DETECTED", String.format("Your account %s has been blocked. " + "Reason is %s. " + "Please contact your bank immediately", accountNumber, reason));
        }
        catch (Exception e) {
            log.error("Error sending fraud alert  : {} ", e.getMessage());
        }
    }

    @KafkaListener(topics = "transaction.refunded")
    public void consumeTransactionRefunded(
            @Payload Map<String, Object> payload){
        try{

            String accountNumber = (String) payload.get("accountNumber");
            String amount = payload.get("amount").toString();
            String reason = (String) payload.get("reason");

            sendAlert(accountNumber,
                    "REFUND PROCESSED",
                    String.format(
                            "Your transaction for %s was cancelled. "+
                                    "Reason: %s. "+
                                    "%s has been refunded to account %s.",
                            amount, reason, amount, accountNumber
                    ));
        }
        catch (Exception e) {
            log.error("Error sending refunded notification : {} ", e.getMessage());
        }
    }

    @KafkaListener(topics = "payment.completed")
    public void consumePaymentCompleted(
            @Payload Map<String, Object> payload){

        try{
            String accountNumber = (String) payload.get("accountNumber");
            String amount = payload.get("amount").toString();

            sendAlert(accountNumber,
                    "PAYMENT SUCCESSFUL",
                    String.format(
                            "Payment of %s completed. "+
                                    "Razorpay ID: %s",
                            amount, payload.get("razorpayPaymentId")
                    ));
        }
        catch (Exception e){
            log.error("Error sending payment notification: {}", e.getMessage());
        }
    }

    @KafkaListener(topics = "payment.failed")
    public void consumePaymentFailed(
            @Payload Map<String, Object> payload){
        try{
            String accountNumber = (String) payload.get("accountNumber");
            String amount = payload.get("amount").toString();

            sendAlert(accountNumber,
                    "PAYMENT FAILED",
                    String.format(
                            "Your payment of %s could not be processed. "+
                                    "Please try again or contact support.",
                            amount
                    ));
        }
        catch (Exception e){
            log.error("Error sending payment failure notification: {}", e.getMessage());
        }
    }

    private void sendAlert(String accountNumber, String subject, String message){

        log.info("---------------------------------------------");

        log.info("Account : {}", accountNumber);
        log.info("Subject : {}", subject);
        log.info("Message : {}", message);

        log.info("---------------------------------------------");

    }

}

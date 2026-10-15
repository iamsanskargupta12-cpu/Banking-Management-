package com.banking.paymentservice.service;

import com.banking.paymentservice.dto.CreatePaymentRequest;
import com.banking.paymentservice.dto.PaymentOrderResponse;
import com.banking.paymentservice.entity.Payment;
import com.banking.paymentservice.entity.PaymentStatus;
import com.banking.paymentservice.repository.PaymentRepository;
import com.razorpay.Order;
import com.razorpay.RazorpayClient;
import com.razorpay.RazorpayException;
import com.razorpay.Utils;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

@Service
@Slf4j
@RequiredArgsConstructor
public class PaymentService {

    private final PaymentRepository paymentRepository;

    @Value("${razorpay.key-id}")
    private String keyId;
    @Value("${razorpay.key-secret}")
    private String keySecret;
    @Value("${razorpay.webhook-secret}")
    private String webhookSecret;

    private static final String PAYMENT_COMPLETED_TOPIC = "payment.completed";
    private static final String PAYMENT_FAILED_TOPIC = "payment.failed";

    private final KafkaTemplate<String, Object> kafkaTemplate;


    /**
     * Create Razorpaypayment order
     * FLOW-
     * 1. Create Order in Razorpay
     * 2. Save Payment
     * 3. Return Order details to frontend
     * 4. Frontend shows Razorpay checkout
     * 5. User pays
     * 6. razorpay calls webhook.
     * @param request
     * @return
     */
    public PaymentOrderResponse createPaymentOrder(CreatePaymentRequest request) throws RazorpayException {
        log.info("Create Payment Order for account : {}  and amount : {}", request.getAccountNumber(), request.getAmount());

        RazorpayClient razorpayClient = new RazorpayClient(keyId, keySecret);

        //Converted Amount
        int convertedAmount = request.getAmount().multiply(BigDecimal.valueOf(100)).intValue();

        JSONObject orderRequest  = new JSONObject();

        orderRequest.put("amount", convertedAmount);
        orderRequest.put("currency" , "INR");
        orderRequest.put("receipt", "rcpt_" + System.currentTimeMillis() + UUID.randomUUID().toString().replace("-", "").substring(0,10));

        Order razorpayOrder = razorpayClient.orders.create(orderRequest);


        log.info("Razorpay order created :{}", razorpayOrder.get("id").toString());


        //Save Payment Record

        Payment payment = new Payment();
        payment.setRazorpayOrderId(razorpayOrder.get("id").toString());
        payment.setAccountNumber(request.getAccountNumber());
        payment.setAmount(request.getAmount());
        payment.setCurrency("INR");

        payment.setStatus(PaymentStatus.CREATED);
        payment.setDescription(request.getDescription());

        Payment savedPayment = paymentRepository.save(payment);

        return new PaymentOrderResponse(
                savedPayment.getId(),
                razorpayOrder.get("id").toString(),
                request.getAmount(),
                "INR",
                "CREATED",
                keyId
        );
    }

    public void handleWebhook(String signature, String rawBody){
        try {
            if (webhookSecret != null && !webhookSecret.isBlank()
                    && !Utils.verifyWebhookSignature(rawBody, signature, webhookSecret)) {
                throw new RazorpayException("Invalid webhook signature");
            }
        } catch (RazorpayException e) {
            log.error("Webhook signature verification failed : {}", e.getMessage());
            throw new IllegalArgumentException("Invalid webhook signature", e);
        }

        Map<String, Object> payload = new JSONObject(rawBody).toMap();

        String event = (String) payload.get("event");
        log.info("Razorpay webhook received : {}", event);

        if("payment.captured".equals(event)){
            handlePaymentSuccess(payload);
        }
        else if("payment.failed".equals(event)){
            handlePaymentFailure(payload);
        }
    }

    private void handlePaymentSuccess(Map<String, Object> payload){

        try{
            Map<String, Object> paymentData = extractPaymentData(payload);

            String orderId = (String) paymentData.get("order_id");
            String paymentId = (String) paymentData.get("id");

            Payment payment = paymentRepository.findByRazorpayOrderId(orderId)
                    .orElseThrow(() -> new RuntimeException("Payment Not Found for order : " + orderId));


            payment.setRazorpayPaymentId(paymentId);
            payment.setStatus(PaymentStatus.COMPLETED);
            paymentRepository.save(payment);

            //Publish payment completed event to kafka.

            Map<String, Object> event = new HashMap<>();

            event.put("paymentId", payment.getId());
            event.put("accountNumber", payment.getAccountNumber());
            event.put("amount",  payment.getAmount());
            event.put("razorpayPaymentId", paymentId);

            kafkaTemplate.send(PAYMENT_COMPLETED_TOPIC, payment.getId(), event);

            log.info("payment Completed : {}", payment.getId());


        } catch (Exception e) {
            log.error("Error handling payment success : {}", e.getMessage());
        }

    }

    private void handlePaymentFailure(Map<String, Object> payload){

        try{

            Map<String, Object> paymentData = extractPaymentData(payload);

            String orderId = (String) paymentData.get("order_id");

            Payment payment = paymentRepository.findByRazorpayOrderId(orderId)
                    .orElseThrow(() -> new RuntimeException("Payment Not Found for order : " + orderId));

            payment.setStatus(PaymentStatus.FAILED);
            payment.setFailureReason("Payment Failed via Razorpay");
            paymentRepository.save(payment);

            Map<String, Object> event = new HashMap<>();

            event.put("paymentId", payment.getId());
            event.put("accountNumber", payment.getAccountNumber());
            event.put("amount",  payment.getAmount());
            event.put("reason", "Payment Failed via Razorpay");

            kafkaTemplate.send(PAYMENT_FAILED_TOPIC, payment.getId(), event);

            log.warn("payment Failed : {}", payment.getId());
        }
        catch (Exception e){

            log.error("Error handling payment failure : {}", e.getMessage());

        }
    }

    private Map<String, Object> extractPaymentData(Map<String, Object> payload){

        Map<String, Object> entity = (Map<String, Object>) payload.get("payload");

        Map<String, Object> paymentWrapper = (Map<String, Object>) entity.get("payment");

        return (Map<String, Object>) paymentWrapper.get("entity");

    }
}

package com.banking.paymentservice.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class PaymentOrderResponse {

    private String paymentID;

    private String razorpayOrderId;

    private BigDecimal amount;

    private String currency;

    private String status;

    private String razorpayKeyId;

}

package com.banking.paymentservice.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class CreatePaymentRequest {

    @NotBlank(message = "Account Number cannot be blank")
    private String accountNumber;

    @NotNull(message = "Amount is Required")
    @Positive(message = "Amount must be a positive number")
    private BigDecimal amount;

    private String Description;

}

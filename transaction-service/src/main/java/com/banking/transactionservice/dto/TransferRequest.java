package com.banking.transactionservice.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.PositiveOrZero;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class TransferRequest {

    @NotBlank(message = "Sender's Account Number is required")
    private String senderAccountNumber;

    @NotBlank(message = "Reciever Account number is required")
    private String recieverAccountNumber;

    @NotBlank(message = "Amount is required")
    @Positive(message = "Amount must be a valid number")
    private BigDecimal amount;

    private String description;
}

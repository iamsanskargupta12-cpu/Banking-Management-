package com.banking.accountservice.controller;

import com.banking.accountservice.dto.AccountResponse;
import com.banking.accountservice.dto.CreateAccountRequest;
import com.banking.accountservice.entity.Account;
import com.banking.accountservice.service.AccountService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.micrometer.observation.autoconfigure.ObservationProperties;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.math.BigDecimal;

@RestController
@RequestMapping("/api/v1/accounts")
@Slf4j
@RequiredArgsConstructor
public class AccountController {

    private final AccountService accountService;

    //ResponseEntity returns the data along with the HTTP status code of the type AccountResponse
    @PostMapping
    public ResponseEntity<AccountResponse> createAccount
    (@RequestBody @Valid CreateAccountRequest request) { //It will take value of type CreateAccountRequest @Valid, validates the data and @RequestBody converts the incoming JSON data into a Java object.
        return ResponseEntity.status(HttpStatus.CREATED).body(accountService.createAccount(request));
    }

    @GetMapping("/{accountNumber}")
    public ResponseEntity<AccountResponse> getAccount
            (@PathVariable String accountNumber) {
        return ResponseEntity.ok(accountService.getAccount(accountNumber));
    }

    @GetMapping("/{accountNumber}/balance")
    public ResponseEntity<BigDecimal> getBalance
            (@PathVariable String accountNumber) {
        return ResponseEntity.ok(accountService.getBalance(accountNumber));
    } 

    @PutMapping("/{accountNumber}/block")
    public ResponseEntity<String> Block
            (@PathVariable String accountNumber) {
        accountService.blockAccount(accountNumber);
        return ResponseEntity.ok("Account Blocked successfully");
    }

    /*
     * SAGA Step 1 - Deduct Balance
     * Called by Transaction Service when transfer is initiated.
     */

    @PutMapping("/{accountNumber}/deduct")
    public ResponseEntity<String> deductBalance
            (@PathVariable String accountNumber,
             @RequestParam BigDecimal amount) {
        accountService.deductBalance(accountNumber, amount);
        return ResponseEntity.ok("Account deducted successfully");
    }

    /*
     *SAGA Step 4 - Compensating transaction endpoint
     *Called by Transaction Service in two scenerios
     * 1. Fraud Detected -> refund Sender(Undo step 1)
     * 2.Transaction Completed -> Credit Reciever
     */

    @PutMapping("/{accountNumber}/credit")
    public ResponseEntity<String> creditBalance
            (@PathVariable String accountNumber,
             @RequestParam BigDecimal amount) {
        accountService.creditBalance(accountNumber, amount);
        return ResponseEntity.ok("Account credited successfully");
    }

}



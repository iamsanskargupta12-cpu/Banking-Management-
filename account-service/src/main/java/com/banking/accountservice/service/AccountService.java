package com.banking.accountservice.service;

import com.banking.accountservice.dto.AccountResponse;
import com.banking.accountservice.dto.CreateAccountRequest;
import com.banking.accountservice.entity.Account;
import com.banking.accountservice.entity.AccountStatus;
import com.banking.accountservice.entity.AccountType;
import com.banking.accountservice.repository.AccountRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import java.math.BigDecimal;
import java.security.SecureRandom;

@Service
@Slf4j
@RequiredArgsConstructor
public class AccountService {

    private final AccountRepository accountRepository;
    private static SecureRandom secureRandom = new SecureRandom();



    public AccountResponse createAccount(CreateAccountRequest request) {
        log.info("Creating account for: {}", request.getEmail());

        if(accountRepository.existsByEmail(request.getEmail())) {
            throw new RuntimeException("Account already exists : " + request.getEmail());
        }

//        Account account = Account.builder()
//                .accountHolderName(request.getAccountHolderName())
//        We can do like this if we use builder annotation in Account Class.

        Account account = new Account();

        account.setAccountHolderName(request.getAccountHolderName());
        account.setEmail(request.getEmail());
        account.setPhone(request.getPhone());
        account.setAccountType(request.getAccountType());
        account.setAccountStatus(AccountStatus.ACTIVE);
        account.setBalance(request.getInitialDeposit());
        //now we need to generate account number, which should be 12 digit long and must be unique.
        account.setAccountNumber(generateAccountNumber());
        account.setDailyTransactionLimit(
                request.getAccountType() == AccountType.SAVINGS
                        ? new BigDecimal("1000000")
                        : new BigDecimal("5000000")
        );

        Account savedAccount = accountRepository.save(account);
        log.info("Account Created : {}", savedAccount.getAccountNumber());

//        Now I need to return of type AccountResponse, but i have got of type Account, so i will create a function to map it to AccountResponse type.

        return mapToResponse(savedAccount);
    }


//    Generate 12 digit unique number;
    private String generateAccountNumber() {
        String accountNumber;

        do {
            long number = secureRandom.nextLong(1000000000000L);
            accountNumber = String.format("%012 d", number);
        }
        while (accountRepository.existsByAccountNumber(accountNumber));
        return accountNumber;
    }


    /**
     * Get account by accountNumber
     * @param accountNumber
     * @return
     */
    public AccountResponse getAccount(String accountNumber) {

        Account account = accountRepository.findByAccountNumber(accountNumber)
                .orElseThrow(()-> new RuntimeException("Account not Found"));

        return mapToResponse(account);
    }

    /**
     * Get Account Balance
     * @param accountNumber
     * @return
     */
    public BigDecimal getBalance(String accountNumber) {

        Account account = accountRepository.findByAccountNumber(accountNumber)
                .orElseThrow(()-> new RuntimeException("Account not Found"));

        return account.getBalance();
    }

    /**
     * Called by Fraud Detection service vio Kafka
     * @param accountNumber
     */
    public void blockAccount(String accountNumber) {
        log.info("Blocking account for: {}", accountNumber);


        Account account = accountRepository.findByAccountNumber(accountNumber)
                .orElseThrow(()-> new RuntimeException("Account not Found"));

        account.setAccountStatus(AccountStatus.BLOCKED);
        accountRepository.save(account);
        log.info("Account blocked: {}", accountNumber);
    }

    /**
     * Deduct balance from Senders acoount
     * Called by Transactoin Service
     * @param accountNumber
     * @param amount
     */
    public void deductBalance(String accountNumber, BigDecimal amount) {
        log.info("Deducting amoount : {} from account: {}", amount, accountNumber);

        Account account = accountRepository.findByAccountNumber(accountNumber)
                .orElseThrow(()-> new RuntimeException("Account not Found"));

        if(account.getAccountStatus() !=  AccountStatus.ACTIVE) {
            throw new RuntimeException("Account Status Not Active");
        }

        account.setBalance(account.getBalance().subtract(amount));
        accountRepository.save(account);
        log.info("Balance updated, New account balance: {}", account.getBalance());
    }

    public void creditBalance(String accountNumber, BigDecimal amount) {
        log.info("Credit amount : {} to account: {}", amount, accountNumber);

        Account account = accountRepository.findByAccountNumber(accountNumber)
                .orElseThrow(()-> new RuntimeException("Account not Found"));

        account.setBalance(account.getBalance().add(amount));
        accountRepository.save(account);

        log.info("Balance updated, New account balance: {}", account.getBalance());
    }


    private AccountResponse mapToResponse(Account account) {
        AccountResponse accountResponse = new AccountResponse();

        accountResponse.setId(account.getId());
        accountResponse.setAccountNumber(account.getAccountNumber());
        accountResponse.setAccountHolderName(account.getAccountHolderName());
        accountResponse.setEmail(account.getEmail());
        accountResponse.setPhone(account.getPhone());
        accountResponse.setAccountType(account.getAccountType());
        accountResponse.setAccountStatus(account.getAccountStatus());
        accountResponse.setBalance(account.getBalance());
        accountResponse.setDailyTransactionLimit(account.getDailyTransactionLimit());
        accountResponse.setCreatedAt(account.getCreatedAt());
//        accountResponse.setAccountStatus(account.getAccountStatus());
        return accountResponse;
    }

}

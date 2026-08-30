package com.banking.accountservice.service;


import com.banking.accountservice.dto.AccountResponse;
import com.banking.accountservice.dto.CreateAccountRequest;
import com.banking.accountservice.entity.Account;
import com.banking.accountservice.entity.AccountStatus;
import com.banking.accountservice.entity.AccountType;
import com.banking.accountservice.repository.AccountRepository;
import jakarta.validation.Valid;
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
    private static SecureRandom random = new SecureRandom();

    public AccountResponse createAccount(@Valid CreateAccountRequest request) {
            log.info("Creating account FOR: {}", request.getEmail());

            if(accountRepository.existsByEmail(request.getEmail())) {
                throw new IllegalArgumentException("Account with email " + request.getEmail() + " already exists");
            }

            Account account = new Account();
            account.setEmail(request.getEmail());
            account.setAccountHolderName(request.getAccountHolderName());
            account.setPhone(request.getPhone());
            account.setAccountType(request.getAccountType());
            account.setStatus(AccountStatus.ACTIVE);
            account.setBalance(request.getInitialDeposit());
            account.setDailyTransactionLimit(
                    request.getAccountType() == AccountType.SAVINGS ? new BigDecimal("100000.00") : new BigDecimal("500000.00")
            );
            account.setAccountNumber(generateAccountNumber());

            Account savedAccount = accountRepository.save(account);
            log.info("Account created :{ }", savedAccount.getAccountNumber());

            return mapToResponse(savedAccount);
    }

    private String generateAccountNumber() {
        String accountNumber;

        do{
            long number= random.nextLong(1_000_000_000_000L);
            accountNumber =String.format("%012d", number);
        }while(accountRepository.existsByAccountNumber(accountNumber));

        return accountNumber;
    }

    private AccountResponse mapToResponse(Account savedAccount) {
        return new AccountResponse(
                savedAccount.getId(),
                savedAccount.getAccountNumber(),
                savedAccount.getAccountHolderName(),
                savedAccount.getEmail(),
                savedAccount.getPhone(),
                savedAccount.getAccountType(),
                savedAccount.getStatus(),
                savedAccount.getBalance(),
                savedAccount.getDailyTransactionLimit(),
                savedAccount.getCreatedAt()
        );
    }

    public AccountResponse getAccount(String accountNumber) {
        Account account =accountRepository.findByAccountNumber(accountNumber)
                .orElseThrow(() -> new IllegalArgumentException("Account with account number " + accountNumber + " not found"));

        return mapToResponse(account);
    }

    public BigDecimal getBalance(String accountNumber) {
        Account account = accountRepository.findByAccountNumber(accountNumber)
                .orElseThrow(() -> new IllegalArgumentException("Account with account number " + accountNumber + " not found"));
        return account.getBalance();
    }


//    block account - called by fraud detection service via kafka
//    @requestparam accounnumber
    public void blockAccount(String accountNumber) {
        log.info("Blocking account with account number: {}", accountNumber);
        Account account = accountRepository.findByAccountNumber(accountNumber)
                .orElseThrow(() -> new IllegalArgumentException("Account with account number " + accountNumber + " not found"));
        account.setStatus(AccountStatus.BLOCKED);
        accountRepository.save(account);

    }


//    deduct balance from sender account
//    called by transaction service
//    @Param accountnumber
//    @Param amount

    public void deductBalance(String accountNumber, BigDecimal amount) {
        log.info("Deduct balance {} from account number: {}", amount, accountNumber);
        Account account = accountRepository.findByAccountNumber(accountNumber)
                .orElseThrow(() -> new IllegalArgumentException("Account with account number " + accountNumber + " not found"));

        if(account.getStatus()!=AccountStatus.ACTIVE) {
            throw new IllegalArgumentException("Account with account number " + accountNumber + " is not active");
        }
        if(account.getBalance().compareTo(amount)<0) {
            throw new IllegalArgumentException("Insufficient balance in account number " + accountNumber);
        }
        account.setBalance(account.getBalance().subtract(amount));
        accountRepository.save(account);

        log.info("Balance deducted successfully from account number: {} and updated balance is : {}", accountNumber, account.getBalance());
    }


//    credit balance
//    called by transaction service  via kafka
//    @param accountnumber
//    @param amount
    public void creditBalance(String accountNumber, BigDecimal amount) {
        log.info("crediting {} to account: {} ",amount,accountNumber);
        Account account = accountRepository.findByAccountNumber(accountNumber)
                .orElseThrow(() -> new IllegalArgumentException("Account with account number " + accountNumber + " not found"));
        account.setBalance(account.getBalance().add(amount));
        accountRepository.save(account);
        log.info("Balance credited successfully to account number: {} and updated balance is : {}", accountNumber, account.getBalance());
    }
}

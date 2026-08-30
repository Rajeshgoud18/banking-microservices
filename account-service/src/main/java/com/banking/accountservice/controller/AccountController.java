package com.banking.accountservice.controller;


import com.banking.accountservice.dto.AccountResponse;
import com.banking.accountservice.dto.CreateAccountRequest;
import com.banking.accountservice.service.AccountService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
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

    @PostMapping
    public ResponseEntity<AccountResponse> createAccount(@Valid @RequestBody CreateAccountRequest request) {
        log.info("Received request to create account: {}", request);
        return ResponseEntity.status(HttpStatus.CREATED).body(accountService.createAccount(request));

    }

    @GetMapping("/{accountNumber}")
    public ResponseEntity<AccountResponse> getAccount(@RequestParam String accountNumber) {
        log.info("Received request to get account with account number: {}", accountNumber);
        return ResponseEntity.ok(accountService.getAccount(accountNumber));
    }

    @GetMapping("/{accountNumber}/balance")
    public ResponseEntity<BigDecimal> getBalance(@PathVariable String accountNumber) {
        log.info("Received request to get balance for account number: {}", accountNumber);
        return ResponseEntity.ok(accountService.getBalance(accountNumber));
    }

    @PutMapping("/{accountNumber}/block")
    public ResponseEntity<String> blockAccount(@PathVariable String accountNumber){
        log.info("Received request to block account with account number: {}", accountNumber);
        accountService.blockAccount(accountNumber);
        return ResponseEntity.ok("Account blocked successfully");
    }

//    saga pattern 1 deduct balance
//    called by transaction service when transfer is initiated

    @PutMapping("/{accountNumber}/deduct")
    public ResponseEntity<String> deductBalance(@PathVariable String accountNumber, @RequestParam BigDecimal amount) {
        log.info("Received request to deduct balance for account number: {} with amount: {}", accountNumber, amount);
        accountService.deductBalance(accountNumber, amount);
        return ResponseEntity.ok("Balance deducted successfully");
    }



//     saga pattern 2 compensating transaction endpoint
//    called when transaction service in two scenarios
//     1. Fraud detected -> refund to the sender (undo step 1)
//     2. Transaction completed credit the receiver

    @PutMapping("/{accountNumber}/credit")
    public ResponseEntity<String> creditBalance(@PathVariable String accountNumber, @RequestParam BigDecimal amount){
        accountService.creditBalance(accountNumber,amount);
        return ResponseEntity.ok("Balance credited successfully");
    }


}

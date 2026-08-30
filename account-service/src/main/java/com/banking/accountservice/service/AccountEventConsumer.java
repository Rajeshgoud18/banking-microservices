package com.banking.accountservice.service;


import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.Map;

@Service
@Slf4j
@RequiredArgsConstructor
public class AccountEventConsumer {

    private final AccountService accountService;

    /*
    * Consume tarnsaction . completed event from kafka
    * credits receiver account
    * @params payload
    * */


    @KafkaListener(topics = "transaction.completed")
    public void consumeTransactionCompleted(@Payload Map<String,Object> payload) {
        log.info("Received transaction completed event: {}", payload);

        try{
            String receiverAccount = (String) payload.get("receiverAccountNumber");
            BigDecimal amount = new BigDecimal(payload.get("amount").toString());

            log.info("crediting account: {} amount: {}",receiverAccount, amount);
            accountService.creditBalance(receiverAccount, amount);

        }catch (Exception e){
            log.error("Error processing transaction completed event: {}", e.getMessage());
        }
    }

    @KafkaListener(topics = "fraud.detected")
    public void consumeFraudDetected(@Payload Map<String,Object> payload) {
        log.info("Received fraud detected event: {}", payload);
        try{
            String accountNumber = (String) payload.get("accountNumber");
            log.info("Fraud detected for account: {}",accountNumber);
            accountService.blockAccount(accountNumber);
        }catch (Exception e){
            log.error("Error processing fraud detected event: {}", e.getMessage());
        }
    }
}

package com.banking.transactionservice.service;


import com.banking.transactionservice.client.AccountServiceClient;
import com.banking.transactionservice.dto.TransactionResponse;
import com.banking.transactionservice.dto.TransferRequest;
import com.banking.transactionservice.entity.Transaction;
import com.banking.transactionservice.entity.TransactionStatus;
import com.banking.transactionservice.entity.TransactionType;
import com.banking.transactionservice.event.TransactionCompletedEvent;
import com.banking.transactionservice.event.TransactionInitiatedEvent;
import com.banking.transactionservice.repository.TransactionRepository;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

@Service
@Slf4j
@RequiredArgsConstructor
public class TransactionService {


    private final TransactionRepository transactionRepository;
    private final AccountServiceClient accountServiceClient;

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final RedisTemplate<String,String> redisTemplate;


    private static final String TRANSACTION_INITIATED_TOPIC="transaction.initiated";
    private static final String TRANSACTION_COMPLTED_TOPIC="transaction.completed";
    private static final String TRANSACTION_REFUNDED_TOPIC="transaction.refunded";
    private static final String FRAUD_DETECTED_TOPIC="fraud.detected";


    /*
    * saga step 1 initiate transfer
    * Deducts from sender via feign
    * saves tranaction as processing
    * publish event  to kafka for fraud check
    * Returns
    * @Param request
    * return
    *
    * */
    public TransactionResponse transfer(@Valid TransferRequest transferRequest) {
        log.info("Initiating transfer from {} to {} for amount {}",
                transferRequest.getSenderAccountNumber(),
                transferRequest.getReceiverAccountNumber(),
                transferRequest.getAmount());

        //step -1 deduct from the sender
        accountServiceClient.deductBalance(transferRequest.getSenderAccountNumber(), transferRequest.getAmount());

        Transaction transaction =new Transaction();
        transaction.setSenderAccountNumber(transferRequest.getSenderAccountNumber());
        transaction.setReceiverAccountNumber(transferRequest.getReceiverAccountNumber());
        transaction.setAmount(transferRequest.getAmount());
        transaction.setStatus(TransactionStatus.PROCESSING);
        transaction.setType(TransactionType.TRANSFER);
        transaction.setDescription(transferRequest.getDescription());
        transaction.setReferenceNumber(UUID.randomUUID().toString());

        Transaction savedTransaction = transactionRepository.save(transaction);
        log.info("transaction saved as processing : {}",savedTransaction.getId());


        //saga step-2 : publish for fraud check
        TransactionInitiatedEvent event =new TransactionInitiatedEvent(
          savedTransaction.getId(),
                savedTransaction.getSenderAccountNumber(),
                savedTransaction.getReceiverAccountNumber(),
                savedTransaction.getAmount(),
                savedTransaction.getDescription()
        );

        kafkaTemplate.send(TRANSACTION_INITIATED_TOPIC,savedTransaction.getId(), event);
        log.info("daga step -2 TransactionInitiatedEvent published to kafka for fraud check : {}",savedTransaction.getId());


        return mapToResponse(savedTransaction);


    }

    private TransactionResponse mapToResponse(Transaction savedTransaction) {
        return new TransactionResponse(
                savedTransaction.getId(),
                savedTransaction.getSenderAccountNumber(),
                savedTransaction.getReceiverAccountNumber(),
                savedTransaction.getAmount(),
                savedTransaction.getType(),
                savedTransaction.getStatus(),
                savedTransaction.getDescription(),
                savedTransaction.getFailureReason(),
                savedTransaction.getReferenceNumber(),
                savedTransaction.getCreatedAt(),
                savedTransaction.getCompletedAt()
        );
    }

    public TransactionResponse getTransaction(String transactionId) {
        log.info("Fetching transaction with ID: {}", transactionId);
        Transaction transaction = transactionRepository.findById(transactionId)
                .orElseThrow(() -> new RuntimeException("Transaction not found with ID: " + transactionId));
        log.info("Transaction found: {}", transaction.getId());
        return mapToResponse(transaction);
    }

    public List<TransactionResponse> getTransactionHistory(String accountNumber) {
        log.info("Fetching transaction history for account: {}", accountNumber);
        List<Transaction> transactions = transactionRepository.findBySenderAccountNumberOrderByCreatedAtDesc(accountNumber, accountNumber);
        log.info("Found {} transactions for account: {}", transactions.size(), accountNumber);
        return transactions.stream().map(this::mapToResponse).collect(Collectors.toList());
    }

    public TransactionResponse verifyOTP(String transactionId, String otp) {
        log.info("Verifying OTP for transaction: {}", transactionId);
       Transaction transaction = transactionRepository.findById(transactionId)
                .orElseThrow(() -> new RuntimeException("Transaction not found with ID: " + transactionId));

       String otpKey="verification:otp"+transactionId;
       String otpStored=redisTemplate.opsForValue().get(otpKey);
       if(otpStored==null){
           //otp expired
           log.warn("OTP expired for  transaction: {}",transactionId);
           compansateTransaction(transaction, "OTP- expired transaction cancelled and amount refunded");
           return mapToResponse(transaction);
       }

       if(!otpStored.equals(otp)){
           log.warn("Wrong otp - blocking account and redunding: {}",transactionId);
           redisTemplate.delete(otpKey);
           blockAccountAndCompansate(transaction,"Wrong otp entered transaction cancelled , account blocked for security");
           return mapToResponse(transaction);
       }

       log.info("otp verified - transaction completeing: {}", transactionId);
       redisTemplate.delete(otpKey);
       completeTransaction(transaction);
       return mapToResponse(transaction);
    }

    private void completeTransaction(Transaction transaction) {
        transaction.setStatus(TransactionStatus.COMPLETED);
        transaction.setCompletedAt(LocalDateTime.now());
        transactionRepository.save(transaction);

        TransactionCompletedEvent transactionCompletedEvent=new TransactionCompletedEvent(
                transaction.getId(),
                transaction.getSenderAccountNumber(),
                transaction.getReceiverAccountNumber(),
                transaction.getAmount(),
                transaction.getDescription()
        );

        kafkaTemplate.send(TRANSACTION_COMPLTED_TOPIC,transaction.getId(),transactionCompletedEvent);
        log.info("Transaction completed and event published: {}",transaction.getId());
    }

    private void blockAccountAndCompansate(Transaction transaction, String reason) {
        //public fraud detected - block event
        Map<String,Object> fraudEvent =new HashMap<>();
        fraudEvent.put("transactionId",transaction.getId());
        fraudEvent.put("accountNumber",transaction.getSenderAccountNumber());
        fraudEvent.put("reason",reason);

        kafkaTemplate.send(FRAUD_DETECTED_TOPIC,transaction.getSenderAccountNumber(),fraudEvent);
        log.warn("fraud detected published - account {} will be blocked kindly contact your bank",transaction.getSenderAccountNumber());

        compansateTransaction(transaction,reason);

    }

    private void compansateTransaction(Transaction transaction, String reason) {
        log.warn("SAGA compensation - refunding: {} amount :{}", transaction.getSenderAccountNumber(),transaction.getAmount());

        //CREDIT back to the sender synchronously
        accountServiceClient.creditBalance(transaction.getSenderAccountNumber(), transaction.getAmount());
        transaction.setStatus(TransactionStatus.FLAGGED);
        transaction.setFailureReason(reason+" SAGA COMPENSATION executed, amount refunded at "+ LocalDateTime.now());
        transactionRepository.save(transaction);

        //PUBLISH refund event - Notification service will alert user
        Map<String,Object> refundEvent=new HashMap<>();
        refundEvent.put("transactionId",transaction.getId());
        refundEvent.put("senderAccountNumber",transaction.getSenderAccountNumber());
        refundEvent.put("amount",transaction.getAmount());
        refundEvent.put("reason", reason);

        kafkaTemplate.send(TRANSACTION_REFUNDED_TOPIC,transaction.getId(),refundEvent);

        log.info("SAGA compensation completed - refund event published for transaction: {}",transaction.getId());
    }

    public void processCleanResult(String transactionId) {
        Transaction transaction = transactionRepository.findById(transactionId)
                .orElseThrow(() -> new RuntimeException("Transaction not found with ID: " + transactionId));

            if(transaction.getStatus()!= TransactionStatus.PROCESSING){
                log.warn("Transaction {} not processing - skipping ",transactionId);
                return;
            }
            completeTransaction(transaction);
    }
}

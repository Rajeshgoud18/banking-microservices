package com.banking.notificationservice.service;


import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import java.util.Map;

@Service
@Slf4j
public class NotificationService {


    @KafkaListener(topics = "transaction.otp.generated")
    public void consumeOtpGenerated(@Payload Map<String,Object> payload){


        try{
            String accountNumber=(String)payload.get("accountNumber");
            String otp=(String)payload.get("otp");
            String transactionId=(String) payload.get("transactionId");
            String amount=payload.get("amount").toString();
            String reason=(String) payload.get("reason");


            sendAlert(accountNumber,"TRANSACTION VERIFICATION ALERT",
                    String.format(
                            "Suspicious activity detected on your account %s." +
                                    "Reason: %s "+
                                    "A transaction of %s is pending verification. "+
                                    "Your OTP is: %s. valid for 5 minutes. "+
                                    "If this wasn't you- ignore this message."
                    ));
        } catch (Exception e) {
            log.error("Error occurred while processing OTP generated message", e);
        }
    }


    @KafkaListener(topics = "transaction.completed")
    public void consumeTransactionCompleted(@Payload Map<String,Object> payload){
        try{
            String senderAccount=(String)payload.get("senderAccountNumber");
            String receiverAccount=(String)payload.get("receiverAccountNumber");
            String amount=payload.get("amount").toString();


            //Debit alert
            sendAlert(senderAccount,"DEBIT ALERT",
                    String.format(
                            "%s debited from account %s",amount,senderAccount
                    ));

            //credit alert
            sendAlert(receiverAccount,"CREDIT ALERT",
                    String.format(
                            "%s credited to account %s",amount,receiverAccount
                    ));
        } catch (Exception e) {
            log.error("Error occurred while processing transaction completed message {}", e.getMessage());
        }
    }


    @KafkaListener(topics = "fraud.detected")
    public void consumeFraudDetected(@Payload Map<String,Object> payload){

        try{
            String accountNumber=(String)payload.get("accountNumber");
            String reason=(String)payload.get("reason");

            sendAlert(accountNumber,"SUSPICIOUS ACTIVITY DETECTED",
                    String.format(
                            "your account %s has been blocked. "+
                                    "Reason: %s. "+
                                    "please contact your bank immediately",accountNumber,reason
                    ));
        } catch (Exception e) {
            log.error("Error occurred while processing fraud detected message {}", e.getMessage());
        }
    }


    @KafkaListener(topics = "transaction.refunded")
    public void consumeTransactionRefunded(@Payload Map<String,Object> payload){
        try{
            String senderAccountNumber=(String)payload.get("senderAccountNumber");
            String amount=payload.get("amount").toString();
            String reason=(String)payload.get("reason");

            sendAlert(senderAccountNumber,"REFUND PROCESSED ",
                    String.format(
                            "your account %s has been refunded with amount %s. "+
                                    "Reason: %s. ",senderAccountNumber,amount,reason
                    ));
        } catch (Exception e) {
            log.error("Error occurred while processing transaction refunded message {}", e.getMessage());
        }
    }


    @KafkaListener(topics = "payment.completed")
    public void consumePaymentCompleted(@Payload Map<String,Object> payload){
        try{
            String accountNumber=(String)payload.get("accountNumber");
            String amount=payload.get("amount").toString();
            String paymentId=(String)payload.get("paymentId");

            sendAlert(accountNumber,"PAYMENT SUCCESSFUL",
                    String.format(
                            "your account %s has been debited with amount %s. "+
                                    "Payment Id: %s. ",accountNumber,amount,paymentId
                    ));
        }
        catch (Exception e){
            log.error("Error occurred while processing payment completed message {}", e.getMessage());
        }
    }


    @KafkaListener(topics = "payment.failed")
    public void consumePaymentFailed(@Payload Map<String,Object> payload){
        try{
            String accountNumber=(String)payload.get("accountNumber");
            String amount=payload.get("amount").toString();
            String paymentId=(String)payload.get("paymentId");
            String reason=(String)payload.get("reason");

            sendAlert(accountNumber,"PAYMENT FAILED",
                    String.format(
                            "your account %s has been debited with amount %s. "+
                                    "Payment Id: %s. "+
                                    "Reason: %s. ",accountNumber,amount,paymentId,reason
                    ));
        }
        catch (Exception e){
            log.error("Error occurred while processing payment failed message {}", e.getMessage());
        }
    }

    private void sendAlert(String accountNumber, String subject, String message) {
            log.info("----------------------------------------");
            log.info("Account: {}", accountNumber);
            log.info("Subject: {}",subject);
            log.info("Message: {}", message);
            log.info("----------------------------------------");
    }
}

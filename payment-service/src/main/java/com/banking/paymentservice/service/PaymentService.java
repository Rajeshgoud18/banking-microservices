package com.banking.paymentservice.service;


import com.banking.paymentservice.dto.CreatePaymentRequest;
import com.banking.paymentservice.dto.PaymentOrderResponse;
import com.banking.paymentservice.entity.Payment;
import com.banking.paymentservice.entity.PaymentStatus;
import com.banking.paymentservice.repository.PaymentRepository;
import com.razorpay.Order;
import com.razorpay.RazorpayClient;
import com.razorpay.RazorpayException;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

@Service
@Slf4j
@RequiredArgsConstructor
public class PaymentService {

    private final PaymentRepository paymentRepository;
    private final KafkaTemplate<String,Object> kafkaTemplate;

    @Value("${razorpay.key-id}")
    private String keyId;

    @Value("${razorpay.key-secret}")
    private String keySecret;


    private static final String PAYMENT_COMPLETED_TOPIC="payment.completed";
    private static final String PAYMENT_FAILED_TOPIC="payment.failed";

    /**
     * Creates a razor payment order based on the provided request.
     * 1. create order in razorpay
     * 2. save payment record in DB
     * 3. Return order details to the frontend
     * 4. frontend shows razorpay checkout
     * 5. user pays
     * 6. razorpay calls  webhook
     * @param request the request containing payment details
     * @return the response containing payment order information
     */

    public PaymentOrderResponse createPaymentOrder(@Valid CreatePaymentRequest request) throws RazorpayException {
        log.info("Creating payment order  for account: {} amount: {}", request.getAccountNumber(), request.getAmount());

        RazorpayClient razorpayClient=new RazorpayClient(keyId,keySecret);

        int convertedAmount= request.getAmount().multiply(BigDecimal.valueOf(100)).intValue(); // convert into paise

        JSONObject orderRequest=new JSONObject();
        orderRequest.put("amount",convertedAmount);
        orderRequest.put("currency", "USD/INR");
        orderRequest.put("receipt","rcpt_"+System.currentTimeMillis()+ UUID.randomUUID().toString().replace("-","").substring(0,10));

        Order razorpayOrder=razorpayClient.orders.create(orderRequest);

        log.info("Razorpay order created: {}",razorpayOrder.get("id").toString());


        //save payment order
        Payment payment =new Payment();
        payment.setRazorpayPaymentId(razorpayOrder.get("id").toString());
        payment.setAccountNumber(request.getAccountNumber());
        payment.setAmount(request.getAmount());
        payment.setCurrency("USD/INR");
        payment.setStatus(PaymentStatus.CREATED);
        payment.setDescription(request.getDescription());

        Payment savedPayment=paymentRepository.save(payment);


        return new PaymentOrderResponse(
                savedPayment.getId(),
                savedPayment.getRazorpayPaymentId(),
                request.getAmount(),
                "USD/INR",
                "CREATED",
                keyId
        );

    }

    public void handleWebhook(Map<String, Object> payload) {
        log.info("Received Razorpay webhook: {}",payload.get("event"));
        String event=(String) payload.get("event");

        if("payment.captured".equals(event)){
            handlePaymentSuccess(payload);
        }
        else if("payment.failed".equals(event)){
            handlePaymentFailure(payload);
        }
        else{
            log.warn("Unhandled webhook event: {}",event);
        }
    }

    private void handlePaymentSuccess(Map<String, Object> payload) {
        try{
            Map<String , Object> paymentData= extractPaymentData(payload);
            String orderId=(String) paymentData.get("order_id");
            String paymentId=(String) paymentData.get("id");

            Payment payment= paymentRepository.findByRazorpayOrderId(orderId).
                    orElseThrow(()-> new RuntimeException("Payment not found for orderId: "+orderId));


            payment.setRazorpayPaymentId(paymentId);
            payment.setStatus(PaymentStatus.COMPLETED);
            paymentRepository.save(payment);


            //publish paymnet  completed event
            Map<String,Object> event =new HashMap<>();
            event.put("paymentId",payment.getId());
            event.put("accountNumber",payment.getAccountNumber());
            event.put("amount",payment.getAmount());
            event.put("razorpayPaymentId",paymentId);

            kafkaTemplate.send(PAYMENT_COMPLETED_TOPIC,payment.getId(),event);

            log.info("Payment Completed: {}", payment.getId());

        } catch (Exception e) {
            log.error("Error Handling payment success: {}",e.getMessage());
        }
    }

    private Map<String, Object> extractPaymentData(Map<String, Object> payload) {
        Map<String,Object>  entity =(Map<String, Object>) payload.get("payload");

        Map<String,Object> paymentWrapper=(Map<String, Object>) entity.get("payment");

        return (Map<String, Object>) paymentWrapper.get("entity");
    }

    private void handlePaymentFailure(Map<String, Object> payload) {
        try{
            Map<String , Object> paymentData= extractPaymentData(payload);
            String orderId=(String) paymentData.get("order_id");


            Payment payment= paymentRepository.findByRazorpayOrderId(orderId).
                    orElseThrow(()-> new RuntimeException("Payment not found for orderId: "+orderId));



            payment.setStatus(PaymentStatus.FAILED);
            payment.setFailureReason("Payement failed via RazorPay");
            paymentRepository.save(payment);


            //publish paymnet  failed event
            Map<String,Object> event =new HashMap<>();
            event.put("paymentId",payment.getId());
            event.put("accountNumber",payment.getAccountNumber());
            event.put("amount",payment.getAmount());
            event.put("reason","Payement failed via RazorPay");

            kafkaTemplate.send(PAYMENT_FAILED_TOPIC,payment.getId(),event);

            log.warn("Payment Failed: {}", payment.getId());

        } catch (Exception e) {
            log.error("Error Handling payment failure: {}",e.getMessage());
        }
    }
}

package com.example.paymentservice.service;

import com.example.paymentservice.entity.DeliveryEvent;
import com.example.paymentservice.entity.OrderEvent;
import com.example.paymentservice.entity.PaymentEvent;
import com.example.paymentservice.repository.PaymentRepo;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
@Slf4j
public class KafkaEventConsuemr {

    private final KafkaEventProducer kafkaEventProducer;
    private final PaymentRepo paymentRepo;

    @KafkaListener(topics = "order-topic", groupId = "group_id", containerFactory = "orderKafkaListenerContainerFactory")
    public void consumeOrderEvent(String orderEvent) throws JsonProcessingException {
        ObjectMapper objectMapper = new ObjectMapper();
        var orderEventObj = objectMapper.readValue(orderEvent, OrderEvent.class);
        paymentRepo.findByUsername(orderEventObj.getUsername())
                .ifPresentOrElse(payment -> {
                    if (payment.getBalance() < orderEventObj.getPrice()) {
                        log.error("Insufficient balance for username: {}", orderEventObj.getUsername());
                        try {
                            kafkaEventProducer.sendPaymentEvent(PaymentEvent.builder()
                                    .orderId(orderEventObj.getOrderId())
                                    .username(orderEventObj.getUsername())
                                    .productName(orderEventObj.getProductName())
                                    .price(orderEventObj.getPrice())
                                    .status("FAILED")
                                    .build());
                        } catch (JsonProcessingException e) {
                            throw new RuntimeException(e);
                        }
                    }
                    else {
                        payment.setBalance(payment.getBalance() - orderEventObj.getPrice());
                        paymentRepo.save(payment);
                        try {
                            kafkaEventProducer.sendPaymentEvent(PaymentEvent.builder()
                                    .orderId(orderEventObj.getOrderId())
                                    .username(orderEventObj.getUsername())
                                    .productName(orderEventObj.getProductName())
                                    .price(orderEventObj.getPrice())
                                    .status("SUCCESS")
                                    .build());
                        } catch (JsonProcessingException e) {
                            throw new RuntimeException(e);
                        }
                    }
                },()->log.error("User not found"));
        log.info("Consumed order event: {}", orderEventObj);
    }

    @KafkaListener(topics = "delivery-topic", groupId = "group_id", containerFactory = "deliveryKafkaListenerContainerFactory")
    public void consumeDeliveryEvent(String deliveryEvent) throws JsonProcessingException {
        ObjectMapper objectMapper = new ObjectMapper();
        var deliveryEventObj = objectMapper.readValue(deliveryEvent, DeliveryEvent.class);
        log.info("Consumed delivery event: {}", deliveryEventObj);
        if (deliveryEventObj.getStatus().equals("FAILED")) {
            var payment = paymentRepo.findByUsername(deliveryEventObj.getUsername()).orElseThrow();
           // payment.setBalance(payment.getBalance() + deliveryEventObj.getPrice());
            paymentRepo.save(payment);
        }
    }
}

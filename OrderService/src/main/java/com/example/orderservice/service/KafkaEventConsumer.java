package com.example.orderservice.service;

import com.example.orderservice.entity.DeliveryEvent;
import com.example.orderservice.entity.PaymentEvent;
import com.example.orderservice.repository.OrderRepo;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
@Slf4j
public class KafkaEventConsumer {

    private final OrderRepo orderRepo;

    @KafkaListener(topics = "payment-topic", groupId = "group_id", containerFactory = "paymentKafkaListenerContainerFactory")
    public void consumePaymentEvent(String paymentEvent) throws JsonProcessingException {
        ObjectMapper objectMapper = new ObjectMapper();
        var paymentEventObj = objectMapper.readValue(paymentEvent, PaymentEvent.class);
        log.info("Consumed payment event: {}", paymentEventObj);
        var order = orderRepo.findById(paymentEventObj.getOrderId()).orElseThrow();
        if (paymentEventObj.getStatus().equals("SUCCESS")) {
            order.setStatus("PLACED");
            orderRepo.save(order);
        } else {
            order.setStatus("CANCELLED");
            orderRepo.save(order);
        }

    }

    @KafkaListener(topics = "delivery-topic", groupId = "group_id", containerFactory = "deliveryKafkaListenerContainerFactory")
    public void consumeDeliveryEvent(String deliveryEvent) throws JsonProcessingException {
        ObjectMapper objectMapper = new ObjectMapper();
        var deliveryEventObj = objectMapper.readValue(deliveryEvent, DeliveryEvent.class);
        log.info("Consumed delivery event: {}", deliveryEventObj);
        var order = orderRepo.findById(deliveryEventObj.getOrderId()).orElseThrow();
        if (deliveryEventObj.getStatus().equals("DELIVERED")) {
            order.setStatus("DELIVERED");
            orderRepo.save(order);
        } else {
            order.setStatus("CANCELLED");
            orderRepo.save(order);
        }
    }
}

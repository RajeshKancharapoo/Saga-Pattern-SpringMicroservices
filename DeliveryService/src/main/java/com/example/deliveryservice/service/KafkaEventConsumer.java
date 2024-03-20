package com.example.deliveryservice.service;


import com.example.deliveryservice.entity.Delivery;
import com.example.deliveryservice.entity.DeliveryEvent;
import com.example.deliveryservice.entity.PaymentEvent;
import com.example.deliveryservice.repository.DeliveryRepo;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
@Slf4j
@RequiredArgsConstructor
public class KafkaEventConsumer {

    private final DeliveryRepo deliveryRepo;
    private final KafkaEventProducer kafkaEventProducer;
    @KafkaListener(topics = "payment-topic", groupId = "group_id", containerFactory = "kafkaListenerContainerFactory")
    public void consume(String paymentEvent) throws JsonProcessingException {
        ObjectMapper objectMapper = new ObjectMapper();
        var paymentEventObj = objectMapper.readValue(paymentEvent, PaymentEvent.class);
        log.info("Consumed payment event: {}", paymentEventObj);
        if (paymentEventObj.getStatus().equals("SUCCESS")) {
            log.info("Payment successful. Order is ready for delivery");
            deliveryRepo.save(Delivery.builder()
                    .username(paymentEventObj.getUsername())
                    .productName(paymentEventObj.getProductName())
                    .price(paymentEventObj.getPrice())
                    .status("DELIVERED")
                    .build());
            try {
                kafkaEventProducer.sendDeliveryEvent(DeliveryEvent.builder()
                        .orderId(paymentEventObj.getOrderId())
                        .username(paymentEventObj.getUsername())
                        .productName(paymentEventObj.getProductName())
                        .price(paymentEventObj.getPrice())
                        .status("DELIVERED")
                        .build());
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        } else {
            deliveryRepo.save(Delivery.builder()
                    .username(paymentEventObj.getUsername())
                    .productName(paymentEventObj.getProductName())
                    .price(paymentEventObj.getPrice())
                    .status("FAILED")
                    .build());
           try {
                kafkaEventProducer.sendDeliveryEvent(DeliveryEvent.builder()
                        .orderId(paymentEventObj.getOrderId())
                        .username(paymentEventObj.getUsername())
                        .productName(paymentEventObj.getProductName())
                        .price(paymentEventObj.getPrice())
                        .status("FAILED")
                        .build());
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
           }
            log.info("Payment failed.");
        }
    }
}

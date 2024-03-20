package com.example.paymentservice.service;

import com.example.paymentservice.entity.PaymentEvent;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class KafkaEventProducer {

    private final KafkaTemplate<String, String> kafkaTemplate;

    public void sendPaymentEvent(PaymentEvent paymentEvent) throws JsonProcessingException {
        ObjectMapper objectMapper = new ObjectMapper();
        String val=objectMapper.writeValueAsString(paymentEvent);
        kafkaTemplate.send("payment-topic", val);
    }
}

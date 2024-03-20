package com.example.orderservice.service;

import com.example.orderservice.entity.OrderEvent;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class KafkaEventProducer {

    private final KafkaTemplate<String, String> kafkaTemplate;

    public void sendOrderEvent(OrderEvent orderEvent) throws JsonProcessingException {
        ObjectMapper objectMapper = new ObjectMapper();
        String val=objectMapper.writeValueAsString(orderEvent);
        kafkaTemplate.send("order-topic", val);
    }
}

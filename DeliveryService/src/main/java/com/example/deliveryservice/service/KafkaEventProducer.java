package com.example.deliveryservice.service;

import com.example.deliveryservice.entity.DeliveryEvent;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class KafkaEventProducer {

    private final KafkaTemplate<String, String> kafkaTemplate;

    public void sendDeliveryEvent(DeliveryEvent deliveryEvent) throws JsonProcessingException {
        ObjectMapper objectMapper = new ObjectMapper();
        String val=objectMapper.writeValueAsString(deliveryEvent);
        kafkaTemplate.send("delivery-topic", val);
    }
}

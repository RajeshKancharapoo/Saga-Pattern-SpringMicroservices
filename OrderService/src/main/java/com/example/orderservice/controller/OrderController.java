package com.example.orderservice.controller;

import com.example.orderservice.entity.Order;
import com.example.orderservice.entity.OrderEvent;
import com.example.orderservice.repository.OrderRepo;
import com.example.orderservice.service.KafkaEventProducer;
import com.fasterxml.jackson.core.JsonProcessingException;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/order")
@RequiredArgsConstructor
public class OrderController {

    private final OrderRepo orderRepo;
    private final KafkaEventProducer kafkaEventProducer;
    @PostMapping("/create")
    public String createOrder(@RequestBody Order order) throws JsonProcessingException {
        var ans=orderRepo.save(order);
        var orderEvent= OrderEvent.builder()
                .orderId(ans.getOrderId())
                .price(ans.getPrice())
                .productName(ans.getProductName())
                .status("CREATED")
                .username(ans.getUsername())
                .build();
        kafkaEventProducer.sendOrderEvent(orderEvent);
        return "Order created successfully";
    }
}

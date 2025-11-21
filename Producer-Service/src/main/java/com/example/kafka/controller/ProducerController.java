package com.example.kafka.controller;

import com.example.kafka.model.Order;
import com.example.kafka.producer.OrderProducer;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;

import java.util.Map;
import java.util.UUID;

@RestController
@RequestMapping("/api/producer/orders")
@RequiredArgsConstructor
public class ProducerController {

    private final OrderProducer orderProducer;

    @PostMapping
    public ResponseEntity<?> publishOrder(@Validated @RequestBody OrderRequest request) {
        if (request.getPrice() == null || request.getPrice() <= 0) {
            return ResponseEntity.badRequest().body(Map.of("error", "price must be > 0"));
        }
        if (request.getProduct() == null || request.getProduct().isBlank()) {
            return ResponseEntity.badRequest().body(Map.of("error", "product is required"));
        }
        if (request.getOrderId() == null || request.getOrderId().isBlank()) {
            request.setOrderId(UUID.randomUUID().toString());
        }
        Order order = toOrder(request);
        orderProducer.send(order);
        return ResponseEntity.status(HttpStatus.ACCEPTED)
                .body(Map.of(
                        "message", "Order sent",
                        "orderId", order.getOrderId(),
                        "product", order.getProduct(),
                        "price", order.getPrice()
                ));
    }

    @PostMapping("/demo")
    public ResponseEntity<?> sendDemoOrder() {
        Order order = new Order();
        order.setOrderId(UUID.randomUUID().toString());
        order.setProduct("demo-item");
        order.setPrice((float) (Math.random() * 100.0));
        orderProducer.send(order);
        return ResponseEntity.status(HttpStatus.ACCEPTED)
                .body(Map.of("message", "Demo order sent", "orderId", order.getOrderId(), "price", order.getPrice()));
    }

    private Order toOrder(OrderRequest request) {
        Order order = new Order();
        order.setOrderId(request.getOrderId());
        order.setProduct(request.getProduct());
        order.setPrice(request.getPrice());
        return order;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class OrderRequest {
        private String orderId;
        private String product;
        private Float price;
    }
}

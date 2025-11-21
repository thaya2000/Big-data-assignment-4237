package com.example.kafka.producer;

import com.example.kafka.model.Order;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class OrderProducer {

    private final KafkaTemplate<String, Order> kafkaTemplate;

    @Value("${kafka.topic.orders}")
    private String ordersTopic;

    public void send(Order order) {
        var future = kafkaTemplate.send(ordersTopic, order.getOrderId().toString(), order);

        future.whenComplete((result, ex) -> {
            if (ex != null) {
                log.error("Failed to send order {} to topic {}: {}", order.getOrderId(), ordersTopic, ex.getMessage(), ex);
            } else if (result != null) {
                log.info("Order {} sent to topic {} partition {} offset {}", order.getOrderId(), ordersTopic,
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());
            }
        });
    }
}

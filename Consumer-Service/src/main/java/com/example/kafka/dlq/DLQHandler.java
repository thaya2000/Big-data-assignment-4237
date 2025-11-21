package com.example.kafka.dlq;

import com.example.kafka.model.Order;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Component;

/**
 * Sends permanently failed messages to the DLQ topic.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class DLQHandler {

    private final KafkaTemplate<String, Order> kafkaTemplate;

    @Value("${kafka.topic.orders-dlq}")
    private String dlqTopic;

    public void sendToDLQ(Order order, Exception exception, int retryCount) {
        Message<Order> message = MessageBuilder.withPayload(order)
                .setHeader(KafkaHeaders.TOPIC, dlqTopic)
                .setHeader(KafkaHeaders.KEY, order.getOrderId().toString())
                .setHeader("retry-count", retryCount)
                .setHeader("error-message", exception.getMessage())
                .build();

        kafkaTemplate.send(message).whenComplete((result, ex) -> {
            if (ex == null) {
                log.warn("Order {} sent to DLQ (partition {}, offset {})", order.getOrderId().toString(),
                        result.getRecordMetadata().partition(), result.getRecordMetadata().offset());
            } else {
                log.error("Failed to send order {} to DLQ: {}", order.getOrderId().toString(), ex.getMessage(), ex);
            }
        });
    }
}

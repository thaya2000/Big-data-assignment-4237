# Kafka Order System

Kafka-based producer/consumer example using Avro-serialized order messages.

## Prerequisites
- Docker + Docker Compose
- JDK 17
- Maven (wrapper provided)

## Run infrastructure
```bash
cp .env.example .env
docker compose up -d
```
This brings up a 3-broker Kafka cluster, Schema Registry, and Kafka UI at http://localhost:8080.

## Build both services (generates Avro classes)
```bash
cd Producer-Service
./mvnw clean package

cd ../Consumer-Service
./mvnw clean package
```

## Start services
- Producer: `cd Producer-Service && ./mvnw spring-boot:run`
- Consumer: `cd Consumer-Service && ./mvnw spring-boot:run`

## Send orders
- POST `http://localhost:8082/api/producer/orders` with JSON `{ "orderId": "1001", "product": "item1", "price": 12.5 }`
- Or POST `http://localhost:8082/api/producer/orders/demo` to send a random demo order.

## Monitor consumer
- Health: `GET http://localhost:8081/api/consumer/health`
- Stats / running average: `GET http://localhost:8081/api/consumer/stats`
- Average details: `GET http://localhost:8081/api/consumer/average`

Messages failing processing are retried with exponential backoff and finally routed to the DLQ topic.

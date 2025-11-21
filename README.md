# Kafka Order System

Kafka producer/consumer example with Avro-serialized order messages, retry + DLQ, and running average aggregation.

## Prerequisites
- Docker + Docker Compose
- JDK 17
- Maven (wrapper provided)

## 1) Bring up Kafka stack
```bash
cp .env.example .env    # tweak ports/versions if needed
docker compose up -d
```
Brings up Zookeeper, 3 Kafka brokers, Schema Registry, and Kafka UI at http://localhost:8080.

## 2) Build services (generates Avro classes)
```bash
cd Producer-Service
./mvnw clean package

cd ../Consumer-Service
./mvnw clean package
```

## 3) Run services (two terminals)
- Producer: `cd Producer-Service && ./mvnw spring-boot:run`
- Consumer: `cd Consumer-Service && ./mvnw spring-boot:run` 

## 4) Send orders
- POST `http://localhost:8082/api/producer/orders`
  ```json
  { "orderId": "1001", "product": "item1", "price": 12.5 }
  ```
- Or POST `http://localhost:8082/api/producer/orders/demo` to send a random demo order.

## 5) Monitor consumer
- Health: `GET http://localhost:8081/api/consumer/health`
- Stats / running average: `GET http://localhost:8081/api/consumer/stats`
- Average details: `GET http://localhost:8081/api/consumer/average`

Messages that fail are retried with exponential backoff; exhausted retries go to the DLQ topic. Use Kafka UI to inspect topics `orders`, `orders-retry`, and `orders-dlq`.

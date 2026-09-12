# 🏦 Banking Microservices

A **Microservices-Based Banking Application** built using Spring Boot, focusing on service decomposition, inter-service communication, transaction workflows, and event-driven architecture.

`Java` · `Spring Boot` · `Spring Cloud` · `MySQL` · `Apache Kafka`

---

## 📖 Overview

This project simulates a real-world banking system broken down into independently deployable microservices. It demonstrates practical patterns used in production-grade financial systems — API gateway routing, asynchronous event-driven communication via Kafka, transactional consistency, and rule-based fraud checks.

---

## ✨ Features

- 🧩 Designed using **Microservices Architecture** with separate services for banking operations
- 🚪 **API Gateway** for routing client requests to the appropriate services
- 🔄 Implemented **inter-service communication** between microservices
- ⚡ Used **Apache Kafka** for asynchronous, event-driven communication
- 🛡️ Implemented validation and business rules such as **velocity checks** for transaction processing
- 💳 Designed **transaction workflows** with a focus on reliability and consistency
- 🗄️ Used **MySQL** with **JPA/Hibernate** for persistent data management

---

## 🏗️ Architecture

```
                        ┌─────────────────┐
                        │   API Gateway    │
                        └────────┬─────────┘
                                 │
        ┌───────────────┬───────┼───────────────┬────────────────┐
        │               │       │               │                │
┌───────▼──────┐ ┌───────▼──────┐ ┌────▼─────┐ ┌───────▼───────┐ ┌────▼───────────────┐
│Account Service│ │Payment Service│ │Transaction│ │Fraud Detection│ │Notification Service│
│               │ │               │ │ Service   │ │   Service     │ │                    │
└───────┬──────┘ └───────┬──────┘ └────┬─────┘ └───────┬───────┘ └────┬───────────────┘
        │               │              │               │              │
        └───────────────┴──────────────┴───────────────┴──────────────┘
                                 │
                        ┌────────▼─────────┐
                        │   Apache Kafka    │
                        │  (Event Streaming)│
                        └────────┬─────────┘
                                 │
                        ┌────────▼─────────┐
                        │      MySQL        │
                        └───────────────────┘
```

---

## 🧩 Microservices

| Service | Responsibility |
|---|---|
| **api-gateway** | Routes incoming client requests to the appropriate downstream service |
| **account-service** | Manages customer accounts — creation, balance, and account details |
| **payment-service** | Handles payment initiation and processing |
| **transaction-service** | Manages transaction workflows, ensuring reliability and consistency |
| **fraud-detection-service** | Applies validation and business rules (e.g., velocity checks) to flag suspicious transactions |
| **notification-service** | Sends event-driven notifications to customers on transaction/account activity |

---

## 🛠️ Tech Stack

- **Language:** Java
- **Framework:** Spring Boot, Spring Cloud
- **Messaging:** Apache Kafka (event-driven communication)
- **Database:** MySQL
- **ORM:** JPA / Hibernate
- **Containerization:** Docker & Docker Compose

---

## 📂 Project Structure

```
banking-microservices/
├── account-service/
├── api-gateway/
├── fraud-detection-service/
├── notification-service/
├── payment-service/
├── transaction-service/
├── docker-compose.yml
└── .gitignore
```

---

## 🚀 Getting Started

### Prerequisites

- Java 17+
- Maven
- Docker & Docker Compose
- MySQL
- Apache Kafka

### Running the Application

1. **Clone the repository**
   ```bash
   git clone https://github.com/Rajeshgoud18/banking-microservices.git
   cd banking-microservices
   ```

2. **Start infrastructure (MySQL, Kafka) via Docker Compose**
   ```bash
   docker-compose up -d
   ```

3. **Build and run each microservice**
   ```bash
   cd account-service && mvn spring-boot:run
   # repeat for api-gateway, payment-service, transaction-service,
   # fraud-detection-service, notification-service
   ```

4. **Access the application via the API Gateway**
   ```
   http://localhost:<gateway-port>
   ```

---

## 🔄 Event-Driven Flow (Example)

1. A client initiates a payment via the **API Gateway**.
2. **payment-service** processes the request and publishes an event to **Kafka**.
3. **transaction-service** consumes the event and records the transaction.
4. **fraud-detection-service** validates the transaction against business rules (e.g., velocity checks).
5. **notification-service** consumes the resulting event and notifies the customer.

---

## 📌 Roadmap

- [ ] Add centralized configuration server (Spring Cloud Config)
- [ ] Add service discovery (Eureka/Consul)
- [ ] Add authentication & authorization (Spring Security / OAuth2)
- [ ] Add centralized logging and distributed tracing
- [ ] Add unit and integration tests

---

## 👤 Author

**Rajesh (Rajeshgoud18)**
B.Tech CSE, RGUKT Srikakulam

---

## 📄 License

This project is for educational and portfolio purposes.

# Banking System with Fraud Detection

A microservices-based banking system with real-time **fraud detection**, **OTP-based transaction verification**, and **Razorpay payment integration**, built with Spring Boot, Apache Kafka, Redis, and MySQL.

Transactions are executed using the **SAGA orchestration pattern**: a transfer is initiated, validated asynchronously by the fraud-detection service, and only completed after a user-provided OTP is verified. Fraudulent activity automatically blocks accounts and triggers refunds (compensation).

---

## Architecture

```
                         ┌────────────────┐
                         │   API Gateway   │  :8080
                         └───────┬────────┘
              ┌──────────────────┼───────────────────┐
              │                  │                   │
       ┌──────▼───────┐  ┌──────▼─────────┐  ┌──────▼───────┐
       │    Account   │  │  Transaction   │  │   Payment    │
       │   Service    │  │    Service     │  │   Service    │
       │     :8081    │  │     :8082      │  │    :8083     │
       └──────┬───────┘  └──────┬─────────┘  └──────┬───────┘
              │                 │                   │
              └─────────────────┼───────────────────┘
                                │   Apache Kafka (Event Bus)
              ┌─────────────────┼───────────────────┐
              │                 │                   │
       ┌──────▼────────┐  ┌────▼──────┐   ┌────────▼─────────┐
       │   Fraud       │  │   Redis   │   │   Notification   │
       │  Detection    │  │  (OTP +   │   │     Service      │
       │   Service     │  │  caching) │   │      :8085       │
       │     :8084     │  └───────────┘   └──────────────────┘
       └───────────────┘
```

### Services

| Service | Port | Responsibility |
|---------|------|----------------|
| **api-gateway** | 8080 | Single entry point; routes `/api/v1/**` to the backing services |
| **account-service** | 8081 | Account lifecycle, balance operations (create, deduct, credit, block) |
| **transaction-service** | 8082 | SAGA orchestrator; transfers, OTP verification, compensation/refunds |
| **payment-service** | 8083 | Razorpay order creation and webhook handling |
| **fraud-detection-service** | 8084 | Asynchronous fraud analysis of initiated transactions |
| **notification-service** | 8085 | Consumes events and sends alerts (OTP, debit/credit, fraud, refunds, payments) |

### Tech Stack

- **Java 21**, **Spring Boot 4.1**
- **Apache Kafka** – event-driven messaging
- **Redis** – OTP storage with TTL, rate-limiting data
- **MySQL 8** – persistence
- **Razorpay** – payment gateway
- **Spring Cloud Gateway** – API routing

---

## Transaction Flow (SAGA)

1. **Transfer initiated** – `transaction-service` deducts the sender's balance (via `account-service`), saves the transaction as `PROCESSING`, and publishes `transaction.initiated`.
2. **Fraud check** – `fraud-detection-service` analyzes the transaction and publishes either `verification.required` (suspicious) or `fraud.check.clean`.
   - *Clean* → transaction completes automatically.
   - *Suspicious* → `transaction-service` generates a **6-digit OTP** (stored in Redis, 5-minute TTL) and publishes `transaction.otp.generated`.
3. **OTP verification** – user submits the OTP via `POST /api/v1/transactions/{transactionId}/verify`.
   - *Correct OTP* → transaction marked `COMPLETED`, publishes `transaction.completed`.
   - *Wrong OTP* → account **blocked**, funds refunded (compensation), publishes `fraud.detected` + `transaction.refunded`.
   - *OTP expired* → refund issued via compensation.
4. **Notifications** – `notification-service` consumes the events and alerts the user (debit/credit alerts, OTP, fraud, refunds).
5. **Payments** – `payment-service` creates a Razorpay order; Razorpay's webhook (`payment.captured` / `payment.failed`) updates the payment and publishes `payment.completed` / `payment.failed`.

### Kafka Topics

| Topic | Producer | Consumer(s) | Purpose |
|-------|----------|-------------|---------|
| `transaction.initiated` | transaction-service | fraud-detection-service | Start fraud analysis |
| `fraud.check.clean` | fraud-detection-service | transaction-service | Transaction passed fraud check |
| `verification.required` | fraud-detection-service | transaction-service | Suspicious transaction, request OTP |
| `transaction.otp.generated` | transaction-service | notification-service | Notify user with OTP |
| `transaction.completed` | transaction-service | notification-service, account-service | Transaction succeeded |
| `transaction.refunded` | transaction-service | notification-service | Refund/compensation executed |
| `fraud.detected` | transaction-service | notification-service, account-service | Block account + alert user |
| `payment.completed` / `payment.failed` | payment-service | notification-service | Payment status updates |

---

## Getting Started

### Prerequisites

- Java 21
- Maven (or use the included `./mvnw` wrapper)
- Docker & Docker Compose

### 1. Start Infrastructure

```bash
docker compose up -d
```

This starts **MySQL (3306)**, **Redis (6379)**, **Zookeeper (2181)**, and **Kafka (9092)**.

### 2. Run the Services

Start each service in its own terminal (order matters for a clean first boot):

```bash
cd account-service        && ./mvnw spring-boot:run
cd api-gateway            && ./mvnw spring-boot:run
cd transaction-service    && ./mvnw spring-boot:run
cd fraud-detection-service && ./mvnw spring-boot:run
cd notification-service   && ./mvnw spring-boot:run
cd payment-service        && ./mvnw spring-boot:run
```

The API Gateway is then available at `http://localhost:8080`.

### 3. Configuration

Sensitive settings are externalized via environment variables (with local defaults):

| Variable | Service | Default |
|----------|---------|---------|
| `RAZORPAY_KEY_ID` | payment-service | Razorpay **test** key |
| `RAZORPAY_KEY_SECRET` | payment-service | Razorpay **test** secret |
| `RAZORPAY_WEBHOOK_SECRET` | payment-service | *(empty — set it and webhook signatures are verified)* |

> The Razorpay webhook endpoint validates the `X-Razorpay-Signature` header whenever `RAZORPAY_WEBHOOK_SECRET` is configured. Always set it in production.

---

## API Overview

All requests go through the gateway: `http://localhost:8080`

### Account Service
| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/api/v1/accounts` | Create account |
| GET | `/api/v1/accounts/{accountNumber}` | Account details |
| GET | `/api/v1/accounts/{accountNumber}/balance` | Check balance |
| PUT | `/api/v1/accounts/{accountNumber}/block` | Block account |
| PUT | `/api/v1/accounts/{accountNumber}/deduct` | Deduct balance |
| PUT | `/api/v1/accounts/{accountNumber}/credit` | Credit balance |

### Transaction Service
| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/api/v1/transactions/transfer` | Initiate a transfer (SAGA step 1) |
| GET | `/api/v1/transactions/{transactionId}` | Transaction details |
| GET | `/api/v1/transactions/account/{accountNumber}` | Transaction history |
| POST | `/api/v1/transactions/{transactionID}/verify` | Verify OTP |

### Payment Service
| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/api/v1/payments/create-order` | Create Razorpay payment order |
| POST | `/api/v1/payments/webhook` | Razorpay webhook (signature-verified) |

---

## Project Structure

```
Banking-System-With-Fraud-Detection-Springboot-Kafka-Redis/
├── api-gateway/                  # Spring Cloud Gateway (port 8080)
├── account-service/              # Account management (port 8081)
├── transaction-service/          # SAGA orchestrator (port 8082)
├── payment-service/              # Razorpay payments (port 8083)
├── fraud-detection-service/      # Fraud analysis (port 8084)
├── notification-service/         # Event-driven alerts (port 8085)
└── docker-compose.yml            # MySQL, Redis, Kafka, Zookeeper
```

---

## Future Improvements

- Persist notifications / email delivery for `notification-service`
- Distributed tracing (e.g., Micrometer Tracing) across the SAGA
- Idempotency handling for Razorpay webhooks and Kafka consumers
- Containerized services (Dockerfiles) and CI/CD

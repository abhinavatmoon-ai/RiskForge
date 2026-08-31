# 🛡️ RiskForge

## Real-Time Financial Fraud Detection & Transaction Scoring Engine

**RiskForge** is a high-throughput, event-driven distributed system designed for **real-time financial transaction scoring, sliding-window velocity detection, and automated fraud mitigation**.

The project is built using **Java 21, Spring Boot 3, Apache Kafka, Redis, PostgreSQL, Docker, and Linux**, with a focus on demonstrating production-oriented backend engineering, distributed systems, event-driven architecture, concurrency, resilience, observability, and infrastructure management on a bare-metal homelab.

---

## 📌 Project Goals

RiskForge is designed to demonstrate how a modern financial transaction-processing platform can:

* Process transactions asynchronously at high throughput.
* Maintain strict per-account transaction ordering.
* Perform real-time fraud scoring.
* Detect transaction velocity anomalies using Redis.
* Apply blacklist and behavioral rules.
* Persist transactions using PostgreSQL.
* Route transactions based on risk scores.
* Automatically trigger fraud alerts.
* Process approved transactions through an ACID-compliant settlement pipeline.
* Expose operational metrics for monitoring.
* Run within constrained homelab hardware resources.
* Eventually deploy to Kubernetes/K3s through an automated CI/CD pipeline.

---

# 🏛️ System Architecture

```text
                    ┌───────────────────────────────────────┐
                    │ Payment Clients / POS / Mobile Apps  │
                    └───────────────────┬───────────────────┘
                                        │ HTTPS / REST
                                        ▼
                           ┌────────────────────────┐
                           │      API Gateway       │
                           │ Spring Cloud Gateway  │
                           │ JWT + Rate Limiting   │
                           └────────────┬───────────┘
                                        │
                                        ▼
                           ┌────────────────────────┐
                           │   Ingestion Service    │
                           │        :8081           │
                           └───────┬────────┬───────┘
                                   │        │
                         PENDING    │        │ Kafka Event
                                   │        │
                                   ▼        ▼
                         ┌──────────────┐  ┌─────────────────────┐
                         │ PostgreSQL 16│  │ Apache Kafka 7.6   │
                         │ Ledger DB    │  │ KRaft Cluster       │
                         └──────────────┘  └──────────┬──────────┘
                                                      │
                                             raw-transactions
                                             Key = accountId
                                                      │
                                                      ▼
                                      ┌──────────────────────────┐
                                      │   Fraud Engine Service   │
                                      │          :8082           │
                                      │                          │
                                      │ Risk Scoring + Rules     │
                                      └─────────────┬────────────┘
                                                    │
                              ┌─────────────────────┴─────────────────────┐
                              │                                           │
                              ▼                                           ▼
                    ┌──────────────────┐                       ┌─────────────────────┐
                    │     Redis 7      │                       │    PostgreSQL 16   │
                    │                  │                       │                     │
                    │ Velocity Windows │                       │ Blacklist Rules     │
                    │ Account State    │                       │ Customer Baselines  │
                    │ Geo Data         │                       │ Audit Data          │
                    └──────────────────┘                       └─────────────────────┘
                              │
                              ▼
                     ┌─────────────────┐
                     │  Risk Decision  │
                     └────────┬────────┘
                              │
                 ┌────────────┴────────────┐
                 │                         │
             Score >= 75               Score < 75
                 │                         │
                 ▼                         ▼
      ┌─────────────────────┐    ┌─────────────────────┐
      │ flagged-fraud-      │    │ settlement-approved │
      │ events              │    │                     │
      └──────────┬──────────┘    └──────────┬──────────┘
                 │                          │
                 ▼                          ▼
      ┌─────────────────────┐    ┌─────────────────────┐
      │ Notification &      │    │ Settlement Service  │
      │ Alert Service       │    │       :8083         │
      │       :8084         │    └──────────┬──────────┘
      └──────────┬──────────┘               │
                 │                          ▼
        ┌────────┴────────┐       ┌─────────────────────┐
        ▼                 ▼       │    PostgreSQL 16   │
    Push / SMS         Webhook    │ Final Ledger State │
                                  └─────────────────────┘
```

---

# 🧩 Microservices

| Service                      |   Port | Responsibilities                                                            | Key Technologies                   |
| ---------------------------- | -----: | --------------------------------------------------------------------------- | ---------------------------------- |
| `api-gateway`                | `8080` | Routing, JWT validation, rate limiting and resilience                       | Spring Cloud Gateway, Resilience4j |
| `ingestion-service`          | `8081` | Validate transactions, create `PENDING` records and publish events          | Spring Web, JPA, Kafka             |
| `fraud-engine-service`       | `8082` | Consume transactions, evaluate velocity/risk rules and calculate risk score | Spring Kafka, Redis, Java 21       |
| `settlement-service`         | `8083` | Process approved transactions and finalize financial ledger updates         | Spring Kafka, JPA, HikariCP        |
| `notification-alert-service` | `8084` | Process fraud events and dispatch monitoring/customer alerts                | Spring WebFlux, Kafka              |

---

# 🔑 Core Design Decisions

## 1. Kafka Partitioning & Ordering

The `accountId` is used as the Kafka partition key.

```text
Kafka Producer
      │
      │ key = accountId
      ▼
┌─────────────────────────────────┐
│           Kafka Topic           │
│       raw-transactions          │
├──────────┬──────────┬───────────┤
│Partition │Partition │ Partition │
│    0     │    1     │    2      │
├──────────┼──────────┼───────────┤
│ Account A│ Account B│ Account C │
│ Account A│ Account B│ Account C │
└──────────┴──────────┴───────────┘
```

Using `accountId` as the key ensures that transactions belonging to the same account are routed to the same Kafka partition.

This provides **partition-level ordering** for an account, which is important when evaluating sequential transaction activity and velocity windows.

> Important: Kafka ordering is guaranteed within a partition. The application should also ensure that transaction timestamps and event-processing semantics are handled correctly when delayed or out-of-order events are possible.

---

# ⚡ 2. Redis Sliding-Window Velocity Detection

Redis Sorted Sets (`ZSET`) are used to maintain transaction timestamps for each account.

For example:

```text
velocity:account:12345

Score              Member
--------------------------------
1710000010000      1710000010000
1710000025000      1710000025000
1710000032000      1710000032000
```

For a 60-second window:

```text
1. Remove expired entries

ZREMRANGEBYSCORE
key
0
(currentTime - 60000)

2. Add current transaction

ZADD
key
currentTime
currentTime

3. Count transactions

ZCARD key

4. Refresh TTL

EXPIRE key 120
```

A simplified detection rule can be:

```text
IF transaction_count_last_60_seconds > 3
THEN velocity_rule = TRIGGERED
```

The same mechanism can be extended to multiple windows:

```text
10 seconds
60 seconds
24 hours
```

This enables detection of both **short bursts** and **longer-term behavioral anomalies**.

---

# 🧠 3. Risk Scoring Engine

The fraud engine calculates a composite risk score between `0` and `100`.

Example scoring model:

```text
                    Transaction
                         │
                         ▼
              ┌────────────────────┐
              │ Feature Extraction │
              └─────────┬──────────┘
                        │
       ┌────────────────┼────────────────┐
       ▼                ▼                ▼
 Velocity           Blacklist        Location
 Analysis            Check           Anomaly
       │                │                │
       └────────────────┼────────────────┘
                        ▼
                 ┌──────────────┐
                 │ Risk Scoring │
                 │   0 - 100    │
                 └──────┬───────┘
                        │
              ┌─────────┴─────────┐
              ▼                   ▼
           >= 75                 < 75
              │                   │
           FRAUD              APPROVED
```

Potential scoring factors include:

* Transaction velocity.
* Number of transactions within defined windows.
* Blacklisted card/account.
* Unusual geographic movement.
* Deviation from customer baseline.
* Transaction amount anomalies.
* Multiple rapid transactions.
* Suspicious account behavior.

Example:

```text
Velocity anomaly       +30
Blacklist match        +40
Location anomaly       +20
Amount anomaly         +10
                       ----
Risk Score              100
```

---

# 💳 4. Transaction Lifecycle

Every transaction moves through an explicit lifecycle:

```text
RECEIVED
   │
   ▼
PENDING
   │
   ▼
FRAUD EVALUATION
   │
   ├───────────────┐
   │               │
   ▼               ▼
APPROVED          FLAGGED
   │               │
   ▼               ▼
SETTLED           BLOCKED
                   │
                   ▼
                ALERTED
```

This makes transaction state auditable and allows downstream services to operate asynchronously.

---

# 🔄 End-to-End Processing Flow

### Step 1 — Client Submission

Client submits:

```http
POST /api/v1/transactions
```

to the API Gateway.

---

### Step 2 — Authentication & Routing

The API Gateway:

* Validates JWT.
* Applies rate limiting.
* Performs request routing.
* Applies resilience policies.

The request is forwarded to `ingestion-service`.

---

### Step 3 — Transaction Ingestion

`ingestion-service`:

1. Validates the request.
2. Generates a transaction UUID.
3. Persists the transaction as `PENDING`.
4. Publishes the transaction to Kafka.

```text
PostgreSQL
    │
    └── status = PENDING

Kafka
    │
    └── raw-transactions
```

---

### Step 4 — Fraud Evaluation

`fraud-engine-service` consumes the Kafka event.

It evaluates:

```text
10-second velocity
60-second velocity
24-hour velocity
Blacklist
Location anomaly
Customer baseline
Transaction characteristics
```

The engine generates:

```text
Risk Score = 0 - 100
```

---

### Step 5 — Risk-Based Routing

```text
                 Risk Score
                     │
          ┌──────────┴──────────┐
          │                     │
        >= 75                  < 75
          │                     │
          ▼                     ▼
       FRAUD                 APPROVED
          │                     │
          ▼                     ▼
flagged-fraud-events     settlement-approved
```

---

### Step 6 — Fraud Alert

For high-risk transactions:

```text
fraud-engine-service
        │
        ▼
flagged-fraud-events
        │
        ▼
notification-alert-service
        │
        ├── WebSocket
        ├── Mock SMS
        ├── Push notification
        └── Webhook
```

The customer/account profile can also be locked depending on the rule configuration.

---

### Step 7 — Settlement

For approved transactions:

```text
settlement-approved
        │
        ▼
settlement-service
        │
        ▼
ACID Transaction
        │
        ├── Update ledger
        ├── Update account balance
        └── Update transaction status
        │
        ▼
     SETTLED
```

---

# 🛡️ Data Consistency Strategy

Financial settlement requires strong consistency.

The settlement service should use a database transaction:

```text
BEGIN TRANSACTION

1. Validate transaction state
2. Validate account balance
3. Create ledger entry
4. Update account balance
5. Update transaction status

COMMIT
```

If any operation fails:

```text
ROLLBACK
```

This prevents partially applied financial transactions.

---

# 📡 Kafka Topics

| Topic                  | Producer     | Consumer     | Purpose                | Key         |
| ---------------------- | ------------ | ------------ | ---------------------- | ----------- |
| `raw-transactions`     | Ingestion    | Fraud Engine | Raw transaction events | `accountId` |
| `flagged-fraud-events` | Fraud Engine | Notification | Fraud alerts           | `accountId` |
| `settlement-approved`  | Fraud Engine | Settlement   | Approved transactions  | `accountId` |

---

# 🗄️ Data Storage Strategy

## PostgreSQL

PostgreSQL acts as the persistent source of truth.

Potential data domains:

```text
accounts
transactions
ledger_entries
fraud_rules
blacklisted_cards
customer_baselines
audit_logs
```

PostgreSQL is responsible for:

* Persistent transaction state.
* Financial ledger.
* Account balances.
* Fraud rules.
* Blacklists.
* Audit history.

---

## Redis

Redis is used for **fast-changing, ephemeral state**.

Examples:

```text
velocity:account:{accountId}:10s
velocity:account:{accountId}:60s
velocity:account:{accountId}:24h

location:account:{accountId}

risk:account:{accountId}
```

Redis should not be treated as the authoritative financial ledger.

---

# 🧵 Concurrency Model

RiskForge uses **Java 21 Virtual Threads** where appropriate.

Virtual threads are particularly useful for I/O-heavy operations such as:

```text
Kafka
  │
  ├── Redis
  │
  ├── PostgreSQL
  │
  └── External APIs
```

The goal is to support a large number of concurrent operations without requiring a large number of heavyweight platform threads.

The Kafka consumer concurrency model should still be designed around Kafka's partitioning and consumer-group semantics rather than assuming virtual threads automatically provide ordering.

---

# 🛠️ Technology Stack

| Layer            | Technology                       | Purpose                       |
| ---------------- | -------------------------------- | ----------------------------- |
| Language         | Java 21 LTS                      | Application runtime           |
| Framework        | Spring Boot 3.x                  | Microservices                 |
| API Gateway      | Spring Cloud Gateway             | Routing/security              |
| Messaging        | Apache Kafka 7.6                 | Event streaming               |
| Kafka Mode       | KRaft                            | ZooKeeper-less Kafka          |
| Cache/State      | Redis 7                          | Real-time state               |
| Database         | PostgreSQL 16                    | Persistent storage            |
| ORM              | Spring Data JPA / Hibernate      | Database access               |
| Connection Pool  | HikariCP                         | JDBC pooling                  |
| Reactive         | Spring WebFlux                   | Notification APIs             |
| Resilience       | Resilience4j                     | Circuit breakers/rate control |
| Containerization | Docker                           | Runtime isolation             |
| Orchestration    | Docker Compose / K3s             | Deployment                    |
| Monitoring       | Prometheus                       | Metrics                       |
| Dashboards       | Grafana                          | Observability                 |
| Kafka UI         | Kafka UI                         | Kafka administration          |
| CI/CD            | Jenkins                          | Automated delivery            |
| Testing          | JUnit / Mockito / Testcontainers | Automated testing             |

---

# 🖥️ Homelab Environment

RiskForge is designed to run on a constrained bare-metal Linux system.

| Resource          | Specification            |
| ----------------- | ------------------------ |
| Node              | Lenovo IdeaPad 330-15IKB |
| CPU               | Intel Core i5-8250U      |
| CPU               | 4 Cores / 8 Threads      |
| Frequency         | 1.60–3.40 GHz            |
| RAM               | 8 GB DDR4                |
| Additional Memory | 4 GB ZRAM / Swap         |
| Storage           | 439 GB Ext4              |
| OS                | Ubuntu Server 24.04 LTS  |

The constrained environment is intentional: it provides an opportunity to practice **resource-aware distributed system design** rather than relying on a large cloud environment.

---

# 📦 Resource Allocation

| Component          | Memory Limit | JVM / Runtime Configuration       |
| ------------------ | -----------: | --------------------------------- |
| `homelab-kafka`    |      1024 MB | `-Xms256M -Xmx512M`               |
| `homelab-postgres` |       512 MB | Shared buffers: `128MB`           |
| `homelab-redis`    |       256 MB | `maxmemory 200mb`, `volatile-lru` |
| `homelab-kafka-ui` |       256 MB | `-Xmx128M`                        |
| Microservices      |  384 MB each | `-Xmx256M` each                   |

> These limits are starting points for the homelab and should be tuned based on actual JVM, Kafka, PostgreSQL, Redis, and system-level metrics.

---

# 🐳 Infrastructure

The initial environment uses Docker Compose.

```text
Docker Host
│
├── Kafka
├── PostgreSQL
├── Redis
├── Kafka UI
│
├── api-gateway
├── ingestion-service
├── fraud-engine-service
├── settlement-service
└── notification-alert-service
```

Persistent storage is used for stateful infrastructure:

```text
Docker Volume
      │
      ├── PostgreSQL data
      ├── Redis AOF
      └── Kafka data
```

---

# 📊 Observability

The target observability architecture is:

```text
Microservices
     │
     ▼
Micrometer
     │
     ▼
Prometheus
     │
     ▼
Grafana
```

Important metrics include:

### Application Metrics

* Transaction throughput.
* Fraud detection rate.
* Approval rate.
* Rejection rate.
* Risk score distribution.
* Transaction processing latency.
* p95 latency.
* p99 latency.

### Kafka Metrics

* Consumer lag.
* Records consumed.
* Records produced.
* Processing failures.
* Rebalances.

### Database Metrics

* HikariCP active connections.
* Connection acquisition time.
* Query latency.
* Database errors.

### JVM Metrics

* Heap usage.
* GC activity.
* Thread count.
* Virtual-thread workload.
* CPU usage.

---

# 🔐 Security

Security considerations include:

* JWT-based authentication.
* Gateway-level authorization.
* API rate limiting.
* Secrets managed outside source code.
* Database credentials through environment/configuration.
* Restricted Docker privileges.
* Input validation.
* Secure service-to-service communication.
* Audit logging.
* Idempotent transaction processing.

Production deployment should additionally consider:

```text
TLS everywhere
Secret management
Key rotation
Network policies
RBAC
Service authentication
Database encryption
Kafka ACLs
```

---

# ♻️ Idempotency & Failure Handling

Distributed systems can encounter:

```text
Network failures
Consumer crashes
Duplicate Kafka delivery
Database failures
Redis failures
Service restarts
Partial downstream failures
```

Therefore, transaction processing should be designed to tolerate duplicate events.

A transaction identifier such as:

```text
transactionId
```

can be used as an idempotency key.

Example:

```text
Kafka Event
     │
     ▼
Settlement Service
     │
     ▼
Check transactionId
     │
     ├── Already SETTLED → Ignore duplicate
     │
     └── Not settled → Process
```

This prevents duplicate settlement when a Kafka message is redelivered.

---

# 🚨 Failure Scenarios

| Failure                          | Expected Behavior                                                        |
| -------------------------------- | ------------------------------------------------------------------------ |
| Fraud engine unavailable         | Kafka retains events until consumer recovers                             |
| Settlement service unavailable   | Approved events remain in Kafka                                          |
| Notification service unavailable | Fraud events remain available for consumption                            |
| Redis unavailable                | Apply configured fail-safe/degraded scoring strategy                     |
| PostgreSQL unavailable           | Persistence fails safely; transaction must not be falsely marked settled |
| Consumer crash                   | Kafka rebalances partition ownership                                     |
| Duplicate event                  | Idempotency prevents duplicate financial processing                      |
| Network timeout                  | Retry/circuit breaker according to operation semantics                   |

---

# 🗺️ Development Roadmap

## Phase 1 — Core Infrastructure

* [x] Ubuntu 24.04 LTS host configuration.
* [x] SSH configuration.
* [x] Non-root Docker access.
* [x] PostgreSQL 16.
* [x] Persistent PostgreSQL volumes.
* [x] Redis 7.
* [x] Redis AOF persistence.
* [x] Apache Kafka in KRaft mode.
* [x] Kafka UI.

---

## Phase 2 — Microservices

* [ ] Create multi-module Maven project.
* [ ] Create `RiskForge-parent`.
* [ ] Implement `api-gateway`.
* [ ] Implement `ingestion-service`.
* [ ] Implement transaction validation.
* [ ] Implement PostgreSQL persistence.
* [ ] Implement Kafka event publishing.
* [ ] Implement `fraud-engine-service`.
* [ ] Implement Redis sliding-window detection.
* [ ] Implement fraud rule chain.
* [ ] Implement composite risk scoring.
* [ ] Implement `settlement-service`.
* [ ] Implement ACID ledger updates.
* [ ] Implement `notification-alert-service`.
* [ ] Implement mock notification providers.

---

## Phase 3 — Testing

* [ ] Unit tests with JUnit.
* [ ] Mockito-based service tests.
* [ ] Repository integration tests.
* [ ] Kafka integration tests.
* [ ] Redis integration tests.
* [ ] PostgreSQL integration tests.
* [ ] Testcontainers-based integration environment.
* [ ] End-to-end transaction flow tests.
* [ ] Failure/retry tests.
* [ ] Idempotency tests.
* [ ] Concurrent transaction tests.
* [ ] Load testing.

---

## Phase 4 — Observability & Resilience

* [ ] Spring Boot Actuator.
* [ ] Micrometer metrics.
* [ ] Prometheus.
* [ ] Grafana dashboards.
* [ ] Kafka consumer-lag monitoring.
* [ ] Database pool monitoring.
* [ ] JVM monitoring.
* [ ] p99 scoring latency dashboard.
* [ ] Fraud-ratio dashboard.
* [ ] Resilience4j circuit breakers.
* [ ] Retry policies.
* [ ] Timeout policies.
* [ ] Dead-letter handling.

---

## Phase 5 — CI/CD & Kubernetes

* [ ] Multi-stage Dockerfiles.
* [ ] Jenkins pipeline.
* [ ] Git → Build → Test → Image → Deploy.
* [ ] Testcontainers in CI.
* [ ] Container image versioning.
* [ ] Security scanning.
* [ ] K3s installation.
* [ ] Kubernetes Deployments.
* [ ] Kubernetes Services.
* [ ] ConfigMaps.
* [ ] Secrets.
* [ ] PersistentVolumes.
* [ ] Health probes.
* [ ] Resource limits.
* [ ] Rolling deployments.

---

# 📁 Proposed Repository Structure

```text
RiskForge/
│
├── README.md
├── pom.xml
│
├── infra/
│   ├── docker-compose.yml
│   ├── kafka/
│   ├── postgres/
│   ├── redis/
│   ├── prometheus/
│   └── grafana/
│
├── services/
│   │
│   ├── api-gateway/
│   │   ├── pom.xml
│   │   └── src/
│   │
│   ├── ingestion-service/
│   │   ├── pom.xml
│   │   └── src/
│   │
│   ├── fraud-engine-service/
│   │   ├── pom.xml
│   │   └── src/
│   │
│   ├── settlement-service/
│   │   ├── pom.xml
│   │   └── src/
│   │
│   └── notification-alert-service/
│       ├── pom.xml
│       └── src/
│
├── shared/
│   ├── common-events/
│   ├── common-models/
│   └── common-security/
│
├── docker/
│   └── ...
│
├── k8s/
│   ├── namespaces/
│   ├── deployments/
│   ├── services/
│   ├── configmaps/
│   └── secrets/
│
└── Jenkinsfile
```

---

# 🚀 Quick Start

## Prerequisites

Install:

```text
Ubuntu Server 24.04+
Docker
Docker Compose
Java 21
Maven
Git
```

Verify:

```bash
java -version
mvn -version
docker --version
docker compose version
```

---

## 1. Clone Repository

```bash
git clone https://github.com/<your-username>/RiskForge.git

cd RiskForge
```

---

## 2. Start Infrastructure

```bash
cd infra

docker compose up -d
```

Check running containers:

```bash
docker ps
```

---

## 3. Verify Infrastructure

Expected services:

```text
Kafka
PostgreSQL
Redis
Kafka UI
```

Default homelab endpoints:

```text
Kafka UI:
http://192.168.29.52:8080

PostgreSQL:
192.168.29.52:5432

Database:
homelab_db

User:
homelab

Redis:
192.168.29.52:6379
```

> Replace the host IP with the actual IP address of the machine running the infrastructure.

---

# 🔨 Build the Application

From the repository root:

```bash
mvn clean install -DskipTests
```

For a full build including tests:

```bash
mvn clean verify
```

---

# ▶️ Run a Microservice

Example:

```bash
cd services/fraud-engine-service

mvn spring-boot:run
```

Other services can be started similarly:

```bash
cd services/ingestion-service
mvn spring-boot:run
```

```bash
cd services/settlement-service
mvn spring-boot:run
```

```bash
cd services/notification-alert-service
mvn spring-boot:run
```

---

# 🧪 Example Transaction

Example request:

```http
POST /api/v1/transactions
Content-Type: application/json
Authorization: Bearer <JWT>
```

Example payload:

```json
{
  "accountId": "ACC-10001",
  "cardId": "CARD-90001",
  "amount": 12500.00,
  "currency": "INR",
  "merchantId": "MERCHANT-100",
  "latitude": 19.0760,
  "longitude": 72.8777,
  "timestamp": "2026-08-26T10:30:00Z"
}
```

Processing:

```text
Client
  │
  ▼
API Gateway
  │
  ▼
Ingestion
  │
  ├── PostgreSQL → PENDING
  │
  └── Kafka → raw-transactions
                  │
                  ▼
             Fraud Engine
                  │
             Risk Score
                  │
          ┌───────┴───────┐
          ▼               ▼
       >= 75             < 75
          │               │
          ▼               ▼
       Fraud          Settlement
          │               │
          ▼               ▼
       Alert            SETTLED
```

---

# 📈 Example Risk Evaluation

Suppose an account performs:

```text
Transaction 1 → ₹2,000
Transaction 2 → ₹4,000
Transaction 3 → ₹7,500
Transaction 4 → ₹8,000
Transaction 5 → ₹9,000
```

within 60 seconds.

The velocity rule detects:

```text
Transactions in 60 seconds = 5
Threshold                  = 3
Velocity Rule              = TRIGGERED
```

Additional rules may detect:

```text
Velocity anomaly       +30
Unusual location       +20
High amount             +10
Customer baseline       +15
Blacklist               +0
                        ---
Risk Score              75
```

Result:

```text
Risk Score >= 75
        │
        ▼
FLAGGED
        │
        ▼
flagged-fraud-events
        │
        ▼
Notification Service
```

---

# 🎯 Engineering Concepts Demonstrated

RiskForge is intended to demonstrate practical knowledge of:

### Java

* Java 21.
* Virtual Threads.
* Records.
* Modern concurrency.
* Exception handling.
* Immutability.
* Design patterns.

### Spring

* Spring Boot 3.
* Spring Web.
* Spring Data JPA.
* Spring Kafka.
* Spring Data Redis.
* Spring Security.
* Spring Cloud Gateway.
* Spring WebFlux.
* Spring Actuator.

### Distributed Systems

* Event-driven architecture.
* Kafka partitioning.
* Consumer groups.
* Ordering guarantees.
* Idempotency.
* Eventual consistency.
* ACID transactions.
* Retry mechanisms.
* Circuit breakers.
* Backpressure.
* Failure recovery.

### Databases

* PostgreSQL.
* Redis.
* SQL transactions.
* Indexing.
* Connection pooling.
* Caching.
* Sorted sets.

### DevOps

* Linux.
* Docker.
* Docker Compose.
* Jenkins.
* CI/CD.
* Prometheus.
* Grafana.
* Kubernetes.
* K3s.

---

# 🧠 Key Architectural Principles

RiskForge follows several important principles:

```text
                    ┌─────────────────────┐
                    │   API Gateway       │
                    └──────────┬──────────┘
                               │
                               ▼
                    ┌─────────────────────┐
                    │ Stateless Services  │
                    └──────────┬──────────┘
                               │
                               ▼
                    ┌─────────────────────┐
                    │ Kafka Event Backbone │
                    └──────────┬──────────┘
                               │
               ┌───────────────┼───────────────┐
               ▼               ▼               ▼
             Redis         PostgreSQL       Consumers
               │               │               │
         Fast State       Source of Truth   Async Work
```

Core principles:

1. **Asynchronous communication** through Kafka where appropriate.
2. **PostgreSQL as the persistent source of truth.**
3. **Redis for low-latency ephemeral state.**
4. **Kafka partitioning for per-account ordering.**
5. **Idempotent consumers for safe event reprocessing.**
6. **ACID transactions for financial settlement.**
7. **Observability as a first-class capability.**
8. **Resource-aware deployment for the homelab environment.**
9. **Horizontal scalability through Kafka consumer groups.**
10. **Failure isolation using retries, timeouts and circuit breakers.**

---

# 🔮 Future Enhancements

Potential future iterations include:

* Machine-learning-based fraud scoring.
* Feature Store integration.
* Real-time geospatial anomaly detection.
* PostgreSQL read replicas.
* Kafka schema registry.
* Avro/Protobuf event schemas.
* Dead Letter Topics.
* Exactly-once processing where appropriate.
* Distributed tracing with OpenTelemetry.
* Loki/ELK-based centralized logging.
* Vault-based secret management.
* Kubernetes HPA.
* GitOps deployment.
* Argo CD.
* k6/Gatling load testing.
* Chaos testing.
* Multi-node Kafka deployment.
* Multi-instance microservices.
* Real-time fraud monitoring dashboard.
* Customer risk profiles.
* Dynamic fraud-rule configuration.

---

# 🧱 Build-From-Scratch Blueprint

The earlier sections describe the architecture. This section is the implementation contract: service ownership, data contracts, failure rules, and the build order needed to recreate RiskForge from an empty repository.

## Scope and boundaries

RiskForge simulates a payment-risk platform. A client submits a transaction; the system records it, evaluates fraud risk asynchronously, then either settles it or creates an alert. It is a learning project, not a real payment processor.

```text
External client
  -> API Gateway (only public service)
  -> Ingestion Service (durable acceptance)
  -> Kafka raw-transactions, key = accountId
  -> Fraud Engine (risk decision)
  -> settlement-approved OR flagged-fraud-events
  -> Settlement Service OR Notification Service
```

- Only the gateway is public.
- Each service owns its own database tables; no cross-service database access.
- PostgreSQL is durable business state. Redis is temporary, low-latency state.
- Kafka events are versioned integration contracts, not shared JPA entities.
- `202 Accepted` means the transaction is recorded for processing; it does not mean it is approved or settled.

## Service LLD and responsibilities

| Service | Port | Owns | Inputs | Outputs |
| --- | ---: | --- | --- | --- |
| API Gateway | 8080 | authentication, routing, request limits | external REST | internal REST to ingestion |
| Ingestion | 8081 | accepted transaction, idempotency, outbox | REST transaction request | `raw-transactions` |
| Fraud Engine | 8082 | risk decision, rules, Redis velocity state | `raw-transactions` | approval or fraud event |
| Settlement | 8083 | immutable ledger entries | `settlement-approved` | final `SETTLED` state/audit |
| Notification | 8084 | alert delivery audit | `flagged-fraud-events` | mock webhook/SMS/email alert |

### API Gateway

```text
request
 -> AuthenticationFilter: validate HS256 JWT and expiry
 -> remove client-supplied X-Auth-User-Id
 -> add validated JWT subject as X-Auth-User-Id
 -> Redis RequestRateLimiter
 -> CircuitBreaker
 -> ingestion-service:8081
```

It must not contain transaction validation, fraud scoring, database writes, or Kafka publishing. Initial public route: `POST /api/v1/transactions`; actuator health stays available at `/actuator/health`.

### Ingestion Service

```text
TransactionController
 -> request validation
 -> IdempotencyService
 -> application service
    -> transactions table: PENDING
    -> outbox_events table: raw-transactions payload
 -> outbox publisher -> Kafka
```

Recommended package boundaries: `api` (controller/DTO/error handler), `application` (use cases), `domain` (transaction and state machine), and `infrastructure` (JPA, Kafka, outbox). Save the transaction and its outbox event in the same database transaction; a background publisher sends unpublished outbox rows after a crash.

### Fraud Engine

```text
KafkaListener(raw-transactions, group=fraud-engine)
 -> duplicate-event guard
 -> Redis VelocityWindowService
 -> RuleEngine
 -> RiskScoreCalculator
 -> risk_decisions audit table
 -> settlement-approved OR flagged-fraud-events
```

Use `accountId` as the Kafka key. It preserves order for one account within a partition, not globally. Start with explainable rules: blacklist (+40), more than 3 transactions in 60 seconds (+30), high amount (+10), and location anomaly (+20). Clamp scores to `0..100`; score `>= 75` is `FLAGGED`, otherwise `APPROVED`. Persist each triggered rule and contribution.

### Settlement Service

```text
KafkaListener(settlement-approved, group=settlement)
 -> idempotency lookup
 -> one PostgreSQL transaction
    -> load/lock account state
    -> create immutable ledger entry
    -> mark transaction SETTLED
    -> record processed event
 -> commit
```

Enforce a unique `transaction_id` in the ledger or processed-event table. Kafka is at-least-once; a duplicate delivery must not apply money twice. Do not add automatic HTTP/Kafka retries to financial writes without this guard.

### Notification Alert Service

```text
KafkaListener(flagged-fraud-events, group=notifications)
 -> duplicate-event guard
 -> alert audit record
 -> NotificationProvider interface
 -> log/mock webhook first; delivery retry and DLQ later
```

Notifications never alter a risk decision. A notification outage must not block fraud processing or settlement decisions.

## API and event contracts

### Transaction submission

```http
POST /api/v1/transactions
Authorization: Bearer <JWT>
Idempotency-Key: <UUID>
Content-Type: application/json
```

```json
{
  "accountId": "ACC-10001",
  "cardId": "CARD-90001",
  "amount": 12500.00,
  "currency": "INR",
  "merchantId": "MERCHANT-100",
  "latitude": 19.0760,
  "longitude": 72.8777,
  "timestamp": "2026-08-26T10:30:00Z"
}
```

Validate nonblank/length-bounded IDs, a positive `BigDecimal` amount, ISO-4217 currency, UTC timestamp, valid coordinate ranges, and a required idempotency key. Repeat of the same key returns the original `202` result instead of creating another transaction.

```json
{
  "transactionId": "txn-uuid",
  "status": "PENDING",
  "receivedAt": "2026-08-26T10:30:01Z"
}
```

### Kafka event envelope

Start with JSON and version every event. Later adopt Avro/Protobuf plus a schema registry.

```json
{
  "eventId": "evt-uuid",
  "eventType": "riskforge.transaction.received.v1",
  "occurredAt": "2026-08-26T10:30:01Z",
  "correlationId": "txn-uuid",
  "producer": "ingestion-service",
  "payload": {}
}
```

| Topic | Producer | Consumer | Key | Required payload |
| --- | --- | --- | --- | --- |
| `raw-transactions` | ingestion | fraud engine | `accountId` | transaction ID, payment data, timestamps |
| `settlement-approved` | fraud engine | settlement | `accountId` | transaction ID, risk score, decision |
| `flagged-fraud-events` | fraud engine | notifications | `accountId` | transaction ID, score, triggered rules |
| `<topic>.dlq` | retry/error handler | operators | original key | original event plus error metadata |

Never remove or rename a field used by consumers in an event version. Add optional fields or publish `v2`.

## Data model and state machine

| Owner | Table | Minimum columns | Constraint |
| --- | --- | --- | --- |
| Ingestion | `transactions` | `id`, `account_id`, `amount`, `currency`, `status`, `received_at` | PK `id`; index `(account_id, received_at)` |
| Ingestion | `idempotency_keys` | `key`, `request_hash`, `transaction_id` | unique `key` |
| Ingestion | `outbox_events` | `id`, `aggregate_id`, `topic`, `payload`, `published_at` | index unpublished rows |
| Fraud | `risk_decisions`, `processed_events`, `blacklist_entries` | decision/rule/audit data | unique transaction/event ID |
| Settlement | `ledger_entries`, `processed_events` | transaction, account, amount, timestamp | unique transaction ID |
| Notification | `alerts` | transaction, channel, status, attempts | unique transaction/channel |

```text
PENDING -> APPROVED -> SETTLED
PENDING -> FLAGGED -> BLOCKED -> ALERTED
```

Define legal state transitions centrally. A `BLOCKED` transaction can never be settled. Ledger rows are immutable; corrections use compensating entries, never an update to historical money movement.

## Redis velocity algorithm

Use `velocity:account:<accountId>` as a sorted set. Use the transaction ID as member so two requests in the same millisecond do not overwrite one another.

```text
windowStart = now - 60 seconds
ZREMRANGEBYSCORE key 0 windowStart
ZADD key now transactionId
count = ZCARD key
EXPIRE key 120
if count > 3: trigger velocity rule
```

Make the sequence atomic with Lua or a Redis transaction/pipeline before handling concurrent production traffic. For the first version, use ingestion time consistently; later explicitly model late/out-of-order event policy.

## Reliability rules

| Failure | Required response |
| --- | --- |
| HTTP retry | idempotency key returns the original transaction |
| DB commit then process crash | unpublished outbox record is published after restart |
| Kafka duplicate | unique event/transaction guard makes consumer safe |
| Fraud engine down | Kafka retains raw events; never auto-approve |
| Redis down | choose/document a policy; starter recommendation is fail closed and audit |
| Settlement down | approved events remain in Kafka |
| Notification down | retry separately; decision remains final |
| Poison event | bounded retries then DLQ with failure metadata |

All services should assume at-least-once delivery. Idempotent state changes, not wishful exactly-once assumptions, provide correctness.

## Dependency-ordered implementation plan

1. **Foundation:** Maven parent/modules, Docker Compose, Kafka, PostgreSQL, Redis, migrations, topic creation, health checks, `.env.example`.
2. **Gateway and ingestion:** JWT/routing/rate limit, request validation, `PENDING` persistence, idempotency, transactional outbox, `raw-transactions` publication.
3. **Fraud engine:** consumer, duplicate guard, Redis velocity window, blacklist/rule scoring, decision persistence, two output topics.
4. **Settlement and notification:** idempotent ledger write, mock alert provider, retry/DLQ.
5. **Operations:** Actuator, Micrometer, Prometheus/Grafana, structured correlation IDs, Testcontainers, Docker images, CI, then Kubernetes.

Definition of done for each phase:

```text
Gateway + ingestion: one request -> one PENDING row -> one Kafka event;
same Idempotency-Key -> no duplicate row/event.

Fraud: deterministic test data -> one explainable score -> exactly one output event.

Settlement: duplicate approved event -> exactly one ledger entry.

Notification: flagged event -> alert audit entry; provider outage -> retry/DLQ, not blocked processing.
```

## Local build and verification checklist

```text
1. Start Kafka, PostgreSQL and Redis with Docker Compose.
2. Run each service's database migration.
3. Start: ingestion -> fraud -> settlement -> notification -> gateway.
4. Submit through gateway with JWT and Idempotency-Key.
5. Inspect transaction/outbox rows, Kafka records, Redis velocity key, risk decision, ledger or alert rows.
6. Repeat the request with the same key.
7. Stop one consumer and verify retained Kafka work is processed after restart.
```

Minimum test suite: JUnit/Mockito for rules and transitions; Testcontainers for PostgreSQL, Kafka, and Redis; an end-to-end test for both approved and flagged paths; duplicate-event, idempotency, outage-recovery, and basic concurrent-velocity scenarios.

## Standard environment variables

```text
SPRING_PROFILES_ACTIVE=local
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DATABASE=<per-service database>
POSTGRES_USER=riskforge
POSTGRES_PASSWORD=<secret>
REDIS_HOST=localhost
REDIS_PORT=6379
JWT_SECRET=<secret>
```

Commit `.env.example` only. Keep real credentials out of Git and use environment variables, Docker/Kubernetes secrets, and key rotation in later deployment phases.

# 📚 Learning Objectives

By completing RiskForge, the project should provide practical experience with:

```text
Java 21
   ↓
Spring Boot
   ↓
Microservices
   ↓
Kafka
   ↓
Redis
   ↓
PostgreSQL
   ↓
Distributed Systems
   ↓
Docker
   ↓
Observability
   ↓
CI/CD
   ↓
Kubernetes / K3s
```

The project is intentionally structured so that each development phase introduces another real-world backend engineering challenge.

---

# ⚠️ Disclaimer

RiskForge is a **learning and portfolio project** intended to demonstrate distributed-system and backend engineering concepts.

It is **not a production financial system** and should not be used to process real financial transactions without substantial additional work around security, compliance, fault tolerance, auditing, data protection, regulatory requirements, operational controls, and financial correctness.

---

# 📜 License

Add the project's chosen license here, for example:

```text
MIT License
```

---

## ⭐ Project Status

**Current Stage:** Infrastructure / Core Development

**Target Architecture:**

```text
Java 21
+ Spring Boot 3
+ Kafka
+ Redis
+ PostgreSQL
+ Docker
+ Prometheus
+ Grafana
+ Jenkins
+ K3s
```

**Primary objective:** Build a realistic, resource-efficient, event-driven financial fraud detection platform while demonstrating modern Java backend, distributed systems, DevOps, and system-design skills.

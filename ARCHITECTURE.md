# simpleMQ – Low-Level Architecture Document

> **Last updated:** 2026-03-03

This document provides a detailed, low-level explanation of how simpleMQ works internally — covering data models, message lifecycle, storage, routing, the REST API layer, cluster consensus, and deployment.

---

## Table of Contents

1. [System Overview](#1-system-overview)
2. [Data Models](#2-data-models)
3. [Message Lifecycle](#3-message-lifecycle)
4. [Storage Architecture](#4-storage-architecture)
5. [Routing Engine](#5-routing-engine)
6. [REST API Layer](#6-rest-api-layer)
7. [Cluster Consensus (Raft)](#7-cluster-consensus-raft)
8. [Leader Proxy Filter](#8-leader-proxy-filter)
9. [Visibility Timeout Mechanism](#9-visibility-timeout-mechanism)
10. [Dead Letter Queue (DLQ)](#10-dead-letter-queue-dlq)
11. [Metrics & Monitoring](#11-metrics--monitoring)
12. [Deployment Architecture](#12-deployment-architecture)
13. [Configuration Reference](#13-configuration-reference)

---

## 1. System Overview

simpleMQ is a distributed message broker built with **Java 21** and **Quarkus**. It provides exchange-based message routing (similar to AMQP/RabbitMQ concepts) with a pull-based consumer model and cluster-wide consistency via the **Raft consensus algorithm** (Apache Ratis).

### Key Design Principles

- **Hybrid Storage:** Messages live in fast, lock-free in-memory buffers (`ConcurrentLinkedQueue`) for low-latency access, while simultaneously being persisted to **SQLite** for durability.
- **Replicated State Machine:** Every write operation (publish, poll, ack, nack, exchange/queue management) is replicated through the Raft log before being applied. This ensures all cluster nodes converge to the same state.
- **Reactive I/O:** The REST layer uses **SmallRye Mutiny** (`Uni<T>`) to keep Quarkus event-loop threads non-blocking. Database and Raft operations are offloaded to worker pools.

### High-Level Data Flow

```
Producer ──POST──▶ PublishResource ──▶ MessagingEngine.publish()
                                            │
                                    ExchangeRoutingEngine
                                    (resolve target queues)
                                            │
                                    RaftService.replicateMessage()
                                            │
                                ┌───────────┼───────────┐
                                ▼           ▼           ▼
                           Node 1       Node 2       Node 3
                        SimpleStateMachine.applyTransaction()
                                │
                        ┌───────┴───────┐
                        ▼               ▼
                  PersistenceManager  InMemoryBuffer
                  (SQLite persist)   (enqueue for poll)
                                        │
Consumer ──GET──▶ PollingResource ──▶ MessagingEngine.poll()
                                        │
                                  buffer.dequeue()  ──▶  Raft replicatePoll
                                        │
                              Message returned to consumer
                                        │
                        ┌───────────────┴────────────────┐
                        ▼                                ▼
              POST /ack/{id}                   POST /nack/{id}
              (Raft replicateAck)              (Raft replicateNack)
```

---

## 2. Data Models

All persistent entities extend Quarkus Panache's `PanacheEntityBase` and are stored in SQLite via Hibernate ORM.

### 2.1 Exchange (`exchanges` table)

| Field    | Type           | Description                                      |
|----------|----------------|--------------------------------------------------|
| `name`   | `String` (PK)  | Unique exchange identifier                       |
| `type`   | `ExchangeType` | Routing strategy: `DIRECT` or `FANOUT`           |
| `durable`| `boolean`      | Whether the exchange survives broker restarts     |

### 2.2 Queue (`queues` table)

| Field        | Type          | Description                                        |
|--------------|---------------|----------------------------------------------------|
| `name`       | `String` (PK) | Unique queue identifier                            |
| `queueGroup` | `String`      | Logical group for management/filtering             |
| `durable`    | `boolean`     | Whether the queue survives broker restarts          |
| `autoDelete` | `boolean`     | Whether the queue auto-deletes when unused          |

### 2.3 Binding (`bindings` table)

| Field          | Type          | Description                                    |
|----------------|---------------|------------------------------------------------|
| `id`           | `Long` (PK)   | Auto-generated primary key                     |
| `exchangeName` | `String`      | The exchange this binding belongs to            |
| `queueName`    | `String`      | The target queue                                |
| `routingKey`   | `String`      | Routing key for DIRECT matching (null for FANOUT)|

### 2.4 Message (`messages` table)

| Field           | Type              | Description                                         |
|-----------------|-------------------|-----------------------------------------------------|
| `id`            | `String` (PK)     | UUID generated at creation                          |
| `payload`       | `String`          | The message body from the producer                  |
| `routingKey`    | `String`          | Routing key used during exchange routing             |
| `exchange`      | `String`          | Name of the exchange it was published to             |
| `queueName`     | `String`          | The queue this message is assigned to                |
| `timestamp`     | `LocalDateTime`   | When the message was created                         |
| `deliveredAt`   | `LocalDateTime`   | When the message was last delivered to a consumer    |
| `deliveryCount` | `int`             | Number of delivery attempts (incremented on each poll)|
| `status`        | `MessageStatus`   | Current lifecycle state                              |
| `headers`       | `Map<String,String>` | Optional key-value headers (stored in `message_headers` collection table) |

### 2.5 MessageStatus (Enum)

```
PENDING ──(poll)──▶ DELIVERED ──(ack)──▶ ACKED
                        │
                    (nack+requeue)──▶ PENDING  (re-enters the cycle)
                        │
                    (nack/timeout + max attempts)──▶ DLQ
```

- **PENDING**: Waiting in the queue for a consumer.
- **DELIVERED**: Handed to a consumer, awaiting ACK/NACK.
- **ACKED**: Successfully processed — terminal state.
- **NACKED**: Explicitly rejected by consumer (transient, then requeued or DLQ).
- **DLQ**: Moved to Dead Letter Queue after max delivery attempts — terminal state.

### 2.6 RaftMetadata (`raft_metadata` table)

| Field              | Type          | Description                                  |
|--------------------|---------------|----------------------------------------------|
| `nodeId`           | `String` (PK) | The cluster node this metadata belongs to    |
| `currentTerm`      | `Long`        | Latest Raft term                             |
| `votedFor`         | `String`      | Candidate voted for in current term          |
| `lastAppliedIndex` | `Long`        | Index of last applied log entry              |
| `lastAppliedTerm`  | `Long`        | Term of last applied log entry               |

---

## 3. Message Lifecycle

### 3.1 Publish

1. Producer sends `POST /api/publish/{exchange}` with `{ routingKey, payload }`.
2. `MessagingEngine.publish()` creates a template `Message` and calls `ExchangeRoutingEngine.getTargetQueues()` on a worker thread.
3. The routing engine looks up the exchange, fetches all its bindings, and applies the routing strategy to find matching queue names.
4. For **each** target queue, a new `Message` object is created (with unique UUID) and replicated via `RaftService.replicateMessage()`.
5. The Raft client sends a pipe-delimited `PUBLISH|id|payload|routingKey|exchange|queue|timestamp` command.
6. Once committed by the Raft quorum, `SimpleStateMachine.applyTransaction()` runs on **every** node:
   - `PersistenceManager.saveMessage()` persists the message to SQLite (idempotent — skips if already exists).
   - `StorageService.getBuffer(queueName).enqueue(msg)` adds it to the in-memory buffer (with deduplication by message ID).
   - `MetricsService.incrementPublished()` updates the published counter.

### 3.2 Poll (Consumer Pull)

1. Consumer sends `GET /api/poll/{queue}`.
2. `MessagingEngine.poll()` atomically dequeues the head message from the `InMemoryBuffer` via `ConcurrentLinkedQueue.poll()`. This guarantees only one consumer gets the message (competing consumers pattern).
3. The dequeued message is simultaneously moved into the buffer's **in-flight map** with the current timestamp for visibility timeout tracking.
4. A `POLL|queueName|messageId` command is replicated through Raft.
5. On **the leader**: the message was already dequeued in step 2.
6. On **followers**: `pollLocal()` calls `buffer.dequeueById(messageId)` to remove the specific message, keeping in-memory state consistent across nodes.
7. `markAsDelivered()` updates the DB: `status = DELIVERED`, `deliveryCount++`, `deliveredAt = now()`.
8. The message is re-fetched from DB in a new transaction and returned to the consumer.

### 3.3 Acknowledge (ACK)

1. Consumer sends `POST /api/poll/ack/{messageId}`.
2. Replicated via `ACK|messageId` through Raft.
3. On every node: `acknowledgeMessageLocal()` sets `status = ACKED`, clears `deliveredAt`, removes from in-flight tracking.

### 3.4 Negative Acknowledge (NACK)

1. Consumer sends `POST /api/poll/nack/{messageId}?requeue=true|false`.
2. Replicated via `NACK|messageId|requeue` through Raft.
3. On every node: `nackMessageLocal()`:
   - If `requeue=true` AND `deliveryCount < MAX_DELIVERY_ATTEMPTS (3)`: sets status to `PENDING`, clears `deliveredAt`, creates a **detached copy** of the message and enqueues it back into the buffer.
   - Otherwise: routes to the Dead Letter Queue.

> **Why a detached copy?** The message returned from `Message.findById()` is a Hibernate-managed entity. Putting it directly into the in-memory buffer would cause `LazyInitializationException` when accessed outside a transaction. The copy is a plain POJO.

---

## 4. Storage Architecture

### 4.1 Dual-Write Design

Every message exists in two places simultaneously:

```
┌─────────────────────────────┐     ┌─────────────────────────────┐
│     InMemoryBuffer          │     │       SQLite (messages)      │
│  (ConcurrentLinkedQueue)    │     │   (Hibernate/Panache ORM)   │
│                             │     │                             │
│  • Fast O(1) enqueue/dequeue│     │  • Durable across restarts  │
│  • Deduplication via ID set │     │  • Queryable by status      │
│  • In-flight tracking map   │     │  • busy_timeout=5000ms      │
│  • Lock-free, thread-safe   │     │  • Single-writer via SM_EXEC│
└─────────────────────────────┘     └─────────────────────────────┘
```

### 4.2 StorageService

`StorageService` is a CDI `@ApplicationScoped` bean that holds a `ConcurrentHashMap<String, InMemoryBuffer>` — one buffer per queue name. Buffers are created lazily via `computeIfAbsent()`.

### 4.3 InMemoryBuffer Internals

```java
// FIFO queue for available messages
Queue<Message> messages = new ConcurrentLinkedQueue<>();

// Deduplication set (prevents double-enqueue during recovery)
Set<String> messageIds = ConcurrentHashMap.newKeySet();

// In-flight tracking: messageId → (message, deliveredAt timestamp)
Map<String, InFlightEntry> inFlight = new ConcurrentHashMap<>();
```

**Key operations:**
- `enqueue(msg)`: Adds to the tail. If `msg.id` is already in `messageIds`, the call is silently ignored (idempotent).
- `dequeue()`: Atomically polls the head. Removes from `messageIds` and adds to `inFlight` with `Instant.now()`.
- `dequeueById(id)`: Removes a specific message by ID (used by followers during Raft replication).
- `expireInFlight(timeoutMs)`: Returns all in-flight messages older than the cutoff and removes them from tracking.

### 4.4 PersistenceManager

- **Startup Recovery:** On application start (via `@Observes StartupEvent`), queries SQLite for all messages where `status != ACKED && status != DLQ`. Messages in `DELIVERED` state are reset to `PENDING` (original consumer is gone). All recovered messages are re-enqueued into their respective in-memory buffers.
- **Ephemeral Mode:** When `simplemq.persistence.enabled=false`, recovery is skipped. Messages are in-memory only.
- **Idempotent Save:** `saveMessage()` checks `Message.findById()` before persisting to avoid duplicate key errors.

---

## 5. Routing Engine

### 5.1 Strategy Pattern

```
ExchangeRoutingEngine
    ├── DirectRoutingStrategy   (ExchangeType.DIRECT)
    └── FanoutRoutingStrategy   (ExchangeType.FANOUT)
```

Both implement the `RoutingStrategy` interface:

```java
public interface RoutingStrategy {
    boolean matches(Message message, Binding binding);
}
```

### 5.2 Direct Routing

Matches when `message.routingKey` exactly equals `binding.routingKey` (null-safe comparison). Only queues with a matching binding routing key receive the message.

```
Exchange(DIRECT) ──binding(key="order.created")──▶ Queue("orders")
                 ──binding(key="order.shipped")──▶ Queue("shipments")

Message(routingKey="order.created") → delivered to "orders" only
```

### 5.3 Fanout Routing

Always returns `true` — every bound queue receives the message regardless of routing key. Implements the classic publish-subscribe pattern.

```
Exchange(FANOUT) ──binding──▶ Queue("analytics")
                 ──binding──▶ Queue("notifications")
                 ──binding──▶ Queue("audit-log")

Message(any routingKey) → delivered to ALL three queues
```

### 5.4 Routing Flow

1. Look up the `Exchange` entity by name from SQLite.
2. Query all `Binding` records where `exchangeName` matches.
3. Select the strategy based on `exchange.type` (switch expression).
4. Filter bindings through the strategy and collect matching `queueName` values.

---

## 6. REST API Layer

All endpoints are reactive (return `Uni<Response>`) and use Jakarta REST (JAX-RS) annotations.

### 6.1 PublishResource (`/api/publish`)

| Method | Path                    | Description                        | Response   |
|--------|-------------------------|------------------------------------|------------|
| POST   | `/api/publish/{exchange}` | Publish a message to an exchange | 202 Accepted |

**Request body:** `{ "routingKey": "...", "payload": "..." }`

### 6.2 PollingResource (`/api/poll`)

| Method | Path                        | Description                        | Response          |
|--------|-----------------------------|------------------------------------|-------------------|
| GET    | `/api/poll/{queue}`          | Poll next message from queue      | 200 + message / 204 empty |
| POST   | `/api/poll/ack/{messageId}`  | Acknowledge a message             | 200 OK            |
| POST   | `/api/poll/nack/{messageId}` | Negative acknowledge              | 200 OK            |

**NACK query parameter:** `?requeue=true` (default) or `?requeue=false`

### 6.3 ManagementResource (`/api/management`)

| Method | Path                               | Description                   | Response     |
|--------|-------------------------------------|-------------------------------|--------------|
| GET    | `/api/management/summary`           | Full broker dashboard summary | 200 + JSON   |
| GET    | `/api/management/exchanges`         | List all exchanges            | 200 + JSON   |
| POST   | `/api/management/exchanges`         | Create an exchange            | 201 Created  |
| DELETE | `/api/management/exchanges/{name}`  | Delete an exchange            | 204 No Content|
| GET    | `/api/management/queues`            | List all queues               | 200 + JSON   |
| POST   | `/api/management/queues`            | Create a queue                | 201 Created  |
| DELETE | `/api/management/queues/{name}`     | Delete a queue                | 204 No Content|
| GET    | `/api/management/bindings`          | List all bindings             | 200 + JSON   |
| POST   | `/api/management/bindings`          | Create a binding              | 201 Created  |
| DELETE | `/api/management/bindings`          | Remove a binding              | 204 No Content|

### 6.4 ClusterResource (`/api/cluster`)

| Method | Path                  | Description                   | Response                    |
|--------|-----------------------|-------------------------------|-----------------------------|
| GET    | `/api/cluster/leader` | Get current Raft leader ID    | 200 + ID / 503 no leader   |
| GET    | `/api/cluster/peers`  | List all cluster peers        | 200 + JSON array            |
| POST   | `/api/cluster/join`   | Add a node to the cluster     | 200 OK / 400 failure        |
| POST   | `/api/cluster/leave`  | Remove a node from cluster    | 200 OK / 400 failure        |

---

## 7. Cluster Consensus (Raft)

### 7.1 Overview

simpleMQ uses **Apache Ratis** to implement the Raft consensus algorithm. Every node runs a `RaftServer` that participates in leader election, log replication, and state machine application.

### 7.2 Command Protocol

All commands are encoded as **pipe-delimited strings** sent through the Raft log. Values that may contain special characters (payload, routing key) are **Base64-encoded**.

| Command          | Format                                                    |
|------------------|-----------------------------------------------------------|
| PUBLISH          | `PUBLISH\|id\|b64(payload)\|b64(routingKey)\|exchange\|queue\|timestamp` |
| POLL             | `POLL\|queueName\|messageId`                              |
| ACK              | `ACK\|messageId`                                          |
| NACK             | `NACK\|messageId\|requeue`                                |
| CREATE_EXCHANGE  | `CREATE_EXCHANGE\|name\|type\|durable`                    |
| DELETE_EXCHANGE  | `DELETE_EXCHANGE\|name`                                   |
| CREATE_QUEUE     | `CREATE_QUEUE\|name\|group\|durable\|autoDelete`          |
| DELETE_QUEUE     | `DELETE_QUEUE\|name`                                      |
| BIND             | `BIND\|exchange\|queue\|b64(routingKey)`                  |
| UNBIND           | `UNBIND\|exchange\|queue\|b64(routingKey)`                |

### 7.3 SimpleStateMachine

`SimpleStateMachine` extends Ratis's `BaseStateMachine`. It is **not** a CDI bean — it is instantiated by the Raft framework via a lambda registry, and obtains CDI beans lazily via `CDI.current().select(...)`.

**Critical threading detail:** All `applyTransaction()` calls are dispatched to a **single-threaded executor** (`SM_EXECUTOR`). This is essential because SQLite only supports one concurrent write transaction. Without serialization, concurrent Raft log applications would cause "database is locked" errors.

**Crash recovery:** After each successful transaction, the `lastAppliedIndex` and `lastAppliedTerm` are persisted to the `raft_metadata` table. On restart, `getLastAppliedTermIndex()` reads this back so the Raft server knows which log entries have already been applied and can skip them.

### 7.4 RaftService Lifecycle

1. **Startup** (`@Observes StartupEvent`):
   - Parses peer configuration from `simplemq.cluster.peers` (format: `id1=host:port,id2=host:port,...`).
   - Creates `RaftGroup` with all peers.
   - Configures storage: `/data/raft-{nodeId}` in Docker (RECOVER mode), or temp dir locally (FORMAT mode — cleaned on shutdown).
   - Starts the `RaftServer` and a shared `RaftClient`.
   - Blocks up to 30s for leader election via `waitForLeader()`.
   - Starts a periodic timer (every 10s) to sync Raft metadata to SQLite and log leadership state.

2. **Command Replication** (`sendCommand()`):
   - Wraps the blocking `sharedClient.io().send()` in `Uni.createFrom().item().runSubscriptionOn(workerPool)` to avoid blocking the event loop.
   - Retry policy: up to 10 retries with 1-second sleep between attempts.
   - Validates the response is "SUCCESS" (from the state machine).

3. **Shutdown** (`@Observes ShutdownEvent`):
   - Closes the Raft client and server.
   - In non-Docker environments, cleans up temporary storage directories.

### 7.5 Raft Group Identity

The Raft group ID is deterministically derived from the string `"simpleMQ-cluster"` using `UUID.nameUUIDFromBytes()`. This ensures all nodes in the cluster refer to the same logical group.

---

## 8. Leader Proxy Filter

`LeaderProxyFilter` is a JAX-RS `@ServerRequestFilter(preMatching = true)` that ensures write operations always reach the leader, regardless of which node the client connects to.

### How It Works

1. For every incoming request, the filter checks if it is a **write operation**:
   - All non-GET requests to `/api/management/*`, `/api/publish/*`, `/api/cluster/*`
   - **All** requests (including GET) to `/api/poll/*` — because polling is a write operation (dequeues a message)
2. If this node **is** the leader → pass through to local processing.
3. If this node is a **follower**:
   - Resolve the leader's Raft address from the peer list.
   - Derive the HTTP address (same host, port `8080`).
   - Forward the entire HTTP request (headers, body, query params) to the leader using a JAX-RS `Client`.
   - Return the leader's response directly to the client.
4. If no leader is elected → return `503 Service Unavailable`.

### Read Operations

GET requests to `/api/management/*` and `/api/cluster/*` are served locally from the follower's own SQLite database. This means read data may be slightly stale on followers (eventual consistency for reads, strong consistency for writes).

---

## 9. Visibility Timeout Mechanism

### Purpose

When a consumer polls a message but crashes or fails to ACK/NACK, the message would be stuck in `DELIVERED` state forever. The visibility timeout automatically requeues such messages.

### Implementation

1. `MessagingEngine.checkVisibilityTimeouts()` runs on a **Quarkus `@Scheduled` timer every 5 seconds**, but only on the **leader node**.
2. It iterates over all `InMemoryBuffer` instances and calls `buffer.expireInFlight(visibilityTimeoutMs)`.
3. `expireInFlight()` checks each in-flight entry's `deliveredAt` timestamp against `Instant.now() - timeoutMs`. Entries older than the cutoff are removed and returned.
4. For each expired message:
   - If `deliveryCount >= MAX_DELIVERY_ATTEMPTS (3)` → route to DLQ.
   - Otherwise → reset to `PENDING`, clear `deliveredAt`, create a detached copy, and enqueue back into the buffer.

### Configuration

```properties
simplemq.visibility.timeout-ms=${SIMPLEMQ_VISIBILITY_TIMEOUT_MS:30000}
```

Default: **30 seconds**. Configurable via environment variable.

---

## 10. Dead Letter Queue (DLQ)

Messages are routed to the DLQ when:
- A consumer NACKs a message with `requeue=false`.
- A consumer NACKs a message with `requeue=true` but `deliveryCount >= 3`.
- A message exceeds the visibility timeout and `deliveryCount >= 3`.

### DLQ Naming Convention

```
DLQ.<originalQueueName>
```

Example: Messages from queue `orders` go to `DLQ.orders`.

### DLQ Behavior

- The message's `status` is set to `MessageStatus.DLQ`.
- The message's `queueName` is updated to the DLQ name.
- The message is enqueued into the DLQ's `InMemoryBuffer` (accessible via `StorageService.getBuffer("DLQ.orders")`).
- DLQ messages can be inspected or reprocessed by polling the DLQ queue name.

---

## 11. Metrics & Monitoring

### MetricsService

An `@ApplicationScoped` service holding a `SystemMetrics` object with thread-safe `AtomicLong` counters:

| Counter                  | Incremented When                          |
|--------------------------|-------------------------------------------|
| `totalMessagesPublished` | A PUBLISH command is applied by the SM    |
| `totalMessagesAcked`     | An ACK command is applied                 |
| `totalMessagesNacked`    | A NACK command is applied                 |
| `totalMessagesDLQ`       | A message is routed to the DLQ            |
| `totalPollRequests`      | A consumer calls the poll endpoint        |

### ManagementSummary DTO

The `/api/management/summary` endpoint returns a comprehensive snapshot:

```json
{
  "nodeId": "node1",
  "leaderId": "node1",
  "isLeader": true,
  "peers": ["node1=node1:9851", "node2=node2:9851", "node3=node3:9851"],
  "exchanges": [...],
  "queues": [
    { "name": "orders", "group": "default", "messageCount": 42, "durable": true }
  ],
  "metrics": {
    "totalMessagesPublished": 1000,
    "totalMessagesAcked": 950,
    "totalMessagesNacked": 30,
    "totalMessagesDLQ": 20,
    "totalPollRequests": 1200
  }
}
```

---

## 12. Deployment Architecture

### Docker Compose (3-Node Cluster)

```
                  ┌─────────────┐
  Client ───────▶ │   NGINX LB  │ :8080
                  │  (round-robin)│
                  └──────┬──────┘
             ┌───────────┼───────────┐
             ▼           ▼           ▼
        ┌─────────┐ ┌─────────┐ ┌─────────┐
        │  node1  │ │  node2  │ │  node3  │
        │ :8081   │ │ :8082   │ │ :8083   │  (HTTP)
        │ :9851   │ │ :9852   │ │ :9853   │  (Raft gRPC)
        └────┬────┘ └────┬────┘ └────┬────┘
             │           │           │
        node1_data  node2_data  node3_data   (Docker volumes)
```

- **NGINX** load balances HTTP traffic across all three nodes.
- The **LeaderProxyFilter** transparently redirects write operations to the leader.
- Each node has its own **SQLite database** (e.g., `/data/simplemq-1.db`) stored on a Docker volume.
- Raft gRPC communication happens on port `9851` across the `simplemq-net` bridge network.

### Storage Modes

| Environment | Raft Storage        | Startup Mode | Cleanup         |
|-------------|---------------------|--------------|-----------------|
| Docker      | `/data/raft-{nodeId}` | RECOVER    | Preserved       |
| Local/Test  | `/tmp/smq-raft-{nodeId}` | FORMAT  | Deleted on shutdown |

---

## 13. Configuration Reference

All properties are set in `application.properties` and can be overridden via environment variables.

| Property                          | Env Variable                    | Default              | Description                              |
|-----------------------------------|---------------------------------|----------------------|------------------------------------------|
| `simplemq.cluster.node-id`        | `NODE_ID`                       | `node1`              | Unique node identifier                   |
| `simplemq.cluster.address`        | `NODE_ADDRESS`                  | `localhost:9851`     | Raft gRPC listen address                 |
| `simplemq.cluster.peers`          | `CLUSTER_PEERS`                 | `node1=localhost:9851` | Comma-separated peer list              |
| `simplemq.persistence.enabled`    | `SIMPLEMQ_PERSISTENCE_ENABLED`  | `true`               | Enable/disable SQLite persistence        |
| `simplemq.visibility.timeout-ms`  | `SIMPLEMQ_VISIBILITY_TIMEOUT_MS`| `30000`              | Visibility timeout in milliseconds       |
| `quarkus.datasource.jdbc.url`     | `QUARKUS_DATASOURCE_JDBC_URL`   | `jdbc:sqlite:simplemq.db?busy_timeout=5000` | SQLite JDBC URL |

### Test Profile Overrides

Tests use high ports to avoid conflicting with a running Docker cluster:

```properties
%test.quarkus.http.port=18080
%test.simplemq.cluster.address=localhost:19851
%test.simplemq.cluster.peers=node1=localhost:19851
```

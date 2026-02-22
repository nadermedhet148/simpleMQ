# simplemq - Simple Message Broker Design Plan

A lightweight message broker built with Java Quarkus, supporting both in-memory and disk-based storage, clustering with Raft, and a management UI.

## Features & Requirements

### Functional Requirements
- **Exchange & Queue Management**: Ability to create and delete exchanges and queues. Queues are assigned to specific groups.
- **Exchange Types**: Support for `Direct` (routing key based) and `Pub-Sub` (Fanout/Broadcast) exchange logic.
- **Producer API**: REST endpoint to publish messages to a specific exchange with a routing key.
- **Consumer API (Pull)**: REST endpoint for consumers to pull messages from assigned queues.
- **Acknowledgment System**: Mechanism to mark messages as 'done' (Ack) or 'failed' (Nack).
- **Dead Letter Queue (DLQ)**: Automated routing of messages to a DLQ after a specific number of failed attempts or a Nack.
- **Management UI**: A simple web-based dashboard to visualize and manage exchanges, queues, and message flow.

### Non-Functional Requirements
- **Hybrid Storage**: Messages must be handled in-memory for speed but persisted to disk using SQLite for durability.
- **High Availability**: Support for cluster mode with replication using Quorums.
- **Consensus & Election**: Implementation of the Raft algorithm for leader election and state consistency across the cluster.
- **Framework**: Developed using the Java Quarkus framework.
- **Quality Assurance**: Inclusion of Unit, Integration, and End-to-End (E2E) test cases.

---

## Development Roadmap & Tasks

### 1. Architecture & Domain Modeling
- [x] **Define Core Domain Entities**: Model the `Message`, `Exchange`, `Queue`, and `Binding` objects.
- [x] **Define Exchange Routing Logic**: Plan the interface for Direct and Fanout routing strategies.
- [x] **Data Schema Design**: Design the SQLite schema for persisting metadata (exchanges, queues) and message payloads.

### 2. Storage & Persistence Layer
- [x] **In-Memory Buffer Implementation**: Design a high-performance in-memory queueing system.
- [x] **SQLite Integration**: Configure Quarkus with SQLite for disk persistence.
- [x] **Persistence Manager**: Create a service that synchronizes in-memory state with the disk and handles recovery on startup.

### 3. Messaging Engine
- [x] **Exchange Routing Engine**: Implement logic to route messages from exchanges to bound queues based on type.
- [x] **Queue Management Service**: Logic for group-based queue assignment and message ordering.
- [x] **Ack/Nack & DLQ Workflow**: Implement the message lifecycle state machine (Pending -> Delivered -> Acked/DLQ).

### 4. API & Integration
- [ ] **Broker Management API**: REST endpoints for CRUD operations on Exchanges and Queues.
- [ ] **Publishing API**: REST endpoint for producers to inject messages into the broker.
- [ ] **Polling API**: REST endpoint for consumers to pull and acknowledge messages.

### 5. Clustering & High Availability
- [ ] **Raft Algorithm Integration**: Implement or integrate a Raft library for leader election and log replication.
- [ ] **Quorum-based Replication**: Ensure messages are replicated to a majority of nodes before being considered "persisted".
- [ ] **Cluster Membership Management**: Logic for nodes to join/leave the cluster and discover the leader.

### 6. Management UI
- [ ] **Dashboard Development**: Create a simple UI to list exchanges, monitor queue depths, and view DLQ status.
- [ ] **API Integration**: Connect the UI to the Management REST APIs.

### 7. Testing & Quality Assurance
- [ ] **Unit Testing**: Test routing logic and individual component behavior.
- [ ] **Integration Testing**: Test API endpoints with an embedded SQLite database.
- [ ] **E2E Testing**: Simulate a multi-node cluster to verify leader election and failover scenarios.

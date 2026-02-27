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

## Cluster Management API

`simpleMQ` supports dynamic cluster membership and leader discovery via REST:

- **Leader Discovery**: `GET /api/cluster/leader`
  - Returns the ID of the current leader node.
- **List Peers**: `GET /api/cluster/peers`
  - Returns the list of all nodes currently in the cluster.
- **Join Cluster**: `POST /api/cluster/join?id=nodeX&address=host:port`
  - Adds a new node to the cluster. **Must be called on the current leader.**
- **Leave Cluster**: `POST /api/cluster/leave?id=nodeX`
  - Removes a node from the cluster. **Must be called on the current leader.**

## Running the Cluster

To start a 3-node simpleMQ cluster, use Docker Compose:

```bash
./gradlew build
docker-compose up --build
```

Nodes will be available at:
- Node 1: http://localhost:8081
- Node 2: http://localhost:8082
- Node 3: http://localhost:8083

Check the logs to see the leader election:
```bash
docker-compose logs -f | grep "LEADER"
```

### 6. Management UI
- [ ] **Dashboard Development**: Create a simple UI to list exchanges, monitor queue depths, and view DLQ status.
- [ ] **API Integration**: Connect the UI to the Management REST APIs.

### 7. Testing & Quality Assurance
- [ ] **Unit Testing**: Test routing logic and individual component behavior.
- [ ] **Integration Testing**: Test API endpoints with an embedded SQLite database.
- [ ] **E2E Testing**: Simulate a multi-node cluster to verify leader election and failover scenarios.

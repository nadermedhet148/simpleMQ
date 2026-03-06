# Consensus & Quorum in simpleMQ

> **Last updated:** 2026-03-06

This document explains how **consensus** and **quorum** are used in simpleMQ, where each applies, and how they work together through the Raft protocol (Apache Ratis).

---

## Table of Contents

1. [Definitions](#1-definitions)
2. [Leader Election (Consensus)](#2-leader-election-consensus)
3. [Message Lifecycle (Quorum)](#3-message-lifecycle-quorum)
4. [Where Raft Is Used in the Codebase](#4-where-raft-is-used-in-the-codebase)
5. [How Quorum Works Internally](#5-how-quorum-works-internally)
6. [Summary](#6-summary)

---

## 1. Definitions

- **Quorum**: A minimum number of nodes (majority, `N/2 + 1`) that must agree for a decision to be valid. It is a **counting rule**.
- **Consensus**: The **algorithm/protocol** (Raft in our case) that ensures all nodes agree on the same value/state. It is the **process** of reaching agreement.

They are not alternatives — **quorum is a tool used by the consensus algorithm**.

---

## 2. Leader Election (Consensus)

Leader election is fundamentally a **consensus** problem. The Raft algorithm defines the rules and process for electing a single leader across the cluster.

### How It Works

```
1. A node times out waiting for a heartbeat from the current leader
2. It increments its term and becomes a CANDIDATE
3. It votes for itself and sends RequestVote RPCs to all other nodes
4. Each node grants its vote if:
   - The candidate's term is higher than its own
   - It hasn't already voted for someone else in this term
   - The candidate's log is at least as up-to-date
5. If the candidate receives votes from a QUORUM (majority) → it becomes LEADER
6. The new leader starts sending heartbeats to maintain authority
```

### In the Code

Apache Ratis handles leader election internally. `RaftService` persists election metadata to SQLite for crash recovery:

```java
// In startSyncAndLogging() - syncs term and votedFor to SQLite
long term = division.getInfo().getCurrentTerm();
RaftPeerId votedFor = division.getRaftStorage().getMetadataFile().getMetadata().getVotedFor();
```

Key methods in `RaftService.java`:
- `isLeader()` — checks if the current node is the Raft leader
- `getLeaderId()` — returns the current leader's peer ID
- `waitForLeader(long timeoutMs)` — blocks until a leader is elected

---

## 3. Message Lifecycle (Quorum)

Every write operation in the message lifecycle uses **quorum-based replication**. A majority of nodes must acknowledge a log entry before it is considered committed.

### Publish Flow (Example)

With a 3-node cluster (Node 1, Node 2, Node 3), quorum = **2 out of 3**.

```
1. Producer sends POST /publish to the Leader node
2. Leader calls MessagingEngine.publish()
3. publish() calls raftService.replicateMessage(msg)
4. replicateMessage() calls sendCommand("PUBLISH|id|payload|...")
5. sendCommand() calls sharedClient.io().send(command)
       │
       ▼  ← THIS IS WHERE QUORUM HAPPENS (inside Apache Ratis)
   ┌─────────────────────────────────────────────┐
   │  Leader appends entry to its own Raft log   │
   │  Leader sends AppendEntries RPC to followers │
   │  Waits until MAJORITY (2/3) acknowledge     │
   │  Entry is now "committed"                    │
   └─────────────────────────────────────────────┘
       │
       ▼
6. send() returns RaftClientReply (success=true)
7. On ALL nodes: SimpleStateMachine.applyTransaction() runs
       │
       ├── getPersistenceManager().saveMessage(msg)  → SQLite
       ├── getStorageService().getBuffer(queue).enqueue(msg) → In-memory
       └── Persists lastAppliedIndex to RaftMetadata table
```

### All Write Operations Follow the Same Pattern

```
Client Request → Leader → raftService.replicate___() → sendCommand()
    → sharedClient.io().send()  ← quorum happens here
    → SimpleStateMachine.applyTransaction() on all nodes
```

- **Publish**: `replicateMessage()` → quorum → save & enqueue on all nodes
- **Poll**: `replicatePoll()` → quorum → mark DELIVERED on all nodes
- **Ack**: `replicateAck()` → quorum → mark ACKED on all nodes
- **Nack**: `replicateNack()` → quorum → requeue/DLQ on all nodes

If quorum is **not** reached (e.g., 2 out of 3 nodes are down), `send()` will **fail** and the operation is rejected.

---

## 4. Where Raft Is Used in the Codebase

### Message Lifecycle Operations (`MessagingEngine.java`)

| Operation | Method | Raft Call |
|-----------|--------|-----------|
| **Publish** | `publish()` | `raftService.replicateMessage(msg)` — replicates each message to all nodes via Raft quorum |
| **Poll** | `poll()` | `raftService.replicatePoll(queueName, msg.id)` — replicates the poll event so all nodes mark the message as DELIVERED |
| **Ack** | `acknowledgeMessage()` | `raftService.replicateAck(messageId)` — replicates ACK so all nodes mark the message as ACKED |
| **Nack** | `nackMessage()` | `raftService.replicateNack(messageId, requeue)` — replicates NACK so all nodes either requeue or route to DLQ |

### Management Operations (`ManagementResource.java` → `QueueService` → `RaftService`)

| Operation | Raft Call |
|-----------|-----------|
| **Create Exchange** | `raftService.replicateCreateExchange()` |
| **Delete Exchange** | `raftService.replicateDeleteExchange()` |
| **Create Queue** | `raftService.replicateCreateQueue()` |
| **Delete Queue** | `raftService.replicateDeleteQueue()` |
| **Bind** | `raftService.replicateBind()` |
| **Unbind** | `raftService.replicateUnbind()` |

### Cluster Management (`RaftService.java`)

| Operation | Method |
|-----------|--------|
| **Add Peer** | `addPeer()` — changes Raft cluster membership |
| **Remove Peer** | `removePeer()` — changes Raft cluster membership |
| **Set Configuration** | `setConfiguration()` — reconfigures the full peer set |

### Read Operations (No Raft)

Read operations like `listExchanges`, `listQueues`, `listBindings`, and `getSummary` do **NOT** go through Raft — they read directly from the local SQLite database.

---

## 5. How Quorum Works Internally

The quorum logic is **entirely inside Apache Ratis**. In `RaftService.sendCommand()`:

```java
RaftClientReply reply = sharedClient.io().send(
    org.apache.ratis.protocol.Message.valueOf(command));
```

This single `send()` call does everything:
1. Leader writes the command to its Raft log
2. Leader replicates the log entry to follower nodes via gRPC
3. Leader **waits until a majority of nodes have written the entry** to their logs (this is the quorum)
4. Only then does `send()` return with `success=true`

Once committed, `SimpleStateMachine.applyTransaction()` runs on **every node** (leader + followers), applying the command locally:

| Command | What happens on each node |
|---------|--------------------------|
| `PUBLISH` | Save to SQLite + enqueue in memory buffer |
| `POLL` | Mark message as DELIVERED in DB, dequeue from buffer (followers) |
| `ACK` | Mark message as ACKED, remove from in-flight tracking |
| `NACK` | Requeue or route to DLQ |

This ensures **all nodes have identical state** — same messages in SQLite, same messages in memory buffers.

---

## 6. Summary

| Aspect | What's Used | Why |
|--------|------------|-----|
| **Leader Election** | **Consensus** (Raft algorithm) using a **quorum** of votes | Need majority agreement to elect exactly one leader |
| **Message Replication** | **Quorum** (majority must acknowledge log entry) | Need majority to commit, ensuring durability even if minority fails |
| **State Application** | **Replicated State Machine** (`SimpleStateMachine`) | Ensures all nodes apply the same operations in the same order |

- **Consensus** = the Raft algorithm that defines the rules and process (terms, voting, log comparison)
- **Quorum** = the requirement that a majority must agree for any decision (votes or log commits)
- **Both work together**: Consensus without quorum wouldn't guarantee a single leader. Quorum without consensus wouldn't have the rules for who to vote for.

# KRaft Deep Dive

## What is KRaft?

**KRaft** (Kafka Raft) is Kafka's built-in consensus protocol that replaces Apache ZooKeeper for managing cluster metadata.

```
OLD WAY (Pre-Kafka 3.3):                 NEW WAY (KRaft):
┌─────────────────────────────┐          ┌─────────────────────────────┐
│      ZooKeeper Cluster      │          │                             │
│  ┌───┐  ┌───┐  ┌───┐       │          │     Kafka Brokers with      │
│  │ZK1│  │ZK2│  │ZK3│       │          │    Built-in Consensus       │
│  └───┘  └───┘  └───┘       │          │                             │
│         ▲                   │          │  ┌──────┐ ┌──────┐ ┌──────┐│
└─────────│───────────────────┘          │  │Broker│ │Broker│ │Broker││
          │                              │  │  +   │ │  +   │ │  +   ││
┌─────────│───────────────────┐          │  │Ctrlr │ │Ctrlr │ │Ctrlr ││
│  ┌──────┴──────┐            │          │  └──────┘ └──────┘ └──────┘│
│  │   Kafka     │            │          │                             │
│  │   Brokers   │            │          │   No external dependencies! │
│  └─────────────┘            │          └─────────────────────────────┘
└─────────────────────────────┘
```

---

## Why Replace ZooKeeper?

### Problems with ZooKeeper

| Problem | Description |
|---------|-------------|
| **Separate System** | ZooKeeper is a separate distributed system to maintain |
| **Scaling Limits** | Metadata updates bottleneck at ~200k partitions |
| **Recovery Time** | Controller failover takes minutes with many partitions |
| **Complexity** | Two different systems with different operational characteristics |
| **Split Brain** | Potential for inconsistencies between ZK and Kafka state |

### KRaft Benefits

| Benefit | Description |
|---------|-------------|
| **Simpler Operations** | One system to deploy, monitor, and maintain |
| **Faster Recovery** | Controller failover in milliseconds |
| **Better Scaling** | Supports millions of partitions |
| **Unified Security** | Single security model |
| **Lower Latency** | No network hop to ZooKeeper |

---

## Understanding Raft Consensus

KRaft is based on the **Raft consensus algorithm**. Here's how it works:

### Leader Election

```
Step 1: All nodes start as FOLLOWERS
┌──────────┐  ┌──────────┐  ┌──────────┐
│ Follower │  │ Follower │  │ Follower │
│  Node 1  │  │  Node 2  │  │  Node 3  │
└──────────┘  └──────────┘  └──────────┘

Step 2: Election timeout triggers CANDIDATE state
┌──────────┐  ┌──────────┐  ┌──────────┐
│ Follower │  │CANDIDATE │  │ Follower │
│  Node 1  │  │  Node 2  │  │  Node 3  │
└──────────┘  └──────────┘  └──────────┘
                   │
                   ▼ "Vote for me!"

Step 3: Majority vote → becomes LEADER
┌──────────┐  ┌──────────┐  ┌──────────┐
│ Follower │◀─│  LEADER  │─▶│ Follower │
│  Node 1  │  │  Node 2  │  │  Node 3  │
└──────────┘  └──────────┘  └──────────┘
                   │
                   ▼ Sends heartbeats
```

### Your Configuration

```yaml
# docker-compose.yml
KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka:9093
```

This defines the voting members of the controller quorum:
- Format: `{node_id}@{host}:{port}`
- Your setup: Node 1 at kafka:9093
- Single node = no election needed (it's always the leader)

**Production would look like:**
```yaml
KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka1:9093,2@kafka2:9093,3@kafka3:9093
```

---

## Process Roles

In KRaft, a node can have one or both roles:

### Role Types

| Role | Responsibility |
|------|----------------|
| **broker** | Stores data, serves clients (producers/consumers) |
| **controller** | Manages cluster metadata, handles leader election |

### Your Configuration

```yaml
KAFKA_PROCESS_ROLES: broker,controller
```

This means your single node handles BOTH roles (combined mode).

```
┌──────────────────────────────────────┐
│            YOUR NODE                 │
│                                      │
│  ┌────────────────────────────────┐  │
│  │         BROKER ROLE            │  │
│  │  • Stores messages             │  │
│  │  • Handles produce/consume     │  │
│  │  • Manages partitions          │  │
│  └────────────────────────────────┘  │
│                                      │
│  ┌────────────────────────────────┐  │
│  │       CONTROLLER ROLE          │  │
│  │  • Manages cluster metadata    │  │
│  │  • Handles topic creation      │  │
│  │  • Partition leader election   │  │
│  └────────────────────────────────┘  │
│                                      │
└──────────────────────────────────────┘
```

### Production Deployment Patterns

**Pattern 1: Combined Mode** (Small clusters)
```
3 nodes, each running broker + controller
```

**Pattern 2: Isolated Mode** (Large clusters)
```
3 controller-only nodes
N broker-only nodes
```

---

## Cluster ID

```yaml
CLUSTER_ID: ${CLUSTER_ID:-MkU3OEVBNTcwNTJENDM2Qk}
```

The **Cluster ID** is a unique identifier that:
- Ties all nodes together as one cluster
- Prevents nodes from accidentally joining wrong clusters
- Is written to the metadata log at cluster initialization

### How It's Generated

```bash
# Generate a new cluster ID
kafka-storage random-uuid

# Output: Something like "MkU3OEVBNTcwNTJENDM2Qk"
```

### What Happens At Startup

```
1. Kafka reads CLUSTER_ID from environment
   ▼
2. Checks /tmp/kraft-logs for existing cluster
   ▼
3a. If NEW: Formats storage with this CLUSTER_ID
3b. If EXISTS: Verifies CLUSTER_ID matches
   ▼
4. If mismatch → FAILS TO START
```

---

## Listeners Deep Dive

Your node has THREE listeners:

```yaml
KAFKA_LISTENERS: PLAINTEXT://0.0.0.0:9092,PLAINTEXT_HOST://0.0.0.0:29092,CONTROLLER://0.0.0.0:9093
```

### Listener Breakdown

```
┌─────────────────────────────────────────────────────────────────────┐
│                         YOUR KAFKA NODE                             │
│                                                                     │
│  Port 9092 (PLAINTEXT)      ◀── Other containers (Spark, etc.)    │
│  └─ Advertised: kafka:9092                                         │
│                                                                     │
│  Port 29092 (PLAINTEXT_HOST) ◀── Your host machine (Producer)     │
│  └─ Advertised: localhost:29092                                    │
│                                                                     │
│  Port 9093 (CONTROLLER)      ◀── Controller-to-controller only    │
│  └─ Internal consensus traffic                                     │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

### Why Two Client Listeners?

| Listener | Advertised As | Used By |
|----------|---------------|---------|
| PLAINTEXT | `kafka:9092` | Containers (Spark) - via Docker network |
| PLAINTEXT_HOST | `localhost:29092` | Host machine (your Producer) |

**The problem this solves:**
```
Container (Spark) connects to "kafka:9092"
  └─ Docker resolves "kafka" to container IP ✓

Host (Producer) connects to "localhost:29092" 
  └─ Port forwarded to container ✓
```

If we only had one listener:
- `kafka:9092` would fail from host (can't resolve "kafka")
- `localhost:9092` would fail from container (localhost is the container itself)

---

## Controller Listener

```yaml
KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka:9093
```

The controller listener handles:
- **Raft protocol messages** (heartbeats, votes, log replication)
- **Metadata updates** (topic creation, partition reassignment)
- **Cluster coordination**

**This listener is NOT for client traffic!**

---

## Metadata Log

KRaft stores all cluster metadata in a special internal topic called `__cluster_metadata`.

```
/tmp/kraft-logs/
├── __cluster_metadata-0/          ◀── The metadata partition
│   ├── 00000000000000000000.log   ◀── The actual log file
│   └── ...
├── prices-0/                      ◀── Your topic partition
│   └── ...
└── ...
```

### What's in the Metadata Log?

| Record Type | Contains |
|-------------|----------|
| TopicRecord | Topic name, ID, partition count |
| PartitionRecord | Partition assignments, leader, ISR |
| BrokerRecord | Broker ID, endpoints, rack |
| ConfigRecord | Topic/broker configurations |
| ProducerIdRecord | Transaction producer IDs |

### Viewing Metadata

```bash
# Inside the container:
docker exec -it kafka kafka-metadata --snapshot /tmp/kraft-logs/__cluster_metadata-0/00000000000000000000.log --command-config /dev/null
```

---

## Node ID

```yaml
KAFKA_NODE_ID: 1
```

Every node needs a unique ID. This ID:
- Is referenced in the quorum voters config
- Appears in logs and metrics
- Must be stable (don't change it!)

---

## How KRaft Handles Failures

### Single Node (Your Setup)
```
Node 1 dies → Everything stops
No redundancy in single-node mode
```

### Multi-Node Cluster
```
Node 1 (Leader) dies:
  ▼
Followers detect missing heartbeat (election timeout)
  ▼
New election begins
  ▼
Node 2 or 3 becomes new leader
  ▼
Cluster continues operating

Quorum: Need majority (2 of 3) for cluster to function
```

---

## Key Configuration Summary

| Config | Your Value | Purpose |
|--------|------------|---------|
| `KAFKA_NODE_ID` | 1 | Unique node identifier |
| `KAFKA_PROCESS_ROLES` | broker,controller | This node does both |
| `KAFKA_CONTROLLER_QUORUM_VOTERS` | 1@kafka:9093 | Voting members |
| `CLUSTER_ID` | MkU3OE... | Cluster identifier |
| `KAFKA_CONTROLLER_LISTENER_NAMES` | CONTROLLER | Listener for Raft |
| `KAFKA_INTER_BROKER_LISTENER_NAME` | PLAINTEXT | Listener for broker-to-broker |

---

## Exercises

### Exercise 1: View Cluster Metadata
```powershell
docker exec -it kafka kafka-metadata --snapshot /tmp/kraft-logs/__cluster_metadata-0/00000000000000000000.log --command-config /dev/null 2>/dev/null | head -50
```

### Exercise 2: Check Cluster ID
```powershell
docker exec -it kafka kafka-cluster cluster-id --bootstrap-server localhost:9092
```

### Exercise 3: View Broker Info
```powershell
docker exec -it kafka kafka-broker-api-versions --bootstrap-server localhost:9092
```

### Exercise 4: Monitor Controller
```powershell
docker exec -it kafka kafka-metadata quorum --bootstrap-server localhost:9092 describe --status
```

---

## Key Takeaways

1. **KRaft replaces ZooKeeper** with built-in Raft consensus
2. **Nodes can be brokers, controllers, or both**
3. **Cluster ID** ties nodes together and prevents misconfigurations
4. **Multiple listeners** solve the Docker networking challenge
5. **The controller listener** is separate from client traffic
6. **Metadata is stored** in the `__cluster_metadata` topic

---

Next: [03-spark-fundamentals.md](03-spark-fundamentals.md) - Understanding Spark architecture

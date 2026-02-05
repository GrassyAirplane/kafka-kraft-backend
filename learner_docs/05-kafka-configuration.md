# Kafka Configuration Deep Dive

This document explains **every configuration** in your `docker-compose.yml` for the Kafka service.

## Your Complete Kafka Configuration

```yaml
kafka:
  image: confluentinc/cp-kafka:7.6.0
  container_name: kafka
  ports:
    - "29092:29092"
  environment:
    KAFKA_NODE_ID: 1
    KAFKA_PROCESS_ROLES: broker,controller
    KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka:9093
    CLUSTER_ID: ${CLUSTER_ID:-MkU3OEVBNTcwNTJENDM2Qk}

    KAFKA_LISTENERS: PLAINTEXT://0.0.0.0:9092,PLAINTEXT_HOST://0.0.0.0:29092,CONTROLLER://0.0.0.0:9093
    KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092,PLAINTEXT_HOST://localhost:29092
    KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT,CONTROLLER:PLAINTEXT
    KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
    KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT

    KAFKA_LOG_DIRS: /tmp/kraft-logs
    KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
    KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
    KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
    KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0
  healthcheck:
    test: kafka-cluster cluster-id --bootstrap-server localhost:9092 || exit 1
    interval: 5s
    timeout: 10s
    retries: 10
```

---

## Section 1: Container Configuration

### `image: confluentinc/cp-kafka:7.6.0`

| Aspect | Details |
|--------|---------|
| **Vendor** | Confluent Platform |
| **Version** | 7.6.0 (maps to Apache Kafka 3.6.x) |
| **Includes** | KRaft support, additional tooling |
| **Alternative** | `apache/kafka:3.6.0` (official Apache image) |

**Why Confluent?**
- Better documentation
- Pre-configured for KRaft
- Additional CLI tools

---

### `container_name: kafka`

Sets a fixed container name instead of Docker's auto-generated name.

**Why it matters:**
- Other services reference it: `kafka:9092`
- Easier to read in logs
- Predictable for scripts

---

### `ports: - "29092:29092"`

```
Host Machine                    Docker Container
     │                                │
     │     Port Mapping               │
     │                                │
localhost:29092  ◀──────────────▶  0.0.0.0:29092
```

**Why 29092 and not 9092?**
- 9092 is for inter-container communication (via Docker network)
- 29092 is exposed to your host for the Python producer

---

## Section 2: KRaft Identity

### `KAFKA_NODE_ID: 1`

| Property | Value | Notes |
|----------|-------|-------|
| Type | Integer | Must be unique per node |
| Required | Yes (for KRaft) | ZooKeeper mode auto-generates |
| Stability | Must not change | Stored in log directory |

**In multi-node clusters:**
```yaml
# Node 1
KAFKA_NODE_ID: 1
# Node 2
KAFKA_NODE_ID: 2
# Node 3
KAFKA_NODE_ID: 3
```

---

### `KAFKA_PROCESS_ROLES: broker,controller`

Defines what roles this node performs.

| Role | Responsibility |
|------|----------------|
| `broker` | Stores data, handles client requests |
| `controller` | Manages cluster metadata, handles elections |

**Deployment patterns:**

```
# Combined mode (your setup) - good for small clusters
KAFKA_PROCESS_ROLES: broker,controller

# Isolated mode - production with many brokers
# Controller nodes:
KAFKA_PROCESS_ROLES: controller
# Broker nodes:
KAFKA_PROCESS_ROLES: broker
```

---

### `KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka:9093`

Defines the Raft quorum members.

**Format:** `{node_id}@{host}:{port},{node_id}@{host}:{port},...`

```yaml
# Your single-node setup
KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka:9093

# 3-node production cluster
KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka1:9093,2@kafka2:9093,3@kafka3:9093
```

**Rules:**
- All controller nodes must be listed
- Must use the controller listener port (9093)
- Node IDs must match KAFKA_NODE_ID of each node

---

### `CLUSTER_ID: ${CLUSTER_ID:-MkU3OEVBNTcwNTJENDM2Qk}`

**Format:** `${VAR:-default}` means "use $VAR if set, otherwise use default"

| Aspect | Details |
|--------|---------|
| **Purpose** | Uniquely identifies the cluster |
| **Format** | Base64-encoded UUID |
| **Generation** | `kafka-storage random-uuid` |
| **Stability** | Must never change after first boot |

**What happens at startup:**

```
1. Kafka reads CLUSTER_ID
   ▼
2. Checks if /tmp/kraft-logs is formatted
   ▼
3a. If not formatted: Runs kafka-storage format
3b. If formatted: Validates CLUSTER_ID matches
   ▼
4. If mismatch → Fails to start
```

---

## Section 3: Listeners

### Understanding Listeners

```
┌─────────────────────────────────────────────────────────────────────────┐
│                           KAFKA NODE                                    │
│                                                                         │
│  ┌─────────────────────────────────────────────────────────────────┐    │
│  │                      LISTENERS                                   │    │
│  │  (What interfaces/ports Kafka binds to)                         │    │
│  │                                                                  │    │
│  │  PLAINTEXT://0.0.0.0:9092    ← Binds to all interfaces          │    │
│  │  PLAINTEXT_HOST://0.0.0.0:29092                                 │    │
│  │  CONTROLLER://0.0.0.0:9093                                      │    │
│  └─────────────────────────────────────────────────────────────────┘    │
│                                                                         │
│  ┌─────────────────────────────────────────────────────────────────┐    │
│  │                 ADVERTISED_LISTENERS                             │    │
│  │  (What Kafka tells clients to connect to)                       │    │
│  │                                                                  │    │
│  │  PLAINTEXT://kafka:9092     ← For containers                    │    │
│  │  PLAINTEXT_HOST://localhost:29092  ← For host machine          │    │
│  └─────────────────────────────────────────────────────────────────┘    │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

---

### `KAFKA_LISTENERS`

```yaml
KAFKA_LISTENERS: PLAINTEXT://0.0.0.0:9092,PLAINTEXT_HOST://0.0.0.0:29092,CONTROLLER://0.0.0.0:9093
```

**Format:** `{listener_name}://{bind_address}:{port}`

| Listener | Bind Address | Port | Purpose |
|----------|--------------|------|---------|
| PLAINTEXT | 0.0.0.0 | 9092 | Inter-container traffic |
| PLAINTEXT_HOST | 0.0.0.0 | 29092 | Host machine traffic |
| CONTROLLER | 0.0.0.0 | 9093 | Raft consensus |

**Why `0.0.0.0`?**
- Binds to all network interfaces
- Allows connections from anywhere
- In production, you might bind to specific IPs

---

### `KAFKA_ADVERTISED_LISTENERS`

```yaml
KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092,PLAINTEXT_HOST://localhost:29092
```

**This is what Kafka tells clients to connect to!**

```
Client connects to bootstrap server
           │
           ▼
Kafka returns advertised listeners
           │
           ▼
Client uses advertised address for actual communication
```

**Why different from LISTENERS?**

| Listener | Bind | Advertise | Why |
|----------|------|-----------|-----|
| PLAINTEXT | 0.0.0.0:9092 | kafka:9092 | Containers resolve "kafka" via Docker DNS |
| PLAINTEXT_HOST | 0.0.0.0:29092 | localhost:29092 | Host connects via port forward |

**Common mistakes:**
```yaml
# WRONG: Host can't resolve "kafka"
KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092

# WRONG: Containers can't use localhost
KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
```

---

### `KAFKA_LISTENER_SECURITY_PROTOCOL_MAP`

```yaml
KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT,CONTROLLER:PLAINTEXT
```

Maps listener names to security protocols.

| Listener Name | Protocol | Meaning |
|---------------|----------|---------|
| PLAINTEXT | PLAINTEXT | No encryption, no auth |
| PLAINTEXT_HOST | PLAINTEXT | No encryption, no auth |
| CONTROLLER | PLAINTEXT | No encryption, no auth |

**Available protocols:**
- `PLAINTEXT` - No security
- `SSL` - TLS encryption
- `SASL_PLAINTEXT` - Authentication, no encryption
- `SASL_SSL` - Authentication + encryption

---

### `KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER`

Tells Kafka which listener(s) are for controller communication.

**Important:** Controller listeners are never advertised to clients!

---

### `KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT`

Which listener brokers use to talk to each other.

```
Broker 1                    Broker 2
    │                          │
    │    PLAINTEXT:9092        │
    │◀────────────────────────▶│
    │    (replication)         │
```

---

## Section 4: Storage

### `KAFKA_LOG_DIRS: /tmp/kraft-logs`

Where Kafka stores everything:

```
/tmp/kraft-logs/
├── __cluster_metadata-0/     ← KRaft metadata
│   └── 00000000000000000000.log
├── prices-0/                 ← Your topic, partition 0
│   ├── 00000000000000000000.log
│   ├── 00000000000000000000.index
│   └── 00000000000000000000.timeindex
└── meta.properties           ← Cluster/node metadata
```

**Production consideration:**
- Use persistent volume, not /tmp!
- Multiple directories for multiple disks

```yaml
# Production
KAFKA_LOG_DIRS: /var/kafka/data1,/var/kafka/data2
volumes:
  - kafka-data1:/var/kafka/data1
  - kafka-data2:/var/kafka/data2
```

---

## Section 5: Replication

### `KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1`

Replication for `__consumer_offsets` topic (stores consumer positions).

| Value | Meaning |
|-------|---------|
| 1 | No redundancy (dev only) |
| 3 | Standard production setting |

---

### `KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1`

Replication for `__transaction_state` topic (for exactly-once semantics).

---

### `KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1`

**Minimum In-Sync Replicas** for transaction state.

| Setting | Replication=1 | Replication=3 |
|---------|---------------|---------------|
| min.isr=1 | OK (dev) | Can lose 2 brokers |
| min.isr=2 | Invalid | Can lose 1 broker |
| min.isr=3 | Invalid | No fault tolerance |

**Formula:** `min.isr <= replication.factor`

---

## Section 6: Consumer Groups

### `KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0`

How long to wait before first rebalance when consumers join.

| Value | Use Case |
|-------|----------|
| 0 | Dev/testing - start immediately |
| 3000 (default) | Production - wait for more consumers |

**Why wait in production?**
```
Without delay:                    With delay (3s):
Consumer 1 joins → Rebalance!     Consumer 1 joins
Consumer 2 joins → Rebalance!     Consumer 2 joins  
Consumer 3 joins → Rebalance!     Consumer 3 joins
                                  3 seconds pass → Single rebalance
```

---

## Section 7: Health Check

```yaml
healthcheck:
  test: kafka-cluster cluster-id --bootstrap-server localhost:9092 || exit 1
  interval: 5s
  timeout: 10s
  retries: 10
```

| Parameter | Value | Meaning |
|-----------|-------|---------|
| test | kafka-cluster cluster-id... | Command to check health |
| interval | 5s | Check every 5 seconds |
| timeout | 10s | Fail if command takes >10s |
| retries | 10 | Unhealthy after 10 failures |

**Why this matters:**
```yaml
kafka-init:
  depends_on:
    kafka:
      condition: service_healthy  # Waits for healthcheck to pass
```

---

## Section 8: Topic Configuration (kafka-init)

```yaml
kafka-init:
  command: >
    bash -c "
      kafka-topics --bootstrap-server kafka:9092 \
        --create --if-not-exists \
        --topic prices \
        --partitions 1 \
        --replication-factor 1
    "
```

### Topic Parameters

| Parameter | Value | Production Recommendation |
|-----------|-------|---------------------------|
| `--partitions` | 1 | 3-12 per topic (based on throughput) |
| `--replication-factor` | 1 | 3 (for fault tolerance) |

### Additional Topic Configs

```bash
kafka-topics --create \
  --topic prices \
  --partitions 6 \
  --replication-factor 3 \
  --config retention.ms=604800000 \        # 7 days
  --config segment.bytes=1073741824 \       # 1 GB segments
  --config cleanup.policy=delete \          # Delete old messages
  --config compression.type=lz4            # Compression
```

---

## Complete Configuration Reference

| Config | Your Value | Production Recommendation |
|--------|------------|---------------------------|
| KAFKA_NODE_ID | 1 | Unique per node (1, 2, 3...) |
| KAFKA_PROCESS_ROLES | broker,controller | Consider isolating in large clusters |
| KAFKA_CONTROLLER_QUORUM_VOTERS | 1@kafka:9093 | 3 or 5 nodes for HA |
| CLUSTER_ID | MkU3OE... | Generate fresh per cluster |
| KAFKA_LOG_DIRS | /tmp/kraft-logs | Persistent volume required! |
| OFFSETS_REPLICATION | 1 | 3 |
| TRANSACTION_REPLICATION | 1 | 3 |
| TRANSACTION_MIN_ISR | 1 | 2 |
| REBALANCE_DELAY | 0 | 3000ms |

---

## Exercises

### Exercise 1: View Effective Config
```bash
docker exec -it kafka kafka-configs \
  --bootstrap-server localhost:9092 \
  --entity-type brokers \
  --entity-name 1 \
  --describe --all
```

### Exercise 2: View Topic Config
```bash
docker exec -it kafka kafka-configs \
  --bootstrap-server localhost:9092 \
  --entity-type topics \
  --entity-name prices \
  --describe --all
```

### Exercise 3: View Log Directory
```bash
docker exec -it kafka ls -la /tmp/kraft-logs/
docker exec -it kafka ls -la /tmp/kraft-logs/prices-0/
```

---

Next: [06-spark-configuration.md](06-spark-configuration.md) - Spark configuration deep dive

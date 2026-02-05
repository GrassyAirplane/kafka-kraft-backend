# Kafka Fundamentals

## What is Kafka?

Apache Kafka is a **distributed event streaming platform**. Think of it as a highly durable, high-throughput message queue that can handle millions of events per second.

```
┌──────────────────────────────────────────────────────────────────┐
│                         KAFKA CLUSTER                            │
│                                                                  │
│  Producer ──▶ ┌─────────────────────────────────┐ ──▶ Consumer   │
│               │           TOPIC                  │               │
│               │  ┌─────┬─────┬─────┬─────┬────┐ │               │
│               │  │  0  │  1  │  2  │  3  │ ...│ │  (offsets)    │
│               │  └─────┴─────┴─────┴─────┴────┘ │               │
│               └─────────────────────────────────┘               │
└──────────────────────────────────────────────────────────────────┘
```

---

## Core Concepts

### 1. Topics

A **topic** is a named stream of records (like a table name in a database).

```python
# In your api_producer.py:
producer.send("prices", event)  # "prices" is the topic name
```

**Your project:** The `prices` topic stores Bitcoin price events.

**Key properties:**
- Topics are append-only (you can't update/delete individual records)
- Topics can be configured with retention policies (time or size based)
- Topics are split into partitions for parallelism

---

### 2. Partitions

A **partition** is an ordered, immutable sequence of records. Topics are divided into partitions for:
- **Parallelism**: Multiple consumers can read different partitions simultaneously
- **Ordering**: Records within a partition are strictly ordered
- **Scalability**: Partitions can be spread across different brokers

```
Topic: prices (3 partitions)
┌─────────────────────────────────────────────────────────────────┐
│                                                                 │
│  Partition 0: [msg0, msg3, msg6, msg9, ...]                    │
│  Partition 1: [msg1, msg4, msg7, msg10, ...]                   │
│  Partition 2: [msg2, msg5, msg8, msg11, ...]                   │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

**Your project:** You created 1 partition (good for dev, but production would have more).

```yaml
# In docker-compose.yml (kafka-init command):
kafka-topics --create --topic prices --partitions 1 --replication-factor 1
```

---

### 3. Offsets

An **offset** is a unique sequential ID for each record within a partition.

```
Partition 0:
┌────────┬────────┬────────┬────────┬────────┐
│ off=0  │ off=1  │ off=2  │ off=3  │ off=4  │
│ msg A  │ msg B  │ msg C  │ msg D  │ msg E  │
└────────┴────────┴────────┴────────┴────────┘
                     ▲
                     │
              Consumer position
```

**Why offsets matter:**
- Consumers track their position via offsets
- Allows replay: "Start reading from offset 100"
- Enables exactly-once processing with checkpoints

**In your Spark logs, you saw:**
```
Resuming at batch 1 with committed offsets {"prices":{"0":12}}
```
This means Spark is resuming from offset 12 in partition 0.

---

### 4. Producers

A **producer** publishes records to topics.

```python
# Your api_producer.py
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers="localhost:29092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

producer.send("prices", event)
```

**Key producer concepts:**

| Concept | Description |
|---------|-------------|
| **Serialization** | Converting Python objects to bytes |
| **Partitioning** | Deciding which partition to write to |
| **Batching** | Grouping messages for efficiency |
| **Acks** | Confirmation from brokers |

---

### 5. Consumers

A **consumer** reads records from topics.

```python
# Spark acts as a consumer:
df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "prices")  # Subscribe to topic
    .load()
)
```

---

### 6. Consumer Groups

A **consumer group** is a set of consumers that cooperate to consume a topic.

```
Topic: prices (3 partitions)
Consumer Group: spark-processor

┌──────────────────────────────────────────────────┐
│                                                  │
│  Partition 0 ──▶ Consumer A                      │
│  Partition 1 ──▶ Consumer B                      │
│  Partition 2 ──▶ Consumer C                      │
│                                                  │
└──────────────────────────────────────────────────┘
```

**Rules:**
- Each partition is consumed by exactly ONE consumer in the group
- More consumers than partitions = some consumers sit idle
- If a consumer fails, its partitions are reassigned (rebalancing)

---

### 7. Brokers

A **broker** is a Kafka server that stores data and serves clients.

```
┌─────────────────────────────────────────────────────────────┐
│                    KAFKA CLUSTER                            │
│                                                             │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐         │
│  │  Broker 1   │  │  Broker 2   │  │  Broker 3   │         │
│  │  (Leader)   │  │  (Follower) │  │  (Follower) │         │
│  │             │  │             │  │             │         │
│  │ Partition 0 │  │ Partition 0 │  │ Partition 0 │         │
│  │   (copy)    │  │   (copy)    │  │   (copy)    │         │
│  └─────────────┘  └─────────────┘  └─────────────┘         │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

**Your project:** You have 1 broker (single-node setup for development).

---

## Message Flow

```
1. Producer creates a record
   ▼
2. Serializer converts to bytes
   ▼
3. Partitioner decides which partition
   ▼
4. Record buffered in producer batch
   ▼
5. Batch sent to broker
   ▼
6. Broker writes to partition log
   ▼
7. Broker sends acknowledgment
   ▼
8. Consumer polls for new records
   ▼
9. Deserializer converts bytes to object
   ▼
10. Application processes record
```

---

## The Commit Log

Kafka stores messages in a **commit log** - an append-only data structure.

```
Partition Log File (on disk):
┌─────────────────────────────────────────────────────────────┐
│ [offset=0][timestamp][key][value][checksum]                 │
│ [offset=1][timestamp][key][value][checksum]                 │
│ [offset=2][timestamp][key][value][checksum]                 │
│ ...                                                         │
└─────────────────────────────────────────────────────────────┘
```

**Why append-only?**
- Sequential writes are FAST (HDDs and SSDs love sequential I/O)
- No locks needed for concurrent access
- Easy replication (just copy the file)

**Your config:**
```yaml
KAFKA_LOG_DIRS: /tmp/kraft-logs
```
This is where Kafka stores the actual message data.

---

## Retention

Kafka doesn't delete messages immediately after consumption. Messages are retained based on:

| Policy | Description | Config |
|--------|-------------|--------|
| Time | Keep for X hours/days | `log.retention.hours` |
| Size | Keep until log reaches X bytes | `log.retention.bytes` |
| Compaction | Keep latest value per key | `cleanup.policy=compact` |

---

## Exercises

### Exercise 1: View Topic Details
```powershell
docker exec -it kafka kafka-topics --bootstrap-server localhost:29092 --describe --topic prices
```

You'll see:
- Partition count
- Replication factor
- Leader broker
- ISR (In-Sync Replicas)

### Exercise 2: View Consumer Groups
```powershell
docker exec -it kafka kafka-consumer-groups --bootstrap-server localhost:29092 --list
docker exec -it kafka kafka-consumer-groups --bootstrap-server localhost:29092 --describe --group <group-name>
```

### Exercise 3: Read Messages Manually
```powershell
docker exec -it kafka kafka-console-consumer --bootstrap-server localhost:29092 --topic prices --from-beginning
```

### Exercise 4: Check Offsets
```powershell
docker exec -it kafka kafka-get-offsets --bootstrap-server localhost:29092 --topic prices
```

---

## Key Takeaways

1. **Topics** are logical channels for messages
2. **Partitions** enable parallelism and ordering
3. **Offsets** track position in the log
4. **Consumer groups** coordinate parallel consumption
5. **The commit log** is append-only for performance
6. **Retention** policies control how long data is kept

---

Next: [02-kraft-deep-dive.md](02-kraft-deep-dive.md) - Understanding KRaft consensus

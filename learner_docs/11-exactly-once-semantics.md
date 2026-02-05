# Exactly-Once Semantics

This document explains end-to-end delivery guarantees across your Kafka → Spark → Iceberg pipeline.

---

## Delivery Guarantee Types

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                      MESSAGE DELIVERY GUARANTEES                             │
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │  AT-MOST-ONCE                                                        │   │
│  │                                                                      │   │
│  │  • Messages may be lost                                              │   │
│  │  • No duplicates                                                     │   │
│  │  • Simplest implementation                                           │   │
│  │  • Use case: Metrics where some loss is acceptable                   │   │
│  │                                                                      │   │
│  │  Send ──▶ Ack lost ──▶ No retry ──▶ Message lost                     │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │  AT-LEAST-ONCE                                                       │   │
│  │                                                                      │   │
│  │  • Messages never lost                                               │   │
│  │  • Duplicates possible                                               │   │
│  │  • Consumer must be idempotent                                       │   │
│  │  • Use case: Log aggregation with deduplication                      │   │
│  │                                                                      │   │
│  │  Send ──▶ Ack lost ──▶ Retry ──▶ Duplicate                           │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │  EXACTLY-ONCE                                                        │   │
│  │                                                                      │   │
│  │  • Messages never lost                                               │   │
│  │  • No duplicates                                                     │   │
│  │  • Most complex implementation                                       │   │
│  │  • Use case: Financial transactions, billing                         │   │
│  │                                                                      │   │
│  │  Send ──▶ Ack lost ──▶ Retry ──▶ Dedup at broker ──▶ No duplicate   │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Your Pipeline's Guarantees

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    END-TO-END GUARANTEE CHAIN                                │
│                                                                             │
│  Producer         ──▶        Kafka         ──▶        Spark/Iceberg        │
│                                                                             │
│  Current:                    Current:                  Current:             │
│  AT-LEAST-ONCE              AT-LEAST-ONCE             EXACTLY-ONCE          │
│                                                                             │
│  ┌───────────┐              ┌───────────┐              ┌───────────┐        │
│  │ acks=1    │              │ Topic     │              │ Checkpoint│        │
│  │ (default) │              │ stores    │              │ + Iceberg │        │
│  │           │              │ durably   │              │ atomic    │        │
│  └───────────┘              └───────────┘              └───────────┘        │
│                                                                             │
│  Overall: AT-LEAST-ONCE (limited by weakest link)                           │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Producer Side Guarantees

### Current Configuration (At-Least-Once)

```python
producer = KafkaProducer(
    bootstrap_servers="localhost:29092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    # Implicit defaults:
    # acks=1           ← Leader acknowledgment only
    # retries=0        ← No retries on failure
)
```

### Upgrade to Exactly-Once Producer

```python
producer = KafkaProducer(
    bootstrap_servers="localhost:29092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    
    # Exactly-once settings
    acks='all',                    # Wait for all replicas
    enable_idempotence=True,       # Prevent duplicate writes
    retries=3,                     # Retry on transient failures
    max_in_flight_requests_per_connection=5,  # Required for idempotence
)
```

### How Idempotent Producer Works

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    IDEMPOTENT PRODUCER                                       │
│                                                                             │
│  Producer State:                                                            │
│  • Producer ID (PID): 1000                                                  │
│  • Sequence Number: 0, 1, 2, 3...                                           │
│                                                                             │
│  ┌────────────────────────────────────────────────────────────────────┐    │
│  │  Message 1: {PID: 1000, Seq: 0, Data: "price1"}                    │    │
│  │  Message 2: {PID: 1000, Seq: 1, Data: "price2"}                    │    │
│  │  Message 3: {PID: 1000, Seq: 2, Data: "price3"}  ← Ack lost        │    │
│  │  Message 3: {PID: 1000, Seq: 2, Data: "price3"}  ← Retry           │    │
│  └────────────────────────────────────────────────────────────────────┘    │
│                                                                             │
│  Broker sees Seq 2 twice:                                                   │
│  • First time: Accept, return success                                       │
│  • Retry: Recognize duplicate, return success (no-op)                       │
│                                                                             │
│  Result: Exactly one copy stored                                            │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Kafka Storage Guarantees

### Replication Factor

```yaml
# docker-compose.yml for production
environment:
  KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 3
  KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 3
  KAFKA_DEFAULT_REPLICATION_FACTOR: 3
  KAFKA_MIN_INSYNC_REPLICAS: 2
```

### How Replication Works

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    KAFKA REPLICATION                                         │
│                                                                             │
│  Broker 1 (Leader)     Broker 2 (Follower)    Broker 3 (Follower)          │
│  ┌────────────────┐    ┌────────────────┐     ┌────────────────┐           │
│  │  Partition 0   │    │  Partition 0   │     │  Partition 0   │           │
│  │                │    │    (Replica)   │     │    (Replica)   │           │
│  │  Offset 0: msg1│───▶│  Offset 0: msg1│────▶│  Offset 0: msg1│           │
│  │  Offset 1: msg2│───▶│  Offset 1: msg2│────▶│  Offset 1: msg2│           │
│  │  Offset 2: msg3│───▶│  Offset 2: msg3│────▶│  Offset 2: msg3│           │
│  │                │    │                │     │                │           │
│  └────────────────┘    └────────────────┘     └────────────────┘           │
│                                                                             │
│  ISR (In-Sync Replicas): [Broker1, Broker2, Broker3]                        │
│                                                                             │
│  With acks='all' and min.insync.replicas=2:                                 │
│  • Producer waits for at least 2 brokers to acknowledge                     │
│  • Message survives loss of 1 broker                                        │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Spark Streaming Guarantees

### How Spark Achieves Exactly-Once

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                SPARK STRUCTURED STREAMING EXACTLY-ONCE                       │
│                                                                             │
│  Key Components:                                                            │
│                                                                             │
│  1. Offset Tracking                                                         │
│     ┌─────────────────────────────────────────────────────────────────┐    │
│     │  checkpoint/offsets/                                             │    │
│     │  ├── 0: {"prices":{"0":0}}    ← Start of batch 0                │    │
│     │  ├── 1: {"prices":{"0":5}}    ← Start of batch 1                │    │
│     │  └── 2: {"prices":{"0":10}}   ← Start of batch 2                │    │
│     └─────────────────────────────────────────────────────────────────┘    │
│                                                                             │
│  2. Write-Ahead Log (WAL)                                                   │
│     ┌─────────────────────────────────────────────────────────────────┐    │
│     │  checkpoint/commits/                                             │    │
│     │  ├── 0   ← Batch 0 fully committed                              │    │
│     │  ├── 1   ← Batch 1 fully committed                              │    │
│     │  └── (2 in progress, not committed yet)                         │    │
│     └─────────────────────────────────────────────────────────────────┘    │
│                                                                             │
│  3. Idempotent Sink (Iceberg)                                               │
│     • Atomic commits                                                        │
│     • Snapshot isolation                                                    │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Failure Recovery Scenarios

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    FAILURE RECOVERY                                          │
│                                                                             │
│  Scenario 1: Failure BEFORE writing data                                    │
│  ──────────────────────────────────────                                     │
│  • Batch 5 starts reading from Kafka offset 50                              │
│  • Spark crashes while processing                                           │
│  • On restart: checkpoint shows batch 4 committed (offset 50)               │
│  • Restart batch 5 from offset 50                                           │
│  • Result: No data loss                                                     │
│                                                                             │
│  Scenario 2: Failure AFTER writing data, BEFORE checkpoint                  │
│  ────────────────────────────────────────────────────────────               │
│  • Batch 5 writes Parquet file to Iceberg                                   │
│  • Spark crashes before checkpointing                                       │
│  • On restart: checkpoint shows batch 4 committed                           │
│  • Restart batch 5, write same data again                                   │
│  • Iceberg: New snapshot replaces incomplete one                            │
│  • Result: Duplicate write, but idempotent (same data)                      │
│                                                                             │
│  Scenario 3: Failure AFTER checkpoint                                       │
│  ──────────────────────────────────────                                     │
│  • Batch 5 fully committed (data + checkpoint)                              │
│  • Spark crashes                                                            │
│  • On restart: checkpoint shows batch 5 committed                           │
│  • Start batch 6 from offset 55                                             │
│  • Result: No data loss, no duplicates                                      │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Iceberg Guarantees

### Atomic Commits

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    ICEBERG ATOMIC COMMIT                                     │
│                                                                             │
│  State BEFORE commit:                                                       │
│  metadata/                                                                  │
│  └── v5.metadata.json ◄── current-snapshot: 100                             │
│                                                                             │
│  Commit Process:                                                            │
│  1. Write new Parquet files (not visible yet)                               │
│  2. Create new manifest files                                               │
│  3. Write v6.metadata.json (new snapshot: 101)                              │
│  4. ATOMIC: Update table pointer to v6.metadata.json                        │
│                                                                             │
│  State AFTER commit:                                                        │
│  metadata/                                                                  │
│  ├── v5.metadata.json                                                       │
│  └── v6.metadata.json ◄── current-snapshot: 101                             │
│                                                                             │
│  Concurrent readers:                                                        │
│  • Reading v5: See snapshot 100 (consistent)                                │
│  • Reading v6: See snapshot 101 (consistent)                                │
│  • Never see partial state                                                  │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Optimistic Concurrency Control

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    CONCURRENT WRITE HANDLING                                 │
│                                                                             │
│  Writer 1                              Writer 2                             │
│  ────────                              ────────                             │
│  Read v5.metadata.json                 Read v5.metadata.json                │
│       │                                     │                               │
│       ▼                                     ▼                               │
│  Write data files                      Write data files                     │
│       │                                     │                               │
│       ▼                                     │                               │
│  Create v6.metadata.json                    │                               │
│       │                                     │                               │
│       ▼                                     ▼                               │
│  COMMIT (success) ◄──────────────     Create v6.metadata.json               │
│                                             │                               │
│                                             ▼                               │
│                                       COMMIT (CONFLICT!)                    │
│                                             │                               │
│                                             ▼                               │
│                                       Retry:                                │
│                                       • Read v6.metadata.json               │
│                                       • Rebase changes                      │
│                                       • Create v7.metadata.json             │
│                                       • COMMIT (success)                    │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Achieving True End-to-End Exactly-Once

### Configuration Checklist

```python
# 1. PRODUCER: Enable idempotence
producer = KafkaProducer(
    bootstrap_servers="localhost:29092",
    acks='all',
    enable_idempotence=True,
    retries=3,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

# 2. KAFKA: Proper replication (production only)
# KAFKA_DEFAULT_REPLICATION_FACTOR: 3
# KAFKA_MIN_INSYNC_REPLICAS: 2

# 3. SPARK: Use checkpointing (already done)
.option("checkpointLocation", "/opt/warehouse/checkpoint/prices")

# 4. ICEBERG: Use atomic sink (already done)
.format("iceberg")
```

### Your Pipeline with Exactly-Once

```
┌─────────────────────────────────────────────────────────────────────────────┐
│              EXACTLY-ONCE PIPELINE (PRODUCTION-READY)                        │
│                                                                             │
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐  │
│  │  Producer   │───▶│    Kafka    │───▶│    Spark    │───▶│   Iceberg   │  │
│  │             │    │             │    │             │    │             │  │
│  │ idempotent  │    │ replicated  │    │ checkpointed│    │   atomic    │  │
│  │ acks=all    │    │ min.isr=2   │    │             │    │   commits   │  │
│  └─────────────┘    └─────────────┘    └─────────────┘    └─────────────┘  │
│                                                                             │
│  Guarantee: EXACTLY-ONCE end-to-end                                         │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Common Exactly-Once Pitfalls

### 1. External Side Effects

```python
# BAD: External API call in streaming
def process_row(row):
    send_email(row)  # ← Not idempotent! May send duplicates
    return row

# GOOD: Make side effects idempotent
def process_row(row):
    # Use unique ID to deduplicate
    if not already_sent(row.id):
        send_email(row)
    return row
```

### 2. Non-Idempotent Aggregations

```python
# BAD: Running counter
df.writeStream \
    .foreachBatch(lambda df, id: 
        update_counter(df.count())  # ← Counter may be updated twice
    )

# GOOD: Use Iceberg's atomic writes
df.writeStream \
    .format("iceberg") \
    .toTable("counts")  # ← Atomic, can be re-processed
```

### 3. Shared Mutable State

```python
# BAD: Global mutable state
total = 0
def process(df):
    global total
    total += df.count()  # ← State may be corrupted on retry

# GOOD: Use DataFrame operations
df.groupBy().count()  # ← Computed fresh each time
```

---

## Verification Exercises

### Exercise 1: Test Recovery

```python
# 1. Start streaming job
# 2. Send 10 messages
# 3. Kill Spark (Ctrl+C)
# 4. Restart streaming job
# 5. Verify:
spark.sql("SELECT COUNT(*) FROM local.default.crypto_prices").show()
# Should match exactly 10 (no duplicates, no loss)
```

### Exercise 2: Check Offsets

```python
# View checkpoint offsets
import json
import os

checkpoint_path = "/opt/warehouse/checkpoint/prices/offsets"
for filename in sorted(os.listdir(checkpoint_path)):
    with open(os.path.join(checkpoint_path, filename)) as f:
        print(f"Batch {filename}: {f.read()}")
```

### Exercise 3: Simulate Failure

```python
# Add artificial failure
def failing_write(df, batch_id):
    if batch_id == 3:
        raise Exception("Simulated failure!")
    df.writeTo("local.default.crypto_prices").append()

# The job will retry batch 3 and eventually succeed
# Final count will be correct
```

---

Next: [12-scaling-and-performance.md](12-scaling-and-performance.md) - Scaling your pipeline

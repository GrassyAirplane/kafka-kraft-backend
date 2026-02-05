# Fault Tolerance and Recovery

This document covers how your pipeline handles failures and recovers gracefully.

---

## Failure Types

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        FAILURE CATEGORIES                                    │
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │  TRANSIENT FAILURES                                                  │   │
│  │  • Network timeouts                                                  │   │
│  │  • Temporary broker unavailability                                   │   │
│  │  • GC pauses                                                         │   │
│  │  → Automatic retry usually succeeds                                  │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │  RECOVERABLE FAILURES                                                │   │
│  │  • Broker crash (with replication)                                   │   │
│  │  • Executor crash                                                    │   │
│  │  • Out of memory (restart with more)                                 │   │
│  │  → Restart component, state preserved                                │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │  DATA LOSS FAILURES                                                  │   │
│  │  • All replicas lost                                                 │   │
│  │  • Checkpoint corruption                                             │   │
│  │  • Storage failure                                                   │   │
│  │  → Requires manual intervention                                      │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Kafka Fault Tolerance

### Broker Failure (Single Node - Development)

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                SINGLE BROKER FAILURE (DEV)                                   │
│                                                                             │
│  Before:                              After broker crash:                   │
│  ┌─────────────────┐                  ┌─────────────────┐                  │
│  │  Kafka Broker   │                  │  Kafka Broker   │                  │
│  │                 │                  │       ❌        │                  │
│  │  Partition 0    │                  │                 │                  │
│  │   offset 0-100  │                  │  DATA LOST      │                  │
│  └─────────────────┘                  └─────────────────┘                  │
│                                                                             │
│  Recovery: None without replication                                         │
│  Mitigation: Use replication in production                                  │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Broker Failure (Multi-Node - Production)

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                REPLICATED BROKER FAILURE (PROD)                              │
│                                                                             │
│  Before:                                                                    │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐                         │
│  │  Broker 1   │  │  Broker 2   │  │  Broker 3   │                         │
│  │  (Leader)   │  │  (Follower) │  │  (Follower) │                         │
│  │  Part 0     │  │  Part 0     │  │  Part 0     │                         │
│  │  0-100      │  │  0-100      │  │  0-100      │                         │
│  └─────────────┘  └─────────────┘  └─────────────┘                         │
│                                                                             │
│  After Broker 1 crash:                                                      │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐                         │
│  │  Broker 1   │  │  Broker 2   │  │  Broker 3   │                         │
│  │     ❌      │  │  (LEADER)   │  │  (Follower) │                         │
│  │             │  │  Part 0     │  │  Part 0     │                         │
│  │             │  │  0-100      │  │  0-100      │                         │
│  └─────────────┘  └─────────────┘  └─────────────┘                         │
│                       ▲                                                     │
│                   Automatic                                                 │
│                   leader election                                           │
│                                                                             │
│  Recovery: Automatic leader election, no data loss                          │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Kafka Recovery Configuration

```yaml
# docker-compose.yml - Production settings
environment:
  # Replication
  KAFKA_DEFAULT_REPLICATION_FACTOR: 3
  KAFKA_MIN_INSYNC_REPLICAS: 2
  
  # Unclean leader election (set to false for no data loss)
  KAFKA_UNCLEAN_LEADER_ELECTION_ENABLE: "false"
  
  # Log retention (keep data for recovery)
  KAFKA_LOG_RETENTION_HOURS: 168  # 7 days
  KAFKA_LOG_RETENTION_BYTES: 10737418240  # 10GB
```

---

## Spark Streaming Fault Tolerance

### Driver Failure

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    DRIVER FAILURE RECOVERY                                   │
│                                                                             │
│  1. Driver crashes during batch 5                                           │
│     ┌─────────────────────────────────────────────────────────────────┐    │
│     │  checkpoint/                                                     │    │
│     │  ├── commits/                                                    │    │
│     │  │   ├── 0, 1, 2, 3, 4  ← Completed batches                     │    │
│     │  │   └── (5 not present) ← In progress when crash               │    │
│     │  └── offsets/                                                    │    │
│     │      ├── 0, 1, 2, 3, 4                                          │    │
│     │      └── 5              ← Offsets for batch 5                   │    │
│     └─────────────────────────────────────────────────────────────────┘    │
│                                                                             │
│  2. Driver restarts                                                         │
│     • Reads checkpoint                                                      │
│     • Sees batch 4 committed, batch 5 not committed                        │
│     • Restarts batch 5 from same offsets                                    │
│                                                                             │
│  3. Result: Batch 5 processed exactly once                                  │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Executor Failure

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    EXECUTOR FAILURE RECOVERY                                 │
│                                                                             │
│  Executor 2 crashes while processing partition 1:                           │
│                                                                             │
│  ┌───────────┐  ┌───────────┐  ┌───────────┐                               │
│  │ Executor 1│  │ Executor 2│  │ Executor 3│                               │
│  │ Part 0 ✓  │  │ Part 1 ❌ │  │ Part 2 ✓  │                               │
│  └───────────┘  └───────────┘  └───────────┘                               │
│                       │                                                     │
│                       ▼                                                     │
│  Driver detects failure, reschedules task:                                  │
│                                                                             │
│  ┌───────────┐  ┌───────────┐  ┌───────────┐                               │
│  │ Executor 1│  │ Executor 2│  │ Executor 3│                               │
│  │ Part 0 ✓  │  │ (new)     │  │ Part 2 ✓  │                               │
│  │ Part 1... │  │           │  │           │                               │
│  └───────────┘  └───────────┘  └───────────┘                               │
│                                                                             │
│  Task re-executed on Executor 1                                             │
│  No data loss, batch completes                                              │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Spark Fault Tolerance Settings

```python
# Enable checkpointing (required)
query = df.writeStream \
    .option("checkpointLocation", "/opt/warehouse/checkpoint/prices")

# Configure retries
spark.conf.set("spark.task.maxFailures", "4")
spark.conf.set("spark.streaming.backpressure.enabled", "true")
spark.conf.set("spark.streaming.kafka.maxRetries", "3")
```

---

## Iceberg Fault Tolerance

### Write Failure

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    ICEBERG WRITE FAILURE                                     │
│                                                                             │
│  Scenario: Spark writes file but crashes before commit                      │
│                                                                             │
│  State before crash:                                                        │
│  ├── metadata/                                                              │
│  │   └── v5.metadata.json  ← Current snapshot                              │
│  └── data/                                                                  │
│      ├── file1.parquet     ← Committed                                     │
│      ├── file2.parquet     ← Committed                                     │
│      └── file3.parquet     ← ORPHAN (written but not committed)            │
│                                                                             │
│  After restart:                                                             │
│  • Spark restarts batch from checkpoint                                     │
│  • Writes new file (file4.parquet)                                         │
│  • Commits successfully                                                     │
│  • file3.parquet remains orphan (cleanup later)                            │
│                                                                             │
│  Cleanup orphans:                                                           │
│  CALL local.system.remove_orphan_files('default.crypto_prices')            │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Commit Conflict

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    COMMIT CONFLICT RESOLUTION                                │
│                                                                             │
│  Two jobs try to commit simultaneously:                                     │
│                                                                             │
│  Job A                                 Job B                                │
│  ──────                                ──────                                │
│  Read v5.metadata.json                 Read v5.metadata.json                │
│       │                                     │                               │
│       ▼                                     ▼                               │
│  Write data1.parquet                   Write data2.parquet                  │
│       │                                     │                               │
│       ▼                                     │                               │
│  Commit v6 (SUCCESS)                        │                               │
│       │                                     ▼                               │
│       │                              Commit v6 (CONFLICT!)                  │
│       │                                     │                               │
│       │                                     ▼                               │
│       │                              Retry:                                 │
│       │                              • Read v6.metadata.json                │
│       │                              • Rebase changes                       │
│       │                              • Commit v7 (SUCCESS)                  │
│                                                                             │
│  Result: Both commits succeed, no data loss                                 │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Time Travel Recovery

```python
# If bad data was written, roll back to previous snapshot
# Find good snapshot
spark.sql("SELECT * FROM local.default.crypto_prices.history").show()

# Roll back
spark.sql("""
    CALL local.system.rollback_to_snapshot(
        'default.crypto_prices',
        snapshot_id_before_bad_data
    )
""")
```

---

## Producer Fault Tolerance

### Retry Configuration

```python
from kafka import KafkaProducer
from kafka.errors import KafkaError

producer = KafkaProducer(
    bootstrap_servers="localhost:29092",
    
    # Retry settings
    retries=3,
    retry_backoff_ms=100,
    
    # Delivery guarantees
    acks='all',
    enable_idempotence=True,
    
    # Timeout settings
    request_timeout_ms=30000,
    max_block_ms=60000,
)
```

### Handling Send Failures

```python
def send_with_retry(producer, topic, message, max_retries=3):
    """Send message with manual retry logic"""
    for attempt in range(max_retries):
        try:
            future = producer.send(topic, message)
            record_metadata = future.get(timeout=10)
            print(f"Sent to {record_metadata.partition}@{record_metadata.offset}")
            return True
        except KafkaError as e:
            print(f"Attempt {attempt + 1} failed: {e}")
            if attempt == max_retries - 1:
                # Log to dead letter queue or file
                log_failed_message(message, e)
                return False
            time.sleep(2 ** attempt)  # Exponential backoff
```

---

## End-to-End Failure Scenarios

### Scenario 1: Network Partition

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    NETWORK PARTITION                                         │
│                                                                             │
│  Producer  ──── ✂ ────  Kafka  ──── ✂ ────  Spark                          │
│                                                                             │
│  Producer behavior:                                                         │
│  • Retries fail, messages buffered                                          │
│  • Buffer fills, throws BufferExhausted                                     │
│  • Application should handle exception                                      │
│                                                                             │
│  Spark behavior:                                                            │
│  • Cannot read new messages                                                 │
│  • Streaming query blocks, waiting                                          │
│  • When network restored, continues from checkpoint                         │
│                                                                             │
│  Recovery:                                                                   │
│  • Automatic when network restored                                          │
│  • Producer may have lost buffered messages                                 │
│  • Spark resumes exactly where it left off                                  │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Scenario 2: Kafka Disk Full

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    DISK FULL                                                 │
│                                                                             │
│  Symptom: Producer send fails, Kafka rejects messages                       │
│                                                                             │
│  Resolution:                                                                │
│  1. Increase disk space                                                     │
│  2. Or reduce retention:                                                    │
│                                                                             │
│  docker exec -it kafka kafka-configs \                                      │
│    --bootstrap-server localhost:9092 \                                      │
│    --alter \                                                                │
│    --entity-type topics \                                                   │
│    --entity-name prices \                                                   │
│    --add-config retention.ms=3600000  # 1 hour                             │
│                                                                             │
│  Prevention:                                                                │
│  • Set log.retention.bytes                                                  │
│  • Monitor disk usage                                                       │
│  • Auto-scale storage                                                       │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Scenario 3: Checkpoint Corruption

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    CHECKPOINT CORRUPTION                                     │
│                                                                             │
│  Symptom: Spark fails to start with checkpoint error                        │
│                                                                             │
│  Options:                                                                   │
│                                                                             │
│  Option A: Start fresh (lose progress)                                      │
│  ────────────────────────────────────                                       │
│  rm -rf /opt/warehouse/checkpoint/prices                                    │
│  # Restart with startingOffsets="earliest" or "latest"                      │
│                                                                             │
│  Option B: Start from specific offset                                       │
│  ──────────────────────────────────────                                     │
│  df = spark.readStream \                                                    │
│      .option("startingOffsets", '{"prices":{"0":1000}}') \                  │
│      .option("checkpointLocation", "/new/checkpoint")                       │
│                                                                             │
│  Option C: Repair checkpoint (advanced)                                     │
│  ─────────────────────────────────────                                      │
│  # Manually edit offset files if you know the correct state                 │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Monitoring and Alerting

### Key Metrics to Monitor

```yaml
# Prometheus/Grafana metrics to track

# Kafka
- kafka_consumer_lag              # Alert if > 10000
- kafka_isr_shrink_rate          # Alert if > 0
- kafka_under_replicated_partitions  # Alert if > 0

# Spark
- spark_streaming_lastProgress_inputRowsPerSecond
- spark_streaming_lastProgress_processedRowsPerSecond
- spark_streaming_lastProgress_batchDuration

# Iceberg
- iceberg_table_snapshot_count   # Alert if too many (needs compaction)
- iceberg_table_data_files       # Monitor file count
```

### Health Check Script

```python
# health_check.py
import subprocess
import sys

def check_kafka():
    """Check if Kafka is responsive"""
    result = subprocess.run(
        ["docker", "exec", "kafka", "kafka-broker-api-versions",
         "--bootstrap-server", "localhost:9092"],
        capture_output=True
    )
    return result.returncode == 0

def check_spark_streaming():
    """Check if streaming job is running"""
    # Check if process is running
    result = subprocess.run(
        ["docker", "exec", "spark", "pgrep", "-f", "spark-submit"],
        capture_output=True
    )
    return result.returncode == 0

def check_iceberg():
    """Check if Iceberg table is accessible"""
    # Try to read table metadata
    # Implementation depends on your setup
    pass

if __name__ == "__main__":
    checks = {
        "Kafka": check_kafka(),
        "Spark": check_spark_streaming(),
    }
    
    for name, status in checks.items():
        print(f"{name}: {'✓' if status else '✗'}")
    
    sys.exit(0 if all(checks.values()) else 1)
```

---

## Recovery Runbook

### Full System Recovery

```powershell
# 1. Start Kafka
docker compose up -d kafka
# Wait for Kafka to be healthy
docker compose logs -f kafka | Select-String "started"

# 2. Verify topic exists
docker exec -it kafka kafka-topics --list --bootstrap-server localhost:9092

# 3. Check consumer lag
docker exec -it kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --all-groups \
  --describe

# 4. Start Spark
docker compose up -d spark

# 5. Start streaming job
docker exec -it spark spark-submit \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2 \
  /opt/spark-apps/stream_to_iceberg.py

# 6. Verify data flowing
docker exec -it spark spark-sql \
  --conf "spark.sql.catalog.local=..." \
  -e "SELECT COUNT(*) FROM local.default.crypto_prices"

# 7. Start producer
uv run python producer/api_producer.py
```

---

## Exercises

### Exercise 1: Simulate Kafka Failure

```powershell
# 1. Start full stack
# 2. Stop Kafka while producer is running
docker stop kafka

# 3. Observe producer errors
# 4. Restart Kafka
docker start kafka

# 5. Verify recovery
```

### Exercise 2: Simulate Spark Failure

```powershell
# 1. Start streaming job
# 2. Send 10 messages
# 3. Kill Spark container
docker kill spark

# 4. Restart Spark and streaming job
docker start spark
# Run streaming job again

# 5. Verify no duplicates
docker exec -it spark spark-sql -e "SELECT COUNT(*) FROM local.default.crypto_prices"
```

### Exercise 3: Corrupt Checkpoint

```powershell
# 1. Stop streaming job
# 2. Delete a commit file
rm warehouse/checkpoint/prices/commits/2

# 3. Restart streaming job
# 4. Observe error and recovery options
```

---

Next: [14-production-checklist.md](14-production-checklist.md) - Production deployment guide

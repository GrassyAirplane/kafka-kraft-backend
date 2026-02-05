# Scaling and Performance Tuning

This document covers how to scale your Kafka → Spark → Iceberg pipeline for production workloads.

---

## Scaling Dimensions

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                       SCALING DIMENSIONS                                     │
│                                                                             │
│  ┌───────────────┐  ┌───────────────┐  ┌───────────────┐                   │
│  │   THROUGHPUT  │  │    LATENCY    │  │   STORAGE     │                   │
│  │               │  │               │  │               │                   │
│  │ Messages/sec  │  │ End-to-end    │  │ Data volume   │                   │
│  │ MB/sec        │  │ processing    │  │ retention     │                   │
│  │               │  │ time          │  │               │                   │
│  └───────────────┘  └───────────────┘  └───────────────┘                   │
│                                                                             │
│  Scaling strategies:                                                        │
│  • Horizontal: Add more instances                                           │
│  • Vertical: Bigger instances                                               │
│  • Partitioning: Parallel processing                                        │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Kafka Scaling

### Partition Strategy

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    KAFKA PARTITION SCALING                                   │
│                                                                             │
│  Single Partition (Current):           Multiple Partitions (Production):    │
│                                                                             │
│  Topic: prices                         Topic: prices                        │
│  ┌─────────────────────┐               ┌─────────────────────┐              │
│  │    Partition 0      │               │    Partition 0      │              │
│  │                     │               │    Partition 1      │              │
│  │  msg1, msg2, msg3   │               │    Partition 2      │              │
│  │  msg4, msg5, msg6   │               │    Partition 3      │              │
│  │        ▼            │               │    Partition 4      │              │
│  │  1 consumer max     │               │    Partition 5      │              │
│  └─────────────────────┘               └─────────────────────┘              │
│                                               ▼                             │
│  Throughput: ~10K msg/sec              6 consumers in parallel              │
│                                        Throughput: ~60K msg/sec             │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Creating Partitioned Topic

```bash
# Create topic with 6 partitions
docker exec -it kafka kafka-topics \
  --bootstrap-server localhost:9092 \
  --create \
  --topic prices \
  --partitions 6 \
  --replication-factor 1

# Check partition count
docker exec -it kafka kafka-topics \
  --bootstrap-server localhost:9092 \
  --describe \
  --topic prices
```

### Partition Key Strategy

```python
# Distribute by cryptocurrency
producer.send(
    "prices",
    key=b"BTC",  # All BTC to same partition
    value=event
)

# Partition assignment:
# hash("BTC") % 6 = partition 2
# hash("ETH") % 6 = partition 5
# hash("SOL") % 6 = partition 0
```

### Multi-Broker Setup

```yaml
# docker-compose.yml (3-broker cluster)
services:
  kafka1:
    image: confluentinc/cp-kafka:7.6.0
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka1:9093,2@kafka2:9093,3@kafka3:9093
      # ... other configs
  
  kafka2:
    image: confluentinc/cp-kafka:7.6.0
    environment:
      KAFKA_NODE_ID: 2
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka1:9093,2@kafka2:9093,3@kafka3:9093
      # ... other configs
  
  kafka3:
    image: confluentinc/cp-kafka:7.6.0
    environment:
      KAFKA_NODE_ID: 3
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka1:9093,2@kafka2:9093,3@kafka3:9093
      # ... other configs
```

---

## Spark Scaling

### Executor Configuration

```python
# Local mode (current - single machine)
spark = SparkSession.builder \
    .master("local[*]") \  # Use all CPU cores
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

# Cluster mode (production)
spark = SparkSession.builder \
    .master("spark://spark-master:7077") \
    .config("spark.executor.instances", "6") \
    .config("spark.executor.cores", "4") \
    .config("spark.executor.memory", "8g") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()
```

### Resource Allocation

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    SPARK CLUSTER RESOURCES                                   │
│                                                                             │
│  Total: 6 executors × 4 cores × 8GB = 24 cores, 48GB                        │
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │  Spark Driver (4GB)                                                  │   │
│  │  • Coordinates execution                                             │   │
│  │  • Tracks offsets                                                    │   │
│  │  • Manages checkpoints                                               │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                              │                                              │
│              ┌───────────────┼───────────────┐                             │
│              ▼               ▼               ▼                             │
│  ┌─────────────────┐ ┌─────────────────┐ ┌─────────────────┐              │
│  │  Executor 1     │ │  Executor 2     │ │  Executor 3     │ ...          │
│  │  4 cores, 8GB   │ │  4 cores, 8GB   │ │  4 cores, 8GB   │              │
│  │                 │ │                 │ │                 │              │
│  │  ┌───────────┐  │ │  ┌───────────┐  │ │  ┌───────────┐  │              │
│  │  │ Task 1    │  │ │  │ Task 3    │  │ │  │ Task 5    │  │              │
│  │  │ Task 2    │  │ │  │ Task 4    │  │ │  │ Task 6    │  │              │
│  │  └───────────┘  │ │  └───────────┘  │ │  └───────────┘  │              │
│  └─────────────────┘ └─────────────────┘ └─────────────────┘              │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Parallelism Tuning

```python
# Match parallelism to Kafka partitions
spark.conf.set("spark.sql.shuffle.partitions", "6")  # = num Kafka partitions
spark.conf.set("spark.default.parallelism", "24")    # = total executor cores

# Control batch size
df = spark.readStream \
    .format("kafka") \
    .option("maxOffsetsPerTrigger", 100000)  # Max messages per batch
```

### Memory Tuning

```python
# Prevent OOM during large batches
spark.conf.set("spark.sql.streaming.stateStore.maintenance.interval", "30s")
spark.conf.set("spark.sql.streaming.metricsEnabled", "true")

# For heavy transformations
spark.conf.set("spark.memory.fraction", "0.8")       # % for execution/storage
spark.conf.set("spark.memory.storageFraction", "0.5") # % of above for caching
```

---

## Iceberg Scaling

### Partitioning Strategies

```sql
-- Partition by day (time-series data)
CREATE TABLE local.default.crypto_prices (
    timestamp STRING,
    symbol STRING,
    usd_price DOUBLE
) USING iceberg
PARTITIONED BY (days(timestamp))

-- Partition by symbol + day (multi-dimension)
PARTITIONED BY (symbol, days(timestamp))

-- Bucket partitioning (high cardinality)
PARTITIONED BY (bucket(16, user_id))
```

### Partition Benefits

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    PARTITION PRUNING                                         │
│                                                                             │
│  Without partitions:                    With partitions:                    │
│                                                                             │
│  Query: WHERE timestamp > '2026-02-01'  Query: WHERE timestamp > '2026-02-01'│
│                                                                             │
│  Scan: ALL files                        Scan: Only Feb files                │
│  ┌─────┐ ┌─────┐ ┌─────┐ ┌─────┐       ┌─────┐ ┌─────┐                     │
│  │ Jan │ │ Feb │ │ Mar │ │ Apr │       │ Feb │ │ Mar │  ← Skip Jan, Apr    │
│  └─────┘ └─────┘ └─────┘ └─────┘       └─────┘ └─────┘                     │
│     ▲       ▲       ▲       ▲             ▲       ▲                         │
│  Read all 4 files                      Read only 2 files                    │
│                                                                             │
│  I/O: 4 files × 100MB = 400MB          I/O: 2 files × 100MB = 200MB         │
│                                        Savings: 50%                          │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### File Size Optimization

```sql
-- Configure target file sizes
ALTER TABLE local.default.crypto_prices SET TBLPROPERTIES (
    'write.target-file-size-bytes' = '134217728',     -- 128MB
    'write.distribution-mode' = 'hash',               -- Distribute writes
    'write.parquet.compression-codec' = 'zstd'        -- Better compression
);
```

### Compaction

```sql
-- Rewrite small files into larger ones
CALL local.system.rewrite_data_files(
    table => 'default.crypto_prices',
    options => map('target-file-size-bytes', '134217728')
);

-- Remove old snapshots
CALL local.system.expire_snapshots(
    table => 'default.crypto_prices',
    older_than => TIMESTAMP '2026-01-01 00:00:00',
    retain_last => 5
);

-- Remove orphan files
CALL local.system.remove_orphan_files(
    table => 'default.crypto_prices'
);
```

---

## Performance Monitoring

### Kafka Metrics

```bash
# Consumer lag (how far behind)
docker exec -it kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --group spark-streaming \
  --describe

# Topic throughput
docker exec -it kafka kafka-run-class kafka.tools.JmxTool \
  --object-name kafka.server:type=BrokerTopicMetrics,name=MessagesInPerSec \
  --jmx-url service:jmx:rmi:///jndi/rmi://localhost:9999/jmxrmi
```

### Spark Streaming Metrics

```python
# Enable metrics
spark.conf.set("spark.sql.streaming.metricsEnabled", "true")

# Check progress
query = df.writeStream...

# Progress report
print(query.lastProgress)
"""
{
  "id": "abc123",
  "batchId": 42,
  "numInputRows": 1000,
  "inputRowsPerSecond": 100.0,
  "processedRowsPerSecond": 500.0,
  "durationMs": {
    "addBatch": 1500,
    "commitOffsets": 50,
    "getBatch": 200,
    "latestOffset": 10,
    "queryPlanning": 100,
    "triggerExecution": 2000,
    "walCommit": 50
  },
  "sources": [{
    "numInputRows": 1000,
    "inputRowsPerSecond": 100.0,
    "startOffset": {"prices": {"0": 5000}},
    "endOffset": {"prices": {"0": 6000}}
  }]
}
"""
```

### Key Metrics to Monitor

| Metric | Target | Action if Exceeded |
|--------|--------|-------------------|
| Consumer Lag | < 10,000 | Add partitions or executors |
| inputRowsPerSecond | Matches source rate | Verify producer throughput |
| processedRowsPerSecond | > inputRowsPerSecond | Increase resources |
| batchDuration | < trigger interval | Optimize transformations |

---

## Bottleneck Analysis

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    BOTTLENECK IDENTIFICATION                                 │
│                                                                             │
│  Symptom: processedRowsPerSecond < inputRowsPerSecond                       │
│                                                                             │
│  1. Check Kafka read time                                                   │
│     └── High? → Add partitions, increase maxOffsetsPerTrigger               │
│                                                                             │
│  2. Check transformation time                                               │
│     └── High? → Optimize UDFs, use built-in functions                       │
│                                                                             │
│  3. Check write time (addBatch)                                             │
│     └── High? → Partition Iceberg table, increase file size                 │
│                                                                             │
│  4. Check GC time                                                           │
│     └── High? → Increase executor memory, tune GC                           │
│                                                                             │
│  5. Check shuffle                                                           │
│     └── High? → Adjust spark.sql.shuffle.partitions                         │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Production Configuration Example

```yaml
# docker-compose.prod.yml
services:
  kafka1:
    image: confluentinc/cp-kafka:7.6.0
    environment:
      KAFKA_HEAP_OPTS: "-Xmx4g -Xms4g"
      KAFKA_NUM_PARTITIONS: 12
      KAFKA_DEFAULT_REPLICATION_FACTOR: 3
      KAFKA_MIN_INSYNC_REPLICAS: 2
    deploy:
      resources:
        limits:
          memory: 6g
          cpus: '2'

  spark-master:
    image: custom-spark:3.5.1
    environment:
      SPARK_MASTER_MEMORY: 4g
      SPARK_WORKER_MEMORY: 8g
      SPARK_WORKER_CORES: 4

  spark-worker:
    image: custom-spark:3.5.1
    deploy:
      replicas: 6
      resources:
        limits:
          memory: 10g
          cpus: '4'
```

---

## Scaling Decision Matrix

| Data Volume | Kafka | Spark | Iceberg |
|-------------|-------|-------|---------|
| < 1K msg/sec | 1 partition | Local mode | No partitioning |
| 1K-10K msg/sec | 3 partitions | 2 executors | Daily partitions |
| 10K-100K msg/sec | 12 partitions | 6 executors | Hourly partitions |
| > 100K msg/sec | 24+ partitions | 12+ executors | Sub-hourly + bucketing |

---

## Exercises

### Exercise 1: Measure Current Performance

```python
import time

start_time = time.time()
# Run 100 messages through pipeline
time.sleep(30)
end_time = time.time()

print(f"Throughput: {100 / (end_time - start_time):.2f} msg/sec")
```

### Exercise 2: Add Partitions

```bash
# Increase partitions
docker exec -it kafka kafka-topics \
  --bootstrap-server localhost:9092 \
  --alter \
  --topic prices \
  --partitions 3

# Verify
docker exec -it kafka kafka-topics \
  --bootstrap-server localhost:9092 \
  --describe \
  --topic prices
```

### Exercise 3: Monitor Batch Duration

```python
import time

while query.isActive:
    if query.lastProgress:
        duration = query.lastProgress.get('durationMs', {})
        print(f"""
        Batch {query.lastProgress['batchId']}:
        - Total: {duration.get('triggerExecution', 0)}ms
        - Read: {duration.get('getBatch', 0)}ms  
        - Write: {duration.get('addBatch', 0)}ms
        - Commit: {duration.get('commitOffsets', 0)}ms
        """)
    time.sleep(10)
```

---

Next: [13-fault-tolerance.md](13-fault-tolerance.md) - Handling failures gracefully

# Data Flow: Kafka to Spark Streaming

This document explains how Apache Spark consumes messages from Kafka using Structured Streaming.

---

## The Complete Flow

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                        KAFKA TO SPARK FLOW                                       │
│                                                                                 │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐    ┌───────────────┐ │
│  │   Kafka      │    │   Spark      │    │   DataFrame  │    │    Parsed     │ │
│  │   Topic      │───▶│   Consumer   │───▶│   (Binary)   │───▶│    DataFrame  │ │
│  └──────────────┘    └──────────────┘    └──────────────┘    └───────────────┘ │
│                                                                                 │
│    Partitioned        Micro-batch         Raw bytes           Typed columns     │
│    Log                 polling           from Kafka          (timestamp, usd)   │
│                                                                                 │
└─────────────────────────────────────────────────────────────────────────────────┘
```

---

## Step 1: SparkSession Initialization

```python
spark = SparkSession.builder \
    .appName("CryptoToIceberg") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.local.type", "hadoop") \
    .config("spark.sql.catalog.local.warehouse", "/opt/warehouse/iceberg") \
    .getOrCreate()
```

### What Happens at Startup

```
1. JVM initializes Spark context
   ▼
2. Spark loads Iceberg extension classes
   │   IcebergSparkSessionExtensions adds:
   │   • Custom SQL syntax (CALL procedures)
   │   • Time travel syntax (AS OF)
   │   • Metadata table access
   ▼
3. Spark registers "local" catalog
   │   SparkCatalog wraps HadoopCatalog
   │   Points to /opt/warehouse/iceberg
   ▼
4. SparkSession ready for queries
```

---

## Step 2: Create Streaming Source

```python
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "prices") \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .load()
```

### Configuration Breakdown

| Option | Value | Purpose |
|--------|-------|---------|
| `format("kafka")` | - | Use Kafka data source |
| `kafka.bootstrap.servers` | kafka:9092 | Broker address (Docker internal) |
| `subscribe` | prices | Topic to consume |
| `startingOffsets` | earliest | Where to start reading |
| `failOnDataLoss` | false | Don't fail if data deleted |

### Why kafka:9092?

```
┌────────────────────────────────────────────────────────────────┐
│                     DOCKER NETWORK                              │
│                                                                │
│  ┌─────────────────┐         ┌─────────────────┐              │
│  │  Spark          │────────▶│  Kafka          │              │
│  │  Container      │  kafka:9092  │  Container      │              │
│  └─────────────────┘         └─────────────────┘              │
│                                     │                          │
│                                     │                          │
└─────────────────────────────────────│──────────────────────────┘
                                      │
                            localhost:29092
                                      │
                              ┌───────▼───────┐
                              │  Host Machine │
                              │  (Producer)   │
                              └───────────────┘
```

- Spark is **inside** Docker → uses `kafka:9092`
- Producer is **outside** Docker → uses `localhost:29092`

---

## Step 3: What readStream Returns

```
┌────────────────────────────────────────────────────────────────────────────────┐
│                      RAW KAFKA DATAFRAME SCHEMA                                 │
│                                                                                │
│  df.printSchema():                                                             │
│                                                                                │
│  root                                                                          │
│   |-- key: binary (nullable = true)           ← Message key (null in our case)│
│   |-- value: binary (nullable = true)         ← JSON payload as bytes         │
│   |-- topic: string (nullable = true)         ← "prices"                       │
│   |-- partition: integer (nullable = true)    ← 0                              │
│   |-- offset: long (nullable = true)          ← 0, 1, 2, 3...                  │
│   |-- timestamp: timestamp (nullable = true)  ← Kafka ingestion time           │
│   |-- timestampType: integer (nullable = true)← 0 = CreateTime                 │
│                                                                                │
└────────────────────────────────────────────────────────────────────────────────┘
```

---

## Step 4: Parse JSON Values

```python
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

schema = StructType([
    StructField("timestamp", StringType(), True),
    StructField("usd_price", DoubleType(), True),
])

parsed_df = df.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.timestamp", "data.usd_price")
```

### Transformation Flow

```
Step 1: col("value")
  Binary: b'{"timestamp":"2026-02-04T12:21:00Z","usd_price":76123.0}'

Step 2: .cast("string")  
  String: '{"timestamp":"2026-02-04T12:21:00Z","usd_price":76123.0}'

Step 3: from_json(..., schema)
  Struct: Row(timestamp='2026-02-04T12:21:00Z', usd_price=76123.0)

Step 4: .select("data.timestamp", "data.usd_price")
  Columns: |timestamp             |usd_price|
           |2026-02-04T12:21:00Z  |76123.0  |
```

---

## Step 5: Structured Streaming Execution Model

### Micro-batch Processing

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    MICRO-BATCH PROCESSING MODEL                              │
│                                                                             │
│  Time: ───────────────────────────────────────────────────────────────────▶ │
│                                                                             │
│        Batch 0        Batch 1        Batch 2        Batch 3                 │
│        ┌────┐         ┌────┐         ┌────┐         ┌────┐                  │
│  Kafka │msg1│         │msg5│         │msg8│         │msg12│                 │
│  Data  │msg2│         │msg6│         │msg9│         │msg13│                 │
│        │msg3│         │msg7│         │msg10│        │msg14│                 │
│        │msg4│         │    │         │msg11│        │    │                  │
│        └────┘         └────┘         └────┘         └────┘                  │
│           │              │              │              │                     │
│           ▼              ▼              ▼              ▼                     │
│        ┌────┐         ┌────┐         ┌────┐         ┌────┐                  │
│  Spark │    │         │    │         │    │         │    │                  │
│  Job   │Parse│         │Parse│         │Parse│         │Parse│                  │
│        │Write│         │Write│         │Write│         │Write│                  │
│        └────┘         └────┘         └────┘         └────┘                  │
│           │              │              │              │                     │
│           ▼              ▼              ▼              ▼                     │
│        ┌────┐         ┌────┐         ┌────┐         ┌────┐                  │
│  Iceberg│snap │         │snap │         │snap │         │snap │                  │
│        │  0 │         │  1 │         │  2 │         │  3 │                  │
│        └────┘         └────┘         └────┘         └────┘                  │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Trigger Interval

```python
# Your configuration: process every 10 seconds
.trigger(processingTime="10 seconds")

# Other options:
.trigger(once=True)                    # Process all available, then stop
.trigger(processingTime="0 seconds")   # As fast as possible
.trigger(continuous="1 second")        # Continuous processing mode
```

---

## Step 6: Offset Tracking

Spark tracks which offsets it has processed:

```
┌────────────────────────────────────────────────────────────────────────────┐
│                          OFFSET TRACKING                                    │
│                                                                            │
│  Checkpoint Location: /opt/warehouse/checkpoint/prices                     │
│                                                                            │
│  ├── commits/                    ← Completed batch markers                 │
│  │   ├── 0                       ← Batch 0 committed                       │
│  │   ├── 1                       ← Batch 1 committed                       │
│  │   └── 2                       ← Batch 2 committed                       │
│  │                                                                         │
│  ├── offsets/                    ← Starting offsets for each batch         │
│  │   ├── 0                       ← {"prices":{"0":0}}                      │
│  │   ├── 1                       ← {"prices":{"0":4}}                      │
│  │   └── 2                       ← {"prices":{"0":7}}                      │
│  │                                                                         │
│  ├── sources/                    ← Source-specific state                   │
│  │   └── 0/                                                                │
│  │                                                                         │
│  └── metadata                    ← Stream ID and version                   │
│                                                                            │
└────────────────────────────────────────────────────────────────────────────┘
```

### Offset File Content

```json
// offsets/2
{
  "batchWatermarkMs": 0,
  "batchTimestampMs": 1770207660000,
  "conf": {
    "spark.sql.streaming.stateStore.providerClass": "..."
  }
}
v1
{
  "prices": {
    "0": 7
  }
}
```

---

## Step 7: How Kafka Consumer Works Inside Spark

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    SPARK KAFKA CONSUMER INTERNALS                            │
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │  Spark Driver                                                        │   │
│  │                                                                      │   │
│  │  1. Query Kafka for partition metadata                               │   │
│  │  2. Calculate offset ranges per partition                            │   │
│  │  3. Create tasks (1 per partition-range)                             │   │
│  │  4. Assign tasks to executors                                        │   │
│  │                                                                      │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                              │                                              │
│              ┌───────────────┼───────────────┐                             │
│              ▼               ▼               ▼                             │
│  ┌─────────────────┐ ┌─────────────────┐ ┌─────────────────┐              │
│  │  Executor 1     │ │  Executor 2     │ │  Executor 3     │              │
│  │                 │ │                 │ │                 │              │
│  │  Task: Read     │ │  Task: Read     │ │  Task: Read     │              │
│  │  Partition 0    │ │  Partition 1    │ │  Partition 2    │              │
│  │  Offsets 0-10   │ │  Offsets 0-8    │ │  Offsets 0-12   │              │
│  │                 │ │                 │ │                 │              │
│  │  ┌───────────┐  │ │  ┌───────────┐  │ │  ┌───────────┐  │              │
│  │  │ Kafka     │  │ │  │ Kafka     │  │ │  │ Kafka     │  │              │
│  │  │ Consumer  │  │ │  │ Consumer  │  │ │  │ Consumer  │  │              │
│  │  └───────────┘  │ │  └───────────┘  │ │  └───────────┘  │              │
│  │                 │ │                 │ │                 │              │
│  └─────────────────┘ └─────────────────┘ └─────────────────┘              │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Step 8: Data Loss Prevention

### What is failOnDataLoss?

```python
.option("failOnDataLoss", "false")
```

**Data loss can happen when:**
1. Kafka retention deletes old messages
2. Topic is deleted and recreated
3. Offsets in checkpoint are no longer valid

**With failOnDataLoss=true (default):**
- Stream throws exception and stops
- Requires manual intervention

**With failOnDataLoss=false:**
- Stream logs warning and continues
- Skips to earliest available offset

---

## Step 9: Starting Offsets Strategies

```python
# From beginning of topic
.option("startingOffsets", "earliest")

# From end of topic (new messages only)
.option("startingOffsets", "latest")

# From specific offsets
.option("startingOffsets", '{"prices":{"0":100}}')
```

### First Run vs Restart

```
┌────────────────────────────────────────────────────────────────┐
│                  OFFSET RESOLUTION LOGIC                        │
│                                                                │
│  Start Stream                                                  │
│       │                                                        │
│       ▼                                                        │
│  ┌─────────────────────────┐                                   │
│  │ Check checkpoint exists? │                                   │
│  └─────────────────────────┘                                   │
│       │                                                        │
│       ├── YES ──▶ Resume from checkpointed offsets             │
│       │          (ignores startingOffsets option)              │
│       │                                                        │
│       └── NO ───▶ Use startingOffsets option                   │
│                   earliest = offset 0                          │
│                   latest = current end offset                  │
│                                                                │
└────────────────────────────────────────────────────────────────┘
```

---

## Common Kafka Source Options

| Option | Default | Description |
|--------|---------|-------------|
| `kafka.bootstrap.servers` | (required) | Broker list |
| `subscribe` | - | Topic to subscribe |
| `subscribePattern` | - | Regex for topics |
| `startingOffsets` | latest | earliest/latest/JSON |
| `endingOffsets` | - | For batch queries only |
| `maxOffsetsPerTrigger` | - | Rate limit |
| `minOffsetsPerTrigger` | - | Minimum per batch |
| `failOnDataLoss` | true | Fail on missing data |
| `kafka.group.id` | - | Consumer group (optional) |

---

## Debugging Kafka Streaming

### Check Current Offsets

```python
# In spark-shell
df.writeStream \
    .format("console") \
    .option("truncate", "false") \
    .start()
```

### View Raw Kafka Data

```python
# See all Kafka metadata columns
df.select("*").writeStream \
    .format("console") \
    .option("truncate", "false") \
    .start()
```

### Monitor Progress

```python
query = df.writeStream...

# In another thread/cell
print(query.lastProgress)  # Latest batch stats
print(query.status)        # Current status
```

---

## Your Streaming Query Summary

```python
# 1. Read from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "prices") \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .load()

# 2. Parse JSON
parsed_df = df.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.timestamp", "data.usd_price")

# 3. Write to Iceberg (covered in next doc)
```

---

## Exercises

### Exercise 1: Rate Limiting

Add rate limiting to control batch sizes:
```python
.option("maxOffsetsPerTrigger", 100)  # Max 100 messages per batch
```

### Exercise 2: Include Kafka Metadata

```python
parsed_df = df.select(
    col("partition"),
    col("offset"),
    col("timestamp").alias("kafka_timestamp"),
    from_json(col("value").cast("string"), schema).alias("data")
).select("partition", "offset", "kafka_timestamp", "data.*")
```

### Exercise 3: Debug to Console

```python
# Instead of writing to Iceberg, print to console
parsed_df.writeStream \
    .format("console") \
    .option("truncate", "false") \
    .trigger(processingTime="10 seconds") \
    .start() \
    .awaitTermination()
```

---

Next: [10-spark-to-iceberg.md](10-spark-to-iceberg.md) - How Spark writes to Iceberg tables

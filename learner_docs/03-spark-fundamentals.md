# Spark Fundamentals

## What is Apache Spark?

Apache Spark is a **unified analytics engine** for large-scale data processing. It can handle batch processing, streaming, machine learning, and graph processing.

```
┌─────────────────────────────────────────────────────────────────┐
│                      SPARK ECOSYSTEM                            │
│                                                                 │
│  ┌───────────┐ ┌───────────┐ ┌───────────┐ ┌───────────┐       │
│  │ Spark SQL │ │ Streaming │ │   MLlib   │ │  GraphX   │       │
│  │           │ │           │ │           │ │           │       │
│  └─────┬─────┘ └─────┬─────┘ └─────┬─────┘ └─────┬─────┘       │
│        │             │             │             │              │
│        └─────────────┴──────┬──────┴─────────────┘              │
│                             │                                   │
│                    ┌────────▼────────┐                          │
│                    │   Spark Core    │                          │
│                    │   (RDD API)     │                          │
│                    └────────┬────────┘                          │
│                             │                                   │
│        ┌────────────────────┼────────────────────┐              │
│        ▼                    ▼                    ▼              │
│   ┌─────────┐         ┌─────────┐          ┌─────────┐         │
│   │  Local  │         │  YARN   │          │  K8s    │         │
│   └─────────┘         └─────────┘          └─────────┘         │
└─────────────────────────────────────────────────────────────────┘
```

---

## Spark Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        SPARK CLUSTER                            │
│                                                                 │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │                     DRIVER                               │    │
│  │  • SparkContext / SparkSession                          │    │
│  │  • DAG Scheduler                                         │    │
│  │  • Task Scheduler                                        │    │
│  │  • Coordinates execution                                 │    │
│  └──────────────────────┬──────────────────────────────────┘    │
│                         │                                       │
│          ┌──────────────┼──────────────┐                       │
│          ▼              ▼              ▼                       │
│  ┌──────────────┐ ┌──────────────┐ ┌──────────────┐            │
│  │   EXECUTOR   │ │   EXECUTOR   │ │   EXECUTOR   │            │
│  │              │ │              │ │              │            │
│  │ ┌──────────┐ │ │ ┌──────────┐ │ │ ┌──────────┐ │            │
│  │ │  Task 1  │ │ │ │  Task 3  │ │ │ │  Task 5  │ │            │
│  │ │  Task 2  │ │ │ │  Task 4  │ │ │ │  Task 6  │ │            │
│  │ └──────────┘ │ │ └──────────┘ │ │ └──────────┘ │            │
│  │              │ │              │ │              │            │
│  │  [Cache]     │ │  [Cache]     │ │  [Cache]     │            │
│  └──────────────┘ └──────────────┘ └──────────────┘            │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### Components

| Component | Role |
|-----------|------|
| **Driver** | Your main program, creates SparkSession, orchestrates work |
| **Executor** | JVM process that runs tasks and caches data |
| **Task** | Unit of work sent to an executor |
| **Cluster Manager** | Allocates resources (YARN, Mesos, K8s, Standalone) |

**Your project:** Running in "local" mode - Driver and Executors run in the same JVM.

---

## SparkSession

The **SparkSession** is your entry point to Spark functionality.

```python
# Your stream_to_iceberg.py
spark = (
    SparkSession.builder
    .appName("KafkaKRaftToIceberg")
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.local.type", "hadoop")
    .config("spark.sql.catalog.local.warehouse", "/opt/warehouse/iceberg")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .getOrCreate()
)
```

### What Each Line Does

| Config | Purpose |
|--------|---------|
| `.appName("KafkaKRaftToIceberg")` | Name shown in Spark UI |
| `.config("spark.sql.catalog.local", ...)` | Registers "local" as an Iceberg catalog |
| `.config("spark.sql.catalog.local.type", "hadoop")` | Uses Hadoop-compatible file storage |
| `.config("spark.sql.catalog.local.warehouse", ...)` | Where tables are stored |
| `.config("spark.sql.extensions", ...)` | Loads Iceberg SQL extensions |
| `.getOrCreate()` | Get existing session or create new one |

---

## DataFrames and Datasets

### Evolution of Spark APIs

```
RDD (2011)           DataFrame (2015)        Dataset (2016)
Low-level API   →    Structured API     →    Type-safe structured
                                             
rdd.map(...)         df.select(...)          ds.map(...) with types
rdd.filter(...)      df.filter(...)          
                     df.groupBy(...)         
```

### DataFrame Basics

A **DataFrame** is a distributed collection of data organized into named columns (like a table).

```python
# Schema definition in your code
schema = StructType() \
    .add("timestamp", StringType()) \
    .add("usd_price", DoubleType())
```

This defines:
```
┌─────────────────┬────────────────┐
│    timestamp    │   usd_price    │
│    (STRING)     │   (DOUBLE)     │
├─────────────────┼────────────────┤
│ 2026-02-04T...  │    76123.0     │
│ 2026-02-04T...  │    76130.0     │
└─────────────────┴────────────────┘
```

---

## Transformations vs Actions

### Transformations (Lazy)

Transformations create a new DataFrame from an existing one. They're **lazy** - nothing executes until an action is called.

```python
# These are transformations - nothing executes yet!
parsed = (
    df.select(from_json(col("value").cast("string"), schema).alias("data"))
      .select("data.*")
)
```

| Transformation | Description |
|----------------|-------------|
| `select()` | Choose columns |
| `filter()` / `where()` | Filter rows |
| `groupBy()` | Group data |
| `join()` | Join DataFrames |
| `orderBy()` | Sort data |

### Actions (Eager)

Actions trigger computation and return results.

| Action | Description |
|--------|-------------|
| `show()` | Display data |
| `count()` | Count rows |
| `collect()` | Bring all data to driver |
| `write()` | Write to storage |
| `writeStream.start()` | Start streaming query |

---

## Spark Execution Model

### DAG (Directed Acyclic Graph)

Spark builds a **DAG** of stages from your transformations.

```
df.read() ──▶ filter() ──▶ select() ──▶ groupBy() ──▶ write()
                │            │            │
                ▼            ▼            ▼
            ┌───────┐   ┌───────┐   ┌───────┐
            │Stage 1│──▶│Stage 2│──▶│Stage 3│
            └───────┘   └───────┘   └───────┘
                            │
                        SHUFFLE
                     (data exchange)
```

### Stages and Shuffles

- **Stage**: A set of tasks that can run in parallel without data exchange
- **Shuffle**: Data redistribution between stages (EXPENSIVE!)

**What triggers a shuffle?**
- `groupBy()`
- `join()`
- `repartition()`
- `orderBy()` (sometimes)

---

## Streaming: Micro-Batch vs Continuous

### Micro-Batch Processing (Default)

Spark Structured Streaming processes data in small batches.

```
     Kafka Topic (prices)
     ───────────────────────────────────────▶ Time
     │    │    │    │    │    │    │    │
     └────┘    └────┘    └────┘    └────┘
      Batch 1   Batch 2   Batch 3   Batch 4
         │         │         │         │
         ▼         ▼         ▼         ▼
     ┌──────┐  ┌──────┐  ┌──────┐  ┌──────┐
     │Process│  │Process│  │Process│  │Process│
     └──────┘  └──────┘  └──────┘  └──────┘
         │         │         │         │
         ▼         ▼         ▼         ▼
       Write     Write     Write     Write
     to Iceberg to Iceberg to Iceberg to Iceberg
```

**Your logs showed this:**
```
INFO MicroBatchExecution: Starting [id = 278567ad...]
INFO MicroBatchExecution: Committed offsets for batch 1
```

### How It Works

```python
# In your stream_to_iceberg.py
df = (
    spark.readStream              # ← Creates a streaming DataFrame
    .format("kafka")              # ← Source is Kafka
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "prices")
    .load()
)
```

**The flow:**
```
1. Check Kafka for new offsets
   ▼
2. Read messages since last checkpoint
   ▼
3. Process batch (apply transformations)
   ▼
4. Write to sink (Iceberg)
   ▼
5. Commit offsets to checkpoint
   ▼
6. Repeat
```

---

## Checkpointing

**Checkpoints** store the state of a streaming query for fault tolerance.

```python
.option("checkpointLocation", "/opt/warehouse/checkpoint/prices")
```

### What's In Your Checkpoint?

```
warehouse/checkpoint/prices/
├── metadata              # Query metadata
├── commits/              # Completed batch info
│   ├── 0
│   ├── 1
│   ├── 2
│   └── ...
├── offsets/              # Kafka offsets per batch
│   ├── 0
│   ├── 1
│   └── ...
└── sources/              # Source-specific state
    └── 0/
```

### What Each Folder Contains

| Folder | Purpose |
|--------|---------|
| `commits/` | Records which batches completed successfully |
| `offsets/` | Stores start/end Kafka offsets for each batch |
| `sources/` | Source-specific metadata |

### Why Checkpoints Matter

```
Scenario: Spark job crashes at batch 5

Without checkpoints:
  ├─ Restart from scratch
  ├─ Reprocess all data
  └─ Potential duplicates

With checkpoints:
  ├─ Read last committed batch (4)
  ├─ Resume from batch 5
  └─ Exactly-once processing!
```

---

## Parsing Kafka Messages

Kafka messages arrive as bytes. You need to parse them.

```python
# Raw Kafka DataFrame columns
df.printSchema()
# root
#  |-- key: binary
#  |-- value: binary       ← Your JSON is here
#  |-- topic: string
#  |-- partition: integer
#  |-- offset: long
#  |-- timestamp: timestamp
#  |-- timestampType: integer

# Parse JSON from value
parsed = (
    df.select(from_json(col("value").cast("string"), schema).alias("data"))
      .select("data.*")
)
```

### Step by Step

```
1. col("value")                    → Binary bytes
   ▼
2. .cast("string")                 → '{"timestamp":"...","usd_price":76123}'
   ▼
3. from_json(..., schema)          → struct<timestamp:string, usd_price:double>
   ▼
4. .alias("data")                  → Named as "data"
   ▼
5. .select("data.*")               → Flatten struct to columns
```

---

## Output Modes

```python
.outputMode("append")
```

| Mode | Behavior | Use Case |
|------|----------|----------|
| **append** | Only new rows | Simple streaming inserts |
| **complete** | All rows (rewrite) | Aggregations with groupBy |
| **update** | Changed rows only | Stateful operations |

**Your use case:** Append mode - each batch adds new rows to Iceberg.

---

## Spark UI

When your job runs, you can access the Spark UI:

```
http://localhost:4040
```

**What you can see:**
- Jobs and stages
- Task execution times
- Shuffle read/write sizes
- Memory usage
- SQL query plans

---

## Memory Management

Spark divides executor memory into regions:

```
┌─────────────────────────────────────────────────────────┐
│                   EXECUTOR MEMORY                        │
│                                                         │
│  ┌───────────────────────────────────────────────────┐  │
│  │              Spark Memory (60%)                    │  │
│  │  ┌─────────────────────┬───────────────────────┐  │  │
│  │  │ Execution Memory    │   Storage Memory      │  │  │
│  │  │ (Shuffles, sorts)   │   (Cache, broadcast)  │  │  │
│  │  │         50%         │         50%           │  │  │
│  │  └─────────────────────┴───────────────────────┘  │  │
│  └───────────────────────────────────────────────────┘  │
│                                                         │
│  ┌───────────────────────────────────────────────────┐  │
│  │         User Memory (40%)                          │  │
│  │         (Your UDFs, data structures)               │  │
│  └───────────────────────────────────────────────────┘  │
│                                                         │
│  ┌───────────────────────────────────────────────────┐  │
│  │         Reserved Memory (300MB)                    │  │
│  └───────────────────────────────────────────────────┘  │
│                                                         │
└─────────────────────────────────────────────────────────┘
```

**Your logs showed:**
```
INFO MemoryStore: MemoryStore started with capacity 434.4 MiB
```

---

## Key Spark Configurations

| Config | Default | Description |
|--------|---------|-------------|
| `spark.executor.memory` | 1g | Memory per executor |
| `spark.executor.cores` | All | Cores per executor |
| `spark.sql.shuffle.partitions` | 200 | Partitions for shuffles |
| `spark.default.parallelism` | Total cores | Default RDD partitions |

---

## Exercises

### Exercise 1: Explore Spark UI
Run a query and open http://localhost:4040 to see:
- SQL tab (query plans)
- Stages tab (execution details)
- Storage tab (cached data)

### Exercise 2: Check Checkpoint Contents
```powershell
docker exec -it spark cat /opt/warehouse/checkpoint/prices/offsets/0
```

### Exercise 3: View Streaming Query Progress
Add this to a Python script:
```python
query = parsed.writeStream...start()
print(query.lastProgress)
```

---

## Key Takeaways

1. **SparkSession** is your entry point
2. **Transformations are lazy**, actions trigger execution
3. **Micro-batch streaming** processes data in small batches
4. **Checkpoints** enable exactly-once processing and recovery
5. **Shuffles are expensive** - minimize them
6. **Spark UI** is essential for debugging

---

Next: [04-iceberg-fundamentals.md](04-iceberg-fundamentals.md) - Understanding the Iceberg table format

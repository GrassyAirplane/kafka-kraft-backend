# Spark Configuration Deep Dive

This document explains the Spark configurations in your project - both the Dockerfile and the SparkSession configuration.

---

## Part 1: Dockerfile Configuration

```dockerfile
FROM apache/spark:3.5.1

USER root

# Download Iceberg and Kafka Spark dependencies
RUN curl -L -o /opt/spark/jars/iceberg-spark-runtime-3.5_2.12-1.5.2.jar \
    https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.5.2/iceberg-spark-runtime-3.5_2.12-1.5.2.jar && \
    curl -L -o /opt/spark/jars/spark-sql-kafka-0-10_2.12-3.5.1.jar \
    https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.1/spark-sql-kafka-0-10_2.12-3.5.1.jar && \
    ...
```

### Base Image

| Image | Version | Includes |
|-------|---------|----------|
| `apache/spark` | 3.5.1 | Spark, Hadoop, PySpark |

### JAR Dependencies

Each JAR serves a specific purpose:

| JAR | Purpose |
|-----|---------|
| `iceberg-spark-runtime-3.5_2.12-1.5.2.jar` | Iceberg table format support |
| `spark-sql-kafka-0-10_2.12-3.5.1.jar` | Kafka source/sink for Structured Streaming |
| `kafka-clients-3.5.1.jar` | Kafka protocol implementation |
| `spark-token-provider-kafka-0-10_2.12-3.5.1.jar` | Kafka authentication support |
| `commons-pool2-2.11.1.jar` | Connection pooling (Kafka dependency) |

### Version Matching

**Critical:** JAR versions must match Spark version!

```
Spark 3.5.1 + Scala 2.12 → spark-sql-kafka-0-10_2.12-3.5.1
                                            │       │
                                      Scala version  Spark version
```

**Wrong version = runtime errors!**

---

## Part 2: Docker Compose Spark Service

```yaml
spark:
  build: ./spark
  container_name: spark
  user: root
  ports:
    - "8080:8080"   # Spark UI
    - "7077:7077"   # Spark master
    - "4040:4040"   # Spark job UI
  volumes:
    - ./spark:/opt/spark-apps
    - ./warehouse:/opt/warehouse
  depends_on:
    kafka:
      condition: service_healthy
  command: >
    bash -c "tail -f /dev/null"
```

### Ports

| Port | Purpose | When Used |
|------|---------|-----------|
| 8080 | Spark Master UI | Cluster mode (not used in local) |
| 7077 | Spark Master RPC | Cluster mode (not used in local) |
| 4040 | Spark Job UI | **Active during job execution** |

**Access Spark UI:** `http://localhost:4040` (while a job is running)

### Volumes

```
Host                              Container
./spark/                    →     /opt/spark-apps/
  stream_to_iceberg.py             stream_to_iceberg.py
  
./warehouse/                →     /opt/warehouse/
  checkpoint/                      checkpoint/
  iceberg/                         iceberg/
```

**Why volumes matter:**
- Python scripts are editable without rebuilding
- Data persists after container stops

### Command

```yaml
command: bash -c "tail -f /dev/null"
```

Keeps container running without doing anything. You then exec into it to run jobs.

**Alternative: Run job directly**
```yaml
command: /opt/spark/bin/spark-submit /opt/spark-apps/stream_to_iceberg.py
```

---

## Part 3: SparkSession Configuration

```python
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

### `.appName("KafkaKRaftToIceberg")`

Sets the application name shown in:
- Spark UI
- Logs
- Cluster manager

---

### Iceberg Catalog Configuration

#### `spark.sql.catalog.local`

```python
.config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
```

| Part | Meaning |
|------|---------|
| `spark.sql.catalog.` | Prefix for catalog configs |
| `local` | Catalog name (you choose this) |
| Value | Iceberg's Spark catalog class |

**Result:** You can reference tables as `local.database.table`

---

#### `spark.sql.catalog.local.type`

```python
.config("spark.sql.catalog.local.type", "hadoop")
```

| Type | Description | Use Case |
|------|-------------|----------|
| `hadoop` | File-system based | Local/HDFS/S3 with no metastore |
| `hive` | Hive Metastore | Integration with Hive ecosystem |
| `rest` | REST Catalog | Cloud-native deployments |

**Hadoop type:**
- Stores metadata in files
- No external service needed
- Good for dev/testing

---

#### `spark.sql.catalog.local.warehouse`

```python
.config("spark.sql.catalog.local.warehouse", "/opt/warehouse/iceberg")
```

Root directory for Iceberg tables:

```
/opt/warehouse/iceberg/
├── default/                    ← database
│   └── crypto_prices/          ← table
│       ├── data/
│       └── metadata/
└── another_db/
    └── another_table/
```

---

#### `spark.sql.extensions`

```python
.config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
```

Loads Iceberg SQL extensions:
- `CALL` procedures (compaction, expiration)
- `ALTER TABLE` extensions
- `MERGE INTO` support
- Time travel syntax

---

## Part 4: Streaming Configuration

### Kafka Source Options

```python
df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "prices")
    .load()
)
```

| Option | Value | Description |
|--------|-------|-------------|
| `kafka.bootstrap.servers` | kafka:9092 | Kafka broker address |
| `subscribe` | prices | Topic to consume |

### Additional Kafka Options

```python
# Starting offset (default: latest)
.option("startingOffsets", "earliest")     # or "latest" or JSON

# Ending offset (for batch)
.option("endingOffsets", "latest")

# Max messages per trigger
.option("maxOffsetsPerTrigger", 10000)

# Consumer group (auto-generated if not set)
.option("kafka.group.id", "my-consumer-group")

# Include timestamp and headers
.option("includeHeaders", "true")
```

---

### Write Stream Options

```python
(
    parsed.writeStream
    .format("iceberg")
    .outputMode("append")
    .option("checkpointLocation", "/opt/warehouse/checkpoint/prices")
    .toTable("local.default.crypto_prices")
    .awaitTermination()
)
```

| Option | Value | Description |
|--------|-------|-------------|
| `format` | iceberg | Use Iceberg sink |
| `outputMode` | append | Only add new rows |
| `checkpointLocation` | /opt/warehouse/... | State storage |
| `toTable` | local.default.crypto_prices | Target table |
| `awaitTermination` | - | Block until stopped |

### Output Modes

| Mode | Behavior | Supported Operations |
|------|----------|---------------------|
| `append` | Only new rows written | No aggregations |
| `complete` | Entire result rewritten | Aggregations |
| `update` | Only changed rows | Aggregations (with watermark) |

---

## Part 5: Important Spark Configurations

### Memory Configuration

```python
# Per executor
.config("spark.executor.memory", "2g")

# Driver memory
.config("spark.driver.memory", "1g")

# Memory overhead (for off-heap)
.config("spark.executor.memoryOverhead", "512m")
```

### Parallelism

```python
# Shuffle partitions (affects groupBy, join)
.config("spark.sql.shuffle.partitions", "200")  # default

# For small data
.config("spark.sql.shuffle.partitions", "10")

# Default parallelism for RDDs
.config("spark.default.parallelism", "8")
```

### Streaming-Specific

```python
# Micro-batch interval
.trigger(processingTime="10 seconds")

# Or process all available data
.trigger(availableNow=True)

# Continuous processing (experimental)
.trigger(continuous="1 second")
```

### Checkpointing

```python
# Checkpoint location (required for streaming)
.option("checkpointLocation", "/path/to/checkpoint")

# Async checkpoint commits
.config("spark.sql.streaming.checkpointFileManagerClass", 
        "org.apache.spark.sql.execution.streaming.FileSystemBasedCheckpointFileManager")
```

---

## Part 6: Iceberg-Specific Configurations

### Write Properties

```python
# Target file size (128 MB default)
.config("spark.sql.catalog.local.write.target-file-size-bytes", "134217728")

# Write format
.config("spark.sql.catalog.local.write.format.default", "parquet")

# Compression
.config("spark.sql.iceberg.compression-codec", "zstd")
```

### Read Properties

```python
# Enable vectorized reads
.config("spark.sql.iceberg.vectorization.enabled", "true")

# Batch size for vectorized reads
.config("spark.sql.iceberg.vectorization.batch-size", "5000")
```

### Streaming Properties

```python
# How often to check for new snapshots
.config("spark.sql.iceberg.streaming.skip-delete-file-reads", "false")
```

---

## Part 7: Debugging Configurations

### Logging

```python
# Set log level
spark.sparkContext.setLogLevel("WARN")  # DEBUG, INFO, WARN, ERROR
```

### Explain Plans

```python
# Show query plan
df.explain(True)

# For streaming
query.explain(True)
```

### Spark UI Settings

```python
# Enable more history
.config("spark.ui.retainedJobs", "1000")
.config("spark.ui.retainedStages", "1000")
```

---

## Part 8: Production Configurations

### Recommended Production Settings

```python
spark = (
    SparkSession.builder
    .appName("ProductionJob")
    
    # Cluster mode
    .master("spark://spark-master:7077")
    
    # Resources
    .config("spark.executor.memory", "4g")
    .config("spark.executor.cores", "2")
    .config("spark.executor.instances", "4")
    
    # Streaming
    .config("spark.sql.shuffle.partitions", "100")
    .config("spark.streaming.backpressure.enabled", "true")
    
    # Fault tolerance
    .config("spark.task.maxFailures", "4")
    .config("spark.speculation", "true")
    
    # Iceberg
    .config("spark.sql.catalog.prod", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.prod.type", "rest")
    .config("spark.sql.catalog.prod.uri", "http://iceberg-catalog:8181")
    
    .getOrCreate()
)
```

---

## Configuration Sources

Spark reads configs from multiple sources (in order of precedence):

1. **SparkSession.builder.config()** - Your code
2. **spark-submit --conf** - Command line
3. **spark-defaults.conf** - File in conf directory
4. **Environment variables** - SPARK_* variables
5. **Default values** - Built into Spark

---

## Exercises

### Exercise 1: View Active Configuration
```python
# In PySpark shell
for conf in spark.sparkContext.getConf().getAll():
    print(conf)
```

### Exercise 2: Explain Query Plan
```python
df = spark.table("local.default.crypto_prices")
df.explain(True)
```

### Exercise 3: View Streaming Query Progress
```python
query = parsed.writeStream...start()

# In another thread or after some time:
print(query.status)
print(query.recentProgress)
print(query.lastProgress)
```

---

Next: [07-iceberg-configuration.md](07-iceberg-configuration.md) - Iceberg-specific configuration

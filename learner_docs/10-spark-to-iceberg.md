# Data Flow: Spark to Iceberg

This document explains how Spark Structured Streaming writes data to Apache Iceberg tables.

---

## The Complete Flow

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                         SPARK TO ICEBERG FLOW                                    │
│                                                                                 │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐    ┌───────────────┐ │
│  │   Parsed     │    │   Write      │    │   Iceberg    │    │   Parquet     │ │
│  │   DataFrame  │───▶│   Stream     │───▶│   Commit     │───▶│   Files       │ │
│  └──────────────┘    └──────────────┘    └──────────────┘    └───────────────┘ │
│                                                                                 │
│    Typed rows         Micro-batch         Metadata           Columnar           │
│                       execution           update             storage            │
│                                                                                 │
└─────────────────────────────────────────────────────────────────────────────────┘
```

---

## Step 1: Configure the Write Stream

```python
query = parsed_df.writeStream \
    .format("iceberg") \
    .outputMode("append") \
    .trigger(processingTime="10 seconds") \
    .option("checkpointLocation", "/opt/warehouse/checkpoint/prices") \
    .toTable("local.default.crypto_prices")
```

### Configuration Breakdown

| Option | Value | Purpose |
|--------|-------|---------|
| `format("iceberg")` | - | Use Iceberg sink |
| `outputMode("append")` | - | Add new rows (don't update) |
| `trigger` | 10 seconds | Batch interval |
| `checkpointLocation` | /opt/warehouse/... | Streaming state |
| `toTable` | local.default.crypto_prices | Target table |

---

## Step 2: Table Name Resolution

```
"local.default.crypto_prices"
   │      │         │
   │      │         └── Table name
   │      │
   │      └── Database/Namespace (default)
   │
   └── Catalog name (from SparkSession config)
```

### Catalog Resolution

```
Spark looks up "local" catalog:
  spark.sql.catalog.local = org.apache.iceberg.spark.SparkCatalog
  spark.sql.catalog.local.type = hadoop
  spark.sql.catalog.local.warehouse = /opt/warehouse/iceberg

Result:
  Table path: /opt/warehouse/iceberg/default/crypto_prices
```

---

## Step 3: Micro-batch Execution

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    MICRO-BATCH WRITE FLOW                                    │
│                                                                             │
│  Trigger fires (every 10 seconds)                                           │
│       │                                                                     │
│       ▼                                                                     │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │  1. Read new data from Kafka                                         │   │
│  │     • Check offsets since last batch                                 │   │
│  │     • Create DataFrame partition for each Kafka partition            │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│       │                                                                     │
│       ▼                                                                     │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │  2. Apply transformations                                            │   │
│  │     • Parse JSON                                                     │   │
│  │     • Select columns                                                 │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│       │                                                                     │
│       ▼                                                                     │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │  3. Write Parquet files                                              │   │
│  │     • One file per Spark partition                                   │   │
│  │     • Files written to data/ directory                               │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│       │                                                                     │
│       ▼                                                                     │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │  4. Iceberg commit                                                   │   │
│  │     • Create new snapshot                                            │   │
│  │     • Write manifest files                                           │   │
│  │     • Update metadata.json                                           │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│       │                                                                     │
│       ▼                                                                     │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │  5. Commit checkpoint                                                │   │
│  │     • Record Kafka offsets processed                                 │   │
│  │     • Mark batch as complete                                         │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Step 4: File Writing Process

### Parquet File Creation

```
┌────────────────────────────────────────────────────────────────────────────┐
│                      PARQUET FILE STRUCTURE                                 │
│                                                                            │
│  /opt/warehouse/iceberg/default/crypto_prices/data/                        │
│                                                                            │
│  00000-0-a1b2c3d4-xxxx.parquet                                             │
│  │                                                                         │
│  └── Contains:                                                             │
│      ┌─────────────────────────────────────────────────────────────────┐  │
│      │  Row Group 1                                                     │  │
│      │  ├── Column: timestamp (string)                                  │  │
│      │  │   └── Data pages (compressed)                                 │  │
│      │  │   └── Statistics (min, max, null count)                       │  │
│      │  │                                                               │  │
│      │  └── Column: usd_price (double)                                  │  │
│      │      └── Data pages (compressed)                                 │  │
│      │      └── Statistics (min, max, null count)                       │  │
│      └─────────────────────────────────────────────────────────────────┘  │
│      ┌─────────────────────────────────────────────────────────────────┐  │
│      │  Footer                                                          │  │
│      │  ├── Schema                                                      │  │
│      │  ├── Row group metadata                                          │  │
│      │  └── Column statistics                                           │  │
│      └─────────────────────────────────────────────────────────────────┘  │
│                                                                            │
└────────────────────────────────────────────────────────────────────────────┘
```

### Why Parquet?

| Feature | Benefit |
|---------|---------|
| Columnar storage | Read only needed columns |
| Compression | Snappy/ZSTD reduces size |
| Statistics | Skip files that don't match filters |
| Schema embedded | Self-describing files |

---

## Step 5: Iceberg Commit Process

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                       ICEBERG COMMIT PROCESS                                 │
│                                                                             │
│  Before Batch 3:                        After Batch 3:                       │
│                                                                             │
│  metadata/                              metadata/                            │
│  ├── v1.metadata.json                   ├── v1.metadata.json                 │
│  ├── v2.metadata.json ◄── current       ├── v2.metadata.json                 │
│  │                                      ├── v3.metadata.json ◄── current    │
│  ├── snap-1111.avro                     ├── snap-1111.avro                   │
│  └── snap-2222.avro                     ├── snap-2222.avro                   │
│                                         └── snap-3333.avro ◄── new          │
│                                                                             │
│  data/                                  data/                                │
│  ├── file1.parquet                      ├── file1.parquet                    │
│  └── file2.parquet                      ├── file2.parquet                    │
│                                         └── file3.parquet ◄── new           │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Atomic Commit Steps

```
1. Write new Parquet file(s)
   │   Files are written but not yet part of table
   ▼
2. Create new manifest file
   │   Lists the new data files
   ▼
3. Create new manifest list
   │   Points to all manifests (old + new)
   ▼
4. Write new metadata.json
   │   Contains new snapshot ID
   ▼
5. Atomic pointer update
   │   "current-snapshot-id" → new snapshot
   ▼
6. Commit complete!
   │   Readers now see new data
```

---

## Step 6: Table Auto-Creation

If the table doesn't exist, Iceberg creates it:

```python
# First write will create the table
# Schema inferred from DataFrame

parsed_df schema:
  timestamp: string
  usd_price: double

Created Iceberg table schema:
  timestamp: string
  usd_price: double
```

### Explicit Table Creation

```sql
-- For more control, create table first:
CREATE TABLE local.default.crypto_prices (
    timestamp STRING,
    usd_price DOUBLE
) USING iceberg
PARTITIONED BY (days(timestamp))
TBLPROPERTIES (
    'write.format.default' = 'parquet',
    'write.parquet.compression-codec' = 'snappy'
);
```

---

## Step 7: Checkpoint Management

```
┌────────────────────────────────────────────────────────────────────────────┐
│                    CHECKPOINT + ICEBERG COORDINATION                        │
│                                                                            │
│  Checkpoint State:                     Iceberg State:                       │
│                                                                            │
│  offsets/                              metadata/                            │
│  ├── 0: {"prices":{"0":0}}             ├── v1.metadata.json                │
│  ├── 1: {"prices":{"0":3}}             ├── v2.metadata.json                │
│  └── 2: {"prices":{"0":6}}             └── v3.metadata.json                │
│                                                                            │
│  commits/                              snapshots:                           │
│  ├── 0 ─────────────────────────────── snap-1 (rows 0-2)                   │
│  ├── 1 ─────────────────────────────── snap-2 (rows 0-5)                   │
│  └── 2 ─────────────────────────────── snap-3 (rows 0-8)                   │
│                                                                            │
│  The checkpoint tracks Kafka offsets                                        │
│  Iceberg tracks table snapshots                                            │
│  Both must be in sync for exactly-once                                     │
│                                                                            │
└────────────────────────────────────────────────────────────────────────────┘
```

---

## Step 8: Write Properties

### Configuring Write Behavior

```python
# Set table properties for write behavior
spark.sql("""
    ALTER TABLE local.default.crypto_prices
    SET TBLPROPERTIES (
        'write.format.default' = 'parquet',
        'write.parquet.compression-codec' = 'zstd',
        'write.target-file-size-bytes' = '134217728',
        'write.distribution-mode' = 'hash'
    )
""")
```

### Common Write Properties

| Property | Default | Description |
|----------|---------|-------------|
| `write.format.default` | parquet | File format |
| `write.parquet.compression-codec` | gzip | Compression |
| `write.target-file-size-bytes` | 512MB | Target file size |
| `write.distribution-mode` | none | How to distribute data |
| `write.metadata.delete-after-commit.enabled` | false | Cleanup old metadata |

---

## Step 9: Output Modes

```python
# Append: Add new rows (default, most common)
.outputMode("append")

# Complete: Replace entire output each batch (aggregations)
.outputMode("complete")

# Update: Only output changed rows (not supported for Iceberg)
# .outputMode("update")  # NOT SUPPORTED
```

### When to Use Each Mode

| Mode | Use Case | Iceberg Support |
|------|----------|-----------------|
| append | Streaming inserts | ✅ Yes |
| complete | Window aggregations | ✅ Yes |
| update | Change data capture | ❌ No |

---

## Step 10: Understanding the Result

After running your streaming job:

```
/opt/warehouse/iceberg/default/crypto_prices/
├── data/
│   ├── 00000-0-a1b2c3d4.parquet  ← Batch 0 data
│   ├── 00000-0-e5f6g7h8.parquet  ← Batch 1 data
│   └── 00000-0-i9j0k1l2.parquet  ← Batch 2 data
│
└── metadata/
    ├── v1.metadata.json           ← Initial table creation
    ├── v2.metadata.json           ← After batch 0
    ├── v3.metadata.json           ← After batch 1
    ├── v4.metadata.json           ← After batch 2 (current)
    │
    ├── snap-1111-m0.avro         ← Snapshot 1 manifest list
    ├── snap-2222-m0.avro         ← Snapshot 2 manifest list
    ├── snap-3333-m0.avro         ← Snapshot 3 manifest list
    │
    ├── a1b2c3d4-m0.avro          ← Manifest file 1
    ├── e5f6g7h8-m0.avro          ← Manifest file 2
    └── i9j0k1l2-m0.avro          ← Manifest file 3
```

---

## Querying Your Data

### Basic Query

```python
spark.sql("SELECT * FROM local.default.crypto_prices").show()

# Output:
# +--------------------+---------+
# |           timestamp|usd_price|
# +--------------------+---------+
# |2026-02-04T12:21:00Z|  76123.0|
# |2026-02-04T12:21:10Z|  76150.0|
# |2026-02-04T12:21:20Z|  76089.0|
# +--------------------+---------+
```

### Time Travel

```python
# Query as of specific snapshot
spark.sql("""
    SELECT * FROM local.default.crypto_prices 
    VERSION AS OF 2
""").show()

# Query as of specific timestamp
spark.sql("""
    SELECT * FROM local.default.crypto_prices 
    TIMESTAMP AS OF '2026-02-04 12:21:00'
""").show()
```

### Snapshot History

```python
spark.sql("""
    SELECT * FROM local.default.crypto_prices.history
""").show()

# Output:
# +--------------------+-------------------+-------------------+
# |     made_current_at|        snapshot_id|          parent_id|
# +--------------------+-------------------+-------------------+
# |2026-02-04 12:21:...|  1234567890123456|               null|
# |2026-02-04 12:21:...|  2345678901234567|  1234567890123456|
# |2026-02-04 12:21:...|  3456789012345678|  2345678901234567|
# +--------------------+-------------------+-------------------+
```

---

## Exercises

### Exercise 1: Add Partitioning

```sql
-- Create partitioned table
CREATE TABLE local.default.crypto_prices_partitioned (
    timestamp STRING,
    usd_price DOUBLE
) USING iceberg
PARTITIONED BY (days(timestamp));
```

### Exercise 2: Monitor Write Progress

```python
query = parsed_df.writeStream...

# Check progress
while query.isActive:
    progress = query.lastProgress
    if progress:
        print(f"Batch: {progress['batchId']}")
        print(f"Rows: {progress['numInputRows']}")
        print(f"Duration: {progress['batchDuration']}ms")
    time.sleep(10)
```

### Exercise 3: Inspect Table Metadata

```python
# View all snapshots
spark.sql("SELECT * FROM local.default.crypto_prices.snapshots").show()

# View all data files
spark.sql("SELECT * FROM local.default.crypto_prices.files").show()

# View partition info
spark.sql("SELECT * FROM local.default.crypto_prices.partitions").show()
```

---

Next: [11-exactly-once-semantics.md](11-exactly-once-semantics.md) - Understanding end-to-end delivery guarantees

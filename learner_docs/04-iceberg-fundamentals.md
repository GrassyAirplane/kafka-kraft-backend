# Iceberg Fundamentals

## What is Apache Iceberg?

Apache Iceberg is an **open table format** for huge analytic datasets. It sits between your compute engine (Spark) and your storage (files).

```
┌─────────────────────────────────────────────────────────────────┐
│                    TRADITIONAL DATA LAKE                        │
│                                                                 │
│  Spark ──▶ Read files directly ──▶ Parquet/ORC files           │
│           (Must know file locations, partitions, etc.)         │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│                    WITH ICEBERG                                 │
│                                                                 │
│  Spark ──▶ Iceberg ──▶ Parquet/ORC files                       │
│            │                                                    │
│            ├── Tracks which files belong to table               │
│            ├── Handles schema evolution                         │
│            ├── Manages snapshots for time travel               │
│            └── Optimizes query planning                         │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

---

## Why Iceberg?

### Problems with Hive-style Tables

| Problem | Description |
|---------|-------------|
| **Slow metadata** | Directory listing for files is slow at scale |
| **No ACID** | Concurrent reads/writes cause corruption |
| **Schema lock-in** | Changing schema requires rewriting all data |
| **No time travel** | Can't query historical data |
| **Partition lock-in** | Changing partition scheme requires rewrite |

### Iceberg Solutions

| Feature | Benefit |
|---------|---------|
| **Fast planning** | Manifest files track exact files to read |
| **ACID transactions** | Snapshot-based isolation |
| **Schema evolution** | Add/remove/rename columns without rewrite |
| **Time travel** | Query any historical snapshot |
| **Hidden partitioning** | Partition without exposing to users |

---

## Table Structure

Your Iceberg table has this structure:

```
warehouse/iceberg/default/crypto_prices/
├── data/                              ◀── Parquet data files
│   ├── 00000-0-278567ad-...-00001.parquet
│   ├── 00000-1-278567ad-...-00001.parquet
│   └── ...
└── metadata/                          ◀── Iceberg metadata
    ├── v1.metadata.json               ◀── Table metadata version 1
    ├── v2.metadata.json               ◀── Table metadata version 2
    ├── ...
    ├── v8.metadata.json               ◀── Current version
    ├── version-hint.text              ◀── Points to current version
    ├── snap-xxxx.avro                 ◀── Snapshot files
    └── xxxx-m0.avro                   ◀── Manifest files
```

---

## Metadata Hierarchy

Iceberg uses a three-level metadata hierarchy:

```
┌─────────────────────────────────────────────────────────────────┐
│                     METADATA.JSON                                │
│                     (Table Metadata)                             │
│                                                                 │
│  • Current schema                                               │
│  • Partition spec                                               │
│  • List of snapshots                                            │
│  • Current snapshot ID                                          │
│  • Table properties                                             │
└─────────────────────┬───────────────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────────────────┐
│                     MANIFEST LIST                                │
│                     (snap-xxxx.avro)                             │
│                                                                 │
│  • List of manifest files                                       │
│  • Partition summaries                                          │
│  • Added/deleted files count                                    │
└─────────────────────┬───────────────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────────────────┐
│                     MANIFEST FILES                               │
│                     (xxxx-m0.avro)                               │
│                                                                 │
│  • List of data files                                           │
│  • Per-file statistics (min/max, row count)                     │
│  • Partition values                                             │
└─────────────────────┬───────────────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────────────────┐
│                     DATA FILES                                   │
│                     (Parquet/ORC/Avro)                           │
│                                                                 │
│  • Actual data rows                                             │
│  • Columnar format (Parquet)                                    │
│  • Compression (Zstd, Snappy, etc.)                             │
└─────────────────────────────────────────────────────────────────┘
```

---

## Snapshots

A **snapshot** represents the state of a table at a point in time.

```
Table: crypto_prices

Snapshot 1 (v1)           Snapshot 2 (v2)           Snapshot 3 (v3)
┌─────────────┐           ┌─────────────┐           ┌─────────────┐
│ file1.parq  │           │ file1.parq  │           │ file1.parq  │
│             │           │ file2.parq  │           │ file2.parq  │
│             │           │             │           │ file3.parq  │
└─────────────┘           └─────────────┘           └─────────────┘
     │                          │                         │
   12:20:57                   12:21:18                  12:21:38
   2 records                  4 records                 6 records
```

**Your logs showed:**
```
INFO SnapshotProducer: Committed snapshot 195918892055781446 (FastAppend)
```

### Why Snapshots Matter

1. **Time Travel**: Query any previous state
2. **Rollback**: Undo bad writes
3. **Incremental Processing**: Find new files since last read
4. **Audit**: See who changed what and when

---

## Time Travel

Query historical data:

```sql
-- Query current data
SELECT * FROM local.default.crypto_prices;

-- Query specific snapshot
SELECT * FROM local.default.crypto_prices VERSION AS OF 195918892055781446;

-- Query at a timestamp
SELECT * FROM local.default.crypto_prices TIMESTAMP AS OF '2026-02-04T12:21:00';

-- View snapshot history
SELECT * FROM local.default.crypto_prices.history;

-- View all snapshots
SELECT * FROM local.default.crypto_prices.snapshots;
```

---

## Schema Evolution

Iceberg tracks schema changes without rewriting data.

```sql
-- Add a column (no data rewrite!)
ALTER TABLE local.default.crypto_prices ADD COLUMN symbol STRING;

-- Rename a column
ALTER TABLE local.default.crypto_prices RENAME COLUMN usd_price TO price_usd;

-- Change column type (if compatible)
ALTER TABLE local.default.crypto_prices ALTER COLUMN timestamp TYPE TIMESTAMP;
```

### How It Works

```
Old data files:                    New query:
┌───────────────────┐              SELECT symbol FROM table
│ timestamp | price │              ▼
│    ...    |  ...  │              Iceberg knows old files
└───────────────────┘              don't have 'symbol' column
                                   ▼
                                   Returns NULL for old rows
```

---

## Partition Evolution

Unlike Hive, Iceberg supports changing partition schemes!

```sql
-- Original partitioning
CREATE TABLE events (
  event_time TIMESTAMP,
  data STRING
) USING iceberg
PARTITIONED BY (days(event_time));

-- Later, change to hourly (no rewrite!)
ALTER TABLE events ADD PARTITION FIELD hours(event_time);
```

**How it works:**
- Old files stay partitioned by day
- New files are partitioned by hour
- Iceberg handles both transparently

---

## Hidden Partitioning

Users don't need to know partition columns:

```sql
-- Traditional Hive (user must know partition)
SELECT * FROM events WHERE event_date = '2026-02-04';

-- Iceberg (just use the actual column)
SELECT * FROM events WHERE event_time = '2026-02-04T12:00:00';
-- Iceberg automatically prunes partitions!
```

---

## ACID Transactions

### Optimistic Concurrency

```
Writer 1                              Writer 2
   │                                     │
   ▼                                     ▼
Read current metadata (v5)           Read current metadata (v5)
   │                                     │
   ▼                                     ▼
Write new data files                 Write new data files
   │                                     │
   ▼                                     ▼
Commit → Create v6                   Commit → v5 already replaced!
   ✓                                     │
                                         ▼
                                     Retry: Read v6, commit v7
                                         ✓
```

### Commit Flow

```
1. Write data files (Parquet)
   ▼
2. Create new manifest file
   ▼
3. Create new manifest list (snapshot)
   ▼
4. Atomically update metadata.json
   ▼
5. Update version-hint.text
```

**Your logs showed:**
```
INFO HadoopTableOperations: Committed a new metadata file v3.metadata.json
```

---

## Catalogs

A **catalog** manages table metadata location.

### Catalog Types

| Type | Storage | Use Case |
|------|---------|----------|
| **Hadoop** | File system | Dev/testing, simple setups |
| **Hive** | Hive Metastore | Integration with Hive ecosystem |
| **REST** | REST API | Cloud-native, multi-engine |
| **AWS Glue** | AWS Glue | AWS ecosystem |
| **Nessie** | Nessie server | Git-like versioning |

### Your Configuration

```python
# In stream_to_iceberg.py
.config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
.config("spark.sql.catalog.local.type", "hadoop")
.config("spark.sql.catalog.local.warehouse", "/opt/warehouse/iceberg")
```

This creates a Hadoop catalog named "local" that stores metadata in the file system.

---

## File Formats

Iceberg supports multiple file formats:

| Format | Pros | Cons |
|--------|------|------|
| **Parquet** | Best compression, columnar, most common | |
| **ORC** | Good compression, Hive-native | Less portable |
| **Avro** | Row-based, schema embedded | Larger files |

**Your table uses Parquet** (the default):
```
INFO SparkWrite: IcebergStreamingWrite(table=..., format=PARQUET)
```

---

## Maintenance Operations

### Compaction

Small files hurt performance. Compaction merges them:

```sql
-- Rewrite small files into larger ones
CALL local.system.rewrite_data_files('default.crypto_prices');
```

```
Before:                          After:
file1.parquet (1 KB)             merged.parquet (50 KB)
file2.parquet (2 KB)        →    
file3.parquet (1 KB)             
file4.parquet (1 KB)             
```

### Snapshot Expiration

Old snapshots take space. Expire them:

```sql
-- Remove snapshots older than 7 days
CALL local.system.expire_snapshots('default.crypto_prices', TIMESTAMP '2026-01-28 00:00:00');
```

### Orphan File Removal

Delete unreferenced files:

```sql
CALL local.system.remove_orphan_files('default.crypto_prices');
```

---

## Examining Your Table

### View Metadata JSON

```powershell
docker exec -it spark cat /opt/warehouse/iceberg/default/crypto_prices/metadata/v8.metadata.json | head -50
```

### Query System Tables

```sql
-- Snapshots
SELECT * FROM local.default.crypto_prices.snapshots;

-- History
SELECT * FROM local.default.crypto_prices.history;

-- Files in current snapshot
SELECT * FROM local.default.crypto_prices.files;

-- Manifests
SELECT * FROM local.default.crypto_prices.manifests;

-- Partitions
SELECT * FROM local.default.crypto_prices.partitions;
```

---

## Streaming Writes

Your streaming job uses **append-only** writes:

```python
(
    parsed.writeStream
    .format("iceberg")              # Use Iceberg sink
    .outputMode("append")           # Add new rows
    .option("checkpointLocation", "/opt/warehouse/checkpoint/prices")
    .toTable("local.default.crypto_prices")
    .awaitTermination()
)
```

**What happens each batch:**

```
1. Spark reads new Kafka messages
   ▼
2. Writes new Parquet file(s)
   ▼
3. Creates new manifest file
   ▼
4. Creates new snapshot
   ▼
5. Updates metadata.json (v8 → v9)
   ▼
6. Commits checkpoint
```

---

## Exercises

### Exercise 1: Explore Metadata
```sql
-- In spark-sql shell:
SELECT * FROM local.default.crypto_prices.snapshots;
SELECT * FROM local.default.crypto_prices.history;
```

### Exercise 2: Time Travel
```sql
-- Query first snapshot
SELECT * FROM local.default.crypto_prices VERSION AS OF 1;
```

### Exercise 3: View Data Files
```sql
SELECT file_path, record_count, file_size_in_bytes 
FROM local.default.crypto_prices.files;
```

### Exercise 4: Check File Count
```powershell
docker exec -it spark ls -la /opt/warehouse/iceberg/default/crypto_prices/data/
```

---

## Key Takeaways

1. **Metadata hierarchy**: metadata.json → manifest list → manifests → data files
2. **Snapshots** enable time travel and atomic commits
3. **Schema evolution** without rewriting data
4. **Partition evolution** without rewriting data
5. **ACID transactions** via optimistic concurrency
6. **Catalogs** manage where table metadata lives
7. **Maintenance** (compaction, expiration) keeps tables healthy

---

Next: [05-kafka-configuration.md](05-kafka-configuration.md) - Deep dive into every Kafka config

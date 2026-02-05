# Iceberg Configuration Deep Dive

This document explains every Iceberg configuration in your pipeline.

---

## Configuration Locations

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    ICEBERG CONFIGURATION LAYERS                              │
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │  SparkSession Config                                                 │   │
│  │  • Catalog registration                                              │   │
│  │  • Catalog type and warehouse                                        │   │
│  │  • Session extensions                                                │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                              │                                              │
│                              ▼                                              │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │  Table Properties                                                    │   │
│  │  • Write format and compression                                      │   │
│  │  • File sizes                                                        │   │
│  │  • Partitioning                                                      │   │
│  │  • Metadata retention                                                │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                              │                                              │
│                              ▼                                              │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │  Write Options                                                       │   │
│  │  • Per-write overrides                                               │   │
│  │  • Distribution mode                                                 │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## SparkSession Iceberg Configuration

### Your Configuration

```python
spark = SparkSession.builder \
    .appName("CryptoToIceberg") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.local.type", "hadoop") \
    .config("spark.sql.catalog.local.warehouse", "/opt/warehouse/iceberg") \
    .getOrCreate()
```

### Configuration Breakdown

#### 1. spark.sql.extensions

```
org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
```

**What it adds to Spark SQL:**

| Feature | SQL Syntax | Purpose |
|---------|------------|---------|
| Time Travel | `SELECT * FROM table VERSION AS OF 5` | Query historical data |
| Procedures | `CALL catalog.system.rewrite_data_files(...)` | Maintenance operations |
| Metadata Tables | `SELECT * FROM table.snapshots` | Inspect table internals |
| Merge Into | `MERGE INTO target USING source ON ...` | Upsert operations |

**Without this extension:**
```sql
-- These would fail:
SELECT * FROM table VERSION AS OF 5;  -- ERROR
CALL system.expire_snapshots(...);    -- ERROR
```

#### 2. spark.sql.catalog.local

```
org.apache.iceberg.spark.SparkCatalog
```

**Registers a catalog named "local":**

```python
# Now you can reference tables as:
spark.sql("SELECT * FROM local.default.crypto_prices")

# Or set as default:
spark.sql("USE local")
spark.sql("SELECT * FROM default.crypto_prices")
```

**SparkCatalog responsibilities:**
- Table discovery and listing
- Schema management
- Namespace (database) operations
- Transaction coordination

#### 3. spark.sql.catalog.local.type

```
hadoop
```

**Catalog types explained:**

| Type | Backend | Use Case |
|------|---------|----------|
| `hadoop` | Local/HDFS filesystem | Development, HDFS deployments |
| `hive` | Hive Metastore | Existing Hive infrastructure |
| `rest` | REST Catalog | Tabular, Snowflake, Dremio |
| `glue` | AWS Glue | AWS-native deployments |
| `nessie` | Nessie | Git-like version control |
| `jdbc` | Any JDBC database | Custom metadata storage |

**Your choice (hadoop):**
```
┌────────────────────────────────────────────────────────────────┐
│                    HADOOP CATALOG                               │
│                                                                │
│  /opt/warehouse/iceberg/                                       │
│  └── default/                    ← Namespace                   │
│      └── crypto_prices/          ← Table                       │
│          ├── metadata/           ← Iceberg metadata            │
│          │   ├── v1.metadata.json                              │
│          │   └── ...                                           │
│          └── data/               ← Parquet files               │
│              └── ...                                           │
│                                                                │
│  Metadata location = warehouse_path + namespace + table_name   │
│                                                                │
└────────────────────────────────────────────────────────────────┘
```

#### 4. spark.sql.catalog.local.warehouse

```
/opt/warehouse/iceberg
```

**Base path for all tables:**

```
Table: local.default.crypto_prices
  └── Stored at: /opt/warehouse/iceberg/default/crypto_prices

Table: local.analytics.metrics  
  └── Stored at: /opt/warehouse/iceberg/analytics/metrics
```

---

## Table Properties

### Default Table Creation

When you write to a non-existent table:

```python
df.writeStream \
    .format("iceberg") \
    .toTable("local.default.crypto_prices")
```

Iceberg creates with these defaults:

```sql
-- Equivalent DDL
CREATE TABLE local.default.crypto_prices (
    timestamp STRING,
    usd_price DOUBLE
) USING iceberg
TBLPROPERTIES (
    'format-version' = '2',
    'write.format.default' = 'parquet',
    'write.parquet.compression-codec' = 'gzip'
);
```

### Custom Table Creation

```sql
CREATE TABLE local.default.crypto_prices (
    timestamp TIMESTAMP,
    symbol STRING,
    usd_price DOUBLE
) USING iceberg
PARTITIONED BY (days(timestamp), symbol)
TBLPROPERTIES (
    -- Format version
    'format-version' = '2',
    
    -- Write settings
    'write.format.default' = 'parquet',
    'write.parquet.compression-codec' = 'zstd',
    'write.target-file-size-bytes' = '134217728',
    'write.distribution-mode' = 'hash',
    
    -- Metadata settings
    'write.metadata.delete-after-commit.enabled' = 'true',
    'write.metadata.previous-versions-max' = '100',
    
    -- History settings
    'history.expire.max-snapshot-age-ms' = '604800000',
    'history.expire.min-snapshots-to-keep' = '5'
);
```

### Property Deep Dive

#### format-version

```
'format-version' = '2'
```

| Version | Features |
|---------|----------|
| 1 | Original format, basic features |
| 2 | Row-level deletes, position deletes, equality deletes |

**Version 2 enables:**
```sql
-- Row-level deletes (without rewriting entire files)
DELETE FROM crypto_prices WHERE usd_price < 1000;

-- Updates (delete + insert)
UPDATE crypto_prices SET usd_price = 50000 WHERE symbol = 'BTC';

-- Merge (upsert)
MERGE INTO crypto_prices t
USING updates u
ON t.timestamp = u.timestamp
WHEN MATCHED THEN UPDATE SET usd_price = u.usd_price
WHEN NOT MATCHED THEN INSERT *;
```

#### write.format.default

```
'write.format.default' = 'parquet'
```

| Format | Pros | Cons |
|--------|------|------|
| parquet | Best compression, columnar | Slower writes |
| avro | Fast writes, row-based | Larger files |
| orc | Good for Hive compatibility | Less common |

#### write.parquet.compression-codec

```
'write.parquet.compression-codec' = 'zstd'
```

| Codec | Compression | Speed | Use Case |
|-------|-------------|-------|----------|
| none | 1x | Fastest | Testing |
| snappy | ~2x | Fast | Balanced |
| gzip | ~3x | Slow | Space-sensitive |
| zstd | ~3.5x | Medium | Best balance |
| lz4 | ~2x | Very fast | Speed-sensitive |

#### write.target-file-size-bytes

```
'write.target-file-size-bytes' = '134217728'  # 128MB
```

**Trade-offs:**

```
Smaller files (16MB):
├── Faster writes
├── More metadata overhead
├── More files to manage
└── Better for streaming

Larger files (512MB):
├── Better query performance
├── Less metadata
├── Slower individual writes
└── Better for batch
```

**Recommendation:**
- Streaming: 64-128MB
- Batch: 256-512MB

#### write.distribution-mode

```
'write.distribution-mode' = 'hash'
```

| Mode | Behavior | Use Case |
|------|----------|----------|
| none | No redistribution | Fast, uneven files |
| hash | Hash by partition key | Balanced files |
| range | Range by sort key | Sorted output |

---

## Partition Configuration

### Partition Transforms

```sql
-- Available transforms:
PARTITIONED BY (
    years(timestamp),     -- Extract year
    months(timestamp),    -- Extract month  
    days(timestamp),      -- Extract day
    hours(timestamp),     -- Extract hour
    bucket(16, user_id),  -- Hash into N buckets
    truncate(10, city)    -- Truncate string to N chars
)
```

### Your Data Partitioning

```sql
-- For crypto prices (time-series data):
PARTITIONED BY (days(timestamp))

-- Result:
-- data/
-- ├── timestamp_day=2026-02-01/
-- │   ├── 00000-0-xxxx.parquet
-- │   └── 00000-0-yyyy.parquet
-- ├── timestamp_day=2026-02-02/
-- │   └── 00000-0-zzzz.parquet
```

### Hidden Partitioning

**Traditional (Hive-style) requires exact partition value:**
```sql
-- Hive: Must know exact partition format
SELECT * FROM prices WHERE date = '2026-02-01'

-- If you query differently, full scan:
SELECT * FROM prices WHERE timestamp >= '2026-02-01'  -- FULL SCAN!
```

**Iceberg hidden partitioning is automatic:**
```sql
-- Iceberg: Any expression works
SELECT * FROM prices 
WHERE timestamp >= '2026-02-01 00:00:00'
  AND timestamp < '2026-02-02 00:00:00'
-- Iceberg automatically prunes to timestamp_day=2026-02-01
```

---

## Metadata Configuration

### Snapshot Retention

```sql
TBLPROPERTIES (
    'history.expire.max-snapshot-age-ms' = '604800000',  -- 7 days
    'history.expire.min-snapshots-to-keep' = '5'
)
```

**What snapshots provide:**
```
┌────────────────────────────────────────────────────────────────┐
│                    SNAPSHOT RETENTION                           │
│                                                                │
│  Snapshot 1 ──▶ Snapshot 2 ──▶ Snapshot 3 ──▶ Snapshot 4       │
│  (Jan 1)        (Jan 2)        (Jan 3)        (Jan 4)          │
│                                                  │              │
│                                                  └─ current     │
│                                                                │
│  With 7-day retention + min 5 snapshots:                       │
│  • Keep all snapshots from last 7 days                         │
│  • Always keep at least 5 even if older                        │
│                                                                │
│  Time travel possible to any retained snapshot                 │
│                                                                │
└────────────────────────────────────────────────────────────────┘
```

### Metadata Cleanup

```sql
TBLPROPERTIES (
    'write.metadata.delete-after-commit.enabled' = 'true',
    'write.metadata.previous-versions-max' = '100'
)
```

**What this controls:**
```
metadata/
├── v1.metadata.json   ← Deleted after 100 newer versions
├── v2.metadata.json   ← Deleted after 100 newer versions  
├── ...
├── v99.metadata.json  ← Deleted after 100 newer versions
├── v100.metadata.json ← Kept
├── v101.metadata.json ← Kept (current - 1)
└── v102.metadata.json ← Current
```

---

## Runtime Configuration

### Write Options

```python
# Per-write configuration
df.writeTo("local.default.crypto_prices") \
    .option("write-format", "parquet") \
    .option("target-file-size-bytes", "67108864") \
    .option("fanout-enabled", "true") \
    .append()
```

### Read Options

```python
# Snapshot selection
spark.read \
    .option("snapshot-id", "1234567890") \
    .table("local.default.crypto_prices")

# As of timestamp
spark.read \
    .option("as-of-timestamp", "1770207660000") \
    .table("local.default.crypto_prices")
```

---

## Catalog Properties for Different Backends

### S3 (AWS)

```python
.config("spark.sql.catalog.prod", "org.apache.iceberg.spark.SparkCatalog") \
.config("spark.sql.catalog.prod.type", "hadoop") \
.config("spark.sql.catalog.prod.warehouse", "s3a://my-bucket/iceberg") \
.config("spark.hadoop.fs.s3a.access.key", "xxx") \
.config("spark.hadoop.fs.s3a.secret.key", "xxx") \
.config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")
```

### Hive Metastore

```python
.config("spark.sql.catalog.hive_prod", "org.apache.iceberg.spark.SparkCatalog") \
.config("spark.sql.catalog.hive_prod.type", "hive") \
.config("spark.sql.catalog.hive_prod.uri", "thrift://hive-metastore:9083")
```

### AWS Glue

```python
.config("spark.sql.catalog.glue", "org.apache.iceberg.spark.SparkCatalog") \
.config("spark.sql.catalog.glue.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
.config("spark.sql.catalog.glue.warehouse", "s3://my-bucket/iceberg")
```

---

## Inspecting Configuration

### View Table Properties

```sql
DESCRIBE EXTENDED local.default.crypto_prices;

-- Or just properties:
SHOW TBLPROPERTIES local.default.crypto_prices;
```

### View Current Metadata

```python
# Metadata location
spark.sql("SELECT * FROM local.default.crypto_prices.metadata_log_entries").show()

# Current snapshot
spark.sql("SELECT * FROM local.default.crypto_prices.snapshots").show()

# All files
spark.sql("SELECT * FROM local.default.crypto_prices.files").show()
```

---

## Exercises

### Exercise 1: Modify Table Properties

```sql
-- Add compression
ALTER TABLE local.default.crypto_prices 
SET TBLPROPERTIES ('write.parquet.compression-codec' = 'zstd');

-- Verify
SHOW TBLPROPERTIES local.default.crypto_prices;
```

### Exercise 2: Create Partitioned Table

```sql
CREATE TABLE local.default.crypto_prices_v2 (
    event_time TIMESTAMP,
    symbol STRING,
    price DOUBLE
) USING iceberg
PARTITIONED BY (days(event_time), symbol);

-- Insert sample data
INSERT INTO local.default.crypto_prices_v2 VALUES
    (TIMESTAMP '2026-02-01 10:00:00', 'BTC', 50000),
    (TIMESTAMP '2026-02-01 11:00:00', 'ETH', 3000),
    (TIMESTAMP '2026-02-02 10:00:00', 'BTC', 51000);

-- Check partition pruning
EXPLAIN SELECT * FROM local.default.crypto_prices_v2 
WHERE event_time >= '2026-02-02';
```

### Exercise 3: Time Travel Query

```python
# Get snapshot IDs
spark.sql("SELECT snapshot_id, committed_at FROM local.default.crypto_prices.snapshots").show()

# Query old snapshot
snapshot_id = 12345  # from above
spark.read \
    .option("snapshot-id", snapshot_id) \
    .table("local.default.crypto_prices") \
    .show()
```

---

Next: [08-producer-to-kafka.md](08-producer-to-kafka.md) - How data flows from producer to Kafka

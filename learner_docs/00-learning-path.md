# Learning Path: Kafka + Spark + Iceberg Mastery

## Your Journey to Expertise

This curriculum will take you from understanding the basics to mastering the internals of each component in your streaming data pipeline.

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        YOUR DATA PIPELINE                                    │
│                                                                              │
│  ┌──────────┐     ┌─────────────┐     ┌─────────────┐     ┌──────────────┐  │
│  │ Producer │────▶│    Kafka    │────▶│    Spark    │────▶│   Iceberg    │  │
│  │ (Python) │     │   (KRaft)   │     │ (Streaming) │     │   (Tables)   │  │
│  └──────────┘     └─────────────┘     └─────────────┘     └──────────────┘  │
│                                                                              │
│  api_producer.py  docker-compose.yml  stream_to_iceberg.py  warehouse/      │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 📚 Curriculum Structure

### Level 1: Foundations
| Doc | Topic | Time |
|-----|-------|------|
| [01-kafka-fundamentals.md](01-kafka-fundamentals.md) | Kafka core concepts, topics, partitions, offsets | 45 min |
| [02-kraft-deep-dive.md](02-kraft-deep-dive.md) | KRaft consensus, why no ZooKeeper, metadata management | 30 min |
| [03-spark-fundamentals.md](03-spark-fundamentals.md) | Spark architecture, RDDs, DataFrames, streaming | 60 min |
| [04-iceberg-fundamentals.md](04-iceberg-fundamentals.md) | Table format, metadata, snapshots, time travel | 45 min |

### Level 2: Configuration Deep Dives
| Doc | Topic | Time |
|-----|-------|------|
| [05-kafka-configuration.md](05-kafka-configuration.md) | Every config in your docker-compose explained | 45 min |
| [06-spark-configuration.md](06-spark-configuration.md) | SparkSession, catalogs, extensions, tuning | 45 min |
| [07-iceberg-configuration.md](07-iceberg-configuration.md) | Catalog types, file formats, partitioning | 30 min |

### Level 3: Integration & Data Flow
| Doc | Topic | Time |
|-----|-------|------|
| [08-producer-to-kafka.md](08-producer-to-kafka.md) | Serialization, partitioning, delivery guarantees | 30 min |
| [09-kafka-to-spark.md](09-kafka-to-spark.md) | Structured Streaming, micro-batches, checkpoints | 45 min |
| [10-spark-to-iceberg.md](10-spark-to-iceberg.md) | Writing, commits, schema evolution | 30 min |

### Level 4: Advanced Topics
| Doc | Topic | Time |
|-----|-------|------|
| [11-exactly-once-semantics.md](11-exactly-once-semantics.md) | End-to-end exactly-once processing | 45 min |
| [12-scaling-and-performance.md](12-scaling-and-performance.md) | Partitions, parallelism, resource tuning | 45 min |
| [13-fault-tolerance.md](13-fault-tolerance.md) | Failures, recovery, rebalancing | 30 min |
| [14-production-checklist.md](14-production-checklist.md) | What changes for production deployment | 30 min |

---

## 🎯 How to Use This Curriculum

1. **Read in order** - Each doc builds on previous concepts
2. **Reference your code** - Each doc points to actual files in your project
3. **Experiment** - Try the suggested exercises
4. **Break things** - Understanding failures teaches you how things work

---

## 🗂️ Project File Reference

```
kafka-kraft-iceberg/
├── docker-compose.yml          # Kafka + Spark infrastructure
├── producer/
│   └── api_producer.py         # Python Kafka producer
├── spark/
│   ├── Dockerfile              # Spark + Iceberg image
│   ├── stream_to_iceberg.py    # Main streaming job
│   ├── query_iceberg.py        # Query helper
│   └── quick_query.py          # Direct parquet query
└── warehouse/
    ├── checkpoint/             # Spark streaming checkpoints
    └── iceberg/                # Iceberg table data
        └── default/
            └── crypto_prices/
                ├── data/       # Parquet files
                └── metadata/   # Iceberg metadata
```

---

## 🔑 Key Questions You'll Be Able to Answer

After completing this curriculum:

**Kafka/KRaft:**
- How does KRaft achieve consensus without ZooKeeper?
- What happens when a broker fails mid-message?
- How do consumer groups coordinate?

**Spark:**
- What's the difference between batch and streaming execution?
- How does Spark guarantee exactly-once processing?
- What triggers a shuffle and why is it expensive?

**Iceberg:**
- How does Iceberg enable time-travel queries?
- What's in those metadata files?
- How does schema evolution work without rewriting data?

**Integration:**
- How does the whole pipeline maintain consistency?
- What happens when Spark fails mid-batch?
- How do checkpoints coordinate with Kafka offsets?

---

Start with → [01-kafka-fundamentals.md](01-kafka-fundamentals.md)

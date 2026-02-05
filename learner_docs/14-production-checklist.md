# Production Checklist

This document provides a comprehensive checklist for deploying your Kafka → Spark → Iceberg pipeline to production.

---

## Development vs Production Comparison

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                   DEVELOPMENT → PRODUCTION                                   │
│                                                                             │
│  Component      Development              Production                         │
│  ─────────────────────────────────────────────────────────────────────────  │
│  Kafka          1 broker                 3+ brokers                         │
│  Replication    1                        3                                  │
│  Partitions     1                        6-12+                              │
│  Spark          Local mode               Cluster mode                       │
│  Executors      1 (driver)               6+                                 │
│  Checkpoints    Local filesystem         S3/HDFS                            │
│  Iceberg        Local filesystem         S3/HDFS                            │
│  Monitoring     Console logs             Prometheus + Grafana               │
│  Secrets        Hardcoded                Vault/K8s Secrets                  │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Pre-Deployment Checklist

### ☐ Infrastructure

```yaml
# [ ] Multi-broker Kafka cluster
# [ ] Distributed storage (S3/HDFS/GCS)
# [ ] Spark cluster (Standalone/YARN/K8s)
# [ ] Load balancer for Kafka
# [ ] Container orchestration (Kubernetes recommended)
# [ ] Monitoring stack (Prometheus + Grafana)
# [ ] Log aggregation (ELK/Loki)
```

### ☐ Kafka Configuration

```yaml
# docker-compose.prod.yml
services:
  kafka:
    environment:
      # Cluster
      KAFKA_BROKER_ID: 1  # Unique per broker
      
      # Replication
      KAFKA_DEFAULT_REPLICATION_FACTOR: 3
      KAFKA_MIN_INSYNC_REPLICAS: 2
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 3
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 3
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 2
      
      # Durability
      KAFKA_UNCLEAN_LEADER_ELECTION_ENABLE: "false"
      KAFKA_LOG_FLUSH_INTERVAL_MESSAGES: 10000
      
      # Retention
      KAFKA_LOG_RETENTION_HOURS: 168  # 7 days
      KAFKA_LOG_RETENTION_BYTES: 107374182400  # 100GB
      KAFKA_LOG_SEGMENT_BYTES: 1073741824  # 1GB
      
      # Performance
      KAFKA_NUM_NETWORK_THREADS: 8
      KAFKA_NUM_IO_THREADS: 16
      KAFKA_SOCKET_SEND_BUFFER_BYTES: 102400
      KAFKA_SOCKET_RECEIVE_BUFFER_BYTES: 102400
      KAFKA_SOCKET_REQUEST_MAX_BYTES: 104857600
      
      # Security (if needed)
      # KAFKA_SECURITY_PROTOCOL: SASL_SSL
      # KAFKA_SASL_MECHANISM: SCRAM-SHA-512
```

### ☐ Topic Configuration

```bash
# Create production topic
kafka-topics --bootstrap-server kafka1:9092 \
  --create \
  --topic prices \
  --partitions 12 \
  --replication-factor 3 \
  --config retention.ms=604800000 \
  --config min.insync.replicas=2 \
  --config cleanup.policy=delete
```

---

### ☐ Producer Configuration

```python
# producer/api_producer.py (production version)
from kafka import KafkaProducer
import ssl

producer = KafkaProducer(
    # Cluster connection
    bootstrap_servers=[
        "kafka1.prod.example.com:9092",
        "kafka2.prod.example.com:9092",
        "kafka3.prod.example.com:9092",
    ],
    
    # Serialization
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=str.encode,
    
    # Delivery guarantees
    acks='all',
    enable_idempotence=True,
    retries=3,
    max_in_flight_requests_per_connection=5,
    
    # Performance
    batch_size=32768,  # 32KB
    linger_ms=20,
    compression_type='lz4',
    buffer_memory=67108864,  # 64MB
    
    # Timeouts
    request_timeout_ms=30000,
    max_block_ms=60000,
    
    # Security (uncomment for SASL)
    # security_protocol='SASL_SSL',
    # sasl_mechanism='SCRAM-SHA-512',
    # sasl_plain_username=os.environ['KAFKA_USER'],
    # sasl_plain_password=os.environ['KAFKA_PASSWORD'],
    # ssl_cafile='/path/to/ca.pem',
)
```

---

### ☐ Spark Configuration

```python
# spark/stream_to_iceberg.py (production version)
spark = SparkSession.builder \
    .appName("CryptoToIceberg-Prod") \
    .master("spark://spark-master:7077") \
    
    # Executors
    .config("spark.executor.instances", "6") \
    .config("spark.executor.cores", "4") \
    .config("spark.executor.memory", "8g") \
    .config("spark.driver.memory", "4g") \
    
    # Parallelism
    .config("spark.sql.shuffle.partitions", "12") \
    .config("spark.default.parallelism", "24") \
    
    # Streaming
    .config("spark.streaming.backpressure.enabled", "true") \
    .config("spark.streaming.kafka.maxRatePerPartition", "10000") \
    
    # Iceberg
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.prod", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.prod.type", "hadoop") \
    .config("spark.sql.catalog.prod.warehouse", "s3a://your-bucket/iceberg") \
    
    # S3 Configuration
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
    .config("spark.hadoop.fs.s3a.access.key", os.environ['AWS_ACCESS_KEY']) \
    .config("spark.hadoop.fs.s3a.secret.key", os.environ['AWS_SECRET_KEY']) \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    
    # Memory management
    .config("spark.memory.fraction", "0.8") \
    .config("spark.memory.storageFraction", "0.3") \
    
    # Metrics
    .config("spark.sql.streaming.metricsEnabled", "true") \
    .config("spark.metrics.conf.*.sink.prometheus.class", 
            "org.apache.spark.metrics.sink.PrometheusSink") \
    
    .getOrCreate()
```

---

### ☐ Iceberg Configuration

```sql
-- Create production table
CREATE TABLE prod.default.crypto_prices (
    timestamp TIMESTAMP,
    symbol STRING,
    usd_price DOUBLE,
    volume_24h DOUBLE,
    ingestion_time TIMESTAMP
) USING iceberg
PARTITIONED BY (days(timestamp), symbol)
TBLPROPERTIES (
    'write.format.default' = 'parquet',
    'write.parquet.compression-codec' = 'zstd',
    'write.target-file-size-bytes' = '134217728',
    'write.distribution-mode' = 'hash',
    'format-version' = '2',
    'write.metadata.delete-after-commit.enabled' = 'true',
    'write.metadata.previous-versions-max' = '100',
    'history.expire.max-snapshot-age-ms' = '604800000'
);
```

---

### ☐ Checkpoint Location

```python
# Use distributed storage for checkpoints
.option("checkpointLocation", "s3a://your-bucket/checkpoints/crypto-prices")

# Or HDFS
.option("checkpointLocation", "hdfs://namenode:8020/checkpoints/crypto-prices")
```

---

## Security Checklist

### ☐ Kafka Security

```yaml
# Enable SASL/SSL
environment:
  KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: SASL_SSL:SASL_SSL
  KAFKA_SECURITY_INTER_BROKER_PROTOCOL: SASL_SSL
  KAFKA_SASL_MECHANISM_INTER_BROKER_PROTOCOL: SCRAM-SHA-512
  KAFKA_SASL_ENABLED_MECHANISMS: SCRAM-SHA-512
  
  # SSL
  KAFKA_SSL_KEYSTORE_LOCATION: /etc/kafka/secrets/kafka.keystore.jks
  KAFKA_SSL_KEYSTORE_PASSWORD: ${KEYSTORE_PASSWORD}
  KAFKA_SSL_KEY_PASSWORD: ${KEY_PASSWORD}
  KAFKA_SSL_TRUSTSTORE_LOCATION: /etc/kafka/secrets/kafka.truststore.jks
  KAFKA_SSL_TRUSTSTORE_PASSWORD: ${TRUSTSTORE_PASSWORD}
```

### ☐ Secrets Management

```yaml
# Kubernetes secrets
apiVersion: v1
kind: Secret
metadata:
  name: kafka-credentials
type: Opaque
stringData:
  username: producer-user
  password: ${KAFKA_PASSWORD}
  
---
# Use in deployment
env:
  - name: KAFKA_USER
    valueFrom:
      secretKeyRef:
        name: kafka-credentials
        key: username
  - name: KAFKA_PASSWORD
    valueFrom:
      secretKeyRef:
        name: kafka-credentials
        key: password
```

### ☐ Network Security

```yaml
# Network policies (Kubernetes)
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: kafka-policy
spec:
  podSelector:
    matchLabels:
      app: kafka
  policyTypes:
    - Ingress
  ingress:
    - from:
        - podSelector:
            matchLabels:
              app: spark
        - podSelector:
            matchLabels:
              app: producer
      ports:
        - port: 9092
```

---

## Monitoring Checklist

### ☐ Metrics Collection

```yaml
# Prometheus scrape config
scrape_configs:
  - job_name: 'kafka'
    static_configs:
      - targets: ['kafka1:9090', 'kafka2:9090', 'kafka3:9090']
    
  - job_name: 'spark'
    static_configs:
      - targets: ['spark-master:4040']
```

### ☐ Key Dashboards

```
Dashboard 1: Kafka Overview
─────────────────────────────
• Messages in/out per second
• Consumer lag by topic
• Partition leadership distribution
• Under-replicated partitions
• Disk usage

Dashboard 2: Spark Streaming
─────────────────────────────
• Input rows per second
• Processing time per batch
• Batch duration trend
• Memory usage
• GC time

Dashboard 3: Iceberg Tables
─────────────────────────────
• Snapshot count
• Data file count
• Table size growth
• Query latency
• Compaction status
```

### ☐ Alerting Rules

```yaml
# Prometheus alerting rules
groups:
  - name: kafka-alerts
    rules:
      - alert: KafkaConsumerLagHigh
        expr: kafka_consumer_lag > 10000
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "High consumer lag detected"
          
      - alert: KafkaUnderReplicatedPartitions
        expr: kafka_server_replica_manager_under_replicated_partitions > 0
        for: 1m
        labels:
          severity: critical
        annotations:
          summary: "Under-replicated partitions detected"

  - name: spark-alerts
    rules:
      - alert: SparkStreamingBatchDelayed
        expr: spark_streaming_last_batch_duration_ms > 60000
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "Spark streaming batch taking too long"
```

---

## Deployment Checklist

### ☐ Kubernetes Deployment

```yaml
# spark-streaming.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: spark-streaming
spec:
  replicas: 1  # Only 1 driver
  selector:
    matchLabels:
      app: spark-streaming
  template:
    metadata:
      labels:
        app: spark-streaming
    spec:
      containers:
        - name: spark
          image: your-registry/spark-iceberg:3.5.1
          resources:
            requests:
              memory: "4Gi"
              cpu: "2"
            limits:
              memory: "8Gi"
              cpu: "4"
          env:
            - name: SPARK_MASTER
              value: "spark://spark-master:7077"
          volumeMounts:
            - name: spark-config
              mountPath: /opt/spark/conf
          command:
            - spark-submit
            - --master
            - spark://spark-master:7077
            - --deploy-mode
            - client
            - /opt/spark-apps/stream_to_iceberg.py
      volumes:
        - name: spark-config
          configMap:
            name: spark-config
```

### ☐ Health Checks

```yaml
# Liveness and readiness probes
livenessProbe:
  httpGet:
    path: /health
    port: 4040
  initialDelaySeconds: 60
  periodSeconds: 10

readinessProbe:
  httpGet:
    path: /ready
    port: 4040
  initialDelaySeconds: 30
  periodSeconds: 5
```

---

## Maintenance Checklist

### ☐ Regular Tasks

```bash
# Weekly: Expire old snapshots
spark-sql -e "CALL prod.system.expire_snapshots('default.crypto_prices', TIMESTAMP '$(date -d '7 days ago' +%Y-%m-%d) 00:00:00', 10)"

# Weekly: Remove orphan files
spark-sql -e "CALL prod.system.remove_orphan_files('default.crypto_prices')"

# Monthly: Rewrite data files (compaction)
spark-sql -e "CALL prod.system.rewrite_data_files('default.crypto_prices')"

# Quarterly: Analyze table statistics
spark-sql -e "ANALYZE TABLE prod.default.crypto_prices COMPUTE STATISTICS"
```

### ☐ Backup Strategy

```bash
# Checkpoint backup
aws s3 sync s3://prod-bucket/checkpoints s3://backup-bucket/checkpoints

# Iceberg metadata backup (point-in-time recovery)
aws s3 sync s3://prod-bucket/iceberg/default/crypto_prices/metadata \
           s3://backup-bucket/iceberg/default/crypto_prices/metadata
```

---

## Final Verification

### ☐ End-to-End Test

```python
# test_pipeline.py
import time
from kafka import KafkaProducer, KafkaConsumer

def test_end_to_end():
    # 1. Send test message
    producer = KafkaProducer(bootstrap_servers='kafka:9092')
    test_msg = {"timestamp": "2026-01-01T00:00:00Z", "usd_price": 99999.99, "test": True}
    producer.send("prices", json.dumps(test_msg).encode())
    producer.flush()
    
    # 2. Wait for processing
    time.sleep(30)
    
    # 3. Verify in Iceberg
    result = spark.sql("""
        SELECT * FROM prod.default.crypto_prices 
        WHERE usd_price = 99999.99
    """).collect()
    
    assert len(result) == 1, "Test message not found in Iceberg"
    print("✓ End-to-end test passed")
    
    # 4. Cleanup test data
    spark.sql("""
        DELETE FROM prod.default.crypto_prices 
        WHERE usd_price = 99999.99
    """)
```

### ☐ Load Test

```python
# load_test.py
import concurrent.futures
from kafka import KafkaProducer

def send_messages(count):
    producer = KafkaProducer(bootstrap_servers='kafka:9092')
    for i in range(count):
        producer.send("prices", json.dumps({
            "timestamp": f"2026-01-01T00:00:{i:02d}Z",
            "usd_price": 50000 + i
        }).encode())
    producer.flush()

# Send 10000 messages in parallel
with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
    futures = [executor.submit(send_messages, 1000) for _ in range(10)]
    concurrent.futures.wait(futures)

# Measure processing time and verify count
```

---

## Summary: Production vs Development

| Aspect | Development | Production |
|--------|-------------|------------|
| Kafka brokers | 1 | 3+ |
| Replication | 1 | 3 |
| Partitions | 1 | 12+ |
| Producer acks | 1 | all |
| Idempotence | No | Yes |
| Spark mode | Local | Cluster |
| Executors | 1 | 6+ |
| Storage | Local | S3/HDFS |
| Checkpoints | Local | S3/HDFS |
| Monitoring | Logs | Prometheus |
| Alerting | None | PagerDuty |
| Security | None | SASL+SSL |
| Backup | None | Daily |

---

## Quick Reference Commands

```bash
# Check Kafka cluster health
kafka-metadata.sh --snapshot /tmp/kraft-logs/__cluster_metadata-0/00000000000000000000.log --command "topic list"

# Check consumer lag
kafka-consumer-groups --bootstrap-server kafka:9092 --all-groups --describe

# Check Spark streaming status
curl http://spark-driver:4040/api/v1/applications

# Check Iceberg table
spark-sql -e "SELECT * FROM prod.default.crypto_prices.snapshots"

# Emergency: Reset streaming to latest
rm -rf /checkpoint/path
# Restart with startingOffsets="latest"
```

---

Congratulations! You now have comprehensive knowledge of the Kafka → Spark → Iceberg pipeline!

Return to: [00-learning-path.md](00-learning-path.md)

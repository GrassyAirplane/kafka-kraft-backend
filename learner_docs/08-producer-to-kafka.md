# Data Flow: Producer to Kafka

This document explains exactly what happens when your Python producer sends messages to Kafka.

---

## The Complete Flow

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           PRODUCER FLOW                                     │
│                                                                             │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐    ┌───────────┐ │
│  │   CoinGecko  │    │   Python     │    │    Kafka     │    │   Topic   │ │
│  │     API      │───▶│   Producer   │───▶│   Protocol   │───▶│  Partition│ │
│  └──────────────┘    └──────────────┘    └──────────────┘    └───────────┘ │
│                                                                             │
│       JSON              Dict              Bytes              Stored         │
│      Response          Object           (Serialized)        on Disk        │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Step 1: API Request

```python
API_URL = "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin&vs_currencies=usd"

response = requests.get(API_URL, timeout=10)
data = response.json()
```

**Response from CoinGecko:**
```json
{
  "bitcoin": {
    "usd": 76123.0
  }
}
```

---

## Step 2: Create Event Object

```python
event = {
    "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    "usd_price": float(data["bitcoin"]["usd"])
}
```

**Result:**
```python
{
    "timestamp": "2026-02-04T12:21:00Z",
    "usd_price": 76123.0
}
```

---

## Step 3: Producer Configuration

```python
producer = KafkaProducer(
    bootstrap_servers="localhost:29092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)
```

### Configuration Breakdown

| Parameter | Value | Purpose |
|-----------|-------|---------|
| `bootstrap_servers` | localhost:29092 | Initial broker to connect to |
| `value_serializer` | lambda... | Converts Python dict to bytes |

### What bootstrap_servers Does

```
1. Producer connects to localhost:29092
   ▼
2. Kafka returns cluster metadata
   ▼
3. Producer learns about all brokers and partitions
   ▼
4. Producer caches this metadata
   ▼
5. Future sends go directly to partition leaders
```

### Serialization Flow

```
Python Dict                    JSON String                    Bytes
{"timestamp": ...}  ──▶  '{"timestamp": ...}'  ──▶  b'{"timestamp": ...}'
                    json.dumps()              .encode("utf-8")
```

---

## Step 4: Send Message

```python
producer.send("prices", event)
```

### What Happens Inside

```
1. Serializer converts event to bytes
   │
   ▼
2. Partitioner decides which partition
   │   (with no key, uses round-robin or sticky partitioning)
   ▼
3. Message added to batch buffer
   │   (batching improves throughput)
   ▼
4. When batch is full OR linger.ms expires
   │
   ▼
5. Batch sent to broker
   │
   ▼
6. Broker writes to partition log
   │
   ▼
7. Broker sends acknowledgment
```

---

## Step 5: Partitioning

### How Partitioning Works

```python
# No key specified - Kafka uses "sticky" partitioning
producer.send("prices", event)

# With key - same key always goes to same partition
producer.send("prices", key=b"BTC", value=event)
```

**Sticky Partitioning (default when no key):**
```
Messages 1-10 → Partition 0  (same batch)
Messages 11-20 → Partition 1  (next batch)
Messages 21-30 → Partition 0  (rotate back)
```

**Key-based Partitioning:**
```
hash(key) % num_partitions = target_partition

hash("BTC") % 3 = 1  → Always partition 1
hash("ETH") % 3 = 2  → Always partition 2
```

---

## Step 6: Batching

Producer batches messages for efficiency:

```
┌────────────────────────────────────────────────────────────────┐
│                      PRODUCER BATCH BUFFER                      │
│                                                                │
│  ┌────────────┐  ┌────────────┐  ┌────────────┐               │
│  │ Partition 0│  │ Partition 1│  │ Partition 2│               │
│  │            │  │            │  │            │               │
│  │ msg1       │  │ (empty)    │  │ (empty)    │               │
│  │ msg2       │  │            │  │            │               │
│  │ msg3       │  │            │  │            │               │
│  └────────────┘  └────────────┘  └────────────┘               │
│                                                                │
│  Batch sent when:                                              │
│  • Batch size reached (batch.size = 16KB)                      │
│  • Time limit reached (linger.ms = 0)                          │
│  • send() with flush() called                                  │
└────────────────────────────────────────────────────────────────┘
```

---

## Step 7: Network Protocol

Kafka uses a binary protocol:

```
┌──────────────────────────────────────────────────────────────────┐
│                    PRODUCE REQUEST                                │
│                                                                  │
│  ┌─────────────────────────────────────────────────────────────┐ │
│  │ Header                                                       │ │
│  │  • API Key: 0 (Produce)                                     │ │
│  │  • Version: 9                                               │ │
│  │  • Correlation ID: 12345                                    │ │
│  │  • Client ID: "kafka-python-producer"                       │ │
│  └─────────────────────────────────────────────────────────────┘ │
│                                                                  │
│  ┌─────────────────────────────────────────────────────────────┐ │
│  │ Body                                                         │ │
│  │  • Transactional ID: null                                   │ │
│  │  • Acks: 1                                                  │ │
│  │  • Timeout: 30000ms                                         │ │
│  │  • Topic: "prices"                                          │ │
│  │  • Partition: 0                                             │ │
│  │  • Records: [batch of compressed messages]                  │ │
│  └─────────────────────────────────────────────────────────────┘ │
│                                                                  │
└──────────────────────────────────────────────────────────────────┘
```

---

## Step 8: Broker Processing

```
1. Broker receives Produce request
   ▼
2. Validates message format and size
   ▼
3. Appends to partition log file
   │   /tmp/kraft-logs/prices-0/00000000000000000000.log
   ▼
4. Updates in-memory index
   ▼
5. If replication > 1:
   │   Waits for followers to replicate
   ▼
6. Sends ProduceResponse with new offset
```

---

## Step 9: Acknowledgments

```python
# Default: acks=1 (leader only)
producer = KafkaProducer(
    bootstrap_servers="localhost:29092",
    acks=1  # 0, 1, or 'all'
)
```

| Acks | Behavior | Durability | Latency |
|------|----------|------------|---------|
| 0 | Don't wait | Lowest | Fastest |
| 1 | Wait for leader | Medium | Medium |
| 'all' | Wait for all ISR | Highest | Slowest |

---

## Message Structure in Kafka

Once stored, your message looks like:

```
┌────────────────────────────────────────────────────────────────┐
│                    MESSAGE IN PARTITION LOG                     │
│                                                                │
│  Offset: 42                                                    │
│  Timestamp: 1770207660000 (Unix millis)                        │
│  Key: null                                                     │
│  Value: b'{"timestamp":"2026-02-04T12:21:00Z","usd_price":76123}' │
│  Headers: []                                                   │
│  Checksum: 0x1A2B3C4D                                          │
│                                                                │
└────────────────────────────────────────────────────────────────┘
```

---

## Producer Best Practices

### 1. Error Handling

```python
from kafka.errors import KafkaError

try:
    future = producer.send("prices", event)
    record_metadata = future.get(timeout=10)  # Block for ack
    print(f"Sent to partition {record_metadata.partition} offset {record_metadata.offset}")
except KafkaError as e:
    print(f"Failed to send: {e}")
```

### 2. Graceful Shutdown

```python
producer.flush()  # Wait for all messages to be sent
producer.close()  # Clean up connections
```

### 3. Idempotent Producer

```python
producer = KafkaProducer(
    bootstrap_servers="localhost:29092",
    enable_idempotence=True,  # Prevents duplicates on retry
    acks='all',
    retries=3,
)
```

### 4. Compression

```python
producer = KafkaProducer(
    bootstrap_servers="localhost:29092",
    compression_type='lz4',  # or 'gzip', 'snappy', 'zstd'
)
```

---

## Your Producer Summary

```python
# Current implementation
producer = KafkaProducer(
    bootstrap_servers="localhost:29092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

# Production-ready version
producer = KafkaProducer(
    bootstrap_servers="localhost:29092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=str.encode,
    acks='all',
    retries=3,
    enable_idempotence=True,
    compression_type='lz4',
    linger_ms=20,  # Small batching delay
    batch_size=32768,  # 32KB batches
)
```

---

## Exercises

### Exercise 1: Add Message Key

Modify your producer to use "BTC" as the key:
```python
producer.send("prices", key=b"BTC", value=event)
```

### Exercise 2: Add Callback

```python
def on_send_success(record_metadata):
    print(f"Sent to {record_metadata.topic}:{record_metadata.partition}@{record_metadata.offset}")

def on_send_error(excp):
    print(f"Error: {excp}")

producer.send("prices", event).add_callback(on_send_success).add_errback(on_send_error)
```

### Exercise 3: View Messages

```powershell
docker exec -it kafka kafka-console-consumer \
  --bootstrap-server localhost:29092 \
  --topic prices \
  --from-beginning \
  --property print.timestamp=true \
  --property print.offset=true
```

---

Next: [09-kafka-to-spark.md](09-kafka-to-spark.md) - How Spark consumes from Kafka

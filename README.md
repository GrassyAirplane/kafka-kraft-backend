# Kafka KRaft + Iceberg Streaming Pipeline

A real-time data pipeline that streams Bitcoin prices from CoinGecko API → Kafka → Spark → Iceberg.

## Prerequisites

- Docker & Docker Compose
- UV (Python package manager) - optional for local producer

## Quick Start

### 1. Start the Infrastructure

```powershell
docker compose up -d
```

This starts:
- **Kafka** (KRaft mode) on port 29092
- **Spark** with Iceberg pre-installed

### 2. Verify Kafka is Running

```powershell
docker exec -it kafka kafka-topics --bootstrap-server localhost:29092 --list
```

Should show: `prices`

### 3. Start the Producer (sends Bitcoin prices to Kafka)

```powershell
cd producer
uv run api_producer.py
```

### 4. Start the Spark Streaming Job (writes to Iceberg)

```powershell
docker exec -it spark /opt/spark/bin/spark-submit /opt/spark-apps/stream_to_iceberg.py
```

### 5. Query the Iceberg Table

```powershell
# Interactive Spark SQL shell
docker exec -it spark /opt/spark/bin/spark-sql

# Then run SQL:
# SELECT * FROM local.default.crypto_prices LIMIT 10;
# SELECT COUNT(*) FROM local.default.crypto_prices;
```

Or run a single query:

```powershell
docker exec -it spark /opt/spark/bin/spark-sql -e "SELECT * FROM local.default.crypto_prices LIMIT 10;"
```

---

## Useful Commands

### Kafka

```powershell
# List topics
docker exec -it kafka kafka-topics --bootstrap-server localhost:29092 --list

# Describe a topic
docker exec -it kafka kafka-topics --bootstrap-server localhost:29092 --describe --topic prices

# Read messages from topic
docker exec -it kafka kafka-console-consumer --bootstrap-server localhost:29092 --topic prices --from-beginning

# List consumer groups
docker exec -it kafka kafka-consumer-groups --bootstrap-server localhost:29092 --list
```

### Spark

```powershell
# Spark SQL shell
docker exec -it spark /opt/spark/bin/spark-sql

# PySpark shell
docker exec -it spark /opt/spark/bin/pyspark

# Scala Spark shell
docker exec -it spark /opt/spark/bin/spark-shell

# Submit a job
docker exec -it spark /opt/spark/bin/spark-submit /opt/spark-apps/stream_to_iceberg.py
```

### Docker

```powershell
# View running containers
docker ps

# View logs
docker logs kafka
docker logs spark

# Stop everything
docker compose down

# Rebuild Spark image (after Dockerfile changes)
docker compose build spark
docker compose up -d spark
```

---

## Project Structure

```
kafka-kraft-iceberg/
├── docker-compose.yml      # Infrastructure definition
├── producer/
│   ├── api_producer.py     # Fetches Bitcoin prices → Kafka
│   └── pyproject.toml      # UV dependencies
├── spark/
│   ├── Dockerfile          # Spark + Iceberg image
│   └── stream_to_iceberg.py # Kafka → Iceberg streaming job
└── warehouse/
    └── iceberg/            # Iceberg table data
```



PS C:\Users\Lameb\OneDrive\Desktop\projects\kafka-kraft-iceberg\producer> docker exec -it spark /opt/spark/bin/spark-sql --conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog --conf spark.sql.catalog.local.type=hadoop --conf spark.sql.catalog.local.warehouse=/opt/warehouse/iceberg --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
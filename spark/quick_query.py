from pyspark.sql import SparkSession

# Simple session without Hive metastore - no conflicts
spark = (
    SparkSession.builder
    .appName("QuickQuery")
    .config("spark.sql.catalogImplementation", "in-memory")
    .getOrCreate()
)

# Read Parquet files directly
df = spark.read.parquet("/opt/warehouse/iceberg/default/crypto_prices/data/")
df.orderBy("timestamp", ascending=False).show(20, truncate=False)
print(f"\nTotal records: {df.count()}")

spark.stop()

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, DoubleType

spark = (
    SparkSession.builder
    .appName("KafkaKRaftToIceberg")
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.local.type", "hadoop")
    .config("spark.sql.catalog.local.warehouse", "/opt/warehouse/iceberg")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .getOrCreate()
)

schema = StructType() \
    .add("timestamp", StringType()) \
    .add("usd_price", DoubleType())

df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "prices")
    .load()
)

parsed = (
    df.select(from_json(col("value").cast("string"), schema).alias("data"))
      .select("data.*")
)

spark.sql("""
CREATE TABLE IF NOT EXISTS local.default.crypto_prices (
  timestamp STRING,
  usd_price DOUBLE
)
USING iceberg
""")

(
    parsed.writeStream
    .format("iceberg")
    .outputMode("append")
    .option("checkpointLocation", "/opt/warehouse/checkpoint/prices")
    .toTable("local.default.crypto_prices")
    .awaitTermination()
)

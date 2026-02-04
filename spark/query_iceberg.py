from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("IcebergQuery")
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.local.type", "hadoop")
    .config("spark.sql.catalog.local.warehouse", "/opt/warehouse/iceberg")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .getOrCreate()
)

# Query the Iceberg table
df = spark.sql("SELECT * FROM local.default.crypto_prices ORDER BY timestamp DESC LIMIT 10")
df.show(truncate=False)

# Count total records
count = spark.sql("SELECT COUNT(*) as total FROM local.default.crypto_prices")
count.show()

spark.stop()

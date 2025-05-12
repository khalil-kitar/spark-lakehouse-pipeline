from pyspark.sql import SparkSession
from pyspark.sql.functions import col, monotonically_increasing_id

spark = SparkSession.builder \
    .appName("Silver to Gold - Fact Ship Movements") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://192.168.49.2:30704") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

df_silver = spark.read.format("delta").load("s3a://ais-data/silver/ais_clean")

df_fact = df_silver.select(
    monotonically_increasing_id().alias("movement_id"),
    col("timestamp"),
    col("imo"),
    col("sog"),
    col("cog"),
    col("rot"),
    col("heading"),
    col("latitude"),
    col("longitude"),
    col("draught"),
    col("destination")
)

df_fact.write.format("delta").mode("overwrite").save("s3a://ais-data/gold/fact_ship_movements/")



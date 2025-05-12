from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import TimestampType

spark = SparkSession.builder \
    .appName("AIS Bronze to Silver") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://192.168.49.2:30704") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

df_bronze = spark.read.format("delta").load("s3a://ais-data/bronze/ais_data")

df_clean = df_bronze \
    .filter(col("mmsi").isNotNull() & col("timestamp").isNotNull()) \
    .dropDuplicates(["timestamp", "mmsi", "latitude", "longitude"])

df_clean = df_clean \
    .withColumn("navigational_status", lower(trim(col("navigational_status")))) \
    .withColumn("ship_type", lower(trim(col("ship_type")))) \
    .withColumn("cargo_type", lower(trim(col("cargo_type")))) \
    .withColumn("eta_ts", to_timestamp(col("eta"), "dd/MM/yyyy HH:mm"))

df_clean.write.format("delta") \
    .mode("overwrite") \
    .save("s3a://ais-data/silver/ais_clean")



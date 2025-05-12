from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp
from pyspark.sql.types import *

spark = SparkSession.builder \
    .appName("AIS Raw to Bronze") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://192.168.49.2:30704") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

schema = StructType([
    StructField("timestamp", StringType(), True),
    StructField("type_of_mobile", StringType(), True),
    StructField("mmsi", LongType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("navigational_status", StringType(), True),
    StructField("rot", DoubleType(), True),
    StructField("sog", DoubleType(), True),
    StructField("cog", DoubleType(), True),
    StructField("heading", DoubleType(), True),
    StructField("imo", LongType(), True),
    StructField("callsign", StringType(), True),
    StructField("name", StringType(), True),
    StructField("ship_type", StringType(), True),
    StructField("cargo_type", StringType(), True),
    StructField("width", DoubleType(), True),
    StructField("length", DoubleType(), True),
    StructField("position_fixing_device_type", StringType(), True),
    StructField("draught", DoubleType(), True),
    StructField("destination", StringType(), True),
    StructField("eta", StringType(), True),
    StructField("data_source_type", StringType(), True),
    StructField("size_a", DoubleType(), True),
    StructField("size_b", DoubleType(), True),
    StructField("size_c", DoubleType(), True),
    StructField("size_d", DoubleType(), True),
])

df_raw = spark.read \
    .option("header", "true") \
    .schema(schema) \
    .csv("s3a://ais-data/raw/ais_data.csv")

df_bronze = df_raw.withColumn(
    "timestamp",
    to_timestamp(col("timestamp"), "dd/MM/yyyy HH:mm:ss")
)

df_bronze.write.format("delta") \
    .mode("overwrite") \
    .save("s3a://ais-data/bronze/ais_data")



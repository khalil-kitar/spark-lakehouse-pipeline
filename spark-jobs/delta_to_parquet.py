from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Export Delta to Parquet") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://192.168.49.2:30704") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()


df_ship_type_category = spark.read.format("delta").load("s3a://ais-data/gold/dim_ship_type_category")
df_vessel = spark.read.format("delta").load("s3a://ais-data/gold/dim_vessel")
df_fact = spark.read.format("delta").load("s3a://ais-data/gold/fact_ship_movements")
df_ship_type= spark.read.format("delta").load("s3a://ais-data/gold/dim_ship_type")
df_time= spark.read.format("delta").load("s3a://ais-data/gold/dim_time")



df_fact.write.mode("overwrite").parquet("s3a://ais-data/gold/fact_ship_movements.parquet")
df_ship_type.write.mode("overwrite").parquet("s3a://ais-data/gold/dim_ship_type.parquet")
df_ship_type_category.write.mode("overwrite").parquet("s3a://ais-data/gold/dim_ship_type_category.parquet")
df_vessel.write.mode("overwrite").parquet("s3a://ais-data/gold/dim_vessel.parquet")
df_time.write.mode("overwrite").parquet("s3a://ais-data/gold/dim_time.parquet")




from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sha2, concat_ws, lit
from delta.tables import DeltaTable
from pyspark.sql.functions import year, month, dayofmonth, hour, minute, second, date_format


spark = SparkSession.builder \
    .appName("AIS Silver to Gold Dimensions") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://192.168.49.2:30704") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

df_silver = spark.read.format("delta").load("s3a://ais-data/silver/ais_clean")

ship_type_category_map = {
    "Cargo": [70, 71, 72, 73, 74, 75, 76, 77, 78, 79],
    "Tanker": [80, 81, 82, 83, 84, 85, 86, 87, 88, 89],
    "Passenger": [60, 61, 62, 63, 64, 65, 66, 67, 68, 69],
    "Tug": [52],
    "Fishing": [30],
    "Other": []
}

def map_category(ship_type):
    for cat, codes in ship_type_category_map.items():
        if ship_type in codes:
            return cat
    return "Other"

from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

map_category_udf = udf(map_category, StringType())

df_silver = df_silver.withColumn("ship_type_category", map_category_udf(col("ship_type")))


# dim_ship_type_category
df_category = df_silver.select("ship_type_category").dropDuplicates()
df_category = df_category.withColumn("ship_type_category_id", sha2("ship_type_category", 256))

df_category.select("ship_type_category_id", "ship_type_category") \
    .write.format("delta") \
    .mode("overwrite") \
    .save("s3a://ais-data/gold/dim_ship_type_category")

# dim_ship_type
df_ship_type = df_silver.select("ship_type").dropDuplicates()
df_ship_type = df_ship_type.withColumn("ship_type_id", sha2(col("ship_type").cast("string"), 256))
df_ship_type = df_ship_type.join(df_silver.select("ship_type", "ship_type_category").dropDuplicates(),
                                 on="ship_type", how="left") \
    .join(df_category, on="ship_type_category", how="left") \
    .select("ship_type_id", col("ship_type").alias("ship_type_code"), "ship_type_category_id")

df_ship_type.write.format("delta") \
    .mode("overwrite") \
    .save("s3a://ais-data/gold/dim_ship_type")

# dim_vessel 
df_vessel = df_silver.select(
    "mmsi", "imo", "callsign", "name", "type_of_mobile", "length", "width"
).dropDuplicates()


df_vessel = df_vessel.withColumn("vessel_id", sha2(concat_ws("-", "mmsi", "imo", "name"), 256))

df_vessel.write.format("delta") \
    .mode("overwrite") \
    .save("s3a://ais-data/gold/dim_vessel")
    
df_time = df_silver.select("base_date_time").dropDuplicates()

df_time = df_time.withColumn("date_id", date_format("base_date_time", "yyyyMMddHHmmss")) \
        .withColumn("year", year("base_date_time")) \
        .withColumn("month", month("base_date_time")) \
        .withColumn("day", dayofmonth("base_date_time")) \
        .withColumn("hour", hour("base_date_time")) \
        .withColumn("minute", minute("base_date_time")) \
        .withColumn("second", second("base_date_time")) \
        .withColumn("date", date_format("base_date_time", "yyyy-MM-dd")) \
        .withColumn("time", date_format("base_date_time", "HH:mm:ss"))
        
df_time.write.format("delta") \
        .mode("overwrite") \
        .save("s3a://ais-data/gold/dim_time")


from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Landing(spark: SparkSession) -> DataFrame:
    return spark.read\
        .format("parquet")\
        .schema(
          StructType([
            StructField("VendorID", IntegerType(), True), StructField("tpep_pickup_datetime", TimestampType(), True), StructField("tpep_dropoff_datetime", TimestampType(), True), StructField("passenger_count", DoubleType(), True), StructField("trip_distance", DoubleType(), True), StructField("RatecodeID", DoubleType(), True), StructField("store_and_fwd_flag", StringType(), True), StructField("PULocationID", IntegerType(), True), StructField("DOLocationID", IntegerType(), True), StructField("payment_type", IntegerType(), True), StructField("fare_amount", DoubleType(), True), StructField("extra", DoubleType(), True), StructField("mta_tax", DoubleType(), True), StructField("tip_amount", DoubleType(), True), StructField("tolls_amount", DoubleType(), True), StructField("improvement_surcharge", DoubleType(), True), StructField("total_amount", DoubleType(), True), StructField("congestion_surcharge", DoubleType(), True), StructField("airport_fee", DoubleType(), True)
        ])
        )\
        .load("dbfs:/mnt/landing/nyctaxi/yellow/yellow_tripdata_2021-01.parquet")

import os
import oci
from urllib.parse import urlparse

from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import functions as F

# Create a Spark Session
spark = SparkSession \
    .builder \
    .appName("Data Flow App") \
    .config("spark.hadoop.fs.s3a.access.key", "AKIAYYHKSFYEGNEUTIEX") \
    .config("spark.hadoop.fs.s3a.secret.key", "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") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

print("Reading data from object storage !")

src_df = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .option("multiLine", "true")
    .load(
        "s3a://publicbucketvv/201306-citibike-tripdata.csv" # Datafile location in OCI Object Storage
    )
    .cache()
)  # cache the dataset to increase computing speed

src_df.show(5)
#src_df.printSchema()
# Replace the space in column names with '_'
src_df = src_df.select([F.col(col).alias(col.replace(' ', '_')) for col in src_df.columns])

# Write data to paraquet file on OCI Object Storage Bucket
src_df.write.mode("overwrite").parquet("oci://Public_Bucket@orasenatdpltintegration01/201306-citibike-tripdata.parquet")

print("Successfully converted {} rows to Parquet and wrote to OCI.".format(src_df.count()))

spark.stop()
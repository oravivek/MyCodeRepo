import os
from urllib.parse import urlparse

from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Create a Spark Session
spark = SparkSession \
    .builder \
    .appName("Data Flow App") \
    .config("spark.hadoop.fs.s3a.access.key", "**") \
    .config("spark.hadoop.fs.s3a.secret.key", "**") \
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

spark.stop()
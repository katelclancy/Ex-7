#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col
from pyspark.sql.types import TimestampType, DoubleType

# Initialize Spark session
spark = SparkSession.builder \
    .appName("StockPriceStreaming") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Read from socket
lines = spark.readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .load()

# Split incoming lines like: "2021-01-04 09:30:00, AAPL, 129.41"
split_cols = split(lines["value"], ",")

# Parse and cast columns
df = lines.select(
    split_cols.getItem(0).alias("datetime"),
    split_cols.getItem(1).alias("symbol"),
    split_cols.getItem(2).cast(DoubleType()).alias("close")
).withColumn("datetime", col("datetime").cast(TimestampType()))

# Filter into two separate streams
aapl_df = df.filter(col("symbol") == "AAPL")
msft_df = df.filter(col("symbol") == "MSFT")

# Show both AAPL and MSFT streams to console (just for testing)
aapl_query = aapl_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .start()

msft_query = msft_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .start()

# Wait for termination
spark.streams.awaitAnyTermination()

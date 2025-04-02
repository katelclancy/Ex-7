#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col
from pyspark.sql.types import StructType, StringType, DoubleType, TimestampType

# Create Spark session with streaming support
spark = SparkSession.builder \
    .appName("StockPriceStreaming") \
    .getOrCreate()

# Set log level lower to avoid clutter
spark.sparkContext.setLogLevel("WARN")

# Read data from the socket
lines = spark.readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .load()
    
# Show raw values
query = lines.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()
    
# Split CSV string: "2021-01-04 09:30:00, AAPL, 129.41"
# schema = StructType() \
#     .add("datetime", StringType()) \
#     .add("symbol", StringType()) \
#     .add("close", StringType())

# split_cols = split(lines.value, ",")

# Apply schema
# df = lines.select(
#     split_cols.getItem(0).alias("datetime"),
#     split_cols.getItem(1).alias("symbol"),
#     split_cols.getItem(2).cast("double").alias("close")
# )

# # Cast datetime to TimestampType
# df = df.withColumn("datetime", col("datetime").cast(TimestampType()))

# # Filter for AAPL and MSFT
# aapl_df = df.filter(col("symbol") == "AAPL")
# msft_df = df.filter(col("symbol") == "MSFT")

# # Output to console for testing
# aapl_query = aapl_df.writeStream \
#     .outputMode("append") \
#     .format("console") \
#     .option("truncate", False) \
#     .start()

# msft_query = msft_df.writeStream \
#     .outputMode("append") \
#     .format("console") \
#     .option("truncate", False) \
#     .start()

spark.streams.awaitAnyTermination()


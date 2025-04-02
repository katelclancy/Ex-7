#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col, trim, window, avg
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
    col("value"),
    trim(split_cols.getItem(0)).alias("datetime"),
    trim(split_cols.getItem(1)).alias("symbol"),
    trim(split_cols.getItem(2)).cast(DoubleType()).alias("close")
).withColumn("datetime", col("datetime").cast(TimestampType()))

# Filter into two separate streams
aapl_df = df.filter(col("symbol") == "AAPL")
msft_df = df.filter(col("symbol") == "MSFT")

# Show both AAPL and MSFT streams to console (just for testing)
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

# 10-day moving average AAPL
aapl_ma10_df = aapl_df.groupBy(
    window("datetime", "3900 minutes", "15 minutes")
).agg(
    avg("close").alias("MA10")
)
      
# 40-day moving average AAPL
aapl_ma40_df = aapl_df.groupBy(
    window("datetime", "15600 minutes", "15 minutes")
).agg(
    avg("close").alias("MA40")
)

# Combine into one df for AAPL moving averages
aapl_ma_df = aapl_ma10_df.join(aapl_ma40_df, on="window")

# Write the moving averages to the console
aapl_ma_df.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", False) \
    .start()

# Wait for termination
spark.streams.awaitAnyTermination()

# -*- coding: utf-8 -*-
"""
Created on Wed Apr  2 20:42:10 2025

@author: KateClancy
"""

import sys
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col, trim, window, avg
from pyspark.sql.types import TimestampType, DoubleType
from pyspark import SparkContext

def setLogLevel(spark, level):
    spark.sparkContext.setLogLevel(level)

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: spark_stock_stream.py <hostname> <port>", file=sys.stderr)
        sys.exit(-1)

    host = sys.argv[1]
    port = int(sys.argv[2])

    # Stop existing SparkContext if it exists
    SparkContext.getOrCreate().stop()

    time.sleep(15)  # Give netcat time to spin up

    # Start Spark session
    spark = SparkSession.builder \
        .appName("StockPriceStreaming") \
        .master("local[*]") \
        .getOrCreate()

    setLogLevel(spark, "WARN")

    # Read from socket
    lines = spark.readStream \
        .format("socket") \
        .option("host", host) \
        .option("port", port) \
        .load()

    # Split and parse the stock data
    split_cols = split(lines["value"], ",")
    df = lines.select(
        col("value"),
        trim(split_cols.getItem(0)).alias("datetime"),
        trim(split_cols.getItem(1)).alias("symbol"),
        trim(split_cols.getItem(2)).cast(DoubleType()).alias("close")
    ).withColumn("datetime", col("datetime").cast(TimestampType()))

    # Filter into two separate streams
    aapl_df = df.filter(col("symbol") == "AAPL")
    msft_df = df.filter(col("symbol") == "MSFT")

    aapl_query = aapl_df.writeStream \
        .outputMode("complete") \
        .format("console") \
        .option("truncate", False) \
        .start()
        
    msft_query = msft_df.writeStream \
        .outputMode("complete") \
        .format("console") \
        .option("truncate", False) \
        .start()

    aapl_query.awaitTermination()
    msft_query.awaitTermination()
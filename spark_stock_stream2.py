# -*- coding: utf-8 -*-
"""
Created on Wed Apr  2 20:42:10 2025

@author: KateClancy
"""

import sys
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col, trim, window, avg, date_trunc, lag, expr
from pyspark.sql.types import TimestampType, DoubleType
from pyspark.sql.window import Window
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

    time.sleep(15)  # Give netcat time to start

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
    aaplPrice = df.filter(col("symbol") == "AAPL")
    msftPrice = df.filter(col("symbol") == "MSFT")

    # Compute daily average
    aapldaily_avg = aaplPrice.groupBy(
    date_trunc("day", col("datetime")).alias("day"),
    col("symbol")
    ).agg(
    avg("close").alias("daily_avg_close")
    )

    msftdaily_avg = msftPrice.groupBy(
    date_trunc("day", col("datetime")).alias("day"),
    col("symbol")
    ).agg(
    avg("close").alias("daily_avg_close")
    )
        
    # Define window specs for moving averages
    window_10 = Window.partitionBy("symbol").orderBy("day").rowsBetween(-9, 0)
    window_40 = Window.partitionBy("symbol").orderBy("day").rowsBetween(-39, 0)

    # Add moving averages
    aapl10Day = aapldaily_avg.withColumn("AAPL_MA10", avg("daily_avg_close").over(window_10))
    aapl40Day = aapldaily_avg.withColumn("AAPL_MA40", avg("daily_avg_close").over(window_40))
    msft10Day = msftdaily_avg.withColumn("MSFT_MA10", avg("daily_avg_close").over(window_10))
    msft40Day = msftdaily_avg.withColumn("MSFT_MA40", avg("daily_avg_close").over(window_40))
    
    # Join 10-day and 40-day MA on day
    aapl_ma = aapl10Day.join(
    aapl40Day.select("day", "symbol", "AAPL_MA40"),
    on=["day", "symbol"],
    how="inner"
    )
    
    msft_ma = msft10Day.join(
    msft40Day.select("day", "symbol", "MSFT_MA40"),
    on=["day", "symbol"],
    how="inner"
    )
    
    # Calculate difference and lag
    aapl_ma = aapl_ma.withColumn("diff", col("AAPL_MA10") - col("AAPL_MA40"))
    msft_ma = msft_ma.withColumn("diff", col("MSFT_MA10") - col("MSFT_MA40"))
    
    # Add previous day's difference
    window_spec = Window.partitionBy("symbol").orderBy("day")
    aapl_ma = aapl_ma.withColumn("prev_diff", lag("diff", 1).over(window_spec))
    msft_ma = msft_ma.withColumn("prev_diff", lag("diff", 1).over(window_spec))

    # Define signal
    aapl_signals = aapl_ma.withColumn(
        "signal",
        expr("""
             CASE
                 WHEN prev_diff < 0 AND diff >= 0 THEN 'buy'
                 WHEN prev_diff > 0 AND diff <= 0 THEN 'sell'
                 ELSE null
             END
        """)
    ).filter(col("signal").isNotNull())

    msft_signals = msft_ma.withColumn(
        "signal",
        expr("""
             CASE
                 WHEN prev_diff < 0 AND diff >= 0 THEN 'buy'
                 WHEN prev_diff > 0 AND diff <= 0 THEN 'sell'
                 ELSE null
             END
        """)
    ).filter(col("signal").isNotNull())
        
    # Select final output format
    aapl_signal_output = aapl_signals.selectExpr(
    "day as datetime",
    "signal",
    "symbol"
    )
    
    msft_signal_output = msft_signals.selectExpr(
    "day as datetime",
    "signal",
    "symbol"
    )
    
    # Write streams
    aaplsignal_query = aapl_signal_output.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", False) \
    .start()
    
    msftsignal_query = msft_signal_output.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", False) \
    .start()
    
    # aapl10Day_query = aapl10Day.writeStream \
    #     .outputMode("complete") \
    #     .format("console") \
    #     .option("truncate", False) \
    #     .start()
    
    # aapl40Day_query = aapl40Day.writeStream \
    #     .outputMode("complete") \
    #     .format("console") \
    #     .option("truncate", False) \
    #     .start()
    # msft10Day_query = msft10Day.writeStream \
    #     .outputMode("complete") \
    #     .format("console") \
    #     .option("truncate", False) \
    #     .start()
     
    # msft40Day_query = msft40Day.writeStream \
    #     .outputMode("complete") \
    #     .format("console") \
    #     .option("truncate", False) \
    #     .start()
    
    # aapl_query = aaplPrice.writeStream \
    #     .outputMode("append") \
    #     .format("console") \
    #     .option("truncate", False) \
    #     .start()
        
    # msft_query = msftPrice.writeStream \
    #      .outputMode("append") \
    #      .format("console") \
    #      .option("truncate", False) \
    #      .start()

    spark.streams.awaitAnyTermination()

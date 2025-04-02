#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
!pip install pandas
"""
import time
import pandas as pd

# Load pre-merged data
df = pd.read_csv("AAPL_MSFT_4yr_15min.csv")

# Convert datetime column to actual datetime type
df['datetime'] = pd.to_datetime(df['datetime'])

# Sort just in case
df.sort_values(by='datetime', inplace=True)

# Group rows by datetime, so we emit one timestamp at a time
grouped = df.groupby('datetime')

# Limit number of rows for testing (remove in final version)
iters = 750

for timestamp, group in grouped:
    for _, row in group.iterrows():
        output_line = f"{timestamp}, {row['symbol']}, {row['close']}"
        print(output_line, flush=True)
    
    time.sleep(1.0)  # Simulate real-time feed every 15 minutes
    iters -= 1
    if iters == 0:
        break

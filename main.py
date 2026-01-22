import time
import pandas as pd
import psutil as ps
import os
import csv
from datetime import datetime

csv_file = "cpu_usage.csv"

if os.path.exists(csv_file):
    os.remove(csv_file)

with open(csv_file, "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["timestamp", "cpu_user_percent", "cpu_system_percent", "cpu_idle_percent", "memory_used_percent", "disk_used_percent"])  # Add headers

# Add data
# csv_file = open("cpu_usage.csv", "w")

for i in range(10):
    cpu_user_percent = ps.cpu_times_percent(interval=1).user
    cpu_system_percent = ps.cpu_times_percent(interval=1).system
    cpu_idle_percent = ps.cpu_times_percent(interval=1).idle
    memory_usage = ps.virtual_memory()
    disc_usage = ps.disk_usage('/')
    with open(csv_file, "a", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([datetime.now().isoformat(), cpu_user_percent, cpu_system_percent, cpu_idle_percent, memory_usage.percent, disc_usage.percent])
    time.sleep(1)

df = pd.read_csv(csv_file)

# Display first few rows
print(df.head())

# Get basic information
print(df.info())
print(df.describe())

# Filter data
# filtered_df = df[df['column_name'] > 0]

# Group and aggregate
# grouped = df.groupby('category').sum()

# Save results
df.to_csv('output.csv', index=False)
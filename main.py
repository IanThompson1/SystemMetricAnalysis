import logging
import time
import pandas as pd
import psutil as ps
import os
import csv
from datetime import datetime

csv_file = "cpu_usage.csv"

logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s %(message)s"
)

def collect_system_metrics():
    if os.path.exists(csv_file):
        os.remove(csv_file)

    with open(csv_file, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["timestamp", "cpu_user_percent", "cpu_system_percent", "cpu_idle_percent", "memory_used_percent", "disk_used_percent"])  # Add headers

    for i in range(100):
        cpu_times = ps.cpu_times_percent(interval=1)
        cpu_user_percent = cpu_times.user
        cpu_system_percent = cpu_times.system
        cpu_idle_percent = cpu_times.idle
        memory_usage = ps.virtual_memory()
        disc_usage = ps.disk_usage('/')
        with open(csv_file, "a", newline="") as f:
            writer = csv.writer(f)
            writer.writerow([datetime.now().replace(microsecond=0).isoformat(), cpu_user_percent, cpu_system_percent, cpu_idle_percent, memory_usage.percent, disc_usage.percent])

def analyze_system_metrics():
    df = pd.read_csv(csv_file)

    logging.info(f"Ingestion started: {csv_file}")

    initial_num_rows = len(df)
    ingestion_start_time = datetime.now()
    #Validate data
    timestamp = df.columns[0]
    temp_time = df[timestamp][0]
    for row in df[timestamp]:
        if pd.isnull(row):
            print(f"Null value found in timestamp column")
            df = df.drop(df[df[timestamp] == row].index)
            break
        try:
            datetime.fromisoformat(row)
        except ValueError:
            print(f"Invalid timestamp format found: {row}")
            df = df.drop(df[df[timestamp] == row].index)
            break
        if row < temp_time:
            print(f"Timestamps are not in chronological order: {temp_time} followed by {row}")
            df = df.drop(df[df[timestamp] == row].index)
        temp_time = row

    cpu_user_percent = df.columns[1]
    for row in df[cpu_user_percent]:
        if pd.isnull(row):
            print(f"Null value found in cpu_user_percent column")
            df = df.drop(df[df[cpu_user_percent] == row].index)
            break
        if not (0 <= row <= 100):
            print(f"Invalid cpu_user_percent value found: {row}")
            df = df.drop(df[df[cpu_user_percent] == row].index)
    cpu_system_percent = df.columns[2]
    for row in df[cpu_system_percent]:
        if pd.isnull(row):
            print(f"Null value found in cpu_system_percent column")
            df = df.drop(df[df[cpu_system_percent] == row].index)
            break
        if not (0 <= row <= 100):
            print(f"Invalid cpu_system_percent value found: {row}")
            df = df.drop(df[df[cpu_system_percent] == row].index)
    cpu_idle_percent = df.columns[3]
    for row in df[cpu_idle_percent]:
        if pd.isnull(row):
            print(f"Null value found in cpu_idle_percent column")
            df = df.drop(df[df[cpu_idle_percent] == row].index)
            break
        if not (0 <= row <= 100):
            print(f"Invalid cpu_idle_percent value found: {row}")
            df = df.drop(df[df[cpu_idle_percent] == row].index)
    tollerance = 0.1
    for row_index in range(len(df)):
        total = df.at[row_index, cpu_user_percent] + df.at[row_index, cpu_system_percent] + df.at[row_index, cpu_idle_percent]
        if not (total <= 100+tollerance):
            print(f"CPU percentages do not sum to 100 at row {row_index}: total={total}")
            df = df.drop(row_index)

    memory_used_percent = df.columns[4]
    for row in df[memory_used_percent]:
        if pd.isnull(row):
            print(f"Null value found in memory_used_percent column")
            df = df.drop(df[df[memory_used_percent] == row].index)
            break
        if not (0 <= row <= 100):
            print(f"Invalid memory_used_percent value found: {row}")
            df = df.drop(df[df[memory_used_percent] == row].index)
    disk_used_percent = df.columns[5]
    for row in df[disk_used_percent]:
        if pd.isnull(row):
            print(f"Null value found in disk_used_percent column")
            df = df.drop(df[df[disk_used_percent] == row].index)
            break
        if not (0 <= row <= 100):
            print(f"Invalid disk_used_percent value found: {row}")
            df = df.drop(df[df[disk_used_percent] == row].index)

    ingestion_end_time = datetime.now()
    final_num_rows = len(df)
    num_rows_deleted = initial_num_rows - final_num_rows
    logging.info(f"Total number of rows: {initial_num_rows}")
    logging.info(f"Valid rows ingested: {final_num_rows}")
    logging.warning(f"Number of rows deleted during validation: {num_rows_deleted}")
    # Compute statistics
    cpu_user_min = df[cpu_user_percent].min()
    cpu_user_max = df[cpu_user_percent].max()
    cpu_user_avg = df[cpu_user_percent].mean()
    logging.info(f"CPU user percent — min: {cpu_user_min:.1f}, max: {cpu_user_max:.1f}, avg: {cpu_user_avg:.1f}")
    memory_used_avg = df[memory_used_percent].mean()
    logging.info(f"Memory used percent — avg: {memory_used_avg:.1f}")
    time_elapsed = (ingestion_end_time - ingestion_start_time).total_seconds()
    logging.info(f"Ingestion completed in {time_elapsed} seconds")

    # Save results
    df.to_csv('cleaned_metrics.csv', index=False)

def transform_system_metrics():
    df = pd.read_csv('cleaned_metrics.csv')
    df_out = pd.DataFrame(columns=["window_start", "window_end", "sample_count", "avg_cpu_total_percent", "min_cpu_idle_percent", "max_memory_usage_percent", "avg_disk_usage_percent", "memory_pressure_flag", "cpu_saturation_flag"])
    
    # setup 5 second windows
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    
    df['start_timestamp'] = df['timestamp']
    df['end_timestamp'] = df['timestamp']
    df.set_index('timestamp', inplace=True)

    df['temp'] = 1  # Temporary column to count samples
    
    # Transform data
    df = df.resample('5s').agg({
        'start_timestamp': 'min',
        'end_timestamp': 'max',
        'cpu_user_percent': 'mean',
        'cpu_system_percent': 'mean',
        'cpu_idle_percent': 'min',
        'memory_used_percent': 'max', 
        'disk_used_percent': 'mean',
        'temp': 'sum'
    })

    df_out['window_start'] = df['start_timestamp']
    df_out['window_end'] = df['end_timestamp']
    df_out['sample_count'] = df['temp']
    df_out['avg_cpu_total_percent'] = (df['cpu_user_percent'] + df['cpu_system_percent']).round(1)
    df_out['min_cpu_idle_percent'] = df['cpu_idle_percent']
    df_out['max_memory_usage_percent'] = df['memory_used_percent'].round(1)
    df_out['avg_disk_usage_percent'] = df['disk_used_percent'].round(1)
    df_out['memory_pressure_flag'] = df_out['max_memory_usage_percent'] > 90.0
    df_out['cpu_saturation_flag'] = df_out['min_cpu_idle_percent'] < 10.0
    df_out = df_out.dropna(subset=['avg_cpu_total_percent', 'max_memory_usage_percent', 'min_cpu_idle_percent'])
    df_out.reset_index(drop=True, inplace=True)

    df_out.to_csv('metrics_5s.csv', index=False, date_format="%Y-%m-%dT%H:%M:%S")


    # setup 15 second windows
    df = pd.read_csv('cleaned_metrics.csv')
    df_out = pd.DataFrame(columns=["window_start", "window_end", "sample_count", "avg_cpu_total_percent", "min_cpu_idle_percent", "max_memory_usage_percent", "avg_disk_usage_percent", "memory_pressure_flag", "cpu_saturation_flag"])
    
    df['timestamp'] = pd.to_datetime(df['timestamp'])

    df['start_timestamp'] = df['timestamp']
    df['end_timestamp'] = df['timestamp']
    df.set_index('timestamp', inplace=True)

    df['temp'] = 1  # Temporary column to count samples

    # Write transformed data
    df = df.resample('15s').agg({
        'start_timestamp': 'min',
        'end_timestamp': 'max',
        'cpu_user_percent': 'mean',
        'cpu_system_percent': 'mean',
        'cpu_idle_percent': 'min',
        'memory_used_percent': 'max', 
        'disk_used_percent': 'mean',
        'temp': 'sum'
    })
    df_out['window_start'] = df['start_timestamp']
    df_out['window_end'] = df['end_timestamp']
    df_out['sample_count'] = df['temp']
    df_out['avg_cpu_total_percent'] = (df['cpu_user_percent'] + df['cpu_system_percent']).round(1)
    df_out['min_cpu_idle_percent'] = df['cpu_idle_percent']
    df_out['max_memory_usage_percent'] = df['memory_used_percent'].round(1)
    df_out['avg_disk_usage_percent'] = df['disk_used_percent'].round(1)
    df_out['memory_pressure_flag'] = df_out['max_memory_usage_percent'] > 90.0
    df_out['cpu_saturation_flag'] = df_out['min_cpu_idle_percent'] < 10.0
    df_out = df_out.dropna(subset=['avg_cpu_total_percent', 'max_memory_usage_percent', 'min_cpu_idle_percent'])
    df_out.reset_index(drop=True, inplace=True)

    df_out.to_csv('metrics_15s.csv', index=False, date_format="%Y-%m-%dT%H:%M:%S")


# print("Would you like to collect system metrics?")
# listen = input("Type 'y' to collect metrics, or 'n' to skip to analysis: ")
# if listen.lower() == 'y':
#     print("Collecting system metrics...")
#     collect_system_metrics()
print("Analyzing system metrics...")
analyze_system_metrics()
print("Transforming system metrics...")
transform_system_metrics()
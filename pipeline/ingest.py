import logging
import pandas as pd
import math
from datetime import datetime

"""
Makes sure data is good and logs/gets rid of bad data

Returns:
    dataframe: valid data
"""
def run_ingestion(input_path):
    df = pd.read_csv(input_path)
    
    logging.info(f"Ingestion started: {input_path}")

    initial_num_rows = len(df)
    ingestion_start_time = datetime.now()
    #Validate data
    df['cpu_user_percent'] = pd.to_numeric(df['cpu_user_percent'], errors="coerce")
    df['cpu_system_percent'] = pd.to_numeric(df['cpu_system_percent'], errors="coerce")
    df['cpu_idle_percent'] = pd.to_numeric(df['cpu_idle_percent'], errors="coerce")
    df['memory_used_percent'] = pd.to_numeric(df['memory_used_percent'], errors="coerce")
    df['disk_used_percent'] = pd.to_numeric(df['disk_used_percent'], errors="coerce")

    timestamp = df.columns[0]
    temp_time = df[timestamp][0]
    for row_index in df.index:
        row_value = df.at[row_index, timestamp]
        if pd.isnull(row_value):
            print(f"Null/bad value found in timestamp column")
            df = df.drop(row_index)
            continue
        try:
            datetime.fromisoformat(row_value)
        except ValueError:
            print(f"Invalid timestamp format found: {row_value}")
            df = df.drop(row_index)
            continue
        if row_value < temp_time:
            print(f"Timestamps are not in chronological order: {temp_time} followed by {row_value}")
            df = df.drop(row_index)
            continue
        temp_time = row_value
    cpu_user_percent = df.columns[1]
    for row_index in df.index:
        row_value = df.at[row_index, cpu_user_percent]
        if pd.isnull(row_value):
            print(f"Null/bad value found in cpu_user_percent column")
            df = df.drop(row_index)
            continue
        if not (0 <= row_value <= 100):
            print(f"Invalid cpu_user_percent value found: {round(row_value,1)}")
            df = df.drop(row_index)
    cpu_system_percent = df.columns[2]
    for row_index in df.index:
        row_value = df.at[row_index, cpu_system_percent]
        if pd.isnull(row_value):
            print(f"Null/bad value found in cpu_system_percent column")
            df = df.drop(row_index)
            continue
        if not (0 <= row_value <= 100):
            print(f"Invalid cpu_system_percent value found: {round(row_value,1)}")
            df = df.drop(row_index)
    cpu_idle_percent = df.columns[3]
    for row_index in df.index:
        row_value = df.at[row_index, cpu_idle_percent]
        if pd.isnull(row_value):
            print(f"Null/bad value found in cpu_idle_percent column")
            df = df.drop(row_index)
            continue
        if not (0 <= row_value <= 100):
            print(f"Invalid cpu_idle_percent value found: {round(row_value,1)}")
            df = df.drop(row_index)
    tollerance = 0.1
    for row_index in df.index:
        total = df.at[row_index, cpu_user_percent] + df.at[row_index, cpu_system_percent] + df.at[row_index, cpu_idle_percent]
        if not (total <= 100+tollerance):
            print(f"CPU percentages do not sum to 100 at row {row_index}: total={round(total,1)}")
            df = df.drop(row_index)

    memory_used_percent = df.columns[4]
    for row_index in df.index:
        row_value = df.at[row_index, memory_used_percent]
        if pd.isnull(row_value):
            print(f"Null/bad value found in memory_used_percent column")
            df = df.drop(row_index)
            continue
        if not (0 <= row_value <= 100):
            print(f"Invalid memory_used_percent value found: {round(row_value,1)}")
            df = df.drop(row_index)
    
    disk_used_percent = df.columns[5]
    for row_index in df.index:
        row_value = df.at[row_index, disk_used_percent]
        if pd.isnull(row_value):
            print(f"Null/bad value found in disk_used_percent column")
            df = df.drop(row_index)
            continue
        if not (0 <= row_value <= 100):
            print(f"Invalid disk_used_percent value found: {round(row_value,1)}")
            df = df.drop(row_index)
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

    # Return results
    return df
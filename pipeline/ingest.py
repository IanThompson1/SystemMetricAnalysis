import logging
import pandas as pd
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

    # Return results
    return df
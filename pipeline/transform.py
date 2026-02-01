import pandas as pd
import yaml
from datetime import datetime

with open("config.yaml", "r") as f:
    config = yaml.safe_load(f)

"""
Applies time window aggregations and writes processed CSVs.

Returns:
    dict[int, str]: mapping window size -> output CSV path
"""
def run_transformation(df_valid, output_dir, window_sizes):
    outputs = {}
    for window in window_sizes:
        out_path = f"{output_dir}/processed/metrics_{window}s.csv"
        df_out = pd.DataFrame(columns=["window_start", "window_end", "sample_count", "avg_cpu_total_percent", "min_cpu_idle_percent", "max_memory_usage_percent", "avg_disk_usage_percent", "memory_pressure_flag", "cpu_saturation_flag"])
        #setup window
        df = df_valid.copy()
        df['timestamp'] = pd.to_datetime(df['timestamp'])
    
        df['start_timestamp'] = df['timestamp']
        df['end_timestamp'] = df['timestamp']
        df.set_index('timestamp', inplace=True)

        df['temp'] = 1  # Temporary column to count samples

        # Transform data
        df = df.resample(str(window)+'s').agg({
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
        df_out['memory_pressure_flag'] = df_out['max_memory_usage_percent'] > config['thresholds']['memory_pressure_percent']
        df_out['cpu_saturation_flag'] = df_out['min_cpu_idle_percent'] < config['thresholds']['cpu_saturation_percent']
        df_out = df_out.dropna(subset=['avg_cpu_total_percent', 'max_memory_usage_percent', 'min_cpu_idle_percent'])
        df_out.reset_index(drop=True, inplace=True)

        df_out.to_csv(out_path, index=False, date_format="%Y-%m-%dT%H:%M:%S")

        outputs[window] = out_path

    return outputs

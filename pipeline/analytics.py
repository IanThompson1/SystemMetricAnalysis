import pandas as pd
import json
import logging
import yaml
import matplotlib.pyplot as plt

with open("config.yaml", "r") as f:
    config = yaml.safe_load(f)

def run_analytics(processed_data, output):
    # cpu_plot = plt.figure()
    # mem_plot = plt.figure()
    fig, (cpu, mem) = plt.subplots(2, 1)
    for window, csv_path in processed_data.items():
        df = pd.read_csv(csv_path)
        
        total_windows = len(df)
        memory_pressure_windows = df['memory_pressure_flag'].sum()
        cpu_saturation_windows = int(df['cpu_saturation_flag'].sum())
        percent_memory_pressure = ((memory_pressure_windows / total_windows) * 100).round(2) if total_windows > 0 else 0
        avg_cpu = df['avg_cpu_total_percent'].mean().round(2)
        max_cpu = df['avg_cpu_total_percent'].max()
        peak_memory_usage = df['max_memory_usage_percent'].max()
        longest_memory_pressure_streak = 0
        current_streak = 0
        for flag in df['memory_pressure_flag']:
            if flag:
                current_streak += 1
                if current_streak > longest_memory_pressure_streak:
                    longest_memory_pressure_streak = current_streak
            else:
                current_streak = 0

        logging.info(f"{window}-second windows: Total={total_windows}, Percent Memory Pressure={percent_memory_pressure}, CPU Saturation={cpu_saturation_windows}, Max CPU={max_cpu}, Avg CPU={avg_cpu}, Longest Memory Pressure Streak={longest_memory_pressure_streak}, Peak Memory Usage={peak_memory_usage}")
        if percent_memory_pressure == 0:
            logging.info(f"Memory pressure threshold: > {config['thresholds']['memory_pressure_percent']}%")
        if cpu_saturation_windows == 0:
            logging.info(f"CPU saturation threshold: > {config['thresholds']['cpu_saturation_percent']}%")
        logging.info(f"Time range with highest CPU usage in {window}s windows: {df.loc[df['avg_cpu_total_percent'].idxmax()]['window_start']} to {df.loc[df['avg_cpu_total_percent'].idxmax()]['window_end']}")
        
        analytics_summary = {
            "window_size_seconds": window,
            "total_windows": total_windows,
            "percent_memory_pressure": percent_memory_pressure,
            "cpu_saturation_count": cpu_saturation_windows,
            "max_cpu_total_percent": max_cpu,
            "avg_cpu_total_percent": avg_cpu,
            "longest_memory_pressure_streak": longest_memory_pressure_streak,
            "peak_memory_used_percent": peak_memory_usage,
            "peak_cpu_time_range": {
                "start": df.loc[df['avg_cpu_total_percent'].idxmax()]['window_start'],
                "end": df.loc[df['avg_cpu_total_percent'].idxmax()]['window_end']
            }
        }
        with open(output+"/analytics/analytics_summary_"+str(window)+"s.json", "w") as f:
            json.dump(analytics_summary, f)

        cpu.plot(pd.to_datetime(df['window_start']), df['avg_cpu_total_percent'], label=str(window)+'s Avg CPU %')
        mem.plot(pd.to_datetime(df['window_start']), df['max_memory_usage_percent'], label=str(window)+'s Max Memory %')

    cpu.set_xlabel('Time')
    cpu.set_ylabel('CPU Usage (%)')
    cpu.set_title('CPU Usage Over Time')
    cpu.legend()
    mem.set_xlabel('Time')
    mem.set_ylabel('Max Memory (%)')
    mem.set_title('Max Memory Over Time')
    mem.legend()
    plt.show()
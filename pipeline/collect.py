import psutil as ps
import csv
from datetime import datetime

"""
Collect system metrics and write to CSV.

Returns:
    str: path to generated CSV file
"""
def collect_metrics(output_path, duration, interval):
    with open(output_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["timestamp", "cpu_user_percent", "cpu_system_percent", "cpu_idle_percent", "memory_used_percent", "disk_used_percent"])  # Add headers

    for i in range(duration):
        cpu_times = ps.cpu_times_percent(interval=interval)
        cpu_user_percent = cpu_times.user
        cpu_system_percent = cpu_times.system
        cpu_idle_percent = cpu_times.idle
        memory_usage = ps.virtual_memory()
        disc_usage = ps.disk_usage('/')
        with open(output_path, "a", newline="") as f:
            writer = csv.writer(f)
            writer.writerow([datetime.now().replace(microsecond=0).isoformat(), cpu_user_percent, cpu_system_percent, cpu_idle_percent, memory_usage.percent, disc_usage.percent])
    return output_path

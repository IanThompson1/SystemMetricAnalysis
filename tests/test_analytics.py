import csv
import os
import pandas as pd
import json
import pytest
from pipeline.transform import run_transformation
from pipeline.ingest import run_ingestion
from pipeline.analytics import run_analytics

test_file = 'data/raw/temp.csv'
temp_file = 'temp/processed/metrics_2s.csv'
temp_file2 = 'temp/analytics/analytics_summary_2s.json'

def test_run_analytics():
    create_temp_csv_data()
    df = run_ingestion(test_file)
    os.makedirs('temp/processed')
    os.makedirs('temp/analytics')
    processed_outputs = run_transformation(df,'temp',[2])
    run_analytics(processed_outputs, 'temp', False)#dont show plots for testing
    analytic_output = pd.read_json(temp_file2)
    with open(temp_file2) as f:
        data = json.load(f)
    print(data)
    assert data['window_size_seconds'] == 2
    assert data['total_windows'] >= 3 and data['total_windows'] <=4
    os.remove(temp_file)
    os.remove(temp_file2)
    os.removedirs('temp/processed')
    os.removedirs('temp/analytics')
    delete_temp_file()


def create_temp_csv_data():
    with open(test_file, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["timestamp", "cpu_user_percent", "cpu_system_percent", "cpu_idle_percent", "memory_used_percent", "disk_used_percent"])  # Add headers
        writer.writerow(['2026-01-30T13:26:39', '9.6', '6.8', '83.1', '82.8', '53.4'])
        writer.writerow(['2026-01-30T13:26:40', '8.1', '5.2', '86.1', '82.8', '53.4'])
        writer.writerow(['2026-01-30T13:26:41', '7.7', '4.5', '87.3', '82.8', '53.4'])
        writer.writerow(['2026-01-30T13:26:42', '9.8', '8.8', '81.1', '82.8', '53.4'])
        writer.writerow(['2026-01-30T13:26:43', '21.2', '13.5', '63.9', '83.0', '53.4'])
        writer.writerow(['2026-01-30T13:26:44', '21.2', '13.5', '63.9', '83.0', '53.4'])

def delete_temp_file():
    os.remove(test_file)
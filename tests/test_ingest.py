import os
import csv
import pytest
from pipeline.ingest import run_ingestion

test_file = 'data/raw/temp.csv'

def test_ingestion_valid_file():
    create_temp_csv_data()
    df = run_ingestion(test_file)
    delete_temp_file()
    assert len(df) > 0

def test_ingestion_bad_values(capsys):
    create_temp_csv_data2()
    df = run_ingestion(test_file)
    delete_temp_file()
    captured = capsys.readouterr()
    assert captured.out == "Invalid cpu_user_percent value found: 100.6\n\
CPU percentages do not sum to 100 at row 1: total=100.4\n\
Invalid memory_used_percent value found: 102.8\n\
Invalid disk_used_percent value found: 103.4\n"

def test_ingestion_bad_value_types(capsys):
    create_temp_csv_data3()
    df = run_ingestion(test_file)
    delete_temp_file()
    captured = capsys.readouterr()
    assert captured.out == "Invalid timestamp format found: 29\n\
Null/bad value found in cpu_user_percent column\n\
Null/bad value found in cpu_user_percent column\n\
Null/bad value found in memory_used_percent column\n"
    

def create_temp_csv_data():
    with open(test_file, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["timestamp", "cpu_user_percent", "cpu_system_percent", "cpu_idle_percent", "memory_used_percent", "disk_used_percent"])  # Add headers
        writer.writerow(['2026-01-30T13:26:39', '9.6', '6.8', '83.1', '82.8', '53.4'])
        writer.writerow(['2026-01-30T13:26:40', '8.1', '5.2', '86.1', '82.8', '53.4'])
        writer.writerow(['2026-01-30T13:26:41', '7.7', '4.5', '87.3', '82.8', '53.4'])
        writer.writerow(['2026-01-30T13:26:42', '9.8', '8.8', '81.1', '82.8', '53.4'])
        writer.writerow(['2026-01-30T13:26:43', '21.2', '13.5', '63.9', '83.0', '53.4'])

def create_temp_csv_data2():
    with open(test_file, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["timestamp", "cpu_user_percent", "cpu_system_percent", "cpu_idle_percent", "memory_used_percent", "disk_used_percent"])  # Add headers
        writer.writerow(['2026-01-30T13:26:39', '100.6', '6.8', '83.1', '82.8', '53.4'])
        writer.writerow(['2026-01-30T13:26:40', '10.1', '10.2', '80.1', '82.8', '53.4'])
        writer.writerow(['2026-01-30T13:26:41', '7.7', '4.5', '87.3', '102.8', '53.4'])
        writer.writerow(['2026-01-30T13:26:42', '9.8', '8.8', '81.1', '82.8', '103.4'])
        writer.writerow(['2026-01-30T13:26:43', '21.2', '13.5', '63.9', '83.0', '53.4'])

def create_temp_csv_data3():
    with open(test_file, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["timestamp", "cpu_user_percent", "cpu_system_percent", "cpu_idle_percent", "memory_used_percent", "disk_used_percent"])  # Add headers
        writer.writerow(['2026-01-30T13:26:40', 'bad', '5.2', '86.1', '82.8', '53.4'])#string in cpu user
        writer.writerow(['2026-01-30T13:26:41', '7.7', '4.5', '87.3', '82.8', '53.4'])#only good row
        writer.writerow(['2026-01-30T13:26:42', '2.2', '8.8', '81.1'])#missing columns
        writer.writerow(['2026-01-30T13:26:43', 'baaad', '13.5', '63.9', '83.0', 'help'])#another string in cpu user
        writer.writerow(['29', '9.6', '6.8', '83.1', '82.8', '53.4'])#bad time
        

def delete_temp_file():
    os.remove(test_file)
# System Metrics Data Pipeline (Python)

## Overview

This project implements a Python-based data pipeline for collecting, validating, transforming, and analyzing **time-series system telemetry**. The pipeline ingests raw system metrics (CPU, memory, disk), applies schema validation and data quality checks, and produces **analytics-ready datasets** using fixed time-window aggregations.

The project is designed to mirror real-world **data engineering and infrastructure monitoring workflows**, emphasizing reliability, clarity, and modular pipeline stages.

---

## Goals

* Collect real system telemetry at a fixed sampling interval
* Validate schema, data types, and value ranges during ingestion
* Log ingestion statistics and data quality metrics
* Transform raw telemetry into structured, windowed datasets
* Produce clean outputs suitable for analytics and downstream ML workflows

---

## Dataset

### Raw Metrics Schema

Raw telemetry is collected at **1-second intervals** and stored as CSV.

```csv
timestamp,cpu_user_percent,cpu_system_percent,cpu_idle_percent,memory_used_percent,disk_used_percent
```

* **timestamp**: ISO 8601 timestamp (UTC)
* **cpu_user_percent**: CPU time spent in user mode
* **cpu_system_percent**: CPU time spent in system mode
* **cpu_idle_percent**: CPU idle percentage
* **memory_used_percent**: Percentage of memory in use
* **disk_used_percent**: Percentage of disk space in use

---

## Pipeline Architecture

```
Raw Metrics (CSV)
      ↓
Schema Validation & Ingestion Logging
      ↓
Data Cleaning & Transformation
      ↓
Windowed Aggregation (5s / 15s)
      ↓
Analytics-Ready Output Datasets
```

Each stage is implemented as a modular component to support clarity, testing, and future extension.

---

## Ingestion & Schema Validation

### Validation Rules

During ingestion, each row is validated to ensure:

* All required columns are present
* Timestamps are valid ISO 8601 datetimes
* Metric fields are numeric
* All percentage values fall within `[0, 100]`

Rows failing validation are excluded from downstream processing.

### Ingestion Metrics Logged

* Total rows read
* Valid rows ingested
* Invalid rows rejected
* Ingestion duration

This ensures **data reliability and observability** at the ingestion stage.

---

## Data Transformation & Aggregation

### Cleaning & Preparation

* Convert timestamps to datetime objects
* Sort records chronologically
* Derive total CPU utilization:

  ```text
  cpu_total_percent = cpu_user_percent + cpu_system_percent
  ```

### Time-Series Windowing

Telemetry is aggregated into **fixed windows**:

* **5-second windows** for short-term signal smoothing
* **15-second windows** for trend analysis

Partial windows (e.g., at start or end of a run) are retained and tracked using a sample count.

---

## Output Schemas

### 5s / 15s Windowed Metrics

```csv
window_start,window_end,sample_count,avg_cpu_total_percent,min_cpu_idle_percent,max_memory_used_percent,avg_disk_used_percent,memory_pressure_flag,cpu_saturation_flag
```

#### Field Definitions

* **window_start / window_end(inclusive)**: Time boundaries of the aggregation window
* **sample_count**: Number of raw samples in the window
* **avg_cpu_total_percent**: Average CPU utilization
* **min_cpu_idle_percent**: Minimum idle CPU observed
* **max_memory_used_percent**: Peak memory usage
* **avg_disk_used_percent**: Average disk usage
* **memory_pressure_flag**: True if memory usage exceeds 90%
* **cpu_saturation_flag**: True if CPU idle drops below 10%

---

## Example Output Row

```csv
2026-01-23T15:12:30,2026-01-23T15:12:44,15,27.4,47.4,91.1,55.1,true,false
```

This row represents a 15-second window summarizing system behavior for analytics or alerting workflows.

---

## Technologies Used

* **Python**
* **psutil** (system metrics collection)
* **CSV / datetime / logging** (standard library)

---

## Key Concepts Demonstrated

* Data ingestion pipelines
* Schema validation & data quality checks
* Time-series processing
* Windowed aggregation
* Feature engineering
* Analytics-ready dataset design

---

## Future Improvements

* Persist outputs to SQLite or Parquet
* Add anomaly detection using statistical or ML techniques
* Support multi-host ingestion
* Parallelize ingestion and transformation stages

---

## Summary

This project demonstrates how raw infrastructure telemetry can be transformed into **reliable, structured datasets** suitable for analytics, monitoring, and machine learning systems. The pipeline emphasizes correctness, clarity, and real-world data engineering practices.

# System Metrics Data Pipeline (Python)

## Overview

This project implements a Python-based data pipeline for collecting, validating, transforming, and analyzing **time-series system telemetry**. The pipeline ingests raw system metrics (CPU, memory, disk), applies schema validation and data quality checks, and produces **analytics-ready datasets** using fixed time-window aggregations.

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

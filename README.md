# System Metrics Pipeline

A configurable, end-to-end Python data pipeline for collecting, ingesting, transforming, and analyzing system performance metrics. The project simulates a production-style metrics pipeline, supporting schema validation, multi-resolution time-window aggregation, analytics reporting, and CLI-based orchestration.

This project was built incrementally through multiple development stages and finalized with testing, configuration management, and production-readiness improvements.

---

## Features

* **Flexible ingestion**: Read existing CSV files or collect live system metrics
* **Schema validation** with detailed ingestion statistics
* **Multi-window aggregation** (arbitrary window sizes via CLI)
* **Analytics summaries** including peak detection and pressure indicators
* **Configurable thresholds** via YAML configuration
* **CLI-based orchestration** with sensible defaults
* **Automated tests** for pipeline stability
* **Structured logging** for observability

---

## Project Structure

```
system-metrics-pipeline/
│
├── data/
│   ├── raw/
│   ├── processed/
│   └── analytics/
│
├── pipeline/
│   ├── ingest.py
│   ├── transform.py
│   ├── analytics.py
│   ├── collect.py
│
├── tests/
│   ├── test_ingest.py
│   ├── test_transform.py
│   └── test_analytics.py
│
├── config.yaml
├── main.py
└── README.md
```

---

## Configuration

Runtime thresholds and defaults are defined in `config.yaml`:

```yaml
thresholds:
  memory_pressure_percent: 80
  cpu_saturation_percent: 20

windows:
  default: [5, 15]

plots:
  default: True
```

This allows tuning system behavior without modifying application code.

---

## Running the Pipeline

### Default execution

If no arguments are provided, the pipeline runs using default input and output paths:

```bash
python main.py
```

Defaults:

* Input: `data/raw/metrics_collected.csv`
* Output directory: `data/`
* Window sizes: values defined in `config.yaml`

---

### Using a custom input file

```bash
python main.py --input data/raw/metrics_raw.csv --output data/
```

---

### Collect live system metrics

```bash
python main.py --collect --duration 60 --interval 1 --output data/
```

This will:

1. Collect system metrics locally
2. Write a raw CSV to `data/raw/`
3. Run the full pipeline on the collected data

---

### Custom window sizes

```bash
python main.py --window-sizes 2 5 10 15
```

Each window size produces an independent processed dataset and analytics summary.

---

## Pipeline Stages

### 0 Collection (optional)

* Gets system data from the computer and logs them into a csvfile
* Customizable period between data checks and how long it measures for

### 1 Ingestion

* Validates schema and data types
* Logs row counts, dropped rows, and summary statistics
* Measures ingestion performance

### 2 Transformation

* Aligns timestamps
* Aggregates metrics into fixed-size time windows
* Writes one processed file per window size

### 3 Analytics

* Computes descriptive statistics per window size
* Detects peak CPU usage periods
* Flags memory pressure and CPU saturation events
* Writes analytics summaries to disk
* Creates readable plots for CPU and MEMORY usage for each window size

---

## Testing

Basic tests:

```bash
python -m pytest
```

Tests cover:

* Successful ingestion of valid data
* Correct windowing behavior
* Analytics execution without runtime failures

---

## Design Decisions

* **Separation of concerns**: Each pipeline stage is isolated and reusable
* **Metadata-based flow**: Transformation returns output paths instead of in-memory data
* **Config-driven behavior**: Thresholds and defaults live outside application logic
* **CLI-first design**: Enables flexible execution and experimentation
* **Lightweight testing**: Focused on stability rather than exhaustive correctness

---

## Why This Project

This project demonstrates practical data engineering skills including:

* More than basics of Python
* Documentation
* Pipeline orchestration
* Time-series data processing
* Observability and logging
* Configuration management
* Testing and reliability

---

## Status

The pipeline is feature-complete, tested, configurable, and production-ready for its intended scope.

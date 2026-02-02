import argparse
import logging
import sys
import yaml
from pipeline import ingest, transform, analytics, collect

with open("config.yaml", "r") as f:
    config = yaml.safe_load(f)

def setup_logging(level="INFO"):
    logging.basicConfig(
        level=level,
        format="%(levelname)s: %(message)s"
    )

def parse_args():
    parser = argparse.ArgumentParser(description="System Metrics Pipeline")
    parser.add_argument("--collect", action="store_true", help="Collect system metrics instead of reading an existing CSV")
    parser.add_argument("--duration", type=int, default=60, help="Collection duration in seconds")
    parser.add_argument("--interval", type=int, default=1, help="Sampling interval in seconds")
    parser.add_argument("--input", default="data/raw/metrics_collected.csv", help="Path to raw metrics CSV, skipped if using --collect")
    parser.add_argument("--output", default="data", help="Directory to store outputs")
    parser.add_argument("--window-sizes", type=int, nargs='+', default=config['windows']['default'], help="Window sizes in seconds")
    parser.add_argument("--verbose", action="store_true", help="Enable verbose logging")
    return parser.parse_args()

def main():
    args = parse_args()
    setup_logging("DEBUG" if args.verbose else "INFO")
    if args.collect:
        logging.info("Collecting system metrics from local machine")
        input_path = collect.collect_metrics(
            output_path="data/raw/metrics_collected.csv",
            duration=args.duration,
            interval=args.interval
        )
    else:
        input_path = args.input
    try:
        logging.info("=== Starting Ingestion Stage ===")
        df_valid = ingest.run_ingestion(input_path)
        logging.info(f"Ingestion complete: {len(df_valid)} valid rows")

        logging.info("=== Starting Transformation Stage ===")
        processed_outputs = transform.run_transformation(df_valid, args.output, args.window_sizes)
        logging.info("Transformation complete")

        logging.info("=== Starting Analytics Stage ===")
        analytics.run_analytics(processed_outputs, args.output, config['plots']['default'])
        logging.info("Analytics complete")

        logging.info("=== Pipeline Completed Successfully ===")

    except Exception as e:
        logging.error(f"Pipeline failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
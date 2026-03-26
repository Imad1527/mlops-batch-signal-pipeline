import argparse
import logging
import json
import time
import yaml
import pandas as pd
import numpy as np
import sys

def parse_args():
    parser = argparse.ArgumentParser(description="MLOps Batch Signal Pipeline")

    parser.add_argument("--input", required=True, help="Input CSV file")
    parser.add_argument("--config", required=True, help="Config YAML file")
    parser.add_argument("--output", required=True, help="Output metrics JSON")
    parser.add_argument("--log-file", required=True, help="Log file")

    return parser.parse_args()


def setup_logging(log_file):
    logging.basicConfig(
        filename=log_file,
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s"
    )

def load_config(config_path):
    try:
        with open(config_path, "r") as f:
            config = yaml.safe_load(f)

        # Validate required fields
        required_fields = ["seed", "window", "version"]
        for field in required_fields:
            if field not in config:
                raise ValueError(f"Missing required config field: {field}")

        return config

    except Exception as e:
        raise RuntimeError(f"Config error: {str(e)}")

def load_dataset(input_path):
    try:
        # Read file as raw text
        df = pd.read_csv(input_path, header=None)

        # Remove quotes
        df = df[0].str.replace('"', '')

        # Split columns
        df = df.str.split(",", expand=True)

        # First row as header
        df.columns = df.iloc[0]
        df = df[1:]

        # Normalize column names
        df.columns = df.columns.str.lower().str.strip()

        # Check empty
        if df.empty:
            raise ValueError("Input dataset is empty")

        # Check required column
        if "close" not in df.columns:
            raise ValueError("Missing required column: close")

        return df

    except FileNotFoundError:
        raise RuntimeError("Input file not found")

    except pd.errors.EmptyDataError:
        raise RuntimeError("CSV file is empty")

    except Exception as e:
        raise RuntimeError(f"Dataset error: {str(e)}")

def compute_rolling_mean(df, window):
    logging.info("Computing rolling mean")

    df["rolling_mean"] = df["close"].astype(float).rolling(window=window).mean()

    return df

def generate_signal(df):
    logging.info("Generating signals")

    df["signal"] = (df["close"].astype(float) > df["rolling_mean"]).astype(int)

    return df

def compute_metrics(df, start_time):
    logging.info("Computing metrics")

    rows_processed = len(df)

    signal_rate = df["signal"].mean()

    latency_ms = int((time.time() - start_time) * 1000)

    metrics = {
        "rows_processed": rows_processed,
        "signal_rate": float(signal_rate),
        "latency_ms": latency_ms
    }

    return metrics

def write_metrics(metrics, config, output_path):
    logging.info("Writing metrics")

    output = {
        "version": config["version"],
        "rows_processed": metrics["rows_processed"],
        "metric": "signal_rate",
        "value": round(metrics["signal_rate"], 4),
        "latency_ms": metrics["latency_ms"],
        "seed": config["seed"],
        "status": "success"
    }

    with open(output_path, "w") as f:
        json.dump(output, f, indent=2)

    return output

def main():
    args = parse_args()

    start_time = time.time()

    setup_logging(args.log_file)

    logging.info("Job started")
    print("Starting MLOps Batch Job")

    try:
        # Load config
        config = load_config(args.config)
        logging.info(f"Config loaded: {config}")

        # Set seed
        np.random.seed(config["seed"])

        # Load dataset
        df = load_dataset(args.input)

        logging.info(f"Rows loaded: {len(df)}")

        # Compute rolling mean
        df = compute_rolling_mean(df, config["window"])

        # Generate signals
        df = generate_signal(df)

        # Compute metrics
        metrics = compute_metrics(df, start_time)

        logging.info(f"Metrics: {metrics}")

        # Write metrics
        output = write_metrics(metrics, config, args.output)
        print(json.dumps(output, indent=2))

        logging.info("Job completed successfully")

    except Exception as e:
        logging.error(str(e))

        error_output = {
            "version": "v1",
            "status": "error",
            "error_message": str(e)
        }

        with open(args.output, "w") as f:
            json.dump(error_output, f, indent=2)

        print(json.dumps(error_output, indent=2))

        sys.exit(1)


if __name__ == "__main__":
    main()
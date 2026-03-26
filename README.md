# MLOps Batch Signal Pipeline

## Overview

This project implements a minimal MLOps-style batch job that:

- Loads configuration from YAML
- Reads OHLCV dataset
- Computes rolling mean on close prices
- Generates binary trading signals
- Outputs structured metrics JSON
- Provides logging and observability
- Runs locally and via Docker

This project demonstrates reproducibility, observability, and deployment readiness.

# MLOps Batch Signal Pipeline

## Overview

This project implements a minimal MLOps-style batch job in Python that demonstrates:

- Reproducibility (deterministic runs via config + seed)
- Observability (logs + machine-readable metrics)
- Deployment readiness (Dockerized, one-command run)

The pipeline processes OHLCV data, computes rolling mean on close prices, generates trading signals, and outputs structured metrics.

---

## Project Structure


mlops-batch-signal-pipeline/
│
├── run.py
├── config.yaml
├── data.csv
├── requirements.txt
├── Dockerfile
├── README.md
├── metrics.json
└── run.log


---

## Requirements

- Python 3.10+
- Docker (for container execution)

---

## Configuration

`config.yaml`

```yaml
seed: 42
window: 5
version: "v1"
Dataset

Input dataset must contain OHLCV columns:

Required column:

close

Example:

timestamp,open,high,low,close,volume_btc,volume_usd
Local Run

Run the pipeline locally:

python run.py --input data.csv --config config.yaml --output metrics.json --log-file run.log
Example Output
{
  "version": "v1",
  "rows_processed": 10000,
  "metric": "signal_rate",
  "value": 0.4989,
  "latency_ms": 120,
  "seed": 42,
  "status": "success"
}
Logging

Logs are written to:

run.log

Example log:

Job started
Config loaded
Rows loaded
Rolling mean computed
Signals generated
Metrics calculated
Job completed successfully
Docker Build

Build Docker image:

docker build -t mlops-task .
Docker Run

Run container:

docker run --rm mlops-task

Expected output:

{
  "version": "v1",
  "rows_processed": 10000,
  "metric": "signal_rate",
  "value": 0.4989,
  "latency_ms": 120,
  "seed": 42,
  "status": "success"
}
Features
Reproducibility
Deterministic runs using seed
Config-driven pipeline
Observability
Structured logging
Metrics JSON output
Deployment Ready
Dockerized pipeline
One command execution
Metrics

The following metrics are generated:

Metric	Description
rows_processed	Number of rows processed
signal_rate	Percentage of buy signals
latency_ms	Runtime latency
Error Handling

If an error occurs:

{
  "version": "v1",
  "status": "error",
  "error_message": "Description of error"
}
CLI Arguments
Argument	Description
--input	Input CSV file
--config	Config YAML file
--output	Output metrics JSON
--log-file	Log file
Technologies Used
Python
Pandas
NumPy
PyYAML
Docker
Evaluation Checklist

-  Deterministic runs
-  Structured logging
-  Metrics JSON
-  Dockerized execution
- Clean error handling
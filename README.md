# PHM Data Pipeline

A functional ETL pipeline for Prognostics and Health Management (PHM), designed to process vehicle telemetry data, identify anomalies, and assign health scores.

## Features

-   **ETL Architecture**: Functional `Extract -> Transform -> Load` workflow.
-   **Automated Context**: Detects vehicle motion, speed bands, RPM, and load context.
-   **Anomaly Detection**: Calculates Z-scores and flags anomalies based on dynamic baselines.
-   **Health Scoring**: Computes engine health scores and identifies potential issues (e.g., "Coolant leak", "Oil starvation").
-   **Smart Paths**: Automatically handles input/output in the `data/` directory.

## Installation

1.  Clone the repository.
2.  Install dependencies:
    ```bash
    pip install -r requirements.txt
    ```

## Usage

The pipeline is executed via `main.py`.

### Basic Usage
Place your input CSV file in the `data/` directory (e.g., `data/data.csv`).

```bash
python3 main.py --input data.csv
```
This will:
1.  Look for `data.csv` in `data/`.
2.  Process the data.
3.  Save the output to `data/phm_etl_output.csv` (default).

### Custom Output Name
You can specify a custom output filename:

```bash
python3 main.py --input data.csv --output my_results.csv
```
This will save the output to `data/my_results.csv`.

### Full Paths
You can also use full paths if your files are not in `data/`:

```bash
python3 main.py --input /path/to/input.csv --output /path/to/output.csv
```

## Directory Structure

```
.
├── config.py           # Configuration thresholds and constants
├── calculations.py     # Pure logic for health scoring and anomalies
├── load.py             # Data loading and validation
├── pipeline.py         # Functional ETL pipeline logic
├── main.py             # Entry point (CLI)
├── requirements.txt    # Project dependencies
└── data/               # Directory for input/output CSVs
    ├── data.csv        # Example input
    └── ...
```

## Logic Overview

1.  **Extract**: Load CSV and validate columns.
2.  **Transform**:
    -   **Context**: Assign driving states (IDLE, HIGHWAY, etc.).
    -   **Baseline**: Compute vehicle-specific baselines per context.
    -   **Z-Scores**: standard deviations from baseline.
    -   **Slopes**: Calculate trends over time (1h, 24h, etc.).
    -   **Health Score**: Weighted score based on temperature, pressure, and levels.
    -   **Issue Classification**: Rule-based detection (Coolant Leak, Fan Failure, etc.).
    -   **Persistence**: Count consecutive anomalous events.
3.  **Load**: Export enriched data to CSV.

## Fuel Pipeline (Simple guide)

- **Overview**: A focused workflow to compute fuel-related features, detect anomalies in fuel usage, and export results as CSV.

- **Install**:
  - Create and activate a virtual environment (recommended).
  - Install project dependencies:

    ```bash
    pip install -r requirements.txt
    ```

- **Run (basic)**:
  - Put your fuel CSV in the `data/` folder (for example `data/fuel_features.csv`).
  - Run the pipeline:

    ```bash
    python main.py --input fuel_features.csv
    ```

  - This reads `data/fuel_features.csv`, processes fuel features, and writes output into the `data/` directory (default filename shown by the CLI).

- **Run (custom output)**:

    ```bash
    python main.py --input fuel_features.csv --output fuel_output.csv
    ```

- **How it works (simple)**:
  - Extract: `load.py` reads and validates the CSV.
  - Transform: `fuel_features.py` and `calculations.py` compute fuel features, baselines, and anomalies.
  - Load: `pipeline.py` / `main.py` write the enriched CSV to `data/`.

- **Important files**:
  - `fuel_features.py` — fuel feature calculations
  - `calculations.py` — scoring and anomaly logic
  - `pipeline.py` / `main.py` — orchestrates the ETL
  - `load.py` — CSV loading and validation

- **Troubleshooting**:
  - Ensure Python 3.8+ is used and the virtualenv is active.
  - Reinstall dependencies if errors occur: `pip install -r requirements.txt`.
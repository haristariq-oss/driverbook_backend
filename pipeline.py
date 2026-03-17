#pipeline.py
from __future__ import annotations

import logging
from typing import Optional
import os
import pandas as pd

import calculations
import config
import load
import fuel_features  
from db_utils import upsert_latest_records
# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def extract(input_path: str, data_type: str = "phm") -> pd.DataFrame:
    """
    Extract phase: Load and validate data.
    """
    logger.info(f"Starting Extract phase for {input_path}")
    
    # Load
    df = load.load_csv(input_path)
    logger.info(f"Loaded {len(df)} rows")

    # Skip validation for fuel CSV
    if data_type.lower() == "phm":
        ok, errors = load.validate_input_dataframe(df)
        if not ok:
            error_msg = "Input validation failed: " + " | ".join(errors)
            logger.error(error_msg)
            raise ValueError(error_msg)
    else:
        logger.info("Skipping PHM column validation for fuel CSV")

    return df


def transform(df: pd.DataFrame, data_type: str = "phm") -> pd.DataFrame:
    """
    Transform phase: Apply PHM calculations or fuel features.
    """
    if data_type.lower() == "fuel":
        logger.info("Detected fuel CSV. Adding fuel features...")
        df = fuel_features.add_fuel_features(df)
        logger.info("Fuel features added successfully")
        return df  # for fuel, skip PHM calculations

    # -------------------------
    # PHM Transform (unchanged)
    # -------------------------
    logger.info("Starting PHM Transform phase")

    # Numeric coercion
    numeric_cols = [
        "speed_mph", "rpm", "load_pct",
        "engineCoolantTemp_F", "engineOilTemp_F",
        "engineCoolantLevel_pct", "engineOilPressure",
    ]
    for c in numeric_cols:
        if c in df.columns:
            df[c] = df[c].astype(str).str.replace(",", "", regex=False).str.strip()
            df[c] = pd.to_numeric(df[c], errors="coerce")

    df[config.TIMESTAMP_COL] = pd.to_datetime(df[config.TIMESTAMP_COL], errors="coerce")
    df = df.dropna(subset=[config.TIMESTAMP_COL, config.VEHICLE_ID_COL] + numeric_cols)
    logger.info(f"After cleanup: {len(df)} rows")

    # Context
    df = calculations.assign_context(df)
    baseline = calculations.compute_baseline_profiles(df)
    df = calculations.attach_baseline(df, baseline)
    df = calculations.compute_zscores(df)
    df = calculations.add_anomaly_flags(df)
    df = calculations.compute_dense_slopes(df)
    df = calculations.compute_engine_health_score(df)
    df = calculations.assign_issue_type(df)
    df = calculations.compute_persistence_counts(df)
    df = calculations.compute_severity_and_confidence(df)
    df = calculations.select_output_columns(df)

    return df


def load_data(df: pd.DataFrame, output_path: str) -> None:
    """
    Load phase: Save results to output.
    """
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    logger.info(f"Saving output to {output_path}")
    df.to_csv(output_path, index=False)
    logger.info(f"Successfully saved {len(df)} rows")


def run_pipeline(input_path: str, output_path: str, data_type: str = "phm") -> None:
    """
    Run the full ETL pipeline.
    """
    df = extract(input_path, data_type=data_type)
    df_transformed = transform(df, data_type=data_type)
    load_data(df_transformed, output_path)
    upsert_latest_records(df_transformed)



if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1:
        out_arg = sys.argv[2] if len(sys.argv) > 2 else "phm_etl_output.csv"
        # Auto-prepend data/ if only filename
        if not os.path.dirname(out_arg):
            out_path = os.path.join("data", out_arg)
        else:
            out_path = out_arg
        
        in_path = sys.argv[1]
        if not os.path.exists(in_path) and not os.path.dirname(in_path):
            possible = os.path.join("data", in_path)
            if os.path.exists(possible):
                in_path = possible

        dtype = sys.argv[3] if len(sys.argv) > 3 else "phm"
        run_pipeline(in_path, out_path, data_type=dtype)
    else:
        print("Usage: python pipeline.py <input_csv> [output_csv] [phm|fuel]")

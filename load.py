#load.py
"""
Data Loading & Basic Validation ONLY
"""

from __future__ import annotations
from typing import List, Tuple
import pandas as pd
import config


def load_csv(path: str) -> pd.DataFrame:
    """
    Load CSV into a DataFrame.
    Normalize column names to match config.
    """
    df = pd.read_csv(path)
    
    # ✅ Normalize column name (case-insensitive match)
    # Find the actual vehicle ID column name
    vehicle_col_candidates = ['vehicleId', 'VehicleId', 'vehicle_id', 'VEHICLEID', 'vehicleid']
    for candidate in vehicle_col_candidates:
        if candidate in df.columns:
            if candidate != config.VEHICLE_ID_COL:
                df.rename(columns={candidate: config.VEHICLE_ID_COL}, inplace=True)
                print(f"Renamed '{candidate}' → '{config.VEHICLE_ID_COL}'")
            break
    
    # Same for timestamp
    timestamp_candidates = ['timestamp', 'Timestamp', 'TIMESTAMP', 'time', 'Time']
    for candidate in timestamp_candidates:
        if candidate in df.columns:
            if candidate != config.TIMESTAMP_COL:
                df.rename(columns={candidate: config.TIMESTAMP_COL}, inplace=True)
                print(f"Renamed '{candidate}' → '{config.TIMESTAMP_COL}'")
            break
    
    return df


def validate_input_dataframe(df: pd.DataFrame) -> Tuple[bool, List[str]]:
    """
    Validate presence of required columns and non-empty data.
    Returns (is_valid, errors).
    """
    errors: List[str] = []

    if df is None or df.empty:
        errors.append("Input dataframe is empty.")
        return False, errors

    missing = [c for c in config.REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        errors.append(f"Missing required columns: {missing}")

    # timestamp and vehicleId are critical
    if config.TIMESTAMP_COL not in df.columns:
        errors.append(f"Missing timestamp column: {config.TIMESTAMP_COL}")
    if config.VEHICLE_ID_COL not in df.columns:
        errors.append(f"Missing vehicle id column: {config.VEHICLE_ID_COL}")

    return (len(errors) == 0), errors

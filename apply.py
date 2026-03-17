# =========================
# File-4: apply.py
# =========================
"""
Pipeline Orchestration Layer ONLY

Responsibilities:
- Call load + calculation functions
- Execute steps in correct order:
STEP-03 -> STEP-04 -> STEP-05 -> STEP-06 -> STEP-07 -> STEP-08
- No thresholds, no formulas, no statistics here
"""

from __future__ import annotations

import pandas as pd

import calculations
import config
import load


def run_phm_pipeline_up_to_step08(input_csv_path: str) -> pd.DataFrame:
    # Load + validate
    df = load.load_csv(input_csv_path)
    print(f"After load: {len(df)} rows")
    
    ok, errors = load.validate_input_dataframe(df)
    if not ok:
        raise ValueError("Input validation failed: " + " | ".join(errors))

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
    print(f"After cleanup: {len(df)} rows")

    # STEP-03
    df = calculations.assign_context(df)
    print(f"After STEP-03: {len(df)} rows, type: {type(df)}")

    # STEP-04
    baseline = calculations.compute_baseline_profiles(df)
    print(f"After baseline compute: {len(baseline)} profiles")
    df = calculations.attach_baseline(df, baseline)
    print(f"After STEP-04: {len(df)} rows, type: {type(df)}")

    # STEP-05
    df = calculations.compute_zscores(df)
    print(f"After STEP-05 zscores: {len(df)} rows, type: {type(df)}")
    df = calculations.add_anomaly_flags(df)
    print(f"After STEP-05 flags: {len(df)} rows, type: {type(df)}")

    # STEP-06
    df = calculations.compute_dense_slopes(df)
    print(f"After STEP-06: {len(df)} rows, type: {type(df)}")

    # STEP-07
    df = calculations.compute_engine_health_score(df)
    print(f"After STEP-07 health: {len(df)} rows, type: {type(df)}")
    df = calculations.assign_issue_type(df)
    print(f"Type of df after assign_issue_type: {type(df)}")
    print(f"Is df None? {df is None}")
    if df is not None:
        print(f"Columns in df: {df.columns.tolist()}")
        print(f"'issue_type' in columns? {'issue_type' in df.columns}")
    else:
        print("ERROR: assign_issue_type returned None!")
    print(f"After STEP-07 issue: {len(df)} rows, type: {type(df)}")
    #df = calculations.assign_issue_type(df)
    

    print("AFTER STEP-07 issue value_counts:")
    print(df["issue_type"].value_counts(dropna=False))

    print("Sample issue_type STEP-07:")
    print(df["issue_type"].head(10).tolist())

        

    # STEP-08
    df = calculations.compute_persistence_counts(df)
    print("AFTER STEP-08 severity value_counts:")
    print(df["issue_type"].value_counts(dropna=False))
    # print(f"After STEP-08 persistence: {len(df)} rows, type: {type(df)}")
    df = calculations.compute_severity_and_confidence(df)
    print(f"After STEP-08 severity: {len(df)} rows, type: {type(df)}")

    # Final output
    print(f"Before select_output_columns: df type = {type(df)}")
    if df is None:
        raise ValueError("DataFrame is None before select_output_columns!")
    
    df_out = calculations.select_output_columns(df)
    print(f"Final output: {len(df_out)} rows")
    return df_out
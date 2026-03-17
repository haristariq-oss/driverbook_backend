# calculations.py
"""
Pure Mathematical & Statistical Logic (Stateless Functions Only)
"""

from __future__ import annotations
from dataclasses import dataclass
from typing import Dict, List, Tuple
import numpy as np
import pandas as pd
import config



def _bucket_score(value: float, buckets: List[Tuple[float, float]]) -> float:
    """Return the score for the first bucket where value < upper_bound."""
    for upper, score in buckets:
        if value < upper:
            return score
    return buckets[-1][1]


def _label_from_score(score: float, thresholds: List[Tuple[float, str]]) -> str:
    """Return label based on score thresholds (score < threshold => label)."""
    for upper, label in thresholds:
        if score < upper:
            return label
    return thresholds[-1][1]



def assign_context(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add motion_ctx, speed_ctx, rpm_ctx, load_ctx, context_key.
    Uses config thresholds only.
    """
    out = df.copy()

    speed = out["speed_mph"].astype(float)
    rpm = out["rpm"].astype(float)
    load = out["load_pct"].astype(float)

    # motion_ctx
    if "speed_mph" in out.columns:
        out["motion_ctx"] = np.where(speed <= config.IDLE_SPEED_MPH, "IDLE", "DRIVING")
    else:
        # fallback to moving only if speed missing (rare; speed is required in config)
        if "moving" in out.columns:
            out["motion_ctx"] = np.where(out["moving"].astype(int) == 0, "IDLE", "DRIVING")
        else:
            out["motion_ctx"] = "UNKNOWN"

    # speed_ctx
    def _speed_band(v: float) -> str:
        if v <= 0:
            return "STOPPED"
        if v <= config.SPEED_CITY_MAX:
            return "CITY"
        if v <= config.SPEED_CRUISE_MAX:
            return "CRUISE"
        return "HIGHWAY"

    out["speed_ctx"] = speed.apply(_speed_band)

    # rpm_ctx
    def _rpm_band(v: float) -> str:
        if v < config.RPM_IDLE_MAX:
            return "IDLE_RPM"
        if v <= config.RPM_CRUISE_MAX:
            return "CRUISE_RPM"
        return "HIGH_RPM"

    out["rpm_ctx"] = rpm.apply(_rpm_band)

    # load_ctx
    def _load_band(v: float) -> str:
        if v < config.LOAD_LOW_MAX:
            return "LOW_LOAD"
        if v <= config.LOAD_MED_MAX:
            return "MED_LOAD"
        return "HIGH_LOAD"

    out["load_ctx"] = load.apply(_load_band)

    # combined context key
    out["context_key"] = (
        out["motion_ctx"].astype(str) + config.CONTEXT_SEP +
        out["speed_ctx"].astype(str) + config.CONTEXT_SEP +
        out["rpm_ctx"].astype(str) + config.CONTEXT_SEP +
        out["load_ctx"].astype(str)
    )
    return out



def compute_baseline_profiles(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute baseline mean/std/count per (vehicleId, context_key) for required signals.
    Apply MIN_SAMPLES_PER_CONTEXT gating and STD_EPS stabilization.
    """
    signals = [
        "engineCoolantTemp_F",
        "engineOilTemp_F",
        "engineCoolantLevel_pct",
        "engineOilPressure",
    ]

    group_cols = [config.VEHICLE_ID_COL, "context_key"]

    agg_map = {}
    for s in signals:
        agg_map[s] = ["count", "mean", "std"]

    base = df.groupby(group_cols, dropna=False).agg(agg_map)
    # flatten columns
    base.columns = [f"{c0}_{c1}" for (c0, c1) in base.columns]
    base = base.reset_index()

    # baseline validity and std stabilization
    # choose a conservative baseline_count = min count across signals
    count_cols = [f"{s}_count" for s in signals]
    base["baseline_sample_count"] = base[count_cols].min(axis=1)
    base["baseline_valid"] = base["baseline_sample_count"] >= config.MIN_SAMPLES_PER_CONTEXT

    std_cols = [f"{s}_std" for s in signals]
    for c in std_cols:
        base[c] = base[c].fillna(0.0)
        base[c] = np.where(base[c] < config.STD_EPS, config.STD_EPS, base[c])

    return base


def attach_baseline(df: pd.DataFrame, baseline_df: pd.DataFrame) -> pd.DataFrame:
    """
    Join baseline profiles back to telemetry rows by (vehicleId, context_key).
    """
    key_cols = [config.VEHICLE_ID_COL, "context_key"]
    out = df.merge(baseline_df, on=key_cols, how="left")
    return out



def compute_zscores(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute z-scores for each signal using attached baseline mean/std.
    Only valid when baseline_valid is True.
    """
    out = df.copy()

    def _z(x, mean, std):
        return (x - mean) / std

    # signals and corresponding baseline columns
    mapping = {
        "engineCoolantTemp_F": ("engineCoolantTemp_F_mean", "engineCoolantTemp_F_std", "z_coolant_temp"),
        "engineOilTemp_F": ("engineOilTemp_F_mean", "engineOilTemp_F_std", "z_oil_temp"),
        "engineCoolantLevel_pct": ("engineCoolantLevel_pct_mean", "engineCoolantLevel_pct_std", "z_coolant_level"),
        "engineOilPressure": ("engineOilPressure_mean", "engineOilPressure_std", "z_oil_pressure"),
    }

    valid = out["baseline_valid"].fillna(False)

    for raw_col, (mcol, scol, zcol) in mapping.items():
        out[zcol] = np.nan
        mask = valid & out[raw_col].notna() & out[mcol].notna() & out[scol].notna()
        out.loc[mask, zcol] = _z(out.loc[mask, raw_col], out.loc[mask, mcol], out.loc[mask, scol])

    return out


def add_anomaly_flags(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add per-signal anomaly flags based on |Z| >= Z_ANOMALY_THRESHOLD.
    """
    out = df.copy()
    for zcol in ["z_coolant_temp", "z_oil_temp", "z_coolant_level", "z_oil_pressure"]:
        out[f"anom_{zcol}"] = (out[zcol].abs() >= config.Z_ANOMALY_THRESHOLD)
    return out



def _rolling_slope_z_per_hour(ts: pd.Series, vals: pd.Series, window: str) -> pd.Series:
    """
    Compute rolling slope as (last-first)/delta_time_hours over a time-based rolling window.
    Requires ts as datetime, vals numeric, both aligned.
    Output is Z per hour (signed).
    """
    temp = pd.DataFrame({"ts": ts, "v": vals}).dropna()
    if temp.empty:
        return pd.Series(index=ts.index, dtype=float)

    temp = temp.sort_values("ts").set_index("ts")

    def slope_func(x: pd.Series) -> float:
        if x.size < 2:
            return np.nan
        t0 = x.index[0]
        t1 = x.index[-1]
        dt_hours = (t1 - t0).total_seconds() / 3600.0
        if dt_hours <= 0:
            return np.nan
        return (x.iloc[-1] - x.iloc[0]) / dt_hours

    rolled = temp["v"].rolling(window=window, min_periods=2).apply(slope_func, raw=False)

    # reindex to original rows (aligned by timestamp)
    # We'll map back using merge_asof-like behavior: exact timestamp alignment.
    out = pd.Series(index=ts.index, dtype=float)
    # exact alignment: timestamps in temp index are subset of ts
    aligned = rolled.reindex(temp.index)
    # write into positions where ts equals those timestamps
    # since ts can have duplicates, we'll merge on ts using a join
    tmp_out = pd.DataFrame({"ts": ts, "idx": ts.index}).merge(
        pd.DataFrame({"ts": aligned.index, "slope": aligned.values}),
        on="ts",
        how="left",
    )
    out.loc[tmp_out["idx"].values] = tmp_out["slope"].values
    return out


def compute_dense_slopes(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute rolling slopes for all four z-score signals within (vehicleId, context_key).
    Produces slope columns for 30m/1h/12h/24h for each signal.
    """
    out = df.copy()
    out[config.TIMESTAMP_COL] = pd.to_datetime(out[config.TIMESTAMP_COL], errors="coerce")

    group_cols = [config.VEHICLE_ID_COL, "context_key"]
    zcols = ["z_coolant_temp", "z_oil_temp", "z_coolant_level", "z_oil_pressure"]

    for zc in zcols:
        for win_key, win_str in config.SLOPE_WINDOWS.items():
            out[f"slope{win_key}_{zc}"] = np.nan

    # compute slopes groupwise (no cross-context mixing)
    # Explicit loop to avoid pandas apply-hide-key issues
    groups = []
    for keys, g in out.groupby(group_cols, dropna=False):
        g = g.copy() # ensure we work on a copy
        ts = g[config.TIMESTAMP_COL]
        for zc in zcols:
            for win_key, win_str in config.SLOPE_WINDOWS.items():
                g[f"slope{win_key}_{zc}"] = _rolling_slope_z_per_hour(ts, g[zc], win_str)
        groups.append(g)
    
    if groups:
        out = pd.concat(groups).reset_index(drop=True)
    else:
        out = out.iloc[0:0] # empty result if input input empty
    return out



def compute_engine_health_score(df: pd.DataFrame) -> pd.DataFrame:
    """
    Option-2 weighted score:
    - Heat score (positive): coolant temp high, oil temp high, coolant level low
    - Pressure score (negative): oil pressure low
    Final score = heat_score - pressure_score
    Positive => heat-dominated, Negative => pressure-dominated
    """
    out = df.copy()

    zct = out["z_coolant_temp"]
    zot = out["z_oil_temp"]
    zcl = out["z_coolant_level"]
    zop = out["z_oil_pressure"]

    heat_score = (
        config.WEIGHT_COOLANT_TEMP * np.maximum(0.0, zct) +
        config.WEIGHT_OIL_TEMP * np.maximum(0.0, zot) +
        config.WEIGHT_COOLANT_LEVEL * np.maximum(0.0, -zcl)
    )
    pressure_score = config.WEIGHT_OIL_PRESSURE * np.maximum(0.0, -zop)

    out["engine_health_score"] = heat_score - pressure_score

    out["dominance"] = np.where(
        out["engine_health_score"].isna(),
        "UNKNOWN",
        np.where(out["engine_health_score"] > 0, "HEAT",
                np.where(out["engine_health_score"] < 0, "PRESSURE", "MIXED"))
    )
    return out



def assign_issue_type(df: pd.DataFrame) -> pd.DataFrame:
    """
    Basic rule-based issue typing (STEP-07).
    Adds:
    - issue_type
    - pattern_match_ratio (0..1)

    Priority (highest wins):
    Oil starvation / pump wear > Fan failure > Radiator clogging > Coolant leak

    Implementation detail:
    - We apply LOWEST priority first, HIGHEST last so higher priority overwrites.
    - pattern_match_ratio is computed safely using a boolean matrix mean (no np.vectorize/zip issues).
    """
    out = df.copy()

    # Default
    out["issue_type"] = "Unclassified"
    out["pattern_match_ratio"] = 0.0

    # convenient variables
    zct = out["z_coolant_temp"]
    zot = out["z_oil_temp"]
    zcl = out["z_coolant_level"]
    zop = out["z_oil_pressure"]

    # helper: compute and set match ratio for rows where mask=True
    def _set_ratio(mask: pd.Series, conds: list[pd.Series]) -> None:
        if mask is None or mask.sum() == 0:
            return
        mat = np.column_stack([c.loc[mask].to_numpy(dtype=bool) for c in conds])
        out.loc[mask, "pattern_match_ratio"] = mat.mean(axis=1)

    # =========================================================
    # Apply rules from LOWEST -> HIGHEST priority
    # =========================================================

    # -------------------------
    # 1) Coolant leak (LOWEST priority)
    # -------------------------
    slope_lvl_24 = out.get(
        "slope24h_z_coolant_level",
        pd.Series(index=out.index, dtype=float)
    )
    conds_leak = [
        (zcl <= config.COOLANT_LEAK_Z_LEVEL_MAX),
        (zct >= config.COOLANT_LEAK_Z_TEMP_MIN) | (zct.isna()),  # temp may rise later
        (slope_lvl_24 <= config.COOLANT_LEAK_LEVEL_SLOPE_NEG_MIN) | (slope_lvl_24.isna()),
    ]
    leak_mask = conds_leak[0]
    out.loc[leak_mask, "issue_type"] = "Coolant leak"
    _set_ratio(leak_mask, conds_leak)

    # -------------------------
    # 2) Radiator clogging
    # -------------------------
    conds_rad = [
        (out["motion_ctx"] == config.RAD_CLOG_MOTION_REQUIRED),
        (zct >= config.RAD_CLOG_Z_TEMP_MIN),
        (zot >= config.RAD_CLOG_Z_OILT_MIN),
        (zcl.abs() < config.RAD_CLOG_LEVEL_ABS_MAX),
    ]
    rad_mask = conds_rad[0] & conds_rad[1] & conds_rad[2]
    out.loc[rad_mask, "issue_type"] = "Radiator clogging"
    _set_ratio(rad_mask, conds_rad)

    # -------------------------
    # 3) Fan failure (idle overheating)
    # -------------------------
    conds_fan = [
        (out["motion_ctx"] == config.FAN_FAIL_MOTION_REQUIRED),
        (zct >= config.FAN_FAIL_Z_TEMP_MIN),
        (zot.abs() < config.FAN_FAIL_OILT_ABS_MAX),
    ]
    fan_mask = conds_fan[0] & conds_fan[1]
    out.loc[fan_mask, "issue_type"] = "Fan failure"
    _set_ratio(fan_mask, conds_fan)

    # -------------------------
    # 4) Oil starvation / pump wear (HIGHEST priority)
    # -------------------------
    conds_starv = [
        (zop <= config.OIL_STARV_Z_PRESS_MAX),
        (out["load_ctx"].isin(config.OIL_STARV_LOAD_REQUIRED)),
        (out["rpm_ctx"].isin(config.OIL_STARV_RPM_REQUIRED)),
        (zct.abs() < config.OIL_STARV_COOLANT_ABS_MAX),
        (zcl.abs() < config.OIL_STARV_LEVEL_ABS_MAX),
    ]
    starv_mask = conds_starv[0] & conds_starv[1] & conds_starv[2]
    out.loc[starv_mask, "issue_type"] = "Oil starvation / pump wear"
    _set_ratio(starv_mask, conds_starv)

    return out



def _dominant_signal_for_issue(row: pd.Series) -> str:
    issue = row.get("issue_type", "Unclassified")
    if issue == "Oil starvation / pump wear":
        return "oil_pressure"
    if issue == "Coolant leak":
        return "coolant_level"
    if issue == "Fan failure":
        return "coolant_temp"
    if issue == "Radiator clogging":
        # choose the larger |Z| between coolant and oil temp
        zct = row.get("z_coolant_temp", np.nan)
        zot = row.get("z_oil_temp", np.nan)
        if np.isnan(zct) and np.isnan(zot):
            return "coolant_temp"
        if np.isnan(zot):
            return "coolant_temp"
        if np.isnan(zct):
            return "oil_temp"
        return "coolant_temp" if abs(zct) >= abs(zot) else "oil_temp"
    # fallback: max |Z| among all
    zmap = {
        "coolant_temp": row.get("z_coolant_temp", np.nan),
        "oil_temp": row.get("z_oil_temp", np.nan),
        "coolant_level": row.get("z_coolant_level", np.nan),
        "oil_pressure": row.get("z_oil_pressure", np.nan),
    }
    best = max(zmap.items(), key=lambda kv: -np.inf if np.isnan(kv[1]) else abs(kv[1]))
    return best[0]


def compute_persistence_counts(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute persistence_count as consecutive occurrences of the same issue_type
    within the same vehicle (time-ordered).

    FINAL RULES (as requested):
    - We DO NOT use severity_label at all.
    - We DO NOT use issue_type == "NORMAL" anywhere.
    - "Unclassified" is the default / no-specific-pattern state.
    - A row is ACTIVE only if anomaly evidence exists:
        - Prefer anomaly flags: anom_z_* == True
        - Fallback: abs(z_*) >= config.Z_ANOMALY_THRESHOLD
    - If NOT ACTIVE → persistence_count = 0 and reset run.
    - If ACTIVE:
        - If same issue_type repeats → run += 1
        - Else (issue changed) → run = 1
    """


    out = df.copy()
    out[config.TIMESTAMP_COL] = pd.to_datetime(out[config.TIMESTAMP_COL], errors="coerce")
    out["persistence_count"] = 0

    # Z columns
    zcols = ["z_coolant_temp", "z_oil_temp", "z_coolant_level", "z_oil_pressure"]
    anom_cols = [f"anom_{z}" for z in zcols]

    has_anom_cols = all(c in out.columns for c in anom_cols)
    has_zcols = all(c in out.columns for c in zcols)

    thr = float(getattr(config, "Z_ANOMALY_THRESHOLD", 3.0))

    def _row_is_anomalous(row: pd.Series) -> bool:
        # Prefer anomaly flags if available
        if has_anom_cols:
            return bool(
                row.get("anom_z_coolant_temp", False)
                or row.get("anom_z_oil_temp", False)
                or row.get("anom_z_coolant_level", False)
                or row.get("anom_z_oil_pressure", False)
            )

        # Fallback: z threshold checks
        if has_zcols:
            vals = [
                row.get("z_coolant_temp", np.nan),
                row.get("z_oil_temp", np.nan),
                row.get("z_coolant_level", np.nan),
                row.get("z_oil_pressure", np.nan),
            ]
            return any(pd.notna(v) and abs(v) >= thr for v in vals)

        # If neither exist, we cannot detect anomaly
        return False

    def _apply_vehicle(g: pd.DataFrame) -> pd.DataFrame:
        g = g.sort_values(config.TIMESTAMP_COL)
        counts: list[int] = []
        prev_issue: str | None = None
        run = 0

        for _, row in g.iterrows():
            issue = str(row.get("issue_type", "Unclassified")).strip()
            if issue == "":
                issue = "Unclassified"

            active = _row_is_anomalous(row)

            # RESET if no anomaly evidence
            if not active:
                run = 0
                prev_issue = None
                counts.append(0)
                continue

            # COUNT if anomaly evidence exists
            if issue == prev_issue:
                run += 1
            else:
                run = 1
                prev_issue = issue

            counts.append(run)

        g["persistence_count"] = counts
        return g

    groups = []
    for v_id, g in out.groupby(config.VEHICLE_ID_COL, dropna=False):
        g = _apply_vehicle(g.copy())
        groups.append(g)

    if groups:
        out = pd.concat(groups).reset_index(drop=True)
    else:
        out = out.iloc[0:0]
    return out


def compute_severity_and_confidence(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add:
    - dominant_signal
    - severity_score, severity_label
    - confidence_score
    - reason_codes
    """
    out = df.copy()

    # dominant signal
    out["dominant_signal"] = out.apply(_dominant_signal_for_issue, axis=1)

    # helpers to fetch dominant Z and dominant slope
    def get_dom_z(row: pd.Series) -> float:
        ds = row["dominant_signal"]
        if ds == "coolant_temp":
            return row.get("z_coolant_temp", np.nan)
        if ds == "oil_temp":
            return row.get("z_oil_temp", np.nan)
        if ds == "coolant_level":
            return row.get("z_coolant_level", np.nan)
        return row.get("z_oil_pressure", np.nan)

    def get_dom_slope(row: pd.Series) -> float:
        ds = row["dominant_signal"]
        # prefer 1h, fallback 30m
        zcol = {
            "coolant_temp": "z_coolant_temp",
            "oil_temp": "z_oil_temp",
            "coolant_level": "z_coolant_level",
            "oil_pressure": "z_oil_pressure",
        }[ds]
        s1h = row.get(f"slope1h_{zcol}", np.nan)
        s30 = row.get(f"slope30m_{zcol}", np.nan)
        return s1h if pd.notna(s1h) else s30

    out["dominant_z"] = out.apply(get_dom_z, axis=1)
    out["dominant_slope"] = out.apply(get_dom_slope, axis=1)

    # -------------------------
    # Severity score
    # -------------------------
    abs_z = out["dominant_z"].abs()
    abs_slope = out["dominant_slope"].abs()

    out["magnitude_points"] = abs_z.apply(lambda v: _bucket_score(v, config.MAG_POINTS) if pd.notna(v) else 0)
    out["trend_points"] = abs_slope.apply(lambda v: _bucket_score(v, config.TREND_POINTS) if pd.notna(v) else 0)

    # persistence points from persistence_count
    def _persist_points(n: int) -> float:
        if n <= 0:
            return 0
        if n == 1:
            return 0
        if n <= 3:
            return 5
        return 10

    out["persistence_points"] = out["persistence_count"].fillna(0).astype(int).apply(_persist_points)

    out["severity_score"] = (out["magnitude_points"] + out["trend_points"] + out["persistence_points"]).clip(0, 100)
    out["severity_label"] = out["severity_score"].apply(lambda s: _label_from_score(s, config.SEVERITY_LABELS))

    # -------------------------
    # Confidence score components
    # -------------------------
    # Pattern score bucket from match ratio
    def _pattern_bucket(r: float) -> float:
        if pd.isna(r):
            return 0.3
        if r < 0.5:
            return 0.3
        if r <= 0.75:
            return 0.6
        return 1.0

    out["pattern_score"] = out["pattern_match_ratio"].apply(_pattern_bucket)

    # persistence score bucket
    def _persist_bucket(n: int) -> float:
        if n <= 1:
            return 0.3
        if n <= 3:
            return 0.6
        return 1.0

    out["persistence_score"] = out["persistence_count"].fillna(0).astype(int).apply(_persist_bucket)

    # multi-signal agreement: how many signals have |Z| >= threshold
    zcols = ["z_coolant_temp", "z_oil_temp", "z_coolant_level", "z_oil_pressure"]
    out["abnormal_signal_count"] = out[zcols].abs().ge(config.Z_ANOMALY_THRESHOLD).sum(axis=1)
    out["agreement_score"] = np.where(out["abnormal_signal_count"] >= 2, 1.0, 0.5)

    # baseline quality from baseline_sample_count
    def _baseline_quality(n: float) -> float:
        if pd.isna(n):
            return 0.7
        for upper, score in config.BASELINE_QUALITY_BUCKETS:
            if n <= upper:
                return score
        return 1.0

    out["baseline_quality_score"] = out.get("baseline_sample_count", np.nan).apply(_baseline_quality)

    out["confidence_score"] = (
        config.CONF_PATTERN_W * out["pattern_score"] +
        config.CONF_PERSIST_W * out["persistence_score"] +
        config.CONF_AGREE_W * out["agreement_score"] +
        config.CONF_BASELINE_W * out["baseline_quality_score"]
    ).clip(0.0, 1.0)

    # -------------------------
    # Reason codes (explainable tags)
    # -------------------------
    def _reason_codes(row: pd.Series) -> str:
        codes: List[str] = []

        # magnitude based
        if pd.notna(row.get("z_coolant_temp")) and row["z_coolant_temp"] >= config.Z_ANOMALY_THRESHOLD:
            codes.append("Z_HIGH_COOLANT_TEMP")
        if pd.notna(row.get("z_oil_temp")) and row["z_oil_temp"] >= config.Z_ANOMALY_THRESHOLD:
            codes.append("Z_HIGH_OIL_TEMP")
        if pd.notna(row.get("z_coolant_level")) and row["z_coolant_level"] <= -config.Z_ANOMALY_THRESHOLD:
            codes.append("Z_LOW_COOLANT_LEVEL")
        if pd.notna(row.get("z_oil_pressure")) and row["z_oil_pressure"] <= -config.Z_ANOMALY_THRESHOLD:
            codes.append("Z_LOW_OIL_PRESSURE")

        # critical flags
        if pd.notna(row.get("dominant_z")) and abs(row["dominant_z"]) >= config.Z_WARNING_MAX:
            codes.append("Z_CRITICAL_DOMINANT")

        # slope direction tags (signed)
        slope = row.get("dominant_slope", np.nan)
        if pd.notna(slope):
            if abs(slope) >= config.SLOPE_STEEP:
                codes.append("SLOPE_FAST")
            if slope > config.SLOPE_EPS:
                codes.append("SLOPE_UP")
            elif slope < -config.SLOPE_EPS:
                codes.append("SLOPE_DOWN")

        # persistence
        if row.get("persistence_count", 0) >= 3:
            codes.append("PERSISTENT")

        # context hints
        if row.get("motion_ctx") == "IDLE":
            codes.append("IDLE_CTX")
        if row.get("load_ctx") == "HIGH_LOAD":
            codes.append("HIGH_LOAD_CTX")

        # if still empty
        if not codes and row.get("issue_type") == "Unclassified":
            codes.append("UNCLASSIFIED_BEHAVIOR")

        return ";".join(sorted(set(codes)))

    out["reason_codes"] = out.apply(_reason_codes, axis=1)

    # keep outputs tidy
    return out



def select_output_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Return ALL calculated columns from the entire pipeline (STEP-03 to STEP-08).
    No filtering - sab kuch CSV mein export hoga.
    """
    # Simply return the complete dataframe with all columns
    return df.copy()
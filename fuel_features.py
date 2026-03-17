# fuel_features.py
import pandas as pd
import numpy as np
import logging

logger = logging.getLogger(__name__)

def add_fuel_features(
    df: pd.DataFrame,
    interval_sec: float = 3600,
    smooth_window: int = 7
) -> pd.DataFrame:
    """
    Fuel intelligence & prediction features.
    Production-hardened with error handling.
    """

    try:
        # ===============================
        # Input validation
        # ===============================
        if df is None or df.empty:
            raise ValueError("Input dataframe is empty or None")

        if not isinstance(df, pd.DataFrame):
            raise TypeError("Input must be a pandas DataFrame")

        if interval_sec <= 0:
            raise ValueError("interval_sec must be greater than zero")

        if smooth_window <= 0:
            raise ValueError("smooth_window must be greater than zero")

        df = df.copy()

        # ===============================
        # Column mapping
        # ===============================
        COLUMN_MAP = {
            "vehicleId": "vehicleId",
            "driverId": "driverId",
            "updatedAt": "timestamp",
            "enginePowerState.value": "engineState",
            "speed.value": "speed_mph",
            "odometer.value": "odometer_miles",
            "fuelLevel.value": "fuelLevel_pct",
            "fuelTemperature.value": "fuelTemperature_C",
            "engineOilPressure.value": "engineOilPressure_psi",
            "engineHours.value": "engineHours_hr",
            "metaData.driverBehavior.longitudinalAcceleration_ms2": "longitudinalAcceleration_ms2",
            "metaData.driverBehavior.brakePosition_pct": "brakePosition_pct",
            "metaData.eldFuelRecord.fuelRateLitersPerHours": "fuelRate_L_per_hr",
            "metaData.eldFuelRecord.idleFuelConsumedLiters": "idleFuelConsumed_L",
            "metaData.eldFuelRecord.fuelIntegratedLiters": "fuelIntegrated_L",
        }

        existing_cols = [c for c in COLUMN_MAP if c in df.columns]
        if not existing_cols:
            raise KeyError("None of the expected telemetry columns exist in input data")

        df = df[existing_cols].rename(columns=COLUMN_MAP)

        # ===============================
        # Timestamp safety
        # ===============================
        try:
            df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
        except Exception as e:
            logger.exception("Timestamp conversion failed")
            raise e

        df = df.dropna(subset=["timestamp"])

        # ===============================
        # Sorting & grouping
        # ===============================
        if "vehicleId" not in df.columns:
            raise KeyError("vehicleId column missing after mapping")

        df = df.sort_values(["vehicleId", "timestamp"])
        g = df.groupby("vehicleId")

        # =====================================================
        # Fuel consumption
        # =====================================================
        try:
            df['fuel_drop_pct'] = g['fuelLevel_pct'].diff().fillna(0) * -1
            df['fuel_drop_pct'] = df['fuel_drop_pct'].clip(lower=0)

            df['distance_miles'] = g['odometer_miles'].diff().fillna(0)
            df['miles_per_fuel_pct'] = (
                df['distance_miles'] /
                df['fuel_drop_pct'].replace(0, np.nan)
            )
        except Exception:
            logger.exception("Fuel consumption calculation failed")
            raise

        # =====================================================
        # Idle vs moving
        # =====================================================
        try:
            df['is_idle'] = ((df['speed_mph'] == 0) & (df['engineState'] == "ON")).astype(int)
            df['fuel_consumed_idle_L'] = df['is_idle'] * df['idleFuelConsumed_L']
            df['fuel_consumed_moving_L'] = (
                df['fuelRate_L_per_hr'] * (interval_sec / 3600) * (1 - df['is_idle'])
            )
        except Exception:
            logger.exception("Idle/moving fuel calculation failed")
            raise

        # =====================================================
        # Driver behavior
        # =====================================================
        try:
            df['acceleration_abs'] = df['longitudinalAcceleration_ms2'].abs()
            df['brake_intensity'] = df['brakePosition_pct'] / 100
            df['driver_aggressiveness'] = (
                df['acceleration_abs'] * 0.7 +
                df['brake_intensity'] * 0.3
            )
        except Exception:
            logger.exception("Driver behavior feature calculation failed")
            raise

        # =====================================================
        # Engine & vehicle context
        # =====================================================
        try:
            df['engine_on_duration_sec'] = df['engineState'].eq("ON").astype(int) * interval_sec
            df['speed_bucket'] = pd.cut(df['speed_mph'], bins=[0, 30, 70, 100], labels=False)
            df['fuel_rate_eff'] = df['fuelRate_L_per_hr'] * (1 + df['driver_aggressiveness'])
        except Exception:
            logger.exception("Engine context feature calculation failed")
            raise

        # =====================================================
        # Fuel cycle / refill
        # =====================================================
        try:
            df['refill_event'] = (g['fuelLevel_pct'].diff() > 5).astype(int)
            df['fuel_cycle'] = g['refill_event'].cumsum()
            df['time_since_refill_sec'] = (
                df.groupby(['vehicleId', 'fuel_cycle']).cumcount() * interval_sec
            )
            df['fuel_pct_remaining'] = df['fuelLevel_pct']
        except Exception:
            logger.exception("Fuel cycle calculation failed")
            raise

        # =====================================================
        # Predictive time-to-empty
        # =====================================================
        try:
            df['fuel_burn_pct_per_sec'] = df['fuel_drop_pct'] / interval_sec
            df['fuel_burn_pct_per_sec_smooth'] = g['fuel_burn_pct_per_sec'].transform(
                lambda x: x.rolling(smooth_window, min_periods=1).mean()
            )
            df['fuel_time_to_empty_sec'] = (
                df['fuel_pct_remaining'] /
                df['fuel_burn_pct_per_sec_smooth'].replace(0, np.nan)
            )
            df['fuel_time_to_empty_min'] = df['fuel_time_to_empty_sec'] / 60
            df['fuel_time_to_empty_hr'] = df['fuel_time_to_empty_sec'] / 3600
        except Exception:
            logger.exception("Time-to-empty calculation failed")
            raise

        # =====================================================
        # Advanced intelligence
        # =====================================================
        try:
            sudden_drop_threshold = 5
            df['fuel_theft_flag'] = (
                (df['fuel_drop_pct'] > sudden_drop_threshold) &
                (df['engineState'] == "OFF") &
                (df['speed_mph'] == 0) &
                (df['refill_event'] == 0)
            ).astype(int)

            fuel_per_mile = (
                g['fuel_drop_pct'].transform('sum') /
                g['distance_miles'].transform('sum')
            )

            threshold = 2
            df['expected_fuel_drop'] = df['distance_miles'] * fuel_per_mile
            df['sudden_drop_trip_flag'] = (
                (df['fuel_drop_pct'] - df['expected_fuel_drop']) > threshold
            ).astype(int)

            df['fuel_efficiency_miles_per_pct'] = (
                df['distance_miles'] /
                df['fuel_drop_pct'].replace(0, np.nan)
            )
            avg_efficiency = g['fuel_efficiency_miles_per_pct'].transform('median')
            df['low_efficiency_flag'] = (
                df['fuel_efficiency_miles_per_pct'] < avg_efficiency * 0.7
            ).astype(int)

            df['idle_fuel_flag'] = (
                (df['engineState'] == "ON") &
                (df['speed_mph'] == 0) &
                (df['fuel_drop_pct'] > 0)
            ).astype(int)

            df['fuel_sensor_tamper_flag'] = (df['fuel_drop_pct'] > 10).astype(int)

            slow_drop_threshold = 5.0
            rolling_intervals = int(3600 / interval_sec)
            df['fuel_leak_flag'] = (
                g['fuel_drop_pct']
                .rolling(rolling_intervals, min_periods=1)
                .mean()
                .reset_index(level=0, drop=True) > slow_drop_threshold
            ).astype(int)

            df['consumption_rate_L_per_sec'] = (
                df['fuelRate_L_per_hr'] / 3600 +
                df['idleFuelConsumed_L'] / interval_sec
            )

            df['fuel_time_to_empty_sec_v2'] = (
                df['fuelLevel_pct'] /
                df['consumption_rate_L_per_sec'].replace(0, np.nan)
            )
            df['fuel_time_to_empty_hr_v2'] = df['fuel_time_to_empty_sec_v2'] / 3600

            df['fuel_per_mile'] = (
                df['fuel_drop_pct'] /
                df['distance_miles'].replace(0, np.nan)
            )
            df['driver_fuel_efficiency_score'] = g['fuel_per_mile'].rank(pct=True)
        except Exception:
            logger.exception("Advanced fuel intelligence failed")
            raise

        # ===============================
        # Fuel temperature features
        # ===============================
        try:
            
            df['fuel_overheat_flag'] = (df['fuelTemperature_C'] > 80).astype(int) #Useful for engine maintenance alerts.
        except Exception:
            logger.exception("Fuel temperature feature calculation failed")
            raise
        # =====================================================
        # STEP 9: Alerts & NaN handling
        # =====================================================
        try:
            df['fuel_level_alert'] = df['fuelLevel_pct'].apply(
                lambda pct: 'HIGH' if pct <= 25 else 'MEDIUM' if pct <= 45 else 'LOW'
            )

            df['fuel_time_to_empty_alert'] = df['fuel_time_to_empty_sec_v2'].apply(
                lambda sec: np.nan if pd.isna(sec)
                else 'HIGH' if sec <= 1800
                else 'MEDIUM' if sec <= 5400
                else 'LOW'
            )

            numeric_cols = df.select_dtypes(include=[np.number]).columns
            df[numeric_cols] = df[numeric_cols].fillna(0)
        except Exception:
            logger.exception("Alert calculation failed")
            raise

        return df

    except Exception as e:
        logger.error("add_fuel_features failed", exc_info=True)
        raise e

# =========================
# File-1: config.py
# =========================
"""
Configuration & Hyperparameters (ONLY constants)

Rules:
- No data loading
- No calculations
- No business logic
- This is the only place where thresholds live
"""

# -------------------------
# Required input columns
# -------------------------
REQUIRED_COLUMNS = [
    "timestamp",
    "vehicleId",
    "speed_mph",
    "rpm",
    "load_pct",
    # moving is optional (fallback)
    "engineCoolantTemp_F",
    "engineOilTemp_F",
    "engineCoolantLevel_pct",
    "engineOilPressure",
]

OPTIONAL_COLUMNS = ["moving"]

# -------------------------
# Time settings
# -------------------------
TIMESTAMP_COL = "timestamp"
VEHICLE_ID_COL = "vehicleId"

# If your data has timezone already, we keep it.
# If not, we parse as naive datetime.
TIMEZONE_AWARE = False  # you can switch later if needed

# -------------------------
# STEP-03: Context thresholds
# -------------------------
# Motion state
IDLE_SPEED_MPH = 0.0

# Speed bands (mph)
SPEED_CITY_MAX = 30
SPEED_CRUISE_MAX = 60
# HIGHWAY is 61+

# RPM bands
RPM_IDLE_MAX = 900
RPM_CRUISE_MAX = 1600
# HIGH_RPM is > 1600

# Load bands (%)
LOAD_LOW_MAX = 30
LOAD_MED_MAX = 60
# HIGH_LOAD is > 60

# Context label formatting
CONTEXT_SEP = "|"

# -------------------------
# STEP-04: Baseline rules
# -------------------------
MIN_SAMPLES_PER_CONTEXT = 30
STD_EPS = 1e-6  # numerical stability for std

# -------------------------
# STEP-05: Z-score thresholds
# -------------------------
Z_NORMAL_MAX = 1.0
Z_SLIGHT_MAX = 2.0
Z_WARNING_MAX = 3.0
# >= 3 => critical

# Used for anomaly flag / persistence
Z_ANOMALY_THRESHOLD = 2.0  # "warning-level" deviation

# -------------------------
# STEP-06: Dense rolling slope windows
# -------------------------
SLOPE_WINDOWS = {
    "30m": "30min",
    "1h": "60min",
    "12h": "720min",
    "24h": "1440min",
}

# Slope unit: Z per hour (Z/hr)
SLOPE_MILD = 0.02   # Z/hr
SLOPE_STEEP = 0.06  # Z/hr
SLOPE_EPS = 1e-9    # treat as ~0 if below

# -------------------------
# STEP-07: Pattern rules (basic / explainable)
# -------------------------
# These are heuristics and can be refined later (still config-driven)

# Radiator clogging (heat rejection)
RAD_CLOG_Z_TEMP_MIN = 2.0
RAD_CLOG_Z_OILT_MIN = 2.0
RAD_CLOG_LEVEL_ABS_MAX = 2.0
RAD_CLOG_MOTION_REQUIRED = "DRIVING"

# Coolant leak
COOLANT_LEAK_Z_LEVEL_MAX = -2.0
COOLANT_LEAK_Z_TEMP_MIN = 1.0
COOLANT_LEAK_LEVEL_SLOPE_NEG_MIN = -0.01  # Z/hr (trend down)

# Fan failure (idle-only overheating)
FAN_FAIL_MOTION_REQUIRED = "IDLE"
FAN_FAIL_Z_TEMP_MIN = 3.0
FAN_FAIL_OILT_ABS_MAX = 2.0

# Oil starvation / pump wear
OIL_STARV_Z_PRESS_MAX = -2.0
OIL_STARV_COOLANT_ABS_MAX = 2.0
OIL_STARV_LEVEL_ABS_MAX = 2.0
OIL_STARV_LOAD_REQUIRED = {"MED_LOAD", "HIGH_LOAD"}
OIL_STARV_RPM_REQUIRED = {"CRUISE_RPM", "HIGH_RPM"}

# -------------------------
# STEP-07/08: Option-2 Engine Health Score weights
# -------------------------
# Positive => Heat-dominated, Negative => Pressure-dominated (as discussed)
WEIGHT_COOLANT_TEMP = 0.25
WEIGHT_OIL_TEMP = 0.25
WEIGHT_COOLANT_LEVEL = 0.10
WEIGHT_OIL_PRESSURE = 0.40

# -------------------------
# STEP-08: Severity scoring
# -------------------------
# Magnitude points by |Z|
MAG_POINTS = [
    (1.0, 0),    # |Z| < 1
    (2.0, 25),   # 1 <= |Z| < 2
    (3.0, 50),   # 2 <= |Z| < 3
    (float("inf"), 75),  # |Z| >= 3
]

# Trend points by |slope| (Z/hr)
TREND_POINTS = [
    (SLOPE_MILD, 0),       # < mild
    (SLOPE_STEEP, 10),     # mild..steep
    (float("inf"), 20),    # >= steep
]

# Persistence points by consecutive occurrences
PERSISTENCE_POINTS = [
    (1, 0),     # 1
    (3, 5),     # 2-3
    (10**9, 10) # >=4
]

# Severity label mapping by score
SEVERITY_LABELS = [
    (25, "NORMAL"),
    (50, "WATCH"),
    (75, "WARNING"),
    (10**9, "CRITICAL"),
]

# Confidence scoring weights (sum to 1.0)
CONF_PATTERN_W = 0.40
CONF_PERSIST_W = 0.25
CONF_AGREE_W = 0.20
CONF_BASELINE_W = 0.15

# Confidence bucket thresholds
CONF_LOW_MAX = 0.40
CONF_MED_MAX = 0.70

# Baseline quality buckets by sample count
BASELINE_QUALITY_BUCKETS = [
    (40, 0.70),   # 30-40
    (80, 0.85),   # 41-80
    (10**9, 1.00) # >80
]

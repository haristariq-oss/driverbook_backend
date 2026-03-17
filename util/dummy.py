import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# ======================================================
# SETTINGS
# ======================================================
vehicle_id = "VEH_003"
driver_id = "68b89b2d4f65cd554d7511bd"

start_date = datetime(2025, 11, 1)
days = 30

interval_hours = 1
interval_min = 60

total_entries = int((24 * days) / interval_hours)

np.random.seed(42)

# ======================================================
# TIME (updatedAt)
# ======================================================
timestamps = [
    start_date + timedelta(hours=i * interval_hours)
    for i in range(total_entries)
]

df = pd.DataFrame({
    "vehicleId": vehicle_id,
    "driverId": driver_id,
    "updatedAt": [t.strftime("%Y-%m-%dT%H:%M:%S") for t in timestamps]
})

# ======================================================
# INITIAL STATE
# ======================================================
engine_state = "ON"
speed = 50.0
engine_hours = 0.0
odometer = 0.0

fuel_level = 85.0
fuel_temp = 40.0

oil_pressure = 45.0
oil_level = 100.0
hours_since_oil_event = 0.0

fuel_integrated_total = 0.0

# ======================================================
# PARAMETERS (UNCHANGED)
# ======================================================
fuel_per_mile = 0.06
idle_fuel = 0.03
max_speed_change = 8
fuel_temp_noise = 0.4
oil_pressure_noise = 2.5
oil_decay = 0.001
oil_event_hours = 96
max_acceleration = 0.5
max_brake = 50
fuel_rate_base = 15  # liters per hour base

# ======================================================
# STORAGE
# ======================================================
engine_states, speeds = [], []
engine_hours_list, odometers = [], []
fuel_levels, fuel_temps = [], []
oil_pressures = []

longitudinal_acc, brake_positions = [], []
fuel_rate_list, idle_fuel_list, fuel_integrated_list = [], [], []

# ======================================================
# SIMULATION LOOP
# ======================================================
for _ in range(total_entries):

    # -----------------------------
    # ENGINE & SPEED
    # -----------------------------
    if fuel_level <= 0:
        engine_state = "OFF"
        speed = 0

    if engine_state == "ON" and fuel_level > 0:
        speed += np.random.uniform(-max_speed_change, max_speed_change)
        speed = max(speed, 0)
    else:
        speed = 0

    # -----------------------------
    # ENGINE HOURS
    # -----------------------------
    if engine_state == "ON":
        engine_hours += interval_hours

    # -----------------------------
    # DISTANCE
    # -----------------------------
    distance = speed * interval_hours
    odometer += distance

    # -----------------------------
    # FUEL CONSUMPTION
    # -----------------------------
    if engine_state == "ON":
        if speed > 0:
            fuel_consumed = distance * fuel_per_mile
        else:
            fuel_consumed = idle_fuel
    else:
        fuel_consumed = 0

    fuel_level -= fuel_consumed
    fuel_level = max(fuel_level, 0)
    fuel_integrated_total += fuel_consumed

    # -----------------------------
    # REFUEL LOGIC (UNCHANGED)
    # -----------------------------
    if fuel_level == 0 and np.random.rand() < 0.25:
        fuel_level = np.random.uniform(30, 60)
        engine_state = "ON"
    elif fuel_level < 15 and np.random.rand() < 0.05:
        fuel_level += np.random.uniform(40, 70)
    elif fuel_level < 40 and np.random.rand() < 0.03:
        fuel_level += np.random.uniform(25, 45)
    elif fuel_level < 60 and np.random.rand() < 0.015:
        fuel_level += np.random.uniform(10, 25)

    fuel_level = min(fuel_level, 100)

    # -----------------------------
    # FUEL TEMP
    # -----------------------------
    fuel_temp += np.random.normal(0, fuel_temp_noise)

    # -----------------------------
    # OIL PRESSURE
    # -----------------------------
    if engine_state == "OFF":
        oil_pressure = np.random.uniform(0, 5)
    elif speed == 0:
        oil_pressure = np.random.uniform(20, 30)
    else:
        oil_pressure = 35 + (speed / 80) * 25 + np.random.normal(0, oil_pressure_noise)

    oil_pressure = max(oil_pressure, 0)

    # -----------------------------
    # OIL LEVEL
    # -----------------------------
    hours_since_oil_event += interval_hours
    oil_level -= oil_decay

    if hours_since_oil_event >= oil_event_hours:
        if np.random.rand() < 0.2:
            oil_level -= np.random.uniform(30, 70)
        elif np.random.rand() < 0.4:
            oil_level = np.random.uniform(90, 100)
            hours_since_oil_event = 0

    if oil_level <= 5:
        oil_level = np.random.uniform(90, 100)
        hours_since_oil_event = 0

    oil_level = max(min(oil_level, 100), 0)

    # -----------------------------
    # DRIVER BEHAVIOR
    # -----------------------------
    longitudinal_acceleration = np.random.uniform(-max_acceleration, max_acceleration)
    brake_pos = np.random.randint(0, max_brake)

    # -----------------------------
    # FUEL RATE & IDLE
    # -----------------------------
    fuel_rate = fuel_rate_base * (speed / 50 + 0.5) if speed > 0 else 0
    idle_fuel_val = idle_fuel if speed == 0 and engine_state == "ON" else 0

    # -----------------------------
    # STORE
    # -----------------------------
    engine_states.append(engine_state)
    speeds.append(round(speed, 2))
    engine_hours_list.append(round(engine_hours, 2))
    odometers.append(round(odometer, 2))
    fuel_levels.append(round(fuel_level, 2))
    fuel_temps.append(round(fuel_temp, 1))
    oil_pressures.append(round(oil_pressure, 1))

    longitudinal_acc.append(round(longitudinal_acceleration, 2))
    brake_positions.append(brake_pos)
    fuel_rate_list.append(round(fuel_rate, 2))
    idle_fuel_list.append(round(idle_fuel_val, 2))
    fuel_integrated_list.append(round(fuel_integrated_total, 2))

# ======================================================
# FINAL DATAFRAME (NEW SCHEMA NAMES)
# ======================================================
df["enginePowerState.value"] = engine_states
df["speed.value"] = speeds
df["engineHours.value"] = engine_hours_list
df["odometer.value"] = odometers
df["fuelLevel.value"] = fuel_levels
df["fuelTemperature.value"] = fuel_temps
df["engineOilPressure.value"] = oil_pressures

df["metaData.driverBehavior.longitudinalAcceleration_ms2"] = longitudinal_acc
df["metaData.driverBehavior.brakePosition_pct"] = brake_positions

df["metaData.eldFuelRecord.fuelRateLitersPerHours"] = fuel_rate_list
df["metaData.eldFuelRecord.idleFuelConsumedLiters"] = idle_fuel_list
df["metaData.eldFuelRecord.fuelIntegratedLiters"] = fuel_integrated_list

# ======================================================
# SAVE CSV
# ======================================================
df.to_csv("./dummy_fuel_analysis_30days_2.csv", index=False)

print("Realistic dummy telemetry generated (1-hour interval)")
print(df.shape)
print(df.head(5))

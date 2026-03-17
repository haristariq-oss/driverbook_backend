import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os

# ===============================
# STEP 1: Load CSV file
# ===============================
csv_file_path = "./fuel_output.csv"   # change this path
df = pd.read_csv(csv_file_path)

# ===============================
# STEP 2: Convert timestamp column
# ===============================
df['timestamp'] = pd.to_datetime(df['timestamp'])

# ===============================
# STEP 3: Sort data by time
# ===============================
df = df.sort_values('timestamp')

# ===============================
# STEP 4: Optional – filter one vehicle
# ===============================
# Uncomment if your CSV has multiple vehicles
# vehicle_id = df['vehicleId'].iloc[0]
# df = df[df['vehicleId'] == vehicle_id]

# ===============================
# STEP 5: Create output directory
# ===============================
output_dir = "fuel_trend_graphs"
os.makedirs(output_dir, exist_ok=True)

# ===============================
# STEP 6: Plot fuel level trend
# ===============================
sns.set_style("whitegrid")
plt.figure(figsize=(12, 6))

sns.lineplot(
    data=df,
    x='timestamp',
    y='fuelLevel_pct'
)

plt.title('Fuel Level Trend Over Time')
plt.xlabel('Time')
plt.ylabel('Fuel Level (%)')
plt.xticks(rotation=45)
plt.tight_layout()

# ===============================
# STEP 6.1: Add dot on new day start
# ===============================
# Identify where day changes
df['date'] = df['timestamp'].dt.date
new_day_idx = df[df['date'] != df['date'].shift(1)].index

# Plot dot for each new day start
plt.scatter(
    df.loc[new_day_idx, 'timestamp'],
    df.loc[new_day_idx, 'fuelLevel_pct'],
    color='red',
    s=50,
    zorder=5,
    label='New Day Start'
)

plt.legend()

# ===============================
# STEP 7: Save figure
# ===============================
output_path = os.path.join(output_dir, "fuel_level_trend.png")
plt.savefig(output_path, dpi=300)

# ===============================
# STEP 8: Show graph
# ===============================
plt.show()
plt.close()

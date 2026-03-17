import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import matplotlib.dates as mdates

# -----------------------------
# Step 1: Load CSV
# -----------------------------
df = pd.read_csv('./dummy_fuel_analysis_30days.csv')  # Replace with your CSV path

# Convert 'updatedAt' to datetime
df['updatedAt'] = pd.to_datetime(df['updatedAt'])

# -----------------------------
# Step 2: Set Seaborn style
# -----------------------------
sns.set_style("whitegrid")  # Clean white grid
plt.rcParams.update({
    "font.size": 12,
    "axes.titlesize": 16,
    "axes.labelsize": 14,
    "xtick.labelsize": 12,
    "ytick.labelsize": 12
})

# -----------------------------
# Step 3: Create figure and axis
# -----------------------------
fig, ax1 = plt.subplots(figsize=(14,7))

# Colors for professionalism
color_fuel = "#1f77b4"      # Blue for Fuel Level
color_temp = "#ff7f0e"      # Orange for Fuel Temp

# -----------------------------
# Step 4: Plot Fuel Level (Left Axis)
# -----------------------------
sns.lineplot(
    x='updatedAt',
    y='fuelLevel.value',
    data=df,
    ax=ax1,
    label='Fuel Level (%)',
    color=color_fuel,
    linewidth=2.5
)
ax1.set_xlabel('Time', fontsize=14)
ax1.set_ylabel('Fuel Level (%)', fontsize=14, color=color_fuel)
ax1.tick_params(axis='y', labelcolor=color_fuel)
ax1.grid(True, linestyle='--', alpha=0.5)

# -----------------------------
# Step 5: Plot Fuel Temperature (Right Axis)
# -----------------------------
ax2 = ax1.twinx()
sns.lineplot(
    x='updatedAt',
    y='fuelTemperature.value',
    data=df,
    ax=ax2,
    label='Fuel Temperature (°C)',
    color=color_temp,
    linewidth=2.5
)
ax2.set_ylabel('Fuel Temperature (°C)', fontsize=14, color=color_temp)
ax2.tick_params(axis='y', labelcolor=color_temp)

# -----------------------------
# Step 6: Format x-axis (Dates)
# -----------------------------
ax1.xaxis.set_major_locator(mdates.AutoDateLocator())
ax1.xaxis.set_major_formatter(mdates.ConciseDateFormatter(ax1.xaxis.get_major_locator()))
fig.autofmt_xdate(rotation=30)

# -----------------------------
# Step 7: Add title and legend
# -----------------------------
fig.suptitle('Fuel Level and Temperature Over Time', fontsize=18, fontweight='bold', y=1.02)

# Combine legends from both axes
lines_1, labels_1 = ax1.get_legend_handles_labels()
lines_2, labels_2 = ax2.get_legend_handles_labels()
ax1.legend(lines_1 + lines_2, labels_1 + labels_2, loc='upper left', frameon=True, fontsize=12)

# -----------------------------
# Step 8: Optional Shaded Area for Fuel Level Threshold
# -----------------------------
ax1.fill_between(df['updatedAt'], 0, 20, color=color_fuel, alpha=0.1, label='Low Fuel Warning')  # Highlight low fuel

# -----------------------------
# Step 9: Save and show
# -----------------------------
plt.tight_layout()
plt.savefig('./fuel_level_and_fuel_temp_graph.png', dpi=300, bbox_inches='tight')
plt.show()

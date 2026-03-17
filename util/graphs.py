import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import matplotlib.dates as mdates
import os

# -----------------------------
# Step 1: Load CSV
# -----------------------------
df = pd.read_csv('./fuel_output.csv')  # Replace with your CSV path

# Convert timestamp column to datetime
df['timestamp'] = pd.to_datetime(df['timestamp'])

# Create folder to save graphs
output_folder = "feature_graphs"
os.makedirs(output_folder, exist_ok=True)

# -----------------------------
# Step 2: Select numeric columns only
# -----------------------------
numeric_cols = df.select_dtypes(include=['float64', 'int64']).columns.tolist()

# Optional: exclude columns you don't want plotted
exclude_cols = ['vehicleId', 'driverId', 'engineState', 'is_idle']  # example non-numeric/categorical
numeric_cols = [col for col in numeric_cols if col not in exclude_cols]

# -----------------------------
# Step 3: Set Seaborn style
# -----------------------------
sns.set_style("whitegrid")
plt.rcParams.update({
    "font.size": 12,
    "axes.titlesize": 16,
    "axes.labelsize": 14,
    "xtick.labelsize": 12,
    "ytick.labelsize": 12
})

# -----------------------------
# Step 4: Loop over features and plot
# -----------------------------
for feature in numeric_cols:
    plt.figure(figsize=(14,6))
    
    # Professional color palette
    color = "#1f77b4"
    
    # Line plot
    sns.lineplot(
        x='timestamp',
        y=feature,
        data=df,
        color=color,
        linewidth=2.5
    )
    
    # Title, labels, grid
    plt.title(f"{feature} Over Time", fontsize=18, fontweight='bold')
    plt.xlabel("Time", fontsize=14)
    plt.ylabel(feature, fontsize=14)
    plt.grid(True, linestyle='--', alpha=0.5)
    
    # Format x-axis dates
    plt.gca().xaxis.set_major_locator(mdates.AutoDateLocator())
    plt.gca().xaxis.set_major_formatter(mdates.ConciseDateFormatter(plt.gca().xaxis.get_major_locator()))
    plt.gcf().autofmt_xdate(rotation=30)
    
    # Save figure
    save_path = os.path.join(output_folder, f"{feature}.png")
    plt.tight_layout()
    plt.savefig(save_path, dpi=300, bbox_inches='tight')
    
    # Close figure to avoid overlap
    plt.close()
    
print(f"Graphs saved in folder: {output_folder}")

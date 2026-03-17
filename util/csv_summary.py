import pandas as pd
import numpy as np

# ==============================
# Step 1: Read CSV
# ==============================
csv_path = "./data/driverdiagnostics.csv"   # <-- update path if needed
df = pd.read_csv(csv_path)

# ==============================
# Step 2: Group by tenantId
# ==============================
tenant_vehicle_summary = (
    df.groupby("tenantId")["vehicleId"]
    .agg(
        vehicle_count="nunique",
        vehicleIds=lambda x: list(x.unique())
    )
    .reset_index()
)

# ==============================
# Step 3: Print tenant-wise summary
# ==============================
for _, row in tenant_vehicle_summary.iterrows():
    print(f"\nTenant ID: {row['tenantId']}")
    print(f"Vehicle Count: {row['vehicle_count']}")
    print("Vehicle IDs:")
    for vid in row["vehicleIds"]:
        print(f"  - {vid}")

# ==============================
# Step 4: Save tenant summary
# ==============================
tenant_vehicle_summary["vehicleIds"] = tenant_vehicle_summary["vehicleIds"].apply(
    lambda x: ", ".join(x)
)

tenant_vehicle_summary.to_csv(
    "./data/tenant_vehicle_summary.csv", index=False
)

print("\n✅ Tenant vehicle summary saved as tenant_vehicle_summary.csv")

# ==============================
# Step 5: Numeric column statistics (STRICT)
# ==============================

# Columns to exclude
exclude_columns = {"_id", "tenantId", "vehicleId"}

# STRICT numeric detection:
# - must be numeric
# - must NOT be boolean
numeric_columns = [
    col for col in df.columns
    if (
        col not in exclude_columns
        and pd.api.types.is_numeric_dtype(df[col])
        and not pd.api.types.is_bool_dtype(df[col])
    )
]

stats_rows = []

for col in numeric_columns:
    col_data = df[col]

    # null_count: ONLY counts string 'null' or 'NULL'
    null_count = col_data.astype(str).str.upper().eq('NULL').sum()

    # missing_or_empty_count: counts actual NaN values
    missing_or_empty_count = col_data.isna().sum()

    stats_rows.append({
        "column_name": col,
        "min": col_data.min(),
        "max": col_data.max(),
        "mean": col_data.mean(),
        "median": col_data.median(),
        "variance": col_data.var(),

        # Data quality checks
        "value_null_count": null_count,
        "missing_or_empty_count": missing_or_empty_count,
        "zero_count": (col_data == 0).sum()
    })

# Create stats DataFrame
numeric_stats_df = pd.DataFrame(stats_rows)

# ==============================
# Step 6: Save numeric stats
# ==============================
numeric_stats_df.to_csv(
    "./data/numeric_column_statistics.csv", index=False
)

print("✅ Numeric column statistics saved as numeric_column_statistics.csv")

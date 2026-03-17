from fastapi import FastAPI
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd

app = FastAPI(title="Fleet Fuel CSV API")

# -----------------------------
# CORS setup
# -----------------------------
origins = [
    "http://localhost:5173/"  # frontend URL
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,           # allow requests from these origins
    allow_credentials=True,
    allow_methods=["*"],             # allow all HTTP methods
    allow_headers=["*"],             # allow all headers
)

# -----------------------------
# CSV file
# -----------------------------
CSV_FILE = "./data/fuel_output.csv"  # path to your CSV


@app.get("/latest-record")
def get_latest_record():
    try:
        # Read CSV
        df = pd.read_csv(CSV_FILE)

        # Ensure timestamp column exists
        if "timestamp" not in df.columns:
            return JSONResponse(
                content={"error": "'timestamp' column not found in CSV"},
                status_code=400
            )

        # Sort by timestamp to find latest row
        df_sorted = df.sort_values("timestamp")

        # Get latest row
        latest_row = df_sorted.iloc[-1].to_dict()

        # Get all historical fuelLevel_pct as list
        if "fuelLevel_pct" in df_sorted.columns:
            latest_row["fuelLevel_pct"] = df_sorted["fuelLevel_pct"].astype(float).tolist()

        return JSONResponse(content=latest_row)

    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)


# -----------------------------
# New endpoint: return all CSV rows as JSON
# -----------------------------
@app.get("/all-records")
def get_all_records():
    try:
        # Read CSV
        df = pd.read_csv(CSV_FILE)

        # Convert all rows to list of dicts
        records = df.to_dict(orient="records")

        return JSONResponse(content=records)

    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)
from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
import numpy as np
import uvicorn

app = FastAPI(title="Fleet Fuel CSV API")

# -----------------------------
# CORS setup
# -----------------------------
origins = [
    "http://localhost:5173","https://driverbook.netlify.app"
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# -----------------------------
# CSV folder
# -----------------------------
DATA_FOLDER = "./data/"


@app.get("/latest-record")
def get_latest_record(vehicleId: str = Query(..., description="Vehicle ID to fetch latest record")):
    try:
        CSV_FILE = f"{DATA_FOLDER}{vehicleId}.csv"

        # Read CSV
        df = pd.read_csv(CSV_FILE)

        # Replace NaN / inf / -inf with None (JSON compliant)
        df = df.replace([np.inf, -np.inf], np.nan)
        df = df.where(pd.notnull(df), None)

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

    except FileNotFoundError:
        return JSONResponse(
            content={"error": f"CSV file for vehicleId {vehicleId} not found"},
            status_code=404
        )
    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)


# -----------------------------
# Return all rows for vehicle CSV using query param
# -----------------------------
@app.get("/all-records")
def get_all_records(vehicleId: str = Query(..., description="Vehicle ID to fetch all records")):
    try:
        CSV_FILE = f"{DATA_FOLDER}{vehicleId}.csv"

        # Read CSV
        df = pd.read_csv(CSV_FILE)

        # Replace NaN / inf / -inf with None (JSON compliant)
        df = df.replace([np.inf, -np.inf], np.nan)
        df = df.where(pd.notnull(df), None)

        # Convert rows to JSON
        records = df.to_dict(orient="records")

        return JSONResponse(content=records)

    except FileNotFoundError:
        return JSONResponse(
            content={"error": f"CSV file for vehicleId {vehicleId} not found"},
            status_code=404
        )
    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))  # <-- Must use Railway PORT
    uvicorn.run(app, host="0.0.0.0", port=port)
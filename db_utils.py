# # db_utils.py

# from pymongo import MongoClient
# from datetime import datetime

# # MongoDB connection (change if needed)
# MONGO_URI = "mongodb://localhost:27017/"
# DB_NAME = "fleet_db"
# COLLECTION_NAME = "fuel_latest_features"


# def get_collection():
#     client = MongoClient(MONGO_URI)
#     db = client[DB_NAME]
#     return db[COLLECTION_NAME]


# def upsert_latest_records(df):
#     """
#     Takes dataframe and upserts latest record per truckId
#     """

#     collection = get_collection()

#     # Ensure timestamp is datetime
#     df["timestamp"] = df["timestamp"].astype("datetime64[ns]")

#     # Sort by timestamp
#     df_sorted = df.sort_values("timestamp")

#     # Get latest record per truck
#     latest_df = df_sorted.groupby("vehicleId").tail(1)

#     for _, row in latest_df.iterrows():
#         record = row.to_dict()

#         # Convert numpy types to native python
#         for key, value in record.items():
#             if hasattr(value, "item"):
#                 record[key] = value.item()

#         record["updated_at"] = datetime.utcnow()

#         # UPSERT
#         collection.update_one(
#             {"vehicleId": record["vehicleId"]},   # match condition
#             {"$set": record},                     # update data
#             upsert=True                           # insert if not exists
#         )

#     print("MongoDB latest records updated successfully.")


# db_utils.py

from pymongo import MongoClient, errors
from datetime import datetime
import logging

# Configure logger
logger = logging.getLogger(__name__)

# MongoDB connection (change if needed)
MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "fleet_db"
COLLECTION_NAME = "fuel_latest_features"


def get_collection():
    """
    Safely connect to MongoDB and return collection.
    Returns None if connection fails.
    """
    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)

        # Force connection check
        client.server_info()

        db = client[DB_NAME]
        return db[COLLECTION_NAME]

    except errors.ServerSelectionTimeoutError as e:
        logger.error(f"MongoDB connection timeout: {e}")
        return None
    except Exception as e:
        logger.error(f"Unexpected MongoDB connection error: {e}")
        return None


def upsert_latest_records(df):
    """
    Takes dataframe and upserts latest record per truckId
    """

    # ✅ Safety checks
    if df is None or df.empty:
        logger.warning("Empty dataframe received. Skipping MongoDB update.")
        return

    if "vehicleId" not in df.columns:
        logger.warning("vehicleId column missing. Skipping MongoDB update.")
        return

    if "timestamp" not in df.columns:
        logger.warning("timestamp column missing. Skipping MongoDB update.")
        return

    collection = get_collection()

    if collection is None:
        logger.warning("MongoDB collection not available. Skipping DB update.")
        return

    try:
        # Ensure timestamp is datetime
        df["timestamp"] = df["timestamp"].astype("datetime64[ns]")

        # Sort by timestamp
        df_sorted = df.sort_values("timestamp")

        # Get latest record per truck
        latest_df = df_sorted.groupby("vehicleId").tail(1)

    except Exception as e:
        logger.error(f"Data processing error before MongoDB upsert: {e}")
        return

    success_count = 0
    fail_count = 0

    for _, row in latest_df.iterrows():
        try:
            record = row.to_dict()

            # Convert numpy types to native python
            for key, value in record.items():
                if hasattr(value, "item"):
                    record[key] = value.item()

            record["updated_at"] = datetime.utcnow()

            # UPSERT
            collection.update_one(
                {"vehicleId": record["vehicleId"]},   # match condition
                {"$set": record},                     # update data
                upsert=True                           # insert if not exists
            )

            success_count += 1

        except Exception as e:
            fail_count += 1
            logger.error(
                f"Failed to upsert vehicleId {row.get('vehicleId')}: {e}"
            )

    logger.info(
        f"MongoDB upsert completed. Success: {success_count}, Failed: {fail_count}"
    )

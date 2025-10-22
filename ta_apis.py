from fastapi import FastAPI, HTTPException
import boto3
import os
import logging
import threading
from contextlib import asynccontextmanager

from utils import standardize_data, get_db_config, run_ohlcv_listener, PG_INPUT_DBNAME


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    On application startup, set up the database trigger and start the listener daemon
    in a background thread.
    """
    logger.info("Starting up and initializing listener...")
    db_config = get_db_config(PG_INPUT_DBNAME)

    listener_thread = threading.Thread(target=run_ohlcv_listener, args=(db_config,), daemon=True)
    listener_thread.start()
    logger.info("Started OHLCV listener in a background thread.")

    yield

    logger.info("Shutting down.")

app = FastAPI(title="TA service")

S3_BUCKET = os.environ.get("S3_BUCKET")
S3_PREFIX = "data/"
LOCAL_PATH = "./data"

s3_client = boto3.client("s3")

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_file_path(file_name: str, extension: str = ".parquet") -> str | None:
    local_file = os.path.join(LOCAL_PATH, file_name + extension)
    logger.info(f"Checking data file {local_file}")
    if os.path.exists(local_file):
        return local_file
    try:
        s3_client.head_object(Bucket=S3_BUCKET, Key=S3_PREFIX + file_name)
        s3_client.download_file(S3_BUCKET, S3_PREFIX + file_name, local_file)
        return local_file
    except s3_client.exceptions.NoSuchKey:
        pass
    except Exception:
        pass
    return None

@app.post(
    "/ta/enrich",
    summary="Enrich data with TA features and save to DB",
    description="""
    This endpoint takes a Parquet file name, loads the file from local storage or S3,
    computes technical analysis (TA) features using pre-defined rules, and inserts the results into the internal PostgreSQL database.
    Returns a success message and row count.
    """,
    response_description="Status and details of the enrichment process"
)
async def enrich_ta(file_name: str):
    file_path = get_file_path(file_name)
    if not file_path:
        raise HTTPException(status_code=404, detail="File not found")
    standardize_data(file_path)
    return {"status": "success", "message": "TA features added and saved to DB"}

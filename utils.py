import os
import logging
from typing import Literal

import pandas as pd
import numpy as np
import psycopg2
from sqlalchemy import create_engine

from ta_configs import TA_CONFIG

logger = logging.getLogger(__name__)

PG_INPUT_DBNAME = "raw_ohlcv"

# ==============================================================
# Executor
# ==============================================================
def _get_required_window(params: dict) -> int:
    """Find the max integer param (e.g., window length)."""
    return max([v for v in params.values() if isinstance(v, int)], default=1)


def compute_indicators(df: pd.DataFrame, config=TA_CONFIG) -> pd.DataFrame:
    """
    Compute indicators; enforce NaN for first n rows based on window.
    """
    for ind in config:
        try:
            # run indicator
            inputs = [df[col] for col in ind["inputs"]]
            indicator = ind["class"](*inputs, **ind["params"])
            codes = ind["code"] if isinstance(ind["code"], list) else [ind["code"]]

            # assign values
            for code, method in zip(codes, ind["methods"]):
                values = getattr(indicator, method)()

                # enforce NaNs in the first (window-1) rows
                n = _get_required_window(ind["params"])
                if n > 1:
                    values.iloc[: n - 1] = np.nan

                df[code] = values

        except Exception as e:
            logger.warning(f"⚠️ {ind.get('class').__name__} failed: {e}")
            codes = ind["code"] if isinstance(ind["code"], list) else [ind["code"]]
            for code in codes:
                df[code] = np.nan
    return df


def add_ta_features(
    df: pd.DataFrame, ticker_col="Ticker", config=TA_CONFIG
) -> pd.DataFrame:
    """Apply TA features to single or multi-ticker dataset."""
    if ticker_col in df.columns:
        return df.groupby(ticker_col, group_keys=False).apply(
            lambda g: compute_indicators(g.copy(), config)
        )
    return compute_indicators(df.copy(), config)


def get_db_config(db_env_var: str = "PG_OUTPUT_DBNAME") -> dict:
    return {
        "host": os.environ.get("PG_HOST"),
        "port": int(os.environ.get("PG_PORT")),
        "user": os.environ.get("PG_USER"),
        "password": os.environ.get("PG_PASSWORD"),
        "dbname": os.environ.get(db_env_var),
    }

def save_to_db(
        df: pd.DataFrame,
        table_name: str,
        if_exists: Literal["fail", "replace", "append"] = "replace"):
    """Save DataFrame to PostgresSQL database."""
    from sqlalchemy import create_engine

    conn_params = get_db_config()
    conn = None
    try:
        conn = psycopg2.connect(**conn_params)
        engine = create_engine("postgresql+psycopg2://", creator=lambda: conn)
        df.to_sql(table_name, con=engine, if_exists=if_exists, index=False)
        logger.info(f"✅ Data successfully inserted into `{table_name}` table")
        logger.info(f"📊 Total rows inserted: {len(df):,}")
        if "TradingDate" in df.columns:
            logger.info(f"📅 Date range: {df['TradingDate'].min()} to {df['TradingDate'].max()}")
        if "Ticker" in df.columns:
            logger.info(f"🎯 Unique tickers: {df['Ticker'].nunique()}")
        logger.info(f"📈 Technical indicators added: {len([col for col in df.columns if col.startswith('crma_ta_')])}")
    except Exception as e:
        logger.error(f"❌ Failed to insert data: {e}")
        raise

    finally:
        if conn is not None:
            conn.close()
            logger.info("🔒 Database connection closed")


def standardize_data(file_path: str):
    df = pd.read_parquet(file_path)
    df_features = add_ta_features(df)
    save_to_db(df_features, table_name="crma_ta")


def dump_parquet_to_postgres(file_path: str, db_config: dict, table_name: str = "ohlcv"):
    """
    Load a Parquet file into a PostgreSQL table (default: ohlcv).
    """
    logger.info(f"Loading Parquet file {file_path} into table {table_name}")
    df = pd.read_parquet(file_path)
    db_url = f"postgresql+psycopg2://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['dbname']}"
    engine = create_engine(db_url)
    df.to_sql(table_name, engine, if_exists='append', index=False)
    logger.info(f"Loaded {len(df)} rows into {table_name}")

def standardize_data_from_row(row: pd.DataFrame) -> pd.DataFrame:
    """
    Compute TA features for a single row DataFrame.
    Returns a DataFrame for the row.
    """
    return add_ta_features(row)

def run_ohlcv_listener(db_config: dict):
    import psycopg2
    import select
    import json
    logger.info("Starting OHLCV listener daemon...")
    conn = psycopg2.connect(**db_config)
    conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = conn.cursor()
    cur.execute("LISTEN new_ohlcv;")
    while True:
        if select.select([conn], [], [], 5) == ([], [], []):
            continue
        conn.poll()
        while conn.notifies:
            notify = conn.notifies.pop(0)
            logger.info(f"Received NOTIFY: {notify.payload}")
            try:
                row = pd.DataFrame([json.loads(notify.payload)])
                df_features = standardize_data_from_row(row)
                save_to_db(df_features, table_name="crma_ta", if_exists="append")
                logger.info("Transformed features saved to crma_ta table.")
            except Exception as e:
                logger.error(f"Failed to process new OHLCV row: {e}")


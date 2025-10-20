from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col,
    pandas_udf, PandasUDFType, to_timestamp
)
from pyspark.sql.types import (
    StructType, StructField, StringType,
    DoubleType, LongType, IntegerType, TimestampType
)
import pandas as pd
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from ta_configs import TA_CONFIG
from utils import compute_indicators

spark = (SparkSession.builder
         .appName("CryptoToClickHouse")
         .config("spark.jars.packages",
                 "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
                 "com.clickhouse:clickhouse-jdbc:0.4.6:all")
         .config("spark.sql.adaptive.enabled", "true")
         .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
         .config("spark.streaming.backpressure.enabled", "true")
         .getOrCreate())


RAW_SCHEMA = StructType([
    StructField("Ticker", StringType()),
    StructField("TradingDate", StringType()),
    StructField("Open", DoubleType()),
    StructField("High", DoubleType()),
    StructField("Low", DoubleType()),
    StructField("Close", DoubleType()),
    StructField("Volume", DoubleType()),
    StructField("Value", DoubleType()),
    StructField("Return", DoubleType()),
    StructField("Change", DoubleType()),
    StructField("on_binance_time", LongType()),
    StructField("label_7d", IntegerType()),
    StructField("label_7d_gt5pct", IntegerType()),
    StructField("value_90", DoubleType())
])


def get_enriched_schema():
    """Add TA indicator columns to base schema"""
    fields = list(RAW_SCHEMA.fields)

    # Add timestamp for processing
    fields.append(StructField("processed_at", TimestampType()))

    # Add TA feature columns from config
    for ind in TA_CONFIG:
        codes = ind["code"] if isinstance(ind["code"], list) else [ind["code"]]
        for code in codes:
            fields.append(StructField(code, DoubleType()))

    return StructType(fields)


enriched_schema = get_enriched_schema()

@pandas_udf(enriched_schema, functionType=PandasUDFType.GROUPED_MAP)
def calculate_ta_features(df: pd.DataFrame) -> pd.DataFrame:
    # Sort by timestamp to ensure proper indicator calculations
    df = df.sort_values("on_binance_time")

    # Apply TA indicators from utils.py
    df_enriched = compute_indicators(df, config=TA_CONFIG)

    # Add processing timestamp
    df_enriched["processed_at"] = pd.Timestamp.now()

    return df_enriched

def create_streaming_pipeline():
    """Read from Kafka and parse messages"""

    # Read from Kafka
    raw_stream = (spark
                  .readStream
                  .format("kafka")
                  .option("kafka.bootstrap.servers", "localhost:9092")
                  .option("subscribe", "raw_ohlcv_topic")
                  .option("startingOffsets", "latest")
                  .option("maxOffsetsPerTrigger", "5000")  # Control throughput
                  .option("failOnDataLoss", "false")
                  .load())

    # Parse JSON messages
    output = (raw_stream
                     .selectExpr("CAST(key AS STRING) as ticker_key",
                                 "CAST(value AS STRING) as json_value",
                                 "timestamp as kafka_timestamp")
                     .select(
        from_json(col("json_value"), RAW_SCHEMA).alias("data"),
        col("kafka_timestamp")
    )
                     .select("data.*", "kafka_timestamp")
                     .withColumn("event_time",
                                 to_timestamp(col("on_binance_time")))
                     .withWatermark("event_time", "5 minutes"))

    return output


def write_to_clickhouse_batch(batch_df, batch_id):
    if batch_df.isEmpty():
        print(f"Batch {batch_id}: Empty batch, skipping")
        return

    print(f"Batch {batch_id}: Processing {batch_df.count()} records")

    enriched_df = (batch_df
                   .repartition("Ticker")
                   .groupBy("Ticker")
                   .apply(calculate_ta_features))

    clickhouse_url = "jdbc:clickhouse://localhost:8123/crypto"

    try:
        (enriched_df
         .write
         .format("jdbc")
         .option("url", clickhouse_url)
         .option("dbtable", "standardized_features")
         .option("user", "default")
         .option("password", "")
         .option("driver", "com.clickhouse.jdbc.ClickHouseDriver")
         .option("batchsize", "10000")
         .option("isolationLevel", "NONE")
         .option("numPartitions", "10")
         .option("rewriteBatchedStatements", "true")
         .mode("append")
         .save())

        print(f"Batch {batch_id}: Successfully wrote to ClickHouse")

    except Exception as e:
        print(f"Batch {batch_id}: Error writing to ClickHouse: {e}")
        # Implement retry logic
        raise

if __name__ == "__main__":
    print("Starting Crypto Streaming to ClickHouse...")

    # Create streaming pipeline
    parsed_stream = create_streaming_pipeline()

    # Write to ClickHouse with 1-minute microbatches
    query = (parsed_stream
             .writeStream
             .foreachBatch(write_to_clickhouse_batch)
             .trigger(processingTime="1 minute")
             .option("checkpointLocation", "/tmp/spark_checkpoint/clickhouse")
             .outputMode("append")
             .start())

    print("Streaming query started. Writing to ClickHouse...")

    # Monitor streaming metrics
    query.awaitTermination()

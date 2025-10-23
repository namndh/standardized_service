import json
import time
import logging
from abc import ABC, abstractmethod
from datetime import datetime

from kafka import KafkaProducer
from typing import List, Dict, Any

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "raw_ohlcv_topic"
TICKERS_TO_TRACK = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT"]  # Example


class CryptoCrawler(ABC):
    """
    An abstract interface for data crawlers.
    """

    @abstractmethod
    def fetch_minute_data(self, tickers: List[str]) -> List[Dict[str, Any]]:
        """
        Fetches the latest 1-minute OHLCV data for the given list of tickers.
        """
        pass

class BinanceAPICrawler(CryptoCrawler):
    def fetch_minute_data(self, tickers: List[str]) -> List[Dict[str, Any]]:
        logging.info(f"Fetching data from Binance API for {len(tickers)} tickers...")
        # Create mock data with complete schema
        mock_data = []
        import random

        for ticker in tickers:
            base_price = random.uniform(0.1, 100.0)
            high = base_price * random.uniform(1.001, 1.05)
            low = base_price * random.uniform(0.95, 0.999)
            open_price = random.uniform(low, high)
            close_price = random.uniform(low, high)
            volume = random.uniform(1000, 1000000)

            change = close_price - open_price
            return_pct = (change / open_price) * 100 if open_price > 0 else 0
            value = close_price * volume

            mock_data.append({
                "Ticker": ticker,
                "TradingDate": datetime.today().strftime("%Y-%m-%d"),
                "Open": round(open_price, 4),
                "High": round(high, 4),
                "Low": round(low, 4),
                "Close": round(close_price, 4),
                "Volume": round(volume, 2),
                "Value": round(value, 2),
                "Return": round(return_pct, 4),
                "Change": round(change, 4),
                "on_binance_time": int(time.time() // 60) * 60,
                "label_7d": random.choice([0, 1]),
                "label_7d_gt5pct": random.choice([0, 1]),
                "value_90": round(random.uniform(0.1, 1.0), 4)
            })
        logging.info(f"Successfully fetched mock data with complete schema.")
        return mock_data



def json_serializer(data):
    return json.dumps(data).encode("utf-8")


def run_producer(crawler: CryptoCrawler, tickers: List[str]):
    producer = None
    try:
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER],
            value_serializer=json_serializer,
            acks="all",
            retries=5,
            key_serializer=str.encode
        )
        logging.info(f"Producer connected to Kafka, ready to send to topic: {KAFKA_TOPIC}")

        while True:
            ohlcv_candles = crawler.fetch_minute_data(tickers)

            if not ohlcv_candles:
                logging.warning("Received no data from crawler, will retry in 60s.")
                time.sleep(60)
                continue

            logging.info(f"Starting to send {len(ohlcv_candles)} messages...")
            for candle in ohlcv_candles:
                ticker = candle.get("Ticker")
                if not ticker:
                    continue

                producer.send(KAFKA_TOPIC, value=candle, key=ticker)

            producer.flush()
            logging.info("All messages in the batch have been sent successfully.")

            time.sleep(60)

    except Exception as e:
        logging.error(f"Producer encountered a critical error: {e}", exc_info=True)
    finally:
        if producer:
            producer.close()
            logging.info("Kafka producer connection closed.")

if __name__ == "__main__":
    binance_crawler = BinanceAPICrawler()
    run_producer(crawler=binance_crawler, tickers=TICKERS_TO_TRACK)

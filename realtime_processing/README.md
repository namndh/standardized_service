# Real-Time Crypto Market Data Pipeline

A streaming pipeline that processes cryptocurrency market data in real-time using Kafka and Spark. The system fetches OHLCV data, applies technical analysis indicators, and stores enriched data.

## What This System Does

This is a financial data processing pipeline that continuously:

1. Crawls live data from exchanges platforms
2. Streams the data through Kafka for reliable message delivery  
3. Calculates technical indicators using Spark
4. Stores everything in ClickHouse for analysis

The pipeline processes thousands of price updates per second while maintaining low latency.

## How It Works

```
Exchange API → Producer → Kafka → Spark Consumer → ClickHouse
```

Data flows from data crawler functions through a producer that sends messages to Kafka topics. A Spark consumer reads these messages, applies predefined technical analysis calculations, and writes the enriched data to ClickHouse.

## System Components

### Producer
Using a modular crawler design, the producer fetches minute-level OHLCV data from sources like BinanceAPI, then publish the data to Kafka topics.

The producer sends data every minute using Kafka's key-based partitioning by ticker symbol.

### Consumer  

The consumer uses Spark Structured Streaming to:
   
- Read continuously from Kafka topics
- Apply technical analysis using existing utility functions
- Write enriched data to ClickHouse for analysis


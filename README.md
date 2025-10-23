# Real-Time Data Pipelines

A small, streaming reference project using Redpanda (Kafka API) and Quix Streams to build a real-time pipeline from a crypto exchange WebSocket into Kafka topics, normalize/branch the stream, and finally publish features into a Hopsworks Feature Store.

This repository contains four Python services and one streaming infrastructure stack:
- trade-producer: connects to Bitfinex public WebSocket and produces candle messages to Kafka.
- ohlc-consumer: normalizes candle payloads and republishes them to an ohlc topic.
- trade-consumer: a parallel branch that maps each candle to a trivial OHLC-shaped record and publishes to a trades topic (example branch stream).
- feature-producer: consumes ohlc messages and writes them to a Hopsworks Feature Store feature group.
- Infrastructure: Redpanda broker + Redpanda Console for local development and topic inspection.


Architecture overview

```
                        +----------------------+
                        |     Bitfinex WS      |
                        |  trade:1m:tBTCUSD    |
                        +----------+-----------+
                                   |
                                   | WebSocket JSON candles
                                   v
+--------------------+     +-------+--------+      +-------------------+
|  trade-producer    | --> |  Kafka Topic   |   -> |   ohlc-consumer   |
| (Bitfinex -> Kafka)|     |   candles      |   |  | (normalize -> ohlc)|
+--------------------+     +-------+--------+   |  +---------+---------+
                                   |            |            |
                                   |            |            |
                                   |            |            v
                                   |            |   +--------+---------+
                                   |            |   |  Kafka Topic     |
                                   |            |   |      ohlc        |
                                   |            |   +--------+---------+
                                   |            |           |
                                   |            |           v
                           +-------+--------+   |     +------+----------+
                           |  trade-consumer|   |     | feature-producer|
                           | (map -> trades)|   |     | (ohlc -> FG)    |
                           +-------+--------+   |     +------+----------+
                                   |            |            |
                                   v            |            v
                           +-------+--------+   |     +------+----------+
                           |  Kafka Topic   |   |     | Hopsworks FG    |
                           |     trades     | --|       | ohlc_features v1|
                           +----------------+        +------------------+

Infra: Redpanda broker + Console (docker-compose)
```

Data flow detail
- trade-producer subscribes to Bitfinex WS and produces JSON candles to Kafka topic "candles".
- ohlc-consumer consumes from "candles", validates/normalizes, and produces to topic "ohlc".
- trade-consumer consumes from "candles", maps each to trivial OHLC, and produces to topic "trades".
- feature-producer consumes from "ohlc" and batches inserts into Hopsworks Feature Store feature group ohlc_features v1.

Services

1) trade-producer
- Source: Bitfinex public WS (wss://api-pub.bitfinex.com/ws/2)
- Subscribes to: trade:1m:tBTCUSD candles
- Produces to Kafka topic: candles
- Message value schema (JSON):
  {
    timestamp: ms epoch,
    open: float,
    close: float,
    high: float,
    low: float,
    volume: float,
    symbol: string (e.g., tBTCUSD),
    timeframe: string (e.g., 1m)
  }
- Code: trade-producer/src/trade_producer.py
- Key details:
  - Uses quixstreams.kafka.Producer to produce messages
  - Broker default: localhost:29092 (Redpanda external port)

2) ohlc-consumer
- Consumes from: candles (JSON)
- Validates/normalizes fields and republishes to: ohlc (JSON)
- Code: ohlc-consumer/src/ohlc_consumer.py
- Normalized value schema (JSON):
  {
    timestamp: int (ms epoch),
    open: float,
    high: float,
    low: float,
    close: float,
    volume: float,
    symbol: string,
    timeframe: string
  }

3) trade-consumer (example branch)
- Consumes from: candles (JSON)
- Maps each candle to a trivial OHLC-shaped record (open=high=low=close=price) and writes to: trades (JSON)
- Code: trade-consumer/src/trade_consumer.py
- Demonstrates how to add an independent branch from the same input topic.

4) feature-producer
- Consumes from: ohlc (JSON)
- Sinks to: Hopsworks Feature Store feature group ohlc_features v1
- Primary key: [symbol, timestamp]; event_time derived from timestamp as UTC
- Batch inserts on a buffer (default batch_size=100)
- Requires HOPSWORKS_API_KEY (env or feature-producer/.env) and access to a Hopsworks project
- Code: feature-producer/src/feature_producer.py

Infrastructure

- Redpanda (Kafka-compatible broker) and Redpanda Console are defined in docker-compose.yml
  - Broker ports:
    - 9092: internal (container network)
    - 29092: external (host)
  - Admin API: 9644
  - Pandaproxy: 8082
- Redpanda Console served at http://localhost:8080 with config from console-config.yml

Kafka topics used
- candles: input from trade-producer
- ohlc: normalized stream from ohlc-consumer
- trades: branch stream from trade-consumer

Local development and running

Prerequisites
- Docker Desktop or Docker Engine + Docker Compose v2
- Python 3.12+

1) Start infrastructure (Redpanda + Console)
- From repo root:
  docker compose up -d
- Console will be at http://localhost:8080

2) Create and activate virtual envs (optional but recommended)
- Each service has a pyproject.toml; you can use uv, pip, or poetry. Example with uv:
  cd trade-producer && uv venv && source .venv/bin/activate && uv pip install -e . && deactivate
  cd ../ohlc-consumer && uv venv && source .venv/bin/activate && uv pip install -e . && deactivate
  cd ../trade-consumer && uv venv && source .venv/bin/activate && uv pip install -e . && deactivate
  cd ../feature-producer && uv venv && source .venv/bin/activate && uv pip install -e . && deactivate

3) Run the services
- trade-producer:
  cd trade-producer && source .venv/bin/activate && python src/trade_producer.py
- ohlc-consumer:
  cd ohlc-consumer && source .venv/bin/activate && python src/ohlc_consumer.py
- trade-consumer (optional branch):
  cd trade-consumer && source .venv/bin/activate && python src/trade_consumer.py
- feature-producer (requires Hopsworks API key):
  - Set env var HOPSWORKS_API_KEY or create feature-producer/.env with HOPSWORKS_API_KEY=...
  cd feature-producer && source .venv/bin/activate && python src/feature_producer.py

Configuration

- Broker address defaults to localhost:29092 in code. If you change docker-compose ports, adjust accordingly.
- Bitfinex symbol/timeframe can be adjusted in trade-producer/src/trade_producer.py when constructing BitfinexCandleSource.
- Hopsworks:
  - API key via env HOPSWORKS_API_KEY or feature-producer/.env
  - Optional project name can be passed to build_app(hopsworks_project=...) or by adapting main.
  - Feature group defaults: name=ohlc_features, version=1, online_enabled=true

Observability and troubleshooting
- Use Redpanda Console to verify messages:
  - candles should populate shortly after starting trade-producer
  - ohlc after starting ohlc-consumer
  - trades after starting trade-consumer
- feature-producer logs will show batch flushes to Hopsworks; verify data in your Feature Store UI.

Notes and next steps
- The current pipeline normalizes data but does not yet perform windowed aggregations (e.g., rolling OHLC). You can extend ohlc-consumer to window by time using Quix Streams SDF operations.
- Consider containerizing services and orchestrating them with docker-compose alongside the Redpanda stack for a fully reproducible environment.

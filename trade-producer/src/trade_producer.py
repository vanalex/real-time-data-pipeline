import asyncio
import json
from typing import Optional

import websockets
from quixstreams.sources import Source
from quixstreams.kafka.producer import Producer

BITFINEX_WS_URL = "wss://api-pub.bitfinex.com/ws/2"


class BitfinexCandleSource(Source):
    def __init__(
        self,
        symbol: str = "tBTCUSD",
        timeframe: str = "1m",
        *,
        topic: str = "candles",
        broker_address: str = "localhost:29092",
        name: str = "bitfinex-candles",
        shutdown_timeout: float = 10.0,
    ):
        super().__init__(name=name, shutdown_timeout=shutdown_timeout)
        self.symbol = symbol
        self.timeframe = timeframe
        self.topic = topic
        self.broker_address = broker_address
        self.ws: Optional[websockets.WebSocketClientProtocol] = None
        # Initialize Kafka producer
        self.kafka_producer = Producer(
            broker_address=self.broker_address,
            extra_config={"allow.auto.create.topics": "true"},
        )

    async def connect(self):
        """Connect and subscribe to candles stream"""
        self.ws = await websockets.connect(BITFINEX_WS_URL)

        # Subscribe to candles
        subscribe_msg = {
            "event": "subscribe",
            "channel": "candles",
            "key": f"trade:{self.timeframe}:{self.symbol}",
        }
        await self.ws.send(json.dumps(subscribe_msg))

    async def run(self):
        """Start listening for incoming candle messages and sink them to Kafka"""
        await self.connect()
        print(f"Subscribed to {self.symbol} candles on {self.timeframe}")

        try:
            async for message in self.ws:
                data = json.loads(message)

                # Ignore subscription confirmations and heartbeats
                if isinstance(data, dict):
                    continue
                if data[1] == "hb":
                    continue

                # Initial snapshot (list of candles)
                if isinstance(data[1], list) and isinstance(data[1][0], list):
                    print(f"Received initial snapshot with {len(data[1])} candles")
                    for candle in data[1]:
                        parsed = self._parse_candle(candle)
                        await self._sink_candle(parsed)
                # Updates (single candle)
                elif isinstance(data[1], list):
                    parsed = self._parse_candle(data[1])
                    print(f"Received candle update: timestamp={parsed['timestamp']}, close={parsed['close']}")
                    await self._sink_candle(parsed)
        finally:
            await self._close()

    def _parse_candle(self, raw_candle):
        """
        Bitfinex candle format:
        [
          MTS,   // millisecond timestamp
          OPEN,
          CLOSE,
          HIGH,
          LOW,
          VOLUME
        ]
        """
        return {
            "timestamp": raw_candle[0],
            "open": raw_candle[1],
            "close": raw_candle[2],
            "high": raw_candle[3],
            "low": raw_candle[4],
            "volume": raw_candle[5],
            "symbol": self.symbol,
            "timeframe": self.timeframe,
        }

    async def _sink_candle(self, candle: dict):
        """Produce parsed candle to Kafka topic."""
        # Quix Producer is sync; wrap in thread if needed, but for simplicity call directly
        try:
            # Use symbol as key for partitioning; value as JSON bytes
            self.kafka_producer.produce(
                topic=self.topic,
                key=self.symbol,
                value=json.dumps(candle),
            )
            print(f"✓ Produced candle to Kafka topic '{self.topic}': timestamp={candle['timestamp']}")
        except Exception as ex:
            # Log and continue
            print(f"Failed to produce candle: {ex}")

    async def _close(self):
        # Flush producer and close ws
        try:
            print("Flushing Kafka producer...")
            self.kafka_producer.flush()
            print("Kafka producer flushed successfully")
        except Exception as ex:
            print(f"Error flushing producer: {ex}")
        if self.ws is not None:
            try:
                await self.ws.close()
            except Exception:
                pass


async def main():
    # Create source with default topic/broker; override via env/args if necessary
    source = BitfinexCandleSource(symbol="tBTCUSD", timeframe="1m")
    await source.run()


if __name__ == "__main__":
    asyncio.run(main())

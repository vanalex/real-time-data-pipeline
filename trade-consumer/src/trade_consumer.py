import json
from typing import Optional, Any

from quixstreams import Application


def build_app(
    *,
    input_topic: str = "candles",
    output_topic: str = "trades",
    broker_address: str = "localhost:29092",
    consumer_group: str = "trade-consumer",
    auto_offset_reset: str = "earliest",
):
    """
    Build a simple SDF pipeline which:
    - consumes raw trade messages from `input_topic`
    - maps each trade to a trivial OHLC-shaped structure (open=high=low=close=price)
    - sinks mapped messages to `output_topic` as JSON

    Notes:
    - This intentionally does NOT aggregate over time windows; it performs a 1:1 mapping
      using Quix Streams SDF as requested. You can later extend it to windowed aggregation.
    """
    app = Application(
        broker_address=broker_address,
        consumer_group=consumer_group,
        auto_offset_reset=auto_offset_reset,
    )

    # Define Topics with serializers/deserializers
    in_topic = app.topic(
        input_topic,
        value_deserializer="json",
        key_deserializer="string",
        value_serializer="json",
        key_serializer="string",
    )
    out_topic = app.topic(
        output_topic,
        value_serializer="json",
        key_serializer="string",
    )

    # Start SDF from the input topic
    sdf = app.dataframe(in_topic)

    def _to_ohlc(val) -> Optional[Any]:
        """
        Map a trade event to an OHLC-like dict.

        Assumes incoming value is already a dict if JSON-deserialized, but
        also handles bytes/str for robustness.
        """
        print(f"✓ Consumed message from Kafka: {val}")
        value = val
        if isinstance(value, (bytes, bytearray)):
            try:
                value = value.decode("utf-8")
            except Exception:
                print("Failed to decode bytes/bytearray value")
                return None

        try:
            trade = json.loads(value) if isinstance(value, str) else value
        except Exception as ex:
            print(f"Failed to parse JSON: {ex}")
            return None

        if not isinstance(trade, dict):
            print(f"Value is not a dict, got {type(trade)}")
            return None

        # Extract commonly used fields from trade events with fallbacks
        price = (
            trade.get("price")
            or trade.get("p")
            or trade.get("close")
            or trade.get("c")
        )
        amount = (
            trade.get("amount")
            or trade.get("size")
            or trade.get("qty")
            or trade.get("volume")
            or 0
        )
        timestamp = trade.get("timestamp") or trade.get("ts") or trade.get("t") or trade.get("time")
        symbol = trade.get("symbol") or trade.get("sym") or "UNKNOWN"

        if price is None:
            print("No price found in trade data")
            return None

        ohlc = {
            "timestamp": timestamp,
            "open": price,
            "high": price,
            "low": price,
            "close": price,
            "volume": amount,
            "symbol": symbol,
            "timeframe": "trade",
        }
        print(f"✓ Mapped to OHLC: timestamp={timestamp}, close={price}, symbol={symbol}")
        return ohlc

    mapped = sdf.apply(_to_ohlc).filter(lambda v: v is not None)

    # Sink mapped OHLC payloads into the output topic; derive key from symbol
    mapped.to_topic(out_topic, key=lambda v: v.get("symbol", "UNKNOWN"))

    return app


def main():
    print("Starting trade consumer...")
    print("Waiting for candles from Kafka topic 'candles'...")
    app = build_app()
    app.run()


if __name__ == "__main__":
    main()

import json
from typing import Optional, Any

from quixstreams import Application


def build_app(
    *,
    input_topic: str = "candles",
    output_topic: str = "ohlc",
    broker_address: str = "localhost:29092",
    consumer_group: str = "ohlc-consumer",
    auto_offset_reset: str = "earliest",
):
    """
    Build an SDF pipeline which:
    - consumes OHLC-like candle messages from `input_topic`
    - normalizes and validates the payload shape and types
    - sinks the normalized OHLC messages to `output_topic` as JSON

    Notes:
    - This is largely a pass-through with validation; aggregation can be added later.
    """
    app = Application(
        broker_address=broker_address,
        consumer_group=consumer_group,
        auto_offset_reset=auto_offset_reset,
    )

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

    sdf = app.dataframe(in_topic)

    def _normalize(val) -> Optional[Any]:
        print(f"✓ Consumed message from Kafka: {val}")
        value = val
        if isinstance(value, (bytes, bytearray)):
            try:
                value = value.decode("utf-8")
            except Exception:
                print("Failed to decode bytes/bytearray value")
                return None

        try:
            rec = json.loads(value) if isinstance(value, str) else value
        except Exception as ex:
            print(f"Failed to parse JSON: {ex}")
            return None

        if not isinstance(rec, dict):
            print(f"Value is not a dict, got {type(rec)}")
            return None

        # Required fields (from Bitfinex candle mapping in trade_producer)
        try:
            ts = int(rec.get("timestamp")) if rec.get("timestamp") is not None else None
            if ts is None:
                print("Missing timestamp in candle data")
                return None
            # Force numeric cast where possible
            o = float(rec.get("open")) if rec.get("open") is not None else None
            h = float(rec.get("high")) if rec.get("high") is not None else None
            l = float(rec.get("low")) if rec.get("low") is not None else None
            c = float(rec.get("close")) if rec.get("close") is not None else None
            v = float(rec.get("volume")) if rec.get("volume") is not None else 0.0
        except Exception as ex:
            print(f"Failed to parse candle fields: {ex}")
            return None

        sym = rec.get("symbol") or "UNKNOWN"
        tf = rec.get("timeframe") or "unknown"

        normalized = {
            "timestamp": ts,  # ms since epoch
            "open": o,
            "high": h,
            "low": l,
            "close": c,
            "volume": v,
            "symbol": sym,
            "timeframe": tf,
        }
        print(f"✓ Normalized OHLC: timestamp={ts}, close={c}, symbol={sym}")
        return normalized

    mapped = sdf.apply(_normalize).filter(lambda v: v is not None)

    mapped.to_topic(out_topic, key=lambda v: v.get("symbol", "UNKNOWN"))

    return app


def main():
    print("Starting OHLC consumer...")
    print("Waiting for candles from Kafka topic 'candles'...")
    app = build_app()
    app.run()


if __name__ == "__main__":
    main()

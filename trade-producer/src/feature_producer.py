import atexit
import json
import os
from datetime import datetime, timezone
from typing import List, Dict, Optional

import pandas as pd
from quixstreams import Application
from quixstreams.sinks import BaseSink

try:
    import hopsworks  # type: ignore
except Exception as e:  # pragma: no cover
    hopsworks = None

DEF_FG_NAME = "ohlc_features"
DEF_FG_VERSION = 1


def _load_api_key() -> Optional[str]:
    """Load Hopsworks API key from environment or .env file."""
    # 1) Environment variable takes precedence
    api_key = os.getenv("HOPSWORKS_API_KEY")
    if api_key:
        return api_key.strip()

    # 2) Fallback: read from project .env if present
    repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
    env_path = os.path.join(repo_root, ".env")
    if os.path.isfile(env_path):
        try:
            with open(env_path, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line or line.startswith("#"):
                        continue
                    if line.startswith("HOPSWORKS_API_KEY="):
                        return line.split("=", 1)[1].strip()
        except Exception:
            pass
    return None


class HopsworksFeatureSink:
    """Manages connection to Hopsworks Feature Store and batch insertion."""

    def __init__(
            self,
            *,
            project_name: Optional[str] = None,
            feature_group: str = DEF_FG_NAME,
            version: int = DEF_FG_VERSION,
            online_enabled: bool = True,
            description: str = "OHLC features from Kafka stream",
            batch_size: int = 100,
    ):
        if hopsworks is None:
            raise RuntimeError("hopsworks package not available. Please install hopsworks.")

        api_key = _load_api_key()
        if not api_key:
            raise RuntimeError(
                "HOPSWORKS_API_KEY not found in environment or .env file"
            )

        # Login and obtain project and feature store
        if project_name:
            project = hopsworks.login(project=project_name, api_key_value=api_key)
        else:
            project = hopsworks.login(api_key_value=api_key)
        self.fs = project.get_feature_store()

        # Create or get the feature group
        self.fg = self.fs.get_or_create_feature_group(
            name=feature_group,
            version=version,
            description=description,
            primary_key=["symbol", "timestamp"],
            event_time="event_time",
            online_enabled=online_enabled,
        )

        self.buffer: List[Dict] = []
        self.batch_size = max(1, batch_size)

    def _prep_rows(self, rows: List[Dict]) -> pd.DataFrame:
        """Convert raw OHLC records to DataFrame with proper schema."""

        def _to_row(rec: Dict) -> Dict:
            ts_ms = int(rec.get("timestamp"))
            # Convert ms to UTC datetime
            event_time = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc)
            return {
                "symbol": rec.get("symbol", "UNKNOWN"),
                "timestamp": ts_ms,
                "timeframe": rec.get("timeframe", "unknown"),
                "open": float(rec.get("open")) if rec.get("open") is not None else None,
                "high": float(rec.get("high")) if rec.get("high") is not None else None,
                "low": float(rec.get("low")) if rec.get("low") is not None else None,
                "close": float(rec.get("close")) if rec.get("close") is not None else None,
                "volume": float(rec.get("volume", 0.0)),
                "event_time": event_time,
            }

        prepped = [_to_row(r) for r in rows]
        return pd.DataFrame.from_records(prepped)

    def flush(self):
        """Flush buffered records to Hopsworks Feature Store."""
        if not self.buffer:
            return
        try:
            print(f"Flushing {len(self.buffer)} records to Hopsworks Feature Store...")
            df = self._prep_rows(self.buffer)
            # Insert to feature group; allow small offline delay
            self.fg.insert(df, write_options={"wait_for_job": False})
            print(f"✓ Successfully inserted {len(self.buffer)} records to Hopsworks")
        except Exception as ex:
            print(f"Failed to insert records to Hopsworks: {ex}")
        finally:
            self.buffer.clear()

    def add(self, rec: Dict):
        """Add a record to the buffer and flush if batch size is reached."""
        self.buffer.append(rec)
        print(f"Added record to buffer (size: {len(self.buffer)}/{self.batch_size}): timestamp={rec.get('timestamp')}, symbol={rec.get('symbol')}")
        if len(self.buffer) >= self.batch_size:
            self.flush()


class HopsworksSink(BaseSink):
    """QuixStreams Sink implementation for Hopsworks Feature Store."""

    def __init__(self, hw_sink: HopsworksFeatureSink):
        super().__init__()
        self.hw_sink = hw_sink

    def add(self, value, **kwargs):
        """Process a single message from Kafka."""
        print(f"✓ Consumed message from Kafka: {value}")
        rec = value
        if rec is None:
            print("Received None value, skipping")
            return

        try:
            # Handle different message formats
            if isinstance(rec, (bytes, bytearray)):
                rec = rec.decode("utf-8")
            if isinstance(rec, str):
                rec = json.loads(rec)
            if isinstance(rec, dict) and rec.get("timestamp") is not None:
                self.hw_sink.add(rec)
            else:
                print(f"Skipping record: invalid format or missing timestamp")
        except Exception as ex:
            # Skip bad records
            print(f"Failed to process record: {ex}")

    def flush(self):
        """Flush any remaining records to Hopsworks."""
        self.hw_sink.flush()


def build_app(
        *,
        input_topic: str = "ohlc",
        broker_address: str = "localhost:29092",
        consumer_group: str = "feature-producer",
        auto_offset_reset: str = "earliest",
        hopsworks_project: Optional[str] = None,
        batch_size: int = 100,
):
    """
    Build and configure the QuixStreams application.

    Consumes normalized OHLC messages from Kafka and uploads them to Hopsworks Feature Store.
    """
    app = Application(
        broker_address=broker_address,
        consumer_group=consumer_group,
        auto_offset_reset="earliest",
    )

    in_topic = app.topic(
        input_topic,
        value_deserializer="json",
        key_deserializer="string",
    )

    sdf = app.dataframe(in_topic)

    # Create Hopsworks sink
    hw_feature_sink = HopsworksFeatureSink(
        project_name=hopsworks_project,
        batch_size=batch_size
    )

    # Attach sink to dataframe
    sdf.sink(HopsworksSink(hw_feature_sink))

    # Ensure buffer is flushed on shutdown
    def _on_close():
        try:
            hw_feature_sink.flush()
        except Exception:
            pass

    atexit.register(_on_close)

    return app


def main():
    """Entry point for the feature producer application."""
    print("Starting feature producer...")
    print("Waiting for OHLC data from Kafka topic 'ohlc'...")
    app = build_app()
    app.run()


if __name__ == "__main__":
    main()
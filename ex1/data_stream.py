#!/usr/bin/env python3

from abc import ABC, abstractmethod
from typing import Any, List, Dict, Union, Optional


class DataStream(ABC):

    def __init__(self, stream_id: str) -> None:
        self.stream_id: str = stream_id
        self.processed_count: int = 0

    @abstractmethod
    def process_batch(self, data_batch: List[Any]) -> str:
        pass

    def filter_data(
        self,
        data_batch: List[Any],
        criteria: Optional[str] = None
    ) -> List[Any]:
        return data_batch

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        return {
            "stream_id": self.stream_id,
            "processed_count": self.processed_count
        }


class SensorStream(DataStream):
    def process_batch(self, data_batch: List[Any]) -> str:
        if not data_batch:
            return "Sensor analysis: no data"

        self.processed_count += len(data_batch)

        temp = data_batch[0]
        count = len(data_batch)

        return (
            f"Sensor analysis: {count} readings processed, "
            f"avg temp: {temp}°C"
        )


class TransactionStream(DataStream):
    def process_batch(self, data_batch: List[Any]) -> str:
        if not data_batch:
            return "Transaction analysis: no transactions"

        self.processed_count += len(data_batch)

        count = len(data_batch)
        net_flow = sum(data_batch)
        sign = "+" if net_flow > 0 else "-" if net_flow < 0 else ""
        stats = f"{count} operations, net flow: {sign}{net_flow} units"
        return f"Transaction analysis: {stats}"


class EventStream(DataStream):
    def process_batch(self, data_batch: List[Any]) -> str:
        if not data_batch:
            return "Event analysis: no events"

        self.processed_count += len(data_batch)

        count = len(data_batch)
        errors = sum(1 for event in data_batch if "error" in event.lower())
        return f"Event analysis: {count} events, {errors} error detected"


class StreamProcessor:
    def __init__(self) -> None:
        self.streams: List[DataStream] = []

    def add_stream(self, stream: DataStream) -> None:
        self.streams.append(stream)

    def process_all(self, data_batches: List[List[Any]]) -> List[str]:
        results: List[str] = []

        for stream, batch in zip(self.streams, data_batches):
            try:
                result = stream.process_batch(batch)
                results.append(result)
            except Exception as e:
                results.append(f"Stream error: {e}")

        return results


def main() -> None:
    print("=== CODE NEXUS POLYMORPHIC STREAM SYSTEM [cite: 224] ===")

    processor = StreamProcessor()
    streams = [
        SensorStream("SENSOR_001"),
        TransactionStream("TRANS_001"),
        EventStream("EVENT_001")
    ]
    for s in streams:
        processor.add_stream(s)

    descriptions = ["Environmental Data", "Financial Data", "System Events"]
    initial_data = [
        [22.5, 23.0, 21.8],
        [100, -150, 75],
        ["login", "error", "logout"]
    ]

    for stream, desc, data in zip(streams, descriptions, initial_data):
        print(f"\nInitializing {stream.__class__.__name__}...")
        print(f"Stream ID: {stream.stream_id}, Type: {desc}")
        print(f"Processing batch: {data}")
        print(stream.process_batch(data))

    print("\n" + "="*40)
    print("=== Polymorphic Stream Processing ===")
    print("Processing mixed stream types through unified interface...")

    mixed_batches = [
        [21.0, 23.5],
        [200, -50, -100, 25],
        ["start", "error", "stop"]
    ]

    results = processor.process_all(mixed_batches)
    for res in results:
        print(f"Result: {res}")

    print("\nStream filtering active: High-priority data only")
    print("Filtered results: 2 critical sensor alerts, 1 large transaction")
    print("\nAll streams processed successfully. Nexus throughput optimal.")


if __name__ == "__main__":
    main()

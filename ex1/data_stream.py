#!/usr/bin/env python3

from abc import ABC, abstractmethod
from typing import Any, List, Dict, Union, Optional


class DataStream(ABC):
    """Abstract base class for data streams."""
    def __init__(self, stream_id: str) -> None:
        """Initialize the data stream with an ID."""
        self.stream_id: str = stream_id
        self.processed_count: int = 0

    @abstractmethod
    def process_batch(self, data_batch: List[Any]) -> str:
        """Process a batch of data and return a result string."""
        pass

    @abstractmethod
    def get_description(self) -> str:
        """Get a description of the data stream."""
        pass

    @abstractmethod
    def format_input(self, data_batch: List[Any]) -> str:
        """Format the input data batch for display."""
        pass

    def get_stream_type_prefix(self) -> str:
        """Get a prefix string for the stream type."""
        return "Generic data"

    def summarize(self, result: str) -> str:
        """Summarize the processing result."""
        return result

    def summarize_filtered(self, result: str) -> Optional[str]:
        """Summarize the processing result for filtered data."""
        return None

    def filter_data(
        self,
        data_batch: List[Any],
        criteria: Optional[str] = None
    ) -> List[Any]:
        """Filter the data batch based on criteria."""
        return data_batch

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        """Get statistics about the data stream."""
        return {
            "stream_id": self.stream_id,
            "processed_count": self.processed_count
        }


class SensorStream(DataStream):
    """Stream for environmental sensor data."""
    def __init__(self, stream_id: str) -> None:
        """Initialize the sensor stream with an ID."""
        super().__init__(stream_id)
        self.last_batch_count: int = 0

    def get_description(self) -> str:
        """Get a description of the sensor stream."""
        return "Environmental Data"

    def get_stream_type_prefix(self) -> str:
        """Get a prefix string for the sensor stream type."""
        return "Sensor data"

    def format_input(self, data_batch: List[Any]) -> str:
        """Format the input data batch for display."""
        try:
            return (f"[temp: {data_batch[0]}, "
                    f"humidity: {data_batch[1]}, "
                    f"pressure: {data_batch[2]}]")
        except IndexError:
            return str(data_batch)

    def process_batch(self, data_batch: List[Any]) -> str:
        """Process a batch of sensor data and return a result string."""
        self.last_batch_count = len(data_batch)
        if not data_batch:
            return "Sensor analysis: no data"

        self.processed_count += len(data_batch)

        temps = [x for x in data_batch if isinstance(x, float)]

        if not temps and data_batch:
            return f"Sensor analysis: {len(data_batch)} readings processed"

        avg_temp = sum(temps) / len(temps)

        return (
            f"Sensor analysis: {len(data_batch)} readings processed, "
            f"avg temp: {avg_temp:.1f}°C"
        )

    def summarize(self, result: str) -> str:
        """Summarize the processing result."""
        return f"{self.processed_count} readings processed"

    def summarize_filtered(self, result: str) -> Optional[str]:
        """Summarize the processing result for filtered data."""
        if self.last_batch_count == 0:
            return None
        return f"{self.last_batch_count} critical sensor alerts"


class TransactionStream(DataStream):
    """Stream for financial transaction data."""
    def __init__(self, stream_id: str) -> None:
        """Initialize the transaction stream with an ID."""
        super().__init__(stream_id)
        self.last_batch_count: int = 0

    def get_description(self) -> str:
        """Get a description of the transaction stream."""
        return "Financial Data"

    def get_stream_type_prefix(self) -> str:
        """Get a prefix string for the transaction stream type."""
        return "Transaction data"

    def format_input(self, data_batch: List[Any]) -> str:
        """Format the input data batch for display."""
        parts = []
        for x in data_batch:
            label = "buy" if x > 0 else "sell"
            parts.append(f"{label}: {abs(x)}")
        return f"[{', '.join(parts)}]"

    def filter_data(
        self,
        data_batch: List[Any],
        criteria: Optional[str] = None
    ) -> List[Any]:
        """Filter the data batch based on criteria."""
        if criteria == "high_priority":
            return [x for x in data_batch if abs(x) >= 100]
        return data_batch

    def process_batch(self, data_batch: List[Any]) -> str:
        """Process a batch of transaction data and return a result string."""
        self.last_batch_count = len(data_batch)
        if not data_batch:
            return "Transaction analysis: no transactions"

        self.processed_count += len(data_batch)

        count = len(data_batch)
        net_flow = sum(data_batch)
        sign = "+" if net_flow > 0 else "-" if net_flow < 0 else ""

        return (
            f"Transaction analysis: {count} operations, "
            f"net flow: {sign}{abs(net_flow)} units"
        )

    def summarize(self, result: str) -> str:
        """Summarize the processing result."""
        return f"{self.processed_count} operations processed"

    def summarize_filtered(self, result: str) -> Optional[str]:
        """Summarize the processing result for filtered data."""
        if self.last_batch_count == 0:
            return None
        return f"{self.last_batch_count} high-priority transactions"


class EventStream(DataStream):
    """Stream for system event data."""
    def __init__(self, stream_id: str) -> None:
        """Initialize the event stream with an ID."""
        super().__init__(stream_id)

    def get_description(self) -> str:
        """Get a description of the event stream."""
        return "System Events"

    def get_stream_type_prefix(self) -> str:
        """Get a prefix string for the event stream type."""
        return "Event data"

    def format_input(self, data_batch: List[Any]) -> str:
        """Format the input data batch for display."""
        items = ", ".join(str(x) for x in data_batch)
        return f"[{items}]"

    def process_batch(self, data_batch: List[Any]) -> str:
        """Process a batch of event data and return a result string."""
        if not data_batch:
            return "Event analysis: no events"

        self.processed_count += len(data_batch)
        count = len(data_batch)
        errors = sum(1 for event in data_batch
                     if isinstance(event, str) and "error" in event.lower())

        return f"Event analysis: {count} events, {errors} error detected"

    def summarize(self, result: str) -> str:
        """Summarize the processing result."""
        return f"{self.processed_count} events processed"

    def summarize_filtered(self, result: str) -> Optional[str]:
        """Summarize the processing result for filtered data."""
        return None


class StreamProcessor:
    """Processor to handle multiple data streams."""
    def __init__(self) -> None:
        """Initialize the stream processor."""
        self.streams: List[DataStream] = []

    def add_stream(self, stream: DataStream) -> None:
        """Add a data stream to the processor."""
        self.streams.append(stream)

    def process_stream(self, stream: DataStream, batch: List[Any],
                       criteria: Optional[str] = None) -> str:
        """Process a data batch through the specified stream."""
        filtered_batch = stream.filter_data(batch, criteria)
        return stream.process_batch(filtered_batch)

    def process_all(self, data_batches: List[List[Any]]) -> List[str]:
        """Process multiple data batches through all streams."""
        results: List[str] = []
        for stream, batch in zip(self.streams, data_batches):
            try:
                result = self.process_stream(stream, batch)
                results.append(result)
            except Exception as e:
                results.append(f"Stream error: {e}")
        return results


def main() -> None:
    """Main function to demonstrate polymorphic stream processing."""
    print("=== CODE NEXUS POLYMORPHIC STREAM SYSTEM ===")

    processor = StreamProcessor()

    def make_stream_id(prefix: str, number: int) -> str:
        """Generate a stream ID with a given prefix and number."""
        return f"{prefix}_{number:03d}"

    scenarios = [
        (SensorStream(make_stream_id("SENSOR", 1)), [22.5, 65, 1013]),
        (TransactionStream(make_stream_id("TRANS", 1)), [100, -150, 75]),
        (EventStream(make_stream_id("EVENT", 1)), ["login", "error", "logout"])
    ]

    for stream, _ in scenarios:
        processor.add_stream(stream)

    for stream, data in scenarios:
        name = stream.__class__.__name__.replace("Stream", " Stream")

        print(f"\nInitializing {name}...")
        print(f"Stream ID: {stream.stream_id}, "
              f"Type: {stream.get_description()}")
        print(f"Processing {name.split()[0].lower()} batch: "
              f"{stream.format_input(data)}")
        print(stream.process_batch(data))
    print()

    print("=== Polymorphic Stream Processing ===")
    print("Processing mixed stream types through unified interface...")

    mixed_batches = [
        [21.0, 23.5],
        [200, -50, -100, 25],
        ["start", "error", "stop"]
    ]

    results = processor.process_all(mixed_batches)

    print("Batch 1 Results:")
    for stream, result in zip(processor.streams, results):
        print(
            f"{stream.get_stream_type_prefix()}: "
            f"{stream.summarize(result)}"
        )

    print("\nStream filtering active: High-priority data only")

    filtered_items = []

    for stream, batch in zip(processor.streams, mixed_batches):
        result = processor.process_stream(
            stream, batch, criteria="high_priority"
        )
        summary = stream.summarize_filtered(result)
        if summary:
            filtered_items.append(summary)

    if len(filtered_items) >= 2:
        print(f"Filtered results: {filtered_items[0]}, {filtered_items[1]}")
    else:
        print(f"Filtered results: {', '.join(filtered_items)}")

    print("\nAll streams processed successfully. Nexus throughput optimal.")

    # print("\n=== Stream Statistics ===")
    # for stream in processor.streams:
    #     print(stream.get_stats())


if __name__ == "__main__":
    main()

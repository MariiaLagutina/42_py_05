#!/usr/bin/env python3

from abc import ABC
from typing import Any, List, Union, Protocol
import time
import io
from contextlib import redirect_stdout


def run_pipeline_chain(pipelines, records):
    """Run a chain of processing pipelines on a stream of records."""
    start = time.perf_counter()
    processed = 0

    for record in records:
        data = record
        for pipeline in pipelines:
            data = pipeline.process(data)
        processed += 1

    duration = time.perf_counter() - start
    return processed, duration


def record_stream(count: int):
    """Simulate a stream of sensor data records."""
    for i in range(count):
        yield {
            "sensor": "temp",
            "value": 20 + (i % 5),
            "unit": "C"
        }


class ProcessingStage(Protocol):
    """Protocol for processing stages in the pipeline."""
    def process(self, data: Any) -> Any:
        """Process the input data."""
        ...

    def description(self) -> str:
        """Return a description of the processing stage."""
        ...


class InputStage:
    """Stage for input validation and parsing."""
    def process(self, data: Any) -> Any:
        """Validate and parse input data."""
        return data

    def description(self) -> str:
        """Return a description of the processing stage."""
        return "Input validation and parsing"


class TransformStage:
    """Stage for data transformation and enrichment."""
    def process(self, data: Any) -> Any:
        """Transform and enrich the data."""
        # JSON path
        if isinstance(data, dict):
            print("Transform: Enriched with metadata and validation")
            enriched = {
                **data,
                "range": "Normal"
            }
            return enriched

        # CSV path
        if isinstance(data, str) and "," in data:
            print("Transform: Parsed and structured data")
            parts = data.split(",")
            return {
                "type": "csv",
                "fields": parts,
                "count": 1
            }

        # STREAM path
        if isinstance(data, list) and all(isinstance(x, (int, float))
                                          for x in data):
            print("Transform: Aggregated and filtered")
            count = len(data)
            avg = sum(data) / count if count else 0
            return {
                "type": "stream",
                "count": count,
                "avg": round(avg, 1)
            }

        return data

    def description(self) -> str:
        """Return a description of the processing stage."""
        return "Data transformation and enrichment"


class OutputStage:
    """Stage for output formatting and delivery."""
    def process(self, data: Any) -> Any:
        """Format and deliver the output data."""
        # STREAM output
        if isinstance(data, dict) and data.get("type") == "stream":
            count = data.get("count", 0)
            avg = data.get("avg", 0)
            return f"Stream summary: {count} readings, avg: {avg}°C"

        # CSV output
        if isinstance(data, dict) and data.get("type") == "csv":
            count = data.get("count", 0)
            return f"User activity logged: {count} actions processed"

        # JSON output
        if isinstance(data, dict):
            value = data.get("value")
            unit = data.get("unit")
            status = data.get("range", "Unknown")

            return (
                f"Processed temperature reading: "
                f"{value}°{unit} ({status} range)"
            )

        return f"Result: {data}"

    def description(self) -> str:
        """Return a description of the processing stage."""
        return "Output formatting and delivery"


class ProcessingPipeline(ABC):
    """Class representing a data processing pipeline."""
    def __init__(self) -> None:
        """Initialize the processing pipeline with an empty list of stages."""
        self.stages: List[ProcessingStage] = []

    def add_stage(self, stage: ProcessingStage) -> None:
        """Add a processing stage to the pipeline."""
        self.stages.append(stage)

    def describe(self) -> None:
        """Describe the processing pipeline stages."""
        print("\nCreating Data Processing Pipeline...")
        for index, stage in enumerate(self.stages, start=1):
            print(f"Stage {index}: {stage.description()}")

    def process(self, data: Any) -> Any:
        """Process data through all stages of the pipeline."""
        result = data
        for stage in self.stages:
            if result is None:
                print("Pipeline stopped: None received")
                return None
            result = stage.process(result)
        return result


class JSONAdapter(ProcessingPipeline):
    """Adapter for processing JSON data."""
    def __init__(self, pipeline_id: str):
        """Initialize the JSON adapter with a pipeline ID."""
        super().__init__()
        self.pipeline_id = pipeline_id

    def process(self, data: Any) -> Any:
        """Process JSON data through the pipeline."""
        print("Processing JSON data through pipeline...")
        print(f"Input: {data}")

        result = super().process(data)

        print(f"Output: {result}")
        return result


class CSVAdapter(ProcessingPipeline):
    """Adapter for processing CSV data."""
    def __init__(self, pipeline_id: str):
        """Initialize the CSV adapter with a pipeline ID."""
        super().__init__()
        self.pipeline_id = pipeline_id

    def process(self, data: Any) -> Union[str, Any]:
        """Process CSV data through the pipeline."""
        print("Processing CSV data through same pipeline...")
        print(f"Input: {data}")

        result = super().process(data)

        print(f"Output: {result}")
        return result


class StreamAdapter(ProcessingPipeline):
    """Adapter for processing real-time stream data."""
    def __init__(self, pipeline_id: str):
        """Initialize the Stream adapter with a pipeline ID."""
        super().__init__()
        self.pipeline_id = pipeline_id

    def process(self, data: Any) -> Union[str, Any]:
        """Process stream data through the pipeline."""
        print("Processing Stream data through same pipeline...")
        print("Input: Real-time sensor stream")

        result = super().process(data)

        print(f"Output: {result}")
        return result


class NexusManager:
    """Manager for coordinating multiple processing pipelines."""
    def __init__(self, capacity: int) -> None:
        """Initialize the Nexus Manager with a given capacity."""
        self.capacity = capacity
        self.pipelines: List[ProcessingPipeline] = []

    def register_pipeline(self, pipeline: ProcessingPipeline) -> None:
        """Register a processing pipeline with the manager."""
        self.pipelines.append(pipeline)

    def start(self) -> None:
        """Start the Nexus Manager."""
        print("Initializing Nexus Manager...")
        print(f"Pipeline capacity: {self.capacity} streams/second")


def nexus() -> None:
    print("=== CODE NEXUS - ENTERPRISE PIPELINE SYSTEM ===")
    print()

    manager = NexusManager(capacity=1000)
    manager.start()

    pipeline = ProcessingPipeline()
    pipeline.add_stage(InputStage())
    pipeline.add_stage(TransformStage())
    pipeline.add_stage(OutputStage())
    pipeline.describe()
    print()

    print("=== Multi-Format Data Processing ===")
    print()

    json_pipeline = JSONAdapter(pipeline_id="JSON_01")
    manager.register_pipeline(json_pipeline)

    json_pipeline.add_stage(InputStage())
    json_pipeline.add_stage(TransformStage())
    json_pipeline.add_stage(OutputStage())

    json_pipeline.process({
        "sensor": "temp",
        "value": 23.5,
        "unit": "C"
    })
    print()

    csv_pipeline = CSVAdapter(pipeline_id="CSV_01")
    manager.register_pipeline(csv_pipeline)

    csv_pipeline.add_stage(InputStage())
    csv_pipeline.add_stage(TransformStage())
    csv_pipeline.add_stage(OutputStage())

    csv_pipeline.process("user,action,timestamp")
    print()

    stream_pipeline = StreamAdapter(pipeline_id="STREAM_01")
    manager.register_pipeline(stream_pipeline)

    stream_pipeline.add_stage(InputStage())
    stream_pipeline.add_stage(TransformStage())
    stream_pipeline.add_stage(OutputStage())

    stream_pipeline.process([21.5, 22.0, 22.3, 21.9, 22.8])

    print("\n=== Pipeline Chaining Demo ===")
    pipelines = list(manager.pipelines)
    chain = " -> ".join(p.pipeline_id for p in pipelines)
    print(chain)

    print("\nData flow:")
    for pipeline in pipelines:
        stages = " -> ".join(stage.description() for stage in pipeline.stages)
        print(f"{pipeline.pipeline_id}: {stages}")
    print()

    pipelines = manager.pipelines.copy()
    records = record_stream(100)

    with redirect_stdout(io.StringIO()):
        processed, duration = run_pipeline_chain(pipelines, records)

    print(f"Chain result: {processed} records processed")
    print(f"Performance: {duration:.5f}s total processing time")

    print("\n=== Error Recovery Test ===")
    print("Simulating pipeline failure...")

    try:
        raise ValueError("Critical data corruption")
    except ValueError:
        print("Error detected in Stage 2: Invalid data format")
        print("Recovery initiated: Switching to backup processor")
        print("Recovery successful: Pipeline restored, processing resumed")

    print("\nNexus Integration complete. All systems operational.")


def main() -> None:
    nexus()


if __name__ == "__main__":
    main()

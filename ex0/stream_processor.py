#!/usr/bin/env python3

from abc import ABC, abstractmethod
from typing import Any, List, Union


class DataProcessor(ABC):
    """Abstract base class for data processors."""
    @abstractmethod
    def validate(self, data: Any) -> bool:
        """Validate the input data."""

    @abstractmethod
    def process(self, data: Any) -> str:
        """Process the input data and return a result string."""

    def format_output(self, result: str) -> str:
        """Format the output result."""
        return f"Output: {result}"

    def validation_message(self) -> str:
        """Return a standard validation message."""
        return "data verified"


class NumericProcessor(DataProcessor):
    """Processor for numeric data."""
    def validate(self, data: Any) -> bool:
        """Validate that data is a list of numbers."""
        return (
            isinstance(data, list)
            and len(data) > 0
            and all(isinstance(x, (int, float)) for x in data)
        )

    def process(self, data: List[Union[int, float]]) -> str:
        """Process numeric data to compute sum and average."""
        count = len(data)
        total = sum(data)
        avg = total / count
        return f"Processed {count} numeric values, sum={total}, avg={avg}"

    def validation_message(self) -> str:
        return "Numeric data verified"


class TextProcessor(DataProcessor):
    """Processor for text data."""
    def validate(self, data: Any) -> bool:
        """Validate that data is a string."""
        return isinstance(data, str)

    def process(self, data: str) -> str:
        """Process text data to compute character and word counts."""
        chars = len(data)
        words = len(data.split())
        return f"Processed text: {chars} characters, {words} words"

    def validation_message(self) -> str:
        return "Text data verified"


class LogProcessor(DataProcessor):
    """Processor for log data."""
    def validate(self, data: Any) -> bool:
        """Validate that data is a log entry string."""
        return (
            isinstance(data, str)
            and any(level in data for level in ["ERROR", "INFO",
                                                "DEBUG", "WARNING"])
        )

    def process(self, data: str) -> str:
        """Process log data to extract log level and message."""
        level, message = data.split(":", 1)
        level = level.strip()
        message = message.strip()
        label = "ALERT" if level == "ERROR" else "INFO"
        return f"[{label}] {level} level detected: {message}"

    def validation_message(self) -> str:
        return "Log entry verified"


def main() -> None:
    print("=== CODE NEXUS - DATA PROCESSOR FOUNDATION ===")
    print()

    # Create processors once
    numeric = NumericProcessor()
    text = TextProcessor()
    log = LogProcessor()

    processors: List[DataProcessor] = [numeric, text, log]

    data_samples: List[Any] = [
        [1, 2, 3, 4, 5],
        "Hello Nexus World",
        "ERROR: Connection timeout"
    ]

    # Scripted demo (one processor – one matching data)
    for processor, data in zip(processors, data_samples):
        name = processor.__class__.__name__.replace("Processor", " Processor")

        print(f"Initializing {name}...")
        print(f"Processing data: {data!r}")

        try:
            if not processor.validate(data):
                raise ValueError("Invalid data for processor")

            print(f"Validation: {processor.validation_message()}")
            result = processor.process(data)
            print(processor.format_output(result))

        except ValueError as error:
            print(f"Validation error: {error}")

        print()

    # Polymorphic demo
    print("=== Polymorphic Processing Demo ===")
    print("Processing multiple data types through same interface...")

    mixed_samples: List[Any] = [
        [1, 2, 3],
        "Hello Nexus",
        "INFO: System ready",
    ]

    for i, (processor, sample) in enumerate(
        zip(processors, mixed_samples),
        start=1
    ):
        result = processor.process(sample)
        print(f"Result {i}: {result}")
    print()

    print("Foundation systems online. Nexus ready for advanced streams.")


if __name__ == "__main__":
    main()

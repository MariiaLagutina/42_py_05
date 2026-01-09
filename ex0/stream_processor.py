#!/usr/bin/env python3

from abc import ABC, abstractmethod
from typing import Any, List, Union


class DataProcessor(ABC):
    """Abstract base class for data processors."""
    @abstractmethod
    def validate(self, data: Any) -> bool:
        """Validate the input data."""
        pass

    @abstractmethod
    def process(self, data: Any) -> str:
        """Process the input data and return a result string."""
        pass

    def format_output(self, result: str) -> str:
        """Format the output result."""
        return f"Output: {result}"


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


def main() -> None:
    """Main function to demonstrate data processors."""
    print("=== CODE NEXUS - DATA PROCESSOR FOUNDATION ===")
    print()

    # Numeric Processor
    numeric = NumericProcessor()
    data = [1, 2, 3, 4, 5]
    print("Initializing Numeric Processor...")
    print(f"Processing data: {data}")
    print("Validation: Numeric data verified")
    print(numeric.format_output(numeric.process(data)))
    print()

    # Text Processor
    text = TextProcessor()
    data = "Hello Nexus World"
    print("Initializing Text Processor...")
    print(f"Processing data: \"{data}\"")
    print("Validation: Text data verified")
    print(text.format_output(text.process(data)))
    print()

    # Log Processor
    log = LogProcessor()
    data = "ERROR: Connection timeout"
    print("Initializing Log Processor...")
    print(f"Processing data: \"{data}\"")
    print("Validation: Log entry verified")
    print(log.format_output(log.process(data)))
    print()

    # Polymorphic demo
    print("=== Polymorphic Processing Demo ===")
    print("Processing multiple data types through same interface...")

    processors: List[DataProcessor] = [numeric, text, log]
    samples = [
        [1, 2, 3],
        "Hello Nexus",
        "INFO: System ready",
    ]

    for i, (proc, sample) in enumerate(zip(processors, samples), start=1):
        result = proc.process(sample)
        print(f"Result {i}: {result}")
    print()

    print("Foundation systems online. Nexus ready for advanced streams.")


if __name__ == "__main__":
    main()

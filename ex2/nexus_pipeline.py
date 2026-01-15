#!/usr/bin/env python3

from abc import ABC, abstractmethod
from typing import Any, List, Dict, Union, Optional, Protocol
from typing import runtime_checkable


@runtime_checkable
class ProcessingStage(Protocol):
    def process(self, data: Any) -> Any:
        ...


class InputStage:
    def process(self, data: Any) -> Any:
        print("InputStage received:", data)
        return data


class TransformStage:
    def process(self, data: Any) -> Any:
        print("TransformStage processing:", data)
        return f"{data} (transformed)"


class OutputStage:
    def process(self, data: Any) -> Any:
        print("OutputStage output:", data)
        return f"Result: {data}"


class ProcessingPipeline(ABC):
    def __init__(self) -> None:
        self.stages: List[ProcessingStage] = []

    def add_stage(self, stage: ProcessingStage) -> None:
        self.stages.append(stage)

    @abstractmethod
    def process(self, data: Any) -> Any:
        result = data
        for stage in self.stages:
            if result is None:
                return None
            result = stage.process(result)
        return result


class JSONAdapter(ProcessingPipeline):
    def process(self, data: Any) -> Any:
        return super().process(data)


class CSVAdapter(ProcessingPipeline):
    def process(self, data: Any) -> Union[str, Any]:
        return super().process(data)


class StreamAdapter(ProcessingPipeline):
    def process(self, data: Any) -> Union[str, Any]:
        return super().process(data)


class NexusManager:
    pass


def main() -> None:
    pipeline = JSONAdapter()

    pipeline.add_stage(InputStage())
    pipeline.add_stage(TransformStage())
    pipeline.add_stage(OutputStage())

    result = pipeline.process("Hello Nexus")
    print("\nFinal result:", result)


if __name__ == "__main__":
    main()

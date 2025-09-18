"""Strategy type definitions for the data parser service.

This module defines the abstract base classes and protocols for different
strategy types used in the data parser service, providing proper type
annotations instead of using generic Callable types.
"""

from abc import ABC, abstractmethod
from typing import Any, IO, AsyncGenerator, BinaryIO

from .jsonl_stream import JSONLStream


class FileSortStrategyBase(ABC):
    """Abstract base class for file sorting strategies.

    Implementations receive a list of (path, mtime) tuples and return a sorted list.
    """

    @abstractmethod
    def sort(
        self, items: list[tuple[str, float | int | None]]
    ) -> list[tuple[str, float | int | None]]:
        """Return a sorted list of (path, mtime) tuples according to strategy."""


class ResourceParserStrategy(ABC):
    """Abstract base class for resource parser strategies.

    Resource parsers convert raw resources (files) into JSONL format.
    """

    @abstractmethod
    def parse(
        self,
        resource_name: str,
        write_stream: IO[str],
    ) -> int:
        """Convert the input resource to JSONL and write to write_stream.

        Args:
            resource_name: Identifier for the raw input (e.g., file path, object storage key).
            write_stream: A text stream to receive JSONL output. The parser writes one JSON object per line.

        Returns:
            The number of JSON records written.
        """

    @abstractmethod
    async def parse_stream(
        self,
        async_input_stream: AsyncGenerator[str, None],
        jsonl_stream: JSONLStream,
    ) -> AsyncGenerator[int, None]:
        """Stream parse from async input stream to jsonl_stream, yielding progress updates.

        Args:
            async_input_stream: Async generator yielding text lines.
            jsonl_stream: JSONLStream helper for writing records.

        Yields:
            Number of records written so far (for progress tracking).
        """


# Strategy configuration types (for YAML config)
ResourceParserStrategyConfig = dict[
    str, Any
]  # Configuration dict for resource parser strategies

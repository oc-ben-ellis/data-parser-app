"""JSONL stream helper for convenient record writing."""

import json
from typing import Any, TextIO


class JSONLStream:
    """Helper class for writing JSONL records with convenient dict serialization."""

    def __init__(self, output_stream: TextIO):
        self.output_stream = output_stream
        self.records_written = 0

    def write_record(self, record: dict[str, Any]) -> None:
        """Write a dictionary as a JSON line to the stream.

        Args:
            record: Dictionary to serialize as JSON and write to stream
        """
        json.dump(record, self.output_stream, ensure_ascii=False)
        self.output_stream.write("\n")
        self.records_written += 1

    def write_records(self, records: list[dict[str, Any]]) -> None:
        """Write multiple records to the stream.

        Args:
            records: List of dictionaries to serialize and write
        """
        for record in records:
            self.write_record(record)

    def get_records_written(self) -> int:
        """Get the total number of records written.

        Returns:
            Number of records written to the stream
        """
        return self.records_written

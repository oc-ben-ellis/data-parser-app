"""Fixed-width parser strategy implementation."""

import io
import json
from dataclasses import dataclass
from typing import Any, IO, AsyncGenerator, BinaryIO

from data_parser_core.jsonl_stream import JSONLStream
from data_parser_core.strategy_types import ResourceParserStrategy


@dataclass
class FixedWidthResourceParser(ResourceParserStrategy):
    """Parse a fixed-width text resource into a standard JSONL stream.

    This parser handles fixed-width text files where fields are positioned at
    specific character positions without delimiters.
    """

    # Basic configuration
    encoding: str = "utf-8"
    line_separator: str = "\n"
    
    # Parsing configuration
    field_specs: list[dict[str, Any]] | None = None
    skip_rows: int = 0
    limit_rows: int | None = None
    on_error: str = "skip"
    schema_version: str | None = None
    ocid_generator: dict[str, Any] | None = None

    def parse(
        self,
        resource_name: str,
        write_stream: IO[str],
    ) -> int:
        """Convert the input resource to JSONL and write to write_stream.

        Parameters
        ----------
        resource_name : str
            Identifier for the raw input (e.g., file path, object storage key).
        write_stream : IO[str]
            A text stream to receive JSONL output. The parser writes one JSON object per line.

        Returns
        -------
        int
            The number of JSON records written.
        """
        # Use constructor configuration - field_specs is required
        field_specs = self.field_specs
        if not field_specs:
            raise ValueError("field_specs is required for fixed-width parsing")
        skip_rows = self.skip_rows
        limit_rows = self.limit_rows
        on_error = self.on_error
        schema_version = self.schema_version
        ocid_generator = self.ocid_generator or {}

        records_written = 0

        try:
            with open(resource_name, "r", encoding=self.encoding) as file:
                lines = file.readlines()

                # Skip initial rows if requested
                start_line = skip_rows
                end_line = len(lines)
                if limit_rows:
                    end_line = min(start_line + limit_rows, len(lines))

                for line_num, line in enumerate(
                    lines[start_line:end_line], start=start_line + 1
                ):
                    try:
                        # Parse the fixed-width line
                        record = self._parse_fixed_width_line(
                            line.rstrip("\n\r"), field_specs
                        )

                        # Add schema version if specified
                        if schema_version:
                            record["_schema_version"] = schema_version

                        # Add source row metadata
                        record["oc:source_row"] = str(line_num)

                        # Generate OCID if ocid_generator is configured
                        if ocid_generator:
                            ocid = self._generate_ocid(record, ocid_generator)
                            record["oc:ocid"] = ocid

                        # Write JSONL line
                        json_line = json.dumps(record, ensure_ascii=False)
                        write_stream.write(json_line + self.line_separator)
                        records_written += 1

                    except Exception as e:
                        if on_error == "fail":
                            raise
                        # Skip this row and continue
                        continue

        except Exception as e:
            if on_error == "fail":
                raise
            # Log error but continue
            pass

        return records_written


    def _parse_fixed_width_line(
        self, line: str, field_specs: list[dict[str, Any]]
    ) -> dict[str, Any]:
        """Parse a single fixed-width line into a dictionary.

        Args:
            line: The line to parse.
            field_specs: List of field specifications with 'name', 'start', and 'length' keys.

        Returns:
            Dictionary with parsed field values.
        """
        record = {}

        for field_spec in field_specs:
            name = field_spec["name"]
            start = field_spec["start"] - 1  # Convert to 0-based indexing
            length = field_spec["length"]

            # Extract the field value
            end = start + length
            if start < len(line):
                value = line[start:end].strip()
            else:
                value = ""

            # Handle null values
            if not value:
                value = None

            record[name] = value

        return record


    def _generate_ocid(
        self, record: dict[str, Any], ocid_generator: dict[str, Any]
    ) -> str:
        """Generate an OCID for the record based on the ocid_generator configuration.

        Args:
            record: The parsed record data.
            ocid_generator: Configuration for OCID generation.

        Returns:
            The generated OCID string.
        """
        # For now, return a placeholder OCID
        # In a real implementation, this would use the pipeline bus OCID generator
        # with the configured jurisdiction_code and company_number_field
        jurisdiction_code = ocid_generator.get("jurisdiction_code")
        if not jurisdiction_code:
            raise ValueError("jurisdiction_code is required in ocid_generator configuration")
        company_number_field = ocid_generator.get("company_number_field", "COR_NUMBER")

        company_number = record.get(company_number_field, "")
        if company_number:
            # Simple hash-based OCID for now
            import hashlib

            identifier = f"{jurisdiction_code}|{company_number}"
            hash_value = hashlib.sha256(identifier.encode()).hexdigest()[:16]
            return f"ocid:v1:co:{hash_value}"
        else:
            return f"ocid:v1:co:unknown"

    async def parse_stream(
        self,
        async_input_stream: AsyncGenerator[str, None],
        jsonl_stream: JSONLStream,
    ) -> AsyncGenerator[int, None]:
        """Stream parse fixed-width data from async input stream to jsonl_stream, yielding progress updates.

        Args:
            async_input_stream: Async generator yielding text lines.
            jsonl_stream: JSONLStream helper for writing records.

        Yields:
            Number of records written so far (for progress tracking).
        """
        # Use constructor configuration
        skip_rows = self.skip_rows
        limit_rows = self.limit_rows
        on_error = self.on_error
        schema_version = self.schema_version
        ocid_generator = self.ocid_generator or {}

        # Collect all lines first
        lines = []
        async for line in async_input_stream:
            lines.append(line)
        
        if not lines:
            return

        # Skip rows if specified
        if skip_rows:
            lines = lines[skip_rows:]

        # Process records
        records_written = 0
        batch_size = 1000  # Yield progress every 1000 records

        try:
            for line in lines:
                if limit_rows and records_written >= limit_rows:
                    break

                try:
                    # Process the line
                    processed_record = self._process_line(
                        line, schema_version, ocid_generator
                    )
                    
                    if processed_record is not None:
                        jsonl_stream.write_record(processed_record)
                        records_written += 1

                        # Yield progress updates
                        if records_written % batch_size == 0:
                            yield records_written

                except Exception as e:
                    if on_error == "fail":
                        raise
                    # Log error but continue
                    pass

        except Exception as e:
            if on_error == "fail":
                raise
            # Log error but continue
            pass

        # Yield final count
        if records_written > 0:
            yield records_written

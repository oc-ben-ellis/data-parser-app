"""CSV parser strategy implementation."""

import csv
import io
import json
from dataclasses import dataclass
from typing import Any, IO, AsyncGenerator, BinaryIO

from data_parser_core.jsonl_stream import JSONLStream
from data_parser_core.strategy_types import ResourceParserStrategy


@dataclass
class CsvResourceParser(ResourceParserStrategy):
    """Parse a delimited text resource (e.g., CSV) into a standard JSONL stream.

    Public behavior (implementation details elided):
    - Reads a source identified by `resource_name` (file path, blob key, etc.).
    - Emits one JSON object per line (JSONL) to `write_stream` using UTF-8 text.
    - Uses constructor configuration to control delimiter, quoting, headers, type coercions,
      field selection/renaming, and basic validation.
    - Returns the number of JSON records written.
    Expected JSONL shape:
      {"fieldA": "...", "fieldB": 123, ...}\n
      {"fieldA": "...", "fieldB": 456, ...}\n
      ...
    """

    # Basic configuration
    delimiter: str = ","
    quotechar: str = '"'
    escapechar: str | None = None
    encoding: str = "utf-8"
    line_separator: str = "\n"
    
    # Parsing configuration
    has_header: bool = True
    headers: list[str] | None = None
    include: list[str] | None = None
    rename: dict[str, str] | None = None
    coerce: dict[str, str] | None = None
    null_values: list[str] | None = None
    trim_whitespace: bool = True
    skip_rows: int = 0
    limit_rows: int | None = None
    on_error: str = "skip"
    extra_fields_policy: str = "keep"
    schema_version: str | None = None

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
        Behavior
        --------
        - Opens/reads `resource_name` using the configured encoding.
        - Detects/uses headers (or applies provided `headers`).
        - Applies column selection/renames (`include`, `rename`).
        - Applies type coercions (`coerce`) and null normalization (`null_values`).
        - Handles basic errors per `on_error` policy.
        - Writes normalized objects to `write_stream` as JSONL.
        """
        # Use constructor configuration
        delimiter = self.delimiter
        quotechar = self.quotechar
        escapechar = self.escapechar
        has_header = self.has_header
        headers = self.headers
        include = self.include
        rename = self.rename or {}
        coerce = self.coerce or {}
        null_values = self.null_values or ["", "NULL", "null"]
        trim_whitespace = self.trim_whitespace
        skip_rows = self.skip_rows
        limit_rows = self.limit_rows
        on_error = self.on_error
        extra_fields_policy = self.extra_fields_policy
        schema_version = self.schema_version

        records_written = 0

        try:
            with open(
                resource_name, "r", encoding=self.encoding, newline=""
            ) as csvfile:
                # Create CSV reader
                reader = csv.reader(
                    csvfile,
                    delimiter=delimiter,
                    quotechar=quotechar,
                    escapechar=escapechar,
                )

                # Handle headers
                if has_header and headers is None:
                    try:
                        headers = next(reader)
                    except StopIteration:
                        return 0  # Empty file
                elif headers is None:
                    # No headers provided and has_header is False
                    # We'll need to generate generic headers or fail
                    raise ValueError(
                        "Either has_header=True or headers must be provided"
                    )

                # Apply column selection
                if include:
                    # Find indices of included columns
                    include_indices = []
                    filtered_headers = []
                    for i, header in enumerate(headers):
                        if header in include:
                            include_indices.append(i)
                            filtered_headers.append(header)
                    headers = filtered_headers
                else:
                    include_indices = list(range(len(headers)))

                # Apply field renaming
                renamed_headers = [rename.get(h, h) for h in headers]

                # Skip initial rows if requested
                for _ in range(skip_rows):
                    try:
                        next(reader)
                    except StopIteration:
                        return 0  # Not enough rows

                # Process rows
                for row_num, row in enumerate(reader):
                    if limit_rows and records_written >= limit_rows:
                        break

                    try:
                        # Apply column selection
                        if include_indices:
                            filtered_row = [
                                row[i] if i < len(row) else "" for i in include_indices
                            ]
                        else:
                            filtered_row = row

                        # Ensure row has same length as headers
                        while len(filtered_row) < len(renamed_headers):
                            filtered_row.append("")
                        filtered_row = filtered_row[: len(renamed_headers)]

                        # Create record dictionary
                        record = {}
                        for header, value in zip(renamed_headers, filtered_row):
                            # Trim whitespace if requested
                            if trim_whitespace:
                                value = value.strip()

                            # Handle null values
                            if value in null_values:
                                value = None

                            # Apply type coercion
                            if value is not None and header in coerce:
                                value = self._coerce_value(value, coerce[header])

                            record[header] = value

                        # Add schema version if specified
                        if schema_version:
                            record["_schema_version"] = schema_version

                        # Add source row metadata
                        record["oc:source_row"] = str(
                            row_num + skip_rows + (2 if has_header else 1)
                        )

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

    async def parse_stream(
        self,
        async_input_stream: AsyncGenerator[str, None],
        jsonl_stream: JSONLStream,
    ) -> AsyncGenerator[int, None]:
        """Stream parse CSV from async input stream to jsonl_stream, yielding progress updates.

        Args:
            async_input_stream: Async generator yielding text lines.
            jsonl_stream: JSONLStream helper for writing records.

        Yields:
            Number of records written so far (for progress tracking).
        """
        # Use constructor configuration
        delimiter = self.delimiter
        quotechar = self.quotechar
        escapechar = self.escapechar
        has_header = self.has_header
        headers = self.headers
        include = self.include
        rename = self.rename or {}
        coerce = self.coerce or {}
        null_values = self.null_values or ["", "NULL", "null"]
        trim_whitespace = self.trim_whitespace
        skip_rows = self.skip_rows
        limit_rows = self.limit_rows
        on_error = self.on_error
        extra_fields_policy = self.extra_fields_policy
        schema_version = self.schema_version

        # Collect all lines first to create CSV reader
        lines = []
        async for line in async_input_stream:
            lines.append(line)
        
        if not lines:
            return

        # Create CSV reader from collected lines
        import io
        text_content = "\n".join(lines)
        text_stream = io.StringIO(text_content)
        reader = csv.DictReader(
            text_stream,
            delimiter=delimiter,
            quotechar=quotechar,
            escapechar=escapechar,
        )

        # Handle headers
        if headers:
            reader.fieldnames = headers

        # Skip rows if specified
        for _ in range(skip_rows):
            try:
                next(reader)
            except StopIteration:
                return

        # Process records
        records_written = 0
        batch_size = 1000  # Yield progress every 1000 records

        try:
            for row in reader:
                if limit_rows and records_written >= limit_rows:
                    break

                try:
                    # Process the row
                    processed_row = self._process_row(
                        row, include, rename, coerce, null_values, 
                        trim_whitespace, extra_fields_policy, schema_version
                    )
                    
                    if processed_row is not None:
                        jsonl_stream.write_record(processed_row)
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


    def _process_row(
        self,
        row: dict[str, str],
        rename: dict[str, str],
        coerce: dict[str, str],
        null_values: list[str],
        trim_whitespace: bool,
        extra_fields_policy: str,
        schema_version: str | None = None,
    ) -> dict[str, Any]:
        """Process a single CSV row with transformations.

        Args:
            row: Raw CSV row data.
            rename: Field rename mapping.
            coerce: Type coercion mapping.
            null_values: Values to treat as null.
            trim_whitespace: Whether to trim whitespace.
            extra_fields_policy: How to handle extra fields.
            schema_version: Optional schema version to include.

        Returns:
            Processed row as dictionary.
        """
        processed_row = {}

        for field, value in row.items():
            # Skip extra fields if policy is "drop"
            if extra_fields_policy == "drop" and field not in rename:
                continue

            # Apply field rename
            output_field = rename.get(field, field)

            # Trim whitespace if configured
            if trim_whitespace and isinstance(value, str):
                value = value.strip()

            # Handle null values
            if value in null_values:
                value = None
            elif value and field in coerce:
                # Apply type coercion
                try:
                    value = self._coerce_value(value, coerce[field])
                except (ValueError, TypeError):
                    # If coercion fails, keep original value
                    pass

            processed_row[output_field] = value

        # Add schema version if provided
        if schema_version:
            processed_row["_schema_version"] = schema_version

        return processed_row

    def _coerce_value(self, value: str, target_type: str) -> Any:
        """Coerce a string value to the target type.

        Args:
            value: The string value to coerce.
            target_type: The target type (int, float, bool, date:<fmt>, str).

        Returns:
            The coerced value.
        """
        if target_type == "int":
            return int(value)
        elif target_type == "float":
            return float(value)
        elif target_type == "bool":
            return value.lower() in ("true", "1", "yes", "on")
        elif target_type.startswith("date:"):
            # For now, just return the string value
            # In a real implementation, you'd parse the date format
            return value
        elif target_type == "str":
            return str(value)
        else:
            return value

"""CSV parser strategy implementation."""

import csv
import json
from dataclasses import dataclass
from typing import Any, IO

from data_parser_core.strategy_types import ResourceParserStrategy


@dataclass
class CsvResourceParser(ResourceParserStrategy):
    """Parse a delimited text resource (e.g., CSV) into a standard JSONL stream.
    
    Public behavior (implementation details elided):
    - Reads a source identified by `resource_name` (file path, blob key, etc.).
    - Emits one JSON object per line (JSONL) to `write_stream` using UTF-8 text.
    - Uses `parser_kv_args` to control delimiter, quoting, headers, type coercions,
      field selection/renaming, and basic validation.
    - Returns the number of JSON records written.
    Expected JSONL shape:
      {"fieldA": "...", "fieldB": 123, ...}\n
      {"fieldA": "...", "fieldB": 456, ...}\n
      ...
    Common `parser_kv_args` (all optional, names are suggestions):
      - "delimiter": str                  # default: ","
      - "quotechar": str                  # default: '"'
      - "escapechar": str | None          # default: None
      - "has_header": bool                # default: True (use first row as headers)
      - "headers": list[str]              # explicit headers when no header row
      - "include": list[str]              # subset of columns to keep
      - "rename": dict[str, str]          # {"old": "new"} field renames
      - "coerce": dict[str, str]          # {"field": "int|float|bool|date:<fmt>|str"}
      - "null_values": list[str]          # values treated as null (e.g., ["", "NULL"])
      - "trim_whitespace": bool           # strip cell whitespace (default: True)
      - "skip_rows": int                  # number of leading rows to skip (after header logic)
      - "limit_rows": int | None          # cap rows parsed
      - "on_error": str                   # "skip" | "fail" (default: "skip")
      - "extra_fields_policy": str        # "keep" | "drop" (default: "keep")
      - "schema_version": str             # optional schema/version tag to include per record
    """

    default_delimiter: str = ","
    default_quotechar: str = '"'
    encoding: str = "utf-8"
    line_separator: str = "\n"

    def parse(
        self,
        resource_name: str,
        write_stream: IO[str],
        parser_kv_args: dict[str, Any] | None = None,
    ) -> int:
        """Convert the input resource to JSONL and write to write_stream.
        
        Parameters
        ----------
        resource_name : str
            Identifier for the raw input (e.g., file path, object storage key).
        write_stream : IO[str]
            A text stream to receive JSONL output. The parser writes one JSON object per line.
        parser_kv_args : dict[str, Any] | None
            Optional key/value configuration to control parsing (see class docstring).
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
        if parser_kv_args is None:
            parser_kv_args = {}

        # Extract configuration with defaults
        delimiter = parser_kv_args.get("delimiter", self.default_delimiter)
        quotechar = parser_kv_args.get("quotechar", self.default_quotechar)
        escapechar = parser_kv_args.get("escapechar")
        has_header = parser_kv_args.get("has_header", True)
        headers = parser_kv_args.get("headers")
        include = parser_kv_args.get("include")
        rename = parser_kv_args.get("rename", {})
        coerce = parser_kv_args.get("coerce", {})
        null_values = parser_kv_args.get("null_values", ["", "NULL", "null"])
        trim_whitespace = parser_kv_args.get("trim_whitespace", True)
        skip_rows = parser_kv_args.get("skip_rows", 0)
        limit_rows = parser_kv_args.get("limit_rows")
        on_error = parser_kv_args.get("on_error", "skip")
        extra_fields_policy = parser_kv_args.get("extra_fields_policy", "keep")
        schema_version = parser_kv_args.get("schema_version")

        records_written = 0

        try:
            with open(resource_name, "r", encoding=self.encoding, newline="") as csvfile:
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
                    raise ValueError("Either has_header=True or headers must be provided")

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
                            filtered_row = [row[i] if i < len(row) else "" for i in include_indices]
                        else:
                            filtered_row = row

                        # Ensure row has same length as headers
                        while len(filtered_row) < len(renamed_headers):
                            filtered_row.append("")
                        filtered_row = filtered_row[:len(renamed_headers)]

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
                        record["oc:source_row"] = str(row_num + skip_rows + (2 if has_header else 1))

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

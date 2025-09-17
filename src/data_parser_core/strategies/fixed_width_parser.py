"""Fixed-width parser strategy implementation for US_FL data format."""

import json
from dataclasses import dataclass
from typing import Any, IO

from data_parser_core.strategy_types import ResourceParserStrategy


@dataclass
class FixedWidthResourceParser(ResourceParserStrategy):
    """Parse a fixed-width text resource into a standard JSONL stream.
    
    This parser is specifically designed for US_FL data format which uses
    fixed-width fields without delimiters.
    """

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
            Optional key/value configuration to control parsing.
            
        Returns
        -------
        int
            The number of JSON records written.
        """
        if parser_kv_args is None:
            parser_kv_args = {}

        # Extract configuration with defaults
        field_specs = parser_kv_args.get("field_specs", self._get_default_us_fl_field_specs())
        skip_rows = parser_kv_args.get("skip_rows", 0)
        limit_rows = parser_kv_args.get("limit_rows")
        on_error = parser_kv_args.get("on_error", "skip")
        schema_version = parser_kv_args.get("schema_version")
        ocid_generator = parser_kv_args.get("ocid_generator", {})

        records_written = 0

        try:
            with open(resource_name, "r", encoding=self.encoding) as file:
                lines = file.readlines()

                # Skip initial rows if requested
                start_line = skip_rows
                end_line = len(lines)
                if limit_rows:
                    end_line = min(start_line + limit_rows, len(lines))

                for line_num, line in enumerate(lines[start_line:end_line], start=start_line + 1):
                    try:
                        # Parse the fixed-width line
                        record = self._parse_fixed_width_line(line.rstrip('\n\r'), field_specs)
                        
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

    def _parse_fixed_width_line(self, line: str, field_specs: list[dict[str, Any]]) -> dict[str, Any]:
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

    def _get_default_us_fl_field_specs(self) -> list[dict[str, Any]]:
        """Get the default field specifications for US_FL data format.
        
        Based on the sample data analysis, this defines the field positions and lengths
        for the US_FL fixed-width format.
        
        Returns:
            List of field specifications.
        """
        return [
            {"name": "COR_NUMBER", "start": 1, "length": 12},      # Company number (L25000418660)
            {"name": "COR_NAME", "start": 13, "length": 120},      # Company name (NEW BLESSING LOGISTICS LLC...)
            {"name": "COR_STATUS", "start": 204, "length": 6},     # Status (AFLAL) - found at position 204
            {"name": "COR_FILING_TYPE", "start": 210, "length": 6}, # Filing type (empty in sample)
            {"name": "COR_FILE_DATE", "start": 216, "length": 8},  # File date (MMDDYYYY)
            {"name": "COR_PRINC_ADD_1", "start": 220, "length": 50}, # Principal address line 1 (5645 RUSTIC THORN RD)
            {"name": "COR_PRINC_ADD_2", "start": 270, "length": 50}, # Principal address line 2
            {"name": "COR_PRINC_CITY", "start": 304, "length": 30},  # Principal city (JACKSONVILLE)
            {"name": "COR_PRINC_ZIP", "start": 334, "length": 10},   # Principal ZIP (32219)
            {"name": "COR_MAIL_ADD_1", "start": 344, "length": 50},  # Mailing address line 1
            {"name": "COR_MAIL_ADD_2", "start": 394, "length": 50},  # Mailing address line 2
            {"name": "COR_MAIL_CITY", "start": 444, "length": 30},   # Mailing city
            {"name": "COR_MAIL_STATE", "start": 474, "length": 2},   # Mailing state
            {"name": "COR_MAIL_ZIP", "start": 476, "length": 10},    # Mailing ZIP
            {"name": "COR_MAIL_COUNTRY", "start": 486, "length": 2}, # Mailing country
            {"name": "RA_NAME", "start": 488, "length": 50},         # Registered agent name
            {"name": "RA_NAME_TYPE", "start": 538, "length": 1},     # Registered agent name type
            {"name": "RA_ADD_1", "start": 539, "length": 50},        # Registered agent address
            {"name": "RA_CITY", "start": 589, "length": 30},         # Registered agent city
            {"name": "RA_STATE", "start": 619, "length": 2},         # Registered agent state
            {"name": "RA_ZIP5", "start": 621, "length": 5},          # Registered agent ZIP
            # Principal 1-6 fields (officers) - starting around position 544 for ROMAN SANTOS
            {"name": "PRINC1_TITLE", "start": 544, "length": 4},     # Principal 1 title
            {"name": "PRINC1_NAME_TYPE", "start": 548, "length": 1}, # Principal 1 name type
            {"name": "PRINC1_NAME", "start": 549, "length": 46},     # Principal 1 name (ROMAN SANTOS)
            {"name": "PRINC1_ADD_1", "start": 595, "length": 50},    # Principal 1 address
            {"name": "PRINC1_CITY", "start": 645, "length": 30},     # Principal 1 city
            {"name": "PRINC1_STATE", "start": 675, "length": 2},     # Principal 1 state
            {"name": "PRINC1_ZIP5", "start": 677, "length": 5},      # Principal 1 ZIP
            {"name": "PRINC2_TITLE", "start": 682, "length": 4},     # Principal 2 title
            {"name": "PRINC2_NAME_TYPE", "start": 686, "length": 1}, # Principal 2 name type
            {"name": "PRINC2_NAME", "start": 687, "length": 46},     # Principal 2 name
            {"name": "PRINC2_ADD_1", "start": 733, "length": 50},    # Principal 2 address
            {"name": "PRINC2_CITY", "start": 783, "length": 30},     # Principal 2 city
            {"name": "PRINC2_STATE", "start": 813, "length": 2},     # Principal 2 state
            {"name": "PRINC2_ZIP5", "start": 815, "length": 5},      # Principal 2 ZIP
            {"name": "PRINC3_TITLE", "start": 820, "length": 4},     # Principal 3 title
            {"name": "PRINC3_NAME_TYPE", "start": 824, "length": 1}, # Principal 3 name type
            {"name": "PRINC3_NAME", "start": 825, "length": 46},     # Principal 3 name
            {"name": "PRINC3_ADD_1", "start": 871, "length": 50},    # Principal 3 address
            {"name": "PRINC3_CITY", "start": 921, "length": 30},     # Principal 3 city
            {"name": "PRINC3_STATE", "start": 951, "length": 2},     # Principal 3 state
            {"name": "PRINC3_ZIP5", "start": 953, "length": 5},      # Principal 3 ZIP
            {"name": "PRINC4_TITLE", "start": 958, "length": 4},     # Principal 4 title
            {"name": "PRINC4_NAME_TYPE", "start": 962, "length": 1}, # Principal 4 name type
            {"name": "PRINC4_NAME", "start": 963, "length": 46},     # Principal 4 name
            {"name": "PRINC4_ADD_1", "start": 1009, "length": 50},   # Principal 4 address
            {"name": "PRINC4_CITY", "start": 1059, "length": 30},    # Principal 4 city
            {"name": "PRINC4_STATE", "start": 1089, "length": 2},    # Principal 4 state
            {"name": "PRINC4_ZIP5", "start": 1091, "length": 5},     # Principal 4 ZIP
            {"name": "PRINC5_TITLE", "start": 1096, "length": 4},    # Principal 5 title
            {"name": "PRINC5_NAME_TYPE", "start": 1100, "length": 1}, # Principal 5 name type
            {"name": "PRINC5_NAME", "start": 1101, "length": 46},    # Principal 5 name
            {"name": "PRINC5_ADD_1", "start": 1147, "length": 50},   # Principal 5 address
            {"name": "PRINC5_CITY", "start": 1197, "length": 30},    # Principal 5 city
            {"name": "PRINC5_STATE", "start": 1227, "length": 2},    # Principal 5 state
            {"name": "PRINC5_ZIP5", "start": 1229, "length": 5},     # Principal 5 ZIP
            {"name": "PRINC6_TITLE", "start": 1234, "length": 4},    # Principal 6 title
            {"name": "PRINC6_NAME_TYPE", "start": 1238, "length": 1}, # Principal 6 name type
            {"name": "PRINC6_NAME", "start": 1239, "length": 46},    # Principal 6 name
            {"name": "PRINC6_ADD_1", "start": 1285, "length": 50},   # Principal 6 address
            {"name": "PRINC6_CITY", "start": 1335, "length": 30},    # Principal 6 city
            {"name": "PRINC6_STATE", "start": 1365, "length": 2},    # Principal 6 state
            {"name": "PRINC6_ZIP5", "start": 1367, "length": 5},     # Principal 6 ZIP
            {"name": "STATE_COUNTRY", "start": 1372, "length": 2},   # State/Country
            {"name": "RetrievedAt", "start": 1374, "length": 10},    # Retrieved at timestamp
        ]

    def _generate_ocid(self, record: dict[str, Any], ocid_generator: dict[str, Any]) -> str:
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
        jurisdiction_code = ocid_generator.get("jurisdiction_code", "us_fl")
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

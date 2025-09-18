"""Tests for parser execution engine.

This module contains unit tests for the parser execution engine,
including async streaming functionality and work queue processing.
"""

import asyncio
import io
import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from data_parser_core.jsonl_stream import JSONLStream
from data_parser_core.parser import process_resource_worker, run_parser
from data_parser_core.strategies.csv_parser import CsvResourceParser
from data_parser_core.strategies.fixed_width_parser import FixedWidthResourceParser


class TestJSONLStream:
    """Test JSONLStream helper class."""

    def test_write_record(self) -> None:
        """Test writing a single record."""
        output = io.StringIO()
        jsonl_stream = JSONLStream(output)

        record = {"name": "test", "value": 123}
        jsonl_stream.write_record(record)

        expected = '{"name": "test", "value": 123}\n'
        assert output.getvalue() == expected
        assert jsonl_stream.get_records_written() == 1

    def test_write_multiple_records(self) -> None:
        """Test writing multiple records."""
        output = io.StringIO()
        jsonl_stream = JSONLStream(output)

        records = [
            {"name": "test1", "value": 123},
            {"name": "test2", "value": 456},
        ]
        jsonl_stream.write_records(records)

        expected = '{"name": "test1", "value": 123}\n{"name": "test2", "value": 456}\n'
        assert output.getvalue() == expected
        assert jsonl_stream.get_records_written() == 2

    def test_records_written_counter(self) -> None:
        """Test that records written counter is accurate."""
        output = io.StringIO()
        jsonl_stream = JSONLStream(output)

        assert jsonl_stream.get_records_written() == 0

        jsonl_stream.write_record({"test": "value"})
        assert jsonl_stream.get_records_written() == 1

        jsonl_stream.write_record({"test2": "value2"})
        assert jsonl_stream.get_records_written() == 2


class TestCsvResourceParserAsync:
    """Test CSV parser async streaming functionality."""

    @pytest.mark.asyncio
    async def test_parse_stream_basic(self) -> None:
        """Test basic async streaming CSV parsing."""
        # Create test CSV data
        csv_data = "name,age,city\nJohn,30,NYC\nJane,25,LA"
        input_stream = io.BytesIO(csv_data.encode("utf-8"))
        output = io.StringIO()
        jsonl_stream = JSONLStream(output)

        parser = CsvResourceParser()

        # Collect progress updates
        progress_updates = []
        async for records_written in parser.parse_stream(input_stream, jsonl_stream):
            progress_updates.append(records_written)

        # Verify output
        lines = output.getvalue().strip().split("\n")
        assert len(lines) == 2

        # Parse first record
        record1 = json.loads(lines[0])
        assert record1["name"] == "John"
        assert record1["age"] == "30"
        assert record1["city"] == "NYC"

        # Parse second record
        record2 = json.loads(lines[1])
        assert record2["name"] == "Jane"
        assert record2["age"] == "25"
        assert record2["city"] == "LA"

        # Verify progress updates
        assert len(progress_updates) == 1  # Should yield once at the end
        assert progress_updates[0] == 2

    @pytest.mark.asyncio
    async def test_parse_stream_with_config(self) -> None:
        """Test async streaming with configuration options."""
        csv_data = "name,age\nJohn,30\nJane,25"
        input_stream = io.BytesIO(csv_data.encode("utf-8"))
        output = io.StringIO()
        jsonl_stream = JSONLStream(output)

        parser = CsvResourceParser()
        config = {
            "rename": {"name": "full_name"},
            "coerce": {"age": "int"},
        }

        async for _ in parser.parse_stream(input_stream, jsonl_stream, config):
            pass

        lines = output.getvalue().strip().split("\n")
        record1 = json.loads(lines[0])
        assert record1["full_name"] == "John"  # Renamed field
        assert record1["age"] == 30  # Coerced to int

    @pytest.mark.asyncio
    async def test_parse_stream_empty_input(self) -> None:
        """Test async streaming with empty input."""
        input_stream = io.BytesIO(b"")
        output = io.StringIO()
        jsonl_stream = JSONLStream(output)

        parser = CsvResourceParser()

        progress_updates = []
        async for records_written in parser.parse_stream(input_stream, jsonl_stream):
            progress_updates.append(records_written)

        assert output.getvalue() == ""
        assert len(progress_updates) == 0


class TestFixedWidthResourceParserAsync:
    """Test Fixed-width parser async streaming functionality."""

    @pytest.mark.asyncio
    async def test_parse_stream_basic(self) -> None:
        """Test basic async streaming fixed-width parsing."""
        # Create test fixed-width data (US_FL format)
        fixed_width_data = (
            "L25000418660NEW BLESSING LOGISTICS LLC"
            + " " * 70  # COR_NUMBER + COR_NAME
            + "AFLAL"
            + " " * 1  # COR_STATUS + COR_FILING_TYPE
            + "01012024"  # COR_FILE_DATE
            + "5645 RUSTIC THORN RD"
            + " " * 31  # COR_PRINC_ADD_1
            + " " * 50  # COR_PRINC_ADD_2
            + "JACKSONVILLE"
            + " " * 19  # COR_PRINC_CITY
            + "32219"
            + " " * 5  # COR_PRINC_ZIP
            + " " * 50  # COR_MAIL_ADD_1
            + " " * 50  # COR_MAIL_ADD_2
            + " " * 30  # COR_MAIL_CITY
            + " " * 2  # COR_MAIL_STATE
            + " " * 10  # COR_MAIL_ZIP
            + " " * 2  # COR_MAIL_COUNTRY
            + "ROMAN SANTOS"
            + " " * 38  # RA_NAME
            + "I"  # RA_NAME_TYPE
            + " " * 50  # RA_ADD_1
            + " " * 30  # RA_CITY
            + " " * 2  # RA_STATE
            + " " * 5  # RA_ZIP5
            + "PRES"
            + "I"
            + "ROMAN SANTOS"
            + " " * 33  # PRINC1 fields
            + " " * 50  # PRINC1_ADD_1
            + " " * 30  # PRINC1_CITY
            + " " * 2  # PRINC1_STATE
            + " " * 5  # PRINC1_ZIP5
            + " " * 4
            + "I"
            + " " * 46  # PRINC2 fields
            + " " * 50  # PRINC2_ADD_1
            + " " * 30  # PRINC2_CITY
            + " " * 2  # PRINC2_STATE
            + " " * 5  # PRINC2_ZIP5
            + " " * 4
            + "I"
            + " " * 46  # PRINC3 fields
            + " " * 50  # PRINC3_ADD_1
            + " " * 30  # PRINC3_CITY
            + " " * 2  # PRINC3_STATE
            + " " * 5  # PRINC3_ZIP5
            + " " * 4
            + "I"
            + " " * 46  # PRINC4 fields
            + " " * 50  # PRINC4_ADD_1
            + " " * 30  # PRINC4_CITY
            + " " * 2  # PRINC4_STATE
            + " " * 5  # PRINC4_ZIP5
            + " " * 4
            + "I"
            + " " * 46  # PRINC5 fields
            + " " * 50  # PRINC5_ADD_1
            + " " * 30  # PRINC5_CITY
            + " " * 2  # PRINC5_STATE
            + " " * 5  # PRINC5_ZIP5
            + " " * 4
            + "I"
            + " " * 46  # PRINC6 fields
            + " " * 50  # PRINC6_ADD_1
            + " " * 30  # PRINC6_CITY
            + " " * 2  # PRINC6_STATE
            + " " * 5  # PRINC6_ZIP5
            + "FL"
            + "20250127"  # STATE_COUNTRY + RetrievedAt
        )

        input_stream = io.BytesIO(fixed_width_data.encode("utf-8"))
        output = io.StringIO()
        jsonl_stream = JSONLStream(output)

        parser = FixedWidthResourceParser()

        # Collect progress updates
        progress_updates = []
        async for records_written in parser.parse_stream(input_stream, jsonl_stream):
            progress_updates.append(records_written)

        # Verify output
        lines = output.getvalue().strip().split("\n")
        assert len(lines) == 1

        # Parse the record
        record = json.loads(lines[0])
        assert record["COR_NUMBER"] == "L25000418660"
        assert record["COR_NAME"] == "NEW BLESSING LOGISTICS LLC"
        assert record["COR_STATUS"] == "AFLAL"
        assert record["COR_FILE_DATE"] == "01012024"
        assert record["COR_PRINC_ADD_1"] == "5645 RUSTIC THORN RD"
        assert record["COR_PRINC_CITY"] == "JACKSONVILLE"
        assert record["COR_PRINC_ZIP"] == "32219"
        assert record["RA_NAME"] == "ROMAN SANTOS"
        assert record["RA_NAME_TYPE"] == "I"
        assert record["PRINC1_TITLE"] == "PRES"
        assert record["PRINC1_NAME_TYPE"] == "I"
        assert record["PRINC1_NAME"] == "ROMAN SANTOS"
        assert record["STATE_COUNTRY"] == "FL"
        assert record["RetrievedAt"] == "20250127"
        assert record["oc:source_row"] == "1"

        # Verify progress updates
        assert len(progress_updates) == 1  # Should yield once at the end
        assert progress_updates[0] == 1

    @pytest.mark.asyncio
    async def test_parse_stream_with_config(self) -> None:
        """Test async streaming with configuration options."""
        # Create minimal test data
        fixed_width_data = "L25000418660" + " " * 200  # Just enough for basic fields

        input_stream = io.BytesIO(fixed_width_data.encode("utf-8"))
        output = io.StringIO()
        jsonl_stream = JSONLStream(output)

        parser = FixedWidthResourceParser()
        config = {
            "schema_version": "v1.0",
            "ocid_generator": {
                "jurisdiction_code": "us_fl",
                "company_number_field": "COR_NUMBER",
            },
        }

        async for _ in parser.parse_stream(input_stream, jsonl_stream, config):
            pass

        lines = output.getvalue().strip().split("\n")
        record = json.loads(lines[0])
        assert record["_schema_version"] == "v1.0"
        assert record["oc:ocid"].startswith("ocid:v1:co:")
        assert record["COR_NUMBER"] == "L25000418660"

    @pytest.mark.asyncio
    async def test_parse_stream_skip_rows(self) -> None:
        """Test async streaming with row skipping."""
        # Create test data with multiple lines
        fixed_width_data = (
            "L25000418660"
            + " " * 200
            + "\n"  # Line 1
            + "L25000418661"
            + " " * 200
            + "\n"  # Line 2
            + "L25000418662"
            + " " * 200  # Line 3
        )

        input_stream = io.BytesIO(fixed_width_data.encode("utf-8"))
        output = io.StringIO()
        jsonl_stream = JSONLStream(output)

        parser = FixedWidthResourceParser()
        config = {"skip_rows": 1}  # Skip first line

        async for _ in parser.parse_stream(input_stream, jsonl_stream, config):
            pass

        lines = output.getvalue().strip().split("\n")
        assert len(lines) == 2  # Should process 2 lines (skip first)

        # Check that we got the correct lines
        record1 = json.loads(lines[0])
        record2 = json.loads(lines[1])
        assert record1["COR_NUMBER"] == "L25000418661"
        assert record2["COR_NUMBER"] == "L25000418662"
        assert record1["oc:source_row"] == "2"  # Line number should be 2
        assert record2["oc:source_row"] == "3"  # Line number should be 3

    @pytest.mark.asyncio
    async def test_parse_stream_limit_rows(self) -> None:
        """Test async streaming with row limiting."""
        # Create test data with multiple lines
        fixed_width_data = (
            "L25000418660"
            + " " * 200
            + "\n"  # Line 1
            + "L25000418661"
            + " " * 200
            + "\n"  # Line 2
            + "L25000418662"
            + " " * 200  # Line 3
        )

        input_stream = io.BytesIO(fixed_width_data.encode("utf-8"))
        output = io.StringIO()
        jsonl_stream = JSONLStream(output)

        parser = FixedWidthResourceParser()
        config = {"limit_rows": 2}  # Limit to 2 rows

        async for _ in parser.parse_stream(input_stream, jsonl_stream, config):
            pass

        lines = output.getvalue().strip().split("\n")
        assert len(lines) == 2  # Should process only 2 lines

        # Check that we got the correct lines
        record1 = json.loads(lines[0])
        record2 = json.loads(lines[1])
        assert record1["COR_NUMBER"] == "L25000418660"
        assert record2["COR_NUMBER"] == "L25000418661"

    @pytest.mark.asyncio
    async def test_parse_stream_empty_input(self) -> None:
        """Test async streaming with empty input."""
        input_stream = io.BytesIO(b"")
        output = io.StringIO()
        jsonl_stream = JSONLStream(output)

        parser = FixedWidthResourceParser()

        progress_updates = []
        async for records_written in parser.parse_stream(input_stream, jsonl_stream):
            progress_updates.append(records_written)

        assert output.getvalue() == ""
        assert len(progress_updates) == 0


class TestParserAsync:
    """Test async parser functionality."""

    @pytest.mark.asyncio
    async def test_run_parser_basic(self) -> None:
        """Test basic async parser execution."""
        # Mock the pipeline bus
        mock_bus = MagicMock()
        mock_bus.get_change_event.return_value = MagicMock(stage="raw", bid="test-bid")
        mock_bus.get_bundle_metadata_json.return_value = {"test": "metadata"}
        mock_bus.get_bundle_resource_list.return_value = ["test.csv"]
        mock_bus.get_bundle_resource.return_value = io.BytesIO(b"name,age\nJohn,30")
        mock_bus.add_bundle_resource_streaming = AsyncMock()
        mock_bus.complete_bundle = MagicMock()

        # Mock app config
        mock_app_config = MagicMock()

        # Mock parser config
        mock_parser_config = MagicMock()
        mock_parser_config.concurrency = 2
        mock_parser_config.resource_parsers = {
            ".*\\.csv": {"CsvResourceParser": {"delimiter": ",", "has_header": True}}
        }

        # Mock the strategy registry
        with patch(
            "data_parser_core.parser.create_strategy_registry"
        ) as mock_registry_factory:
            mock_registry = MagicMock()
            mock_registry_factory.return_value = mock_registry
            mock_registry.create.return_value = CsvResourceParser()

            # Mock DataPipelineBus constructor
            with patch(
                "data_parser_core.parser.DataPipelineBus", return_value=mock_bus
            ):
                await run_parser(
                    app_config=mock_app_config,
                    parser_config=mock_parser_config,
                    data_registry_id="test-registry",
                    stage="raw",
                )

        # Verify calls
        mock_bus.get_change_event.assert_called_once()
        mock_bus.get_bundle_metadata_json.assert_called_once()
        mock_bus.get_bundle_resource_list.assert_called_once()
        mock_bus.add_bundle_resource_streaming.assert_called_once()
        mock_bus.complete_bundle.assert_called_once()

    @pytest.mark.asyncio
    async def test_process_resource_worker(self) -> None:
        """Test resource worker processing."""
        # Create work queue
        work_queue = asyncio.Queue()

        # Mock parser config
        mock_parser_config = MagicMock()
        mock_parser_config.resource_parsers = {
            ".*\\.csv": {"CsvResourceParser": {"delimiter": ",", "has_header": True}}
        }

        # Add work item
        await work_queue.put(
            {
                "resource_name": "test.csv",
                "bid": "test-bid",
                "parser_config": mock_parser_config,
            }
        )

        # Mock pipeline bus
        mock_bus = MagicMock()
        mock_bus.get_bundle_resource.return_value = io.BytesIO(b"name,age\nJohn,30")
        mock_bus.add_bundle_resource_streaming = AsyncMock()

        # Mock strategy registry
        with patch(
            "data_parser_core.parser.create_strategy_registry"
        ) as mock_registry_factory:
            mock_registry = MagicMock()
            mock_registry_factory.return_value = mock_registry
            mock_registry.create.return_value = CsvResourceParser()

            # Create worker task
            worker_task = asyncio.create_task(
                process_resource_worker(work_queue, mock_bus)
            )

            # Wait for work to complete
            await work_queue.join()

            # Cancel worker
            worker_task.cancel()
            try:
                await worker_task
            except asyncio.CancelledError:
                pass

        # Verify calls
        mock_bus.get_bundle_resource.assert_called_once_with("test-bid", "test.csv")
        mock_bus.add_bundle_resource_streaming.assert_called_once()

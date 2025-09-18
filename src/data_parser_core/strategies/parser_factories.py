"""Parser strategy factories for the parser service."""

from dataclasses import dataclass, is_dataclass
from typing import Any

from oc_pipeline_bus.strategy_registry import (
    InvalidArgumentStrategyException,
    StrategyFactory,
    StrategyFactoryRegistry,
)

from data_parser_core.strategies.csv_parser import CsvResourceParser
from data_parser_core.strategies.fixed_width_parser import FixedWidthResourceParser
from data_parser_core.strategy_types import ResourceParserStrategy


@dataclass
class CsvParserConfig:
    """Configuration for CSV parser."""

    delimiter: str = ","
    quotechar: str = '"'
    escapechar: str | None = None
    encoding: str = "utf-8"
    line_separator: str = "\n"


@dataclass
class FixedWidthParserConfig:
    """Configuration for fixed-width parser."""

    encoding: str = "utf-8"
    line_separator: str = "\n"


class CsvParserFactory(StrategyFactory):
    """Factory for creating CSV parser instances."""

    def validate(self, params: Any) -> None:
        """Validate CSV parser parameters.
        
        Args:
            params: Dictionary of parameters to validate
            
        Raises:
            InvalidArgumentStrategyException: If validation fails
        """
        params_dict = params if isinstance(params, dict) else {}
        
        # Validate optional parameters if provided
        if "delimiter" in params_dict and not isinstance(params_dict["delimiter"], str):
            raise InvalidArgumentStrategyException(
                "delimiter must be a string",
                CsvResourceParser,
                "csv",
                params,
            )
        
        if "quotechar" in params_dict and not isinstance(params_dict["quotechar"], str):
            raise InvalidArgumentStrategyException(
                "quotechar must be a string",
                CsvResourceParser,
                "csv",
                params,
            )
        
        if "encoding" in params_dict and not isinstance(params_dict["encoding"], str):
            raise InvalidArgumentStrategyException(
                "encoding must be a string",
                CsvResourceParser,
                "csv",
                params,
            )

    def create(self, params: Any) -> CsvResourceParser:
        """Create a CSV parser instance.
        
        Args:
            params: Dictionary of parameters for parser creation (may be processed config object)
            
        Returns:
            Created CsvResourceParser instance
        """
        if is_dataclass(params):
            delimiter = getattr(params, "delimiter", ",")
            quotechar = getattr(params, "quotechar", '"')
            escapechar = getattr(params, "escapechar", None)
            encoding = getattr(params, "encoding", "utf-8")
            line_separator = getattr(params, "line_separator", "\n")
            has_header = getattr(params, "has_header", True)
            headers = getattr(params, "headers", None)
            include = getattr(params, "include", None)
            rename = getattr(params, "rename", None)
            coerce = getattr(params, "coerce", None)
            null_values = getattr(params, "null_values", None)
            trim_whitespace = getattr(params, "trim_whitespace", True)
            skip_rows = getattr(params, "skip_rows", 0)
            limit_rows = getattr(params, "limit_rows", None)
            on_error = getattr(params, "on_error", "skip")
            extra_fields_policy = getattr(params, "extra_fields_policy", "keep")
            schema_version = getattr(params, "schema_version", None)
        else:
            params_dict = params if isinstance(params, dict) else {}
            delimiter = params_dict.get("delimiter", ",")
            quotechar = params_dict.get("quotechar", '"')
            escapechar = params_dict.get("escapechar")
            encoding = params_dict.get("encoding", "utf-8")
            line_separator = params_dict.get("line_separator", "\n")
            has_header = params_dict.get("has_header", True)
            headers = params_dict.get("headers")
            include = params_dict.get("include")
            rename = params_dict.get("rename")
            coerce = params_dict.get("coerce")
            null_values = params_dict.get("null_values")
            trim_whitespace = params_dict.get("trim_whitespace", True)
            skip_rows = params_dict.get("skip_rows", 0)
            limit_rows = params_dict.get("limit_rows")
            on_error = params_dict.get("on_error", "skip")
            extra_fields_policy = params_dict.get("extra_fields_policy", "keep")
            schema_version = params_dict.get("schema_version")

        return CsvResourceParser(
            delimiter=delimiter,
            quotechar=quotechar,
            escapechar=escapechar,
            encoding=encoding,
            line_separator=line_separator,
            has_header=has_header,
            headers=headers,
            include=include,
            rename=rename,
            coerce=coerce,
            null_values=null_values,
            trim_whitespace=trim_whitespace,
            skip_rows=skip_rows,
            limit_rows=limit_rows,
            on_error=on_error,
            extra_fields_policy=extra_fields_policy,
            schema_version=schema_version,
        )

    def get_config_type(self, params: Any) -> type | None:
        """Get the configuration type for further processing.
        
        Args:
            params: Dictionary of parameters that may contain nested configurations
            
        Returns:
            CsvParserConfig - for processing configuration
        """
        return CsvParserConfig


class FixedWidthParserFactory(StrategyFactory):
    """Factory for creating fixed-width parser instances."""

    def validate(self, params: Any) -> None:
        """Validate fixed-width parser parameters.
        
        Args:
            params: Dictionary of parameters to validate
            
        Raises:
            InvalidArgumentStrategyException: If validation fails
        """
        params_dict = params if isinstance(params, dict) else {}
        
        # Validate optional parameters if provided
        if "encoding" in params_dict and not isinstance(params_dict["encoding"], str):
            raise InvalidArgumentStrategyException(
                "encoding must be a string",
                FixedWidthResourceParser,
                "fixed_width",
                params,
            )

    def create(self, params: Any) -> FixedWidthResourceParser:
        """Create a fixed-width parser instance.
        
        Args:
            params: Dictionary of parameters for parser creation (may be processed config object)
            
        Returns:
            Created FixedWidthResourceParser instance
        """
        if is_dataclass(params):
            encoding = getattr(params, "encoding", "utf-8")
            line_separator = getattr(params, "line_separator", "\n")
            field_specs = getattr(params, "field_specs", None)
            skip_rows = getattr(params, "skip_rows", 0)
            limit_rows = getattr(params, "limit_rows", None)
            on_error = getattr(params, "on_error", "skip")
            schema_version = getattr(params, "schema_version", None)
            ocid_generator = getattr(params, "ocid_generator", None)
        else:
            params_dict = params if isinstance(params, dict) else {}
            encoding = params_dict.get("encoding", "utf-8")
            line_separator = params_dict.get("line_separator", "\n")
            field_specs = params_dict.get("field_specs")
            skip_rows = params_dict.get("skip_rows", 0)
            limit_rows = params_dict.get("limit_rows")
            on_error = params_dict.get("on_error", "skip")
            schema_version = params_dict.get("schema_version")
            ocid_generator = params_dict.get("ocid_generator")

        return FixedWidthResourceParser(
            encoding=encoding,
            line_separator=line_separator,
            field_specs=field_specs,
            skip_rows=skip_rows,
            limit_rows=limit_rows,
            on_error=on_error,
            schema_version=schema_version,
            ocid_generator=ocid_generator,
        )

    def get_config_type(self, params: Any) -> type | None:
        """Get the configuration type for further processing.
        
        Args:
            params: Dictionary of parameters that may contain nested configurations
            
        Returns:
            FixedWidthParserConfig - for processing configuration
        """
        return FixedWidthParserConfig


def register_parser_strategies(registry: StrategyFactoryRegistry) -> None:
    """Register all parser strategy factories with the registry.

    Args:
        registry: The strategy factory registry to register with.
    """
    # Register CSV parser
    registry.register(ResourceParserStrategy, "csv", CsvParserFactory())

    # Register fixed-width parser
    registry.register(ResourceParserStrategy, "fixed_width", FixedWidthParserFactory())

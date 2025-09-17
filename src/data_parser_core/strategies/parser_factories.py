"""Parser strategy factories for the parser service."""

from oc_pipeline_bus.strategy_registry import StrategyFactoryRegistry

from data_parser_core.strategies.csv_parser import CsvResourceParser
from data_parser_core.strategies.fixed_width_parser import FixedWidthResourceParser
from data_parser_core.strategy_types import ResourceParserStrategy


class CsvParserFactory:
    """Factory for creating CSV parser instances."""

    def validate(self, params: dict[str, any]) -> None:
        """Validate CSV parser parameters."""
        # CSV parser parameters are optional, so no validation needed
        pass

    def create(self, params: dict[str, any]) -> CsvResourceParser:
        """Create a CSV parser instance."""
        return CsvResourceParser()


class FixedWidthParserFactory:
    """Factory for creating fixed-width parser instances."""

    def validate(self, params: dict[str, any]) -> None:
        """Validate fixed-width parser parameters."""
        # Fixed-width parser parameters are optional, so no validation needed
        pass

    def create(self, params: dict[str, any]) -> FixedWidthResourceParser:
        """Create a fixed-width parser instance."""
        return FixedWidthResourceParser()


def register_parser_strategies(registry: StrategyFactoryRegistry) -> None:
    """Register all parser strategy factories with the registry.
    
    Args:
        registry: The strategy factory registry to register with.
    """
    # Register CSV parser
    registry.register(ResourceParserStrategy, "csv", CsvParserFactory())
    
    # Register fixed-width parser
    registry.register(ResourceParserStrategy, "fixed_width", FixedWidthParserFactory())

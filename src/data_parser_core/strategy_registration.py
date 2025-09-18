"""Strategy registration for the parser service.

This module provides a centralized way to register all strategy factories
with the StrategyFactoryRegistry, enabling YAML-based configuration loading.
"""

from oc_pipeline_bus.strategy_registry import StrategyFactoryRegistry

from data_parser_core.strategies.parser_factories import register_parser_strategies


def create_strategy_registry() -> StrategyFactoryRegistry:
    """Create and register all available strategy factories with a new registry.

    Returns:
        Registry with all strategies registered
    """
    registry = StrategyFactoryRegistry()

    # Register all strategy types
    register_parser_strategies(registry)

    # Add your custom strategy registrations here
    # Example:
    # register_custom_strategies(registry, custom_manager=custom_manager)

    return registry

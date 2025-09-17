"""Core framework components and base classes.

This module provides the fundamental building blocks of the OC parser framework,
including the base DataRegistryParserConfig and configuration creation utilities.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    # Imports used only for type checking to avoid runtime import side effects
    from oc_pipeline_bus.config import Annotated, strategy
    from data_parser_core.strategy_types import ResourceParserStrategy


@dataclass
class DataRegistryParserConfig:
    """YAML-based parser configuration using strategy factory registry."""

    resource_parsers: dict[str, Annotated[ResourceParserStrategy, strategy]]
    concurrency: int = 10
    # Optional fields for backward compatibility with storage hooks
    config_id: str = ""


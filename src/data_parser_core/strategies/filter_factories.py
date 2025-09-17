"""Filter strategy factories for the parser service."""

from datetime import datetime
from typing import Any, Dict, List, Optional, Union

from oc_pipeline_bus.strategy_registry import StrategyFactoryRegistry

from data_parser_core.strategy_types import FilterStrategyBase


class DateFilterStrategy(FilterStrategyBase):
    """Filter strategy for date-based filtering of records.
    
    This filter can be used to filter records based on date fields,
    supporting various date formats and comparison operations.
    """
    
    def __init__(
        self,
        date_field: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        date_pattern: str = "YYYYMMDD",
        comparison_operator: str = "between"
    ):
        """Initialize the date filter strategy.
        
        Args:
            date_field: The field name containing the date to filter on
            start_date: Start date for filtering (inclusive)
            end_date: End date for filtering (inclusive)
            date_pattern: Date format pattern (YYYYMMDD, YYYY-MM-DD, etc.)
            comparison_operator: Comparison operator (between, after, before, equals)
        """
        self.date_field = date_field
        self.start_date = start_date
        self.end_date = end_date
        self.date_pattern = date_pattern
        self.comparison_operator = comparison_operator
        
        # Parse dates if provided
        self._parsed_start_date = self._parse_date(start_date) if start_date else None
        self._parsed_end_date = self._parse_date(end_date) if end_date else None
    
    def _parse_date(self, date_str: str) -> datetime:
        """Parse a date string according to the configured pattern."""
        if self.date_pattern == "YYYYMMDD":
            return datetime.strptime(date_str, "%Y%m%d")
        elif self.date_pattern == "YYYY-MM-DD":
            return datetime.strptime(date_str, "%Y-%m-%d")
        elif self.date_pattern == "MM/DD/YYYY":
            return datetime.strptime(date_str, "%m/%d/%Y")
        elif self.date_pattern == "DD/MM/YYYY":
            return datetime.strptime(date_str, "%d/%m/%Y")
        else:
            # Try to parse as ISO format
            return datetime.fromisoformat(date_str.replace('Z', '+00:00'))
    
    def _extract_date_from_record(self, record: Dict[str, Any]) -> Optional[datetime]:
        """Extract and parse the date from a record."""
        if self.date_field not in record:
            return None
            
        date_value = record[self.date_field]
        if not date_value or date_value == "":
            return None
            
        try:
            return self._parse_date(str(date_value))
        except (ValueError, TypeError):
            return None
    
    def filter(self, data: Dict[str, Any]) -> bool:
        """Filter records based on date criteria.
        
        Args:
            data: The record data to filter
            
        Returns:
            True if the record should be included, False otherwise
        """
        record_date = self._extract_date_from_record(data)
        if record_date is None:
            return False
        
        if self.comparison_operator == "between":
            if self._parsed_start_date and record_date < self._parsed_start_date:
                return False
            if self._parsed_end_date and record_date > self._parsed_end_date:
                return False
            return True
        elif self.comparison_operator == "after":
            return self._parsed_start_date is None or record_date >= self._parsed_start_date
        elif self.comparison_operator == "before":
            return self._parsed_end_date is None or record_date <= self._parsed_end_date
        elif self.comparison_operator == "equals":
            if self._parsed_start_date:
                return record_date.date() == self._parsed_start_date.date()
            return False
        else:
            return True


class FieldValueFilterStrategy(FilterStrategyBase):
    """Filter strategy for field value-based filtering of records.
    
    This filter can be used to filter records based on specific field values,
    supporting exact matches, pattern matching, and value lists.
    """
    
    def __init__(
        self,
        field_name: str,
        filter_values: Optional[List[str]] = None,
        exclude_values: Optional[List[str]] = None,
        pattern: Optional[str] = None,
        case_sensitive: bool = True
    ):
        """Initialize the field value filter strategy.
        
        Args:
            field_name: The field name to filter on
            filter_values: List of values to include (if provided, only these values pass)
            exclude_values: List of values to exclude
            pattern: Regex pattern to match against field values
            case_sensitive: Whether pattern matching should be case sensitive
        """
        self.field_name = field_name
        self.filter_values = filter_values or []
        self.exclude_values = exclude_values or []
        self.pattern = pattern
        self.case_sensitive = case_sensitive
        
        # Compile regex pattern if provided
        if self.pattern:
            import re
            flags = 0 if case_sensitive else re.IGNORECASE
            self._compiled_pattern = re.compile(pattern, flags)
        else:
            self._compiled_pattern = None
    
    def filter(self, data: Dict[str, Any]) -> bool:
        """Filter records based on field value criteria.
        
        Args:
            data: The record data to filter
            
        Returns:
            True if the record should be included, False otherwise
        """
        if self.field_name not in data:
            return False
            
        field_value = str(data[self.field_name])
        
        # Check exclude values first
        if self.exclude_values:
            if field_value in self.exclude_values:
                return False
        
        # Check filter values (whitelist)
        if self.filter_values:
            if field_value not in self.filter_values:
                return False
        
        # Check pattern matching
        if self._compiled_pattern:
            if not self._compiled_pattern.search(field_value):
                return False
        
        return True


class CompositeFilterStrategy(FilterStrategyBase):
    """Composite filter strategy that combines multiple filters with logical operators.
    
    This filter allows combining multiple filter strategies using AND/OR logic.
    """
    
    def __init__(
        self,
        filters: List[FilterStrategyBase],
        operator: str = "AND"
    ):
        """Initialize the composite filter strategy.
        
        Args:
            filters: List of filter strategies to combine
            operator: Logical operator to use ("AND" or "OR")
        """
        self.filters = filters
        self.operator = operator.upper()
        
        if self.operator not in ["AND", "OR"]:
            raise ValueError("Operator must be 'AND' or 'OR'")
    
    def filter(self, data: Dict[str, Any]) -> bool:
        """Filter records using composite logic.
        
        Args:
            data: The record data to filter
            
        Returns:
            True if the record should be included, False otherwise
        """
        if not self.filters:
            return True
        
        results = [f.filter(data) for f in self.filters]
        
        if self.operator == "AND":
            return all(results)
        else:  # OR
            return any(results)


class DateFilterFactory:
    """Factory for creating date filter instances."""
    
    def validate(self, params: Dict[str, Any]) -> None:
        """Validate date filter parameters."""
        required_fields = ["date_field"]
        for field in required_fields:
            if field not in params:
                raise ValueError(f"Missing required field: {field}")
        
        if not isinstance(params["date_field"], str):
            raise ValueError("date_field must be a string")
        
        # Validate date format if provided
        if "date_pattern" in params:
            valid_patterns = ["YYYYMMDD", "YYYY-MM-DD", "MM/DD/YYYY", "DD/MM/YYYY"]
            if params["date_pattern"] not in valid_patterns:
                raise ValueError(f"Invalid date_pattern. Must be one of: {valid_patterns}")
        
        # Validate comparison operator
        if "comparison_operator" in params:
            valid_operators = ["between", "after", "before", "equals"]
            if params["comparison_operator"] not in valid_operators:
                raise ValueError(f"Invalid comparison_operator. Must be one of: {valid_operators}")
    
    def create(self, params: Dict[str, Any]) -> DateFilterStrategy:
        """Create a date filter instance."""
        return DateFilterStrategy(
            date_field=params["date_field"],
            start_date=params.get("start_date"),
            end_date=params.get("end_date"),
            date_pattern=params.get("date_pattern", "YYYYMMDD"),
            comparison_operator=params.get("comparison_operator", "between")
        )
    
    def get_config_type(self, params: Dict[str, Any]) -> type:
        """Get the configuration type for further processing."""
        return None


class FieldValueFilterFactory:
    """Factory for creating field value filter instances."""
    
    def validate(self, params: Dict[str, Any]) -> None:
        """Validate field value filter parameters."""
        required_fields = ["field_name"]
        for field in required_fields:
            if field not in params:
                raise ValueError(f"Missing required field: {field}")
        
        if not isinstance(params["field_name"], str):
            raise ValueError("field_name must be a string")
        
        # Validate that at least one filtering criteria is provided
        has_criteria = any([
            "filter_values" in params,
            "exclude_values" in params,
            "pattern" in params
        ])
        
        if not has_criteria:
            raise ValueError("At least one of filter_values, exclude_values, or pattern must be provided")
    
    def create(self, params: Dict[str, Any]) -> FieldValueFilterStrategy:
        """Create a field value filter instance."""
        return FieldValueFilterStrategy(
            field_name=params["field_name"],
            filter_values=params.get("filter_values"),
            exclude_values=params.get("exclude_values"),
            pattern=params.get("pattern"),
            case_sensitive=params.get("case_sensitive", True)
        )
    
    def get_config_type(self, params: Dict[str, Any]) -> type:
        """Get the configuration type for further processing."""
        return None


class CompositeFilterFactory:
    """Factory for creating composite filter instances."""
    
    def validate(self, params: Dict[str, Any]) -> None:
        """Validate composite filter parameters."""
        required_fields = ["filters"]
        for field in required_fields:
            if field not in params:
                raise ValueError(f"Missing required field: {field}")
        
        if not isinstance(params["filters"], list):
            raise ValueError("filters must be a list")
        
        if not params["filters"]:
            raise ValueError("filters list cannot be empty")
        
        # Validate operator
        if "operator" in params:
            valid_operators = ["AND", "OR"]
            if params["operator"].upper() not in valid_operators:
                raise ValueError(f"Invalid operator. Must be one of: {valid_operators}")
    
    def create(self, params: Dict[str, Any]) -> CompositeFilterStrategy:
        """Create a composite filter instance."""
        # Note: This is a simplified implementation. In a real scenario,
        # you would need to recursively create the nested filters using
        # the strategy registry.
        filters = []
        for filter_config in params["filters"]:
            # This would need to be implemented to create nested filters
            # For now, we'll create a placeholder
            filters.append(FieldValueFilterStrategy("placeholder", []))
        
        return CompositeFilterStrategy(
            filters=filters,
            operator=params.get("operator", "AND")
        )
    
    def get_config_type(self, params: Dict[str, Any]) -> type:
        """Get the configuration type for further processing."""
        return None


def register_filter_strategies(registry: StrategyFactoryRegistry) -> None:
    """Register all filter strategy factories with the registry.
    
    Args:
        registry: The strategy factory registry to register with.
    """
    # Register date filter
    registry.register(FilterStrategyBase, "date_filter", DateFilterFactory())
    
    # Register field value filter
    registry.register(FilterStrategyBase, "field_value_filter", FieldValueFilterFactory())
    
    # Register composite filter
    registry.register(FilterStrategyBase, "composite_filter", CompositeFilterFactory())

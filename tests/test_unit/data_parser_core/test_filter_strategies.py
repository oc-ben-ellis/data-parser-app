"""Unit tests for filter strategies."""

import pytest
from datetime import datetime

from data_parser_core.strategies.filter_factories import (
    DateFilterStrategy,
    FieldValueFilterStrategy,
    CompositeFilterStrategy,
    DateFilterFactory,
    FieldValueFilterFactory,
    CompositeFilterFactory,
    register_filter_strategies
)
from data_parser_core.strategy_types import FilterStrategyBase
from oc_pipeline_bus.strategy_registry import StrategyFactoryRegistry


class TestDateFilterStrategy:
    """Test cases for DateFilterStrategy."""
    
    def test_date_filter_between_dates(self):
        """Test filtering records between two dates."""
        filter_strategy = DateFilterStrategy(
            date_field="COR_FILE_DATE",
            start_date="20250801",
            end_date="20250831",
            date_pattern="YYYYMMDD"
        )
        
        # Test record within date range
        record1 = {"COR_FILE_DATE": "20250815", "COR_NUMBER": "12345"}
        assert filter_strategy.filter(record1) is True
        
        # Test record before date range
        record2 = {"COR_FILE_DATE": "20250731", "COR_NUMBER": "12346"}
        assert filter_strategy.filter(record2) is False
        
        # Test record after date range
        record3 = {"COR_FILE_DATE": "20250901", "COR_NUMBER": "12347"}
        assert filter_strategy.filter(record3) is False
    
    def test_date_filter_after_date(self):
        """Test filtering records after a specific date."""
        filter_strategy = DateFilterStrategy(
            date_field="COR_FILE_DATE",
            start_date="20250801",
            comparison_operator="after",
            date_pattern="YYYYMMDD"
        )
        
        # Test record after start date
        record1 = {"COR_FILE_DATE": "20250815", "COR_NUMBER": "12345"}
        assert filter_strategy.filter(record1) is True
        
        # Test record before start date
        record2 = {"COR_FILE_DATE": "20250731", "COR_NUMBER": "12346"}
        assert filter_strategy.filter(record2) is False
    
    def test_date_filter_missing_field(self):
        """Test filtering when date field is missing."""
        filter_strategy = DateFilterStrategy(
            date_field="COR_FILE_DATE",
            start_date="20250801",
            end_date="20250831"
        )
        
        # Test record without date field
        record = {"COR_NUMBER": "12345"}
        assert filter_strategy.filter(record) is False
        
        # Test record with empty date field
        record2 = {"COR_FILE_DATE": "", "COR_NUMBER": "12346"}
        assert filter_strategy.filter(record2) is False
    
    def test_date_filter_invalid_date(self):
        """Test filtering with invalid date format."""
        filter_strategy = DateFilterStrategy(
            date_field="COR_FILE_DATE",
            start_date="20250801",
            end_date="20250831"
        )
        
        # Test record with invalid date
        record = {"COR_FILE_DATE": "invalid-date", "COR_NUMBER": "12345"}
        assert filter_strategy.filter(record) is False


class TestFieldValueFilterStrategy:
    """Test cases for FieldValueFilterStrategy."""
    
    def test_field_value_filter_include_values(self):
        """Test filtering records with specific field values."""
        filter_strategy = FieldValueFilterStrategy(
            field_name="COR_STATUS",
            filter_values=["Active", "Inactive"]
        )
        
        # Test record with included value
        record1 = {"COR_STATUS": "Active", "COR_NUMBER": "12345"}
        assert filter_strategy.filter(record1) is True
        
        # Test record with excluded value
        record2 = {"COR_STATUS": "Pending", "COR_NUMBER": "12346"}
        assert filter_strategy.filter(record2) is False
    
    def test_field_value_filter_exclude_values(self):
        """Test filtering records excluding specific field values."""
        filter_strategy = FieldValueFilterStrategy(
            field_name="COR_STATUS",
            exclude_values=["Pending", "Draft"]
        )
        
        # Test record with excluded value
        record1 = {"COR_STATUS": "Pending", "COR_NUMBER": "12345"}
        assert filter_strategy.filter(record1) is False
        
        # Test record with allowed value
        record2 = {"COR_STATUS": "Active", "COR_NUMBER": "12346"}
        assert filter_strategy.filter(record2) is True
    
    def test_field_value_filter_pattern(self):
        """Test filtering records with regex pattern."""
        filter_strategy = FieldValueFilterStrategy(
            field_name="COR_NUMBER",
            pattern=r"^\d{5}$"  # 5 digits
        )
        
        # Test record matching pattern
        record1 = {"COR_NUMBER": "12345", "COR_STATUS": "Active"}
        assert filter_strategy.filter(record1) is True
        
        # Test record not matching pattern
        record2 = {"COR_NUMBER": "1234", "COR_STATUS": "Active"}
        assert filter_strategy.filter(record2) is False
    
    def test_field_value_filter_missing_field(self):
        """Test filtering when field is missing."""
        filter_strategy = FieldValueFilterStrategy(
            field_name="COR_STATUS",
            filter_values=["Active"]
        )
        
        # Test record without field
        record = {"COR_NUMBER": "12345"}
        assert filter_strategy.filter(record) is False


class TestCompositeFilterStrategy:
    """Test cases for CompositeFilterStrategy."""
    
    def test_composite_filter_and_operator(self):
        """Test composite filter with AND operator."""
        date_filter = DateFilterStrategy(
            date_field="COR_FILE_DATE",
            start_date="20250801",
            end_date="20250831"
        )
        status_filter = FieldValueFilterStrategy(
            field_name="COR_STATUS",
            filter_values=["Active"]
        )
        
        composite_filter = CompositeFilterStrategy(
            filters=[date_filter, status_filter],
            operator="AND"
        )
        
        # Test record matching both criteria
        record1 = {"COR_FILE_DATE": "20250815", "COR_STATUS": "Active", "COR_NUMBER": "12345"}
        assert composite_filter.filter(record1) is True
        
        # Test record matching only date criteria
        record2 = {"COR_FILE_DATE": "20250815", "COR_STATUS": "Inactive", "COR_NUMBER": "12346"}
        assert composite_filter.filter(record2) is False
        
        # Test record matching only status criteria
        record3 = {"COR_FILE_DATE": "20250731", "COR_STATUS": "Active", "COR_NUMBER": "12347"}
        assert composite_filter.filter(record3) is False
    
    def test_composite_filter_or_operator(self):
        """Test composite filter with OR operator."""
        date_filter = DateFilterStrategy(
            date_field="COR_FILE_DATE",
            start_date="20250801",
            end_date="20250831"
        )
        status_filter = FieldValueFilterStrategy(
            field_name="COR_STATUS",
            filter_values=["Active"]
        )
        
        composite_filter = CompositeFilterStrategy(
            filters=[date_filter, status_filter],
            operator="OR"
        )
        
        # Test record matching both criteria
        record1 = {"COR_FILE_DATE": "20250815", "COR_STATUS": "Active", "COR_NUMBER": "12345"}
        assert composite_filter.filter(record1) is True
        
        # Test record matching only date criteria
        record2 = {"COR_FILE_DATE": "20250815", "COR_STATUS": "Inactive", "COR_NUMBER": "12346"}
        assert composite_filter.filter(record2) is True
        
        # Test record matching only status criteria
        record3 = {"COR_FILE_DATE": "20250731", "COR_STATUS": "Active", "COR_NUMBER": "12347"}
        assert composite_filter.filter(record3) is True
        
        # Test record matching neither criteria
        record4 = {"COR_FILE_DATE": "20250731", "COR_STATUS": "Inactive", "COR_NUMBER": "12348"}
        assert composite_filter.filter(record4) is False


class TestFilterFactories:
    """Test cases for filter factories."""
    
    def test_date_filter_factory_validation(self):
        """Test date filter factory validation."""
        factory = DateFilterFactory()
        
        # Test valid parameters
        valid_params = {
            "date_field": "COR_FILE_DATE",
            "start_date": "20250801",
            "end_date": "20250831",
            "date_pattern": "YYYYMMDD",
            "comparison_operator": "between"
        }
        factory.validate(valid_params)
        
        # Test missing required field
        with pytest.raises(ValueError, match="Missing required field: date_field"):
            factory.validate({})
        
        # Test invalid date pattern
        with pytest.raises(ValueError, match="Invalid date_pattern"):
            factory.validate({
                "date_field": "COR_FILE_DATE",
                "date_pattern": "INVALID"
            })
        
        # Test invalid comparison operator
        with pytest.raises(ValueError, match="Invalid comparison_operator"):
            factory.validate({
                "date_field": "COR_FILE_DATE",
                "comparison_operator": "invalid"
            })
    
    def test_date_filter_factory_creation(self):
        """Test date filter factory creation."""
        factory = DateFilterFactory()
        
        params = {
            "date_field": "COR_FILE_DATE",
            "start_date": "20250801",
            "end_date": "20250831"
        }
        
        filter_instance = factory.create(params)
        assert isinstance(filter_instance, DateFilterStrategy)
        assert filter_instance.date_field == "COR_FILE_DATE"
        assert filter_instance.start_date == "20250801"
        assert filter_instance.end_date == "20250831"
    
    def test_field_value_filter_factory_validation(self):
        """Test field value filter factory validation."""
        factory = FieldValueFilterFactory()
        
        # Test valid parameters
        valid_params = {
            "field_name": "COR_STATUS",
            "filter_values": ["Active", "Inactive"]
        }
        factory.validate(valid_params)
        
        # Test missing required field
        with pytest.raises(ValueError, match="Missing required field: field_name"):
            factory.validate({})
        
        # Test missing filtering criteria
        with pytest.raises(ValueError, match="At least one of filter_values, exclude_values, or pattern must be provided"):
            factory.validate({"field_name": "COR_STATUS"})
    
    def test_field_value_filter_factory_creation(self):
        """Test field value filter factory creation."""
        factory = FieldValueFilterFactory()
        
        params = {
            "field_name": "COR_STATUS",
            "filter_values": ["Active", "Inactive"],
            "case_sensitive": False
        }
        
        filter_instance = factory.create(params)
        assert isinstance(filter_instance, FieldValueFilterStrategy)
        assert filter_instance.field_name == "COR_STATUS"
        assert filter_instance.filter_values == ["Active", "Inactive"]
        assert filter_instance.case_sensitive is False
    
    def test_composite_filter_factory_validation(self):
        """Test composite filter factory validation."""
        factory = CompositeFilterFactory()
        
        # Test valid parameters
        valid_params = {
            "filters": [
                {"type": "date_filter", "date_field": "COR_FILE_DATE"},
                {"type": "field_value_filter", "field_name": "COR_STATUS"}
            ],
            "operator": "AND"
        }
        factory.validate(valid_params)
        
        # Test missing required field
        with pytest.raises(ValueError, match="Missing required field: filters"):
            factory.validate({})
        
        # Test empty filters list
        with pytest.raises(ValueError, match="filters list cannot be empty"):
            factory.validate({"filters": []})
        
        # Test invalid operator
        with pytest.raises(ValueError, match="Invalid operator"):
            factory.validate({
                "filters": [{"type": "date_filter"}],
                "operator": "INVALID"
            })


class TestFilterStrategyRegistration:
    """Test cases for filter strategy registration."""
    
    def test_register_filter_strategies(self):
        """Test that filter strategies are properly registered."""
        registry = StrategyFactoryRegistry()
        register_filter_strategies(registry)
        
        # Test that all filter strategies are registered
        assert registry.is_registered(FilterStrategyBase, "date_filter")
        assert registry.is_registered(FilterStrategyBase, "field_value_filter")
        assert registry.is_registered(FilterStrategyBase, "composite_filter")
        
        # Test that we can create instances
        date_filter = registry.create(
            FilterStrategyBase,
            "date_filter",
            {"date_field": "COR_FILE_DATE", "start_date": "20250801"}
        )
        assert isinstance(date_filter, DateFilterStrategy)
        
        field_filter = registry.create(
            FilterStrategyBase,
            "field_value_filter",
            {"field_name": "COR_STATUS", "filter_values": ["Active"]}
        )
        assert isinstance(field_filter, FieldValueFilterStrategy)
    
    def test_filter_strategy_validation_through_registry(self):
        """Test filter strategy validation through the registry."""
        registry = StrategyFactoryRegistry()
        register_filter_strategies(registry)
        
        # Test validation of invalid parameters
        with pytest.raises(ValueError):
            registry.validate(
                FilterStrategyBase,
                "date_filter",
                {}  # Missing required date_field
            )
        
        with pytest.raises(ValueError):
            registry.validate(
                FilterStrategyBase,
                "field_value_filter",
                {"field_name": "COR_STATUS"}  # Missing filtering criteria
            )

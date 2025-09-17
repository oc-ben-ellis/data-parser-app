# Filter Strategies for Data Parser

This document describes the filter strategies implemented for the data parser service, which allow filtering of records during the parsing process.

## Overview

Filter strategies provide a flexible way to filter records during the parsing process, allowing you to:
- Filter records based on date ranges
- Include or exclude records based on field values
- Use regex patterns for complex matching
- Combine multiple filters with logical operators

## Available Filter Strategies

### 1. Date Filter Strategy (`date_filter`)

Filters records based on date fields with various comparison operations.

#### Configuration Parameters

- `date_field` (required): The field name containing the date to filter on
- `start_date` (optional): Start date for filtering (inclusive)
- `end_date` (optional): End date for filtering (inclusive)
- `date_pattern` (optional): Date format pattern (default: "YYYYMMDD")
- `comparison_operator` (optional): Comparison operator (default: "between")

#### Supported Date Patterns

- `YYYYMMDD`: 20250815
- `YYYY-MM-DD`: 2025-08-15
- `MM/DD/YYYY`: 08/15/2025
- `DD/MM/YYYY`: 15/08/2025
- ISO format: 2025-08-15T10:30:00Z

#### Supported Comparison Operators

- `between`: Record date must be between start_date and end_date (inclusive)
- `after`: Record date must be after start_date (inclusive)
- `before`: Record date must be before end_date (inclusive)
- `equals`: Record date must equal start_date (date part only)

#### Example Configuration

```yaml
filters:
  - date_filter:
      date_field: "COR_FILE_DATE"
      start_date: "20250801"
      end_date: "20250831"
      date_pattern: "YYYYMMDD"
      comparison_operator: "between"
```

### 2. Field Value Filter Strategy (`field_value_filter`)

Filters records based on specific field values, supporting exact matches, pattern matching, and value lists.

#### Configuration Parameters

- `field_name` (required): The field name to filter on
- `filter_values` (optional): List of values to include (whitelist)
- `exclude_values` (optional): List of values to exclude (blacklist)
- `pattern` (optional): Regex pattern to match against field values
- `case_sensitive` (optional): Whether pattern matching should be case sensitive (default: true)

#### Example Configurations

```yaml
# Include only specific values
filters:
  - field_value_filter:
      field_name: "COR_STATUS"
      filter_values: ["Active", "Inactive"]

# Exclude specific values
filters:
  - field_value_filter:
      field_name: "COR_STATUS"
      exclude_values: ["Draft", "Pending", "Cancelled"]

# Use regex pattern
filters:
  - field_value_filter:
      field_name: "COR_NUMBER"
      pattern: "^[0-9]{8,12}$"
      case_sensitive: false
```

### 3. Composite Filter Strategy (`composite_filter`)

Combines multiple filter strategies using logical operators.

#### Configuration Parameters

- `filters` (required): List of filter configurations to combine
- `operator` (optional): Logical operator to use ("AND" or "OR", default: "AND")

#### Example Configuration

```yaml
filters:
  - composite_filter:
      operator: "AND"
      filters:
        - field_value_filter:
            field_name: "COR_FILING_TYPE"
            filter_values: ["Corporation", "LLC", "Partnership"]
        - field_value_filter:
            field_name: "COR_NUMBER"
            pattern: "^[0-9]{8,12}$"
```

## Integration with Parser Configuration

Filter strategies can be integrated into the parser configuration in several ways:

### 1. Per-Parser Filters

Filters can be applied to specific resource parsers:

```yaml
resource_parsers:
  "^.*\\.txt$":
    fixed_width:
      # ... parser configuration ...
      filters:
        - date_filter:
            date_field: "COR_FILE_DATE"
            start_date: "20250801"
            end_date: "20250831"
```

### 2. Global Filters

Filters can be applied globally to all parsers:

```yaml
global_filters:
  - field_value_filter:
      field_name: "COR_NAME"
      exclude_values: ["TEST", "DUMMY", "SAMPLE"]
      case_sensitive: false
```

## Usage Examples

### Example 1: Filter Recent Records Only

```yaml
filters:
  - date_filter:
      date_field: "COR_FILE_DATE"
      start_date: "2025-01-01"
      date_pattern: "YYYY-MM-DD"
      comparison_operator: "after"
```

### Example 2: Filter Active Companies Only

```yaml
filters:
  - field_value_filter:
      field_name: "COR_STATUS"
      filter_values: ["Active", "Good Standing"]
```

### Example 3: Complex Filtering with Multiple Criteria

```yaml
filters:
  - composite_filter:
      operator: "AND"
      filters:
        - date_filter:
            date_field: "COR_FILE_DATE"
            start_date: "20250801"
            end_date: "20250831"
        - field_value_filter:
            field_name: "COR_STATUS"
            filter_values: ["Active", "Good Standing"]
        - field_value_filter:
            field_name: "COR_FILING_TYPE"
            exclude_values: ["Test", "Dummy"]
```

### Example 4: Pattern-Based Filtering

```yaml
filters:
  - field_value_filter:
      field_name: "COR_NUMBER"
      pattern: "^[0-9]{8,12}$"  # Valid company number pattern
  - field_value_filter:
      field_name: "COR_NAME"
      pattern: ".*LLC.*|.*Corporation.*|.*Inc.*"  # Common company name patterns
      case_sensitive: false
```

## Error Handling

- Records with missing filter fields are excluded by default
- Invalid date formats are treated as non-matching
- Regex compilation errors are logged and the filter is skipped
- Filter validation errors prevent parser startup

## Performance Considerations

- Filters are applied during parsing, not after
- Date parsing is optimized for common formats
- Regex patterns are compiled once and reused
- Composite filters short-circuit when possible (AND stops on first false, OR stops on first true)

## Testing

The filter strategies include comprehensive unit tests covering:

- Date filtering with various formats and operators
- Field value filtering with include/exclude lists and patterns
- Composite filtering with AND/OR operators
- Error handling for invalid configurations
- Edge cases (missing fields, invalid dates, etc.)

Run the tests with:

```bash
python -m pytest tests/test_unit/data_parser_core/test_filter_strategies.py -v
```

## Future Enhancements

Potential future enhancements to the filter strategies:

1. **Numeric Range Filters**: Filter based on numeric field ranges
2. **Array Field Filters**: Filter based on array/list field contents
3. **Cross-Field Filters**: Filter based on relationships between multiple fields
4. **Custom Filter Functions**: Allow custom Python functions as filters
5. **Performance Optimizations**: Indexing and caching for large datasets
6. **Filter Statistics**: Metrics on filter effectiveness and performance

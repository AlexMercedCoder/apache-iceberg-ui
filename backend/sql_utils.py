"""
SQL parsing utilities for converting SQL to PyIceberg expressions.
"""
import sqlparse
from sqlparse.sql import Where, Comparison, Identifier, Token
from pyiceberg.expressions import (
    And, Or, Not,
    EqualTo, NotEqualTo, LessThan, LessThanOrEqual,
    GreaterThan, GreaterThanOrEqual,
    IsNull, NotNull, In, NotIn,
    AlwaysTrue
)
import re

def extract_where_clause(sql: str):
    """Extract WHERE clause from SQL statement."""
    parsed = sqlparse.parse(sql)
    if not parsed:
        return None
    
    statement = parsed[0]
    for token in statement.tokens:
        if isinstance(token, Where):
            # Get the condition part (skip 'WHERE' keyword)
            condition_str = str(token)[6:].strip()  # Remove 'WHERE '
            return condition_str
    return None

def extract_select_columns(sql: str):
    """Extract column names from SELECT clause."""
    parsed = sqlparse.parse(sql)
    if not parsed:
        return None
    
    statement = parsed[0]
    select_seen = False
    columns = []
    
    for token in statement.tokens:
        if token.ttype is sqlparse.tokens.Keyword.DML and token.value.upper() == 'SELECT':
            select_seen = True
            continue
        
        if select_seen:
            if token.ttype is sqlparse.tokens.Keyword:
                break  # Hit FROM or other keyword
            if token.ttype is sqlparse.tokens.Wildcard:
                return None  # SELECT *
            if not token.is_whitespace:
                # Clean up column names
                col_str = str(token).strip()
                if col_str.upper() not in ['FROM', 'WHERE']:
                    # Split by comma and clean
                    for col in col_str.split(','):
                        col = col.strip()
                        if col:
                            # Remove table prefixes if present
                            if '.' in col:
                                col = col.split('.')[-1]
                            columns.append(col)
                    if 'FROM' in col_str.upper():
                        break
    
    return columns if columns else None

def parse_simple_condition(condition_str: str):
    """
    Parse a simple WHERE condition to PyIceberg expression.
    Supports basic operators: =, !=, <, <=, >, >=, IS NULL, IS NOT NULL
    
    This is a simplified parser - for complex conditions we fall back to no filter.
    """
    condition_str = condition_str.strip()
    
    # IS NULL / IS NOT NULL
    is_null_match = re.match(r'(\w+)\s+IS\s+NULL', condition_str, re.IGNORECASE)
    if is_null_match:
        return IsNull(is_null_match.group(1))
    
    is_not_null_match = re.match(r'(\w+)\s+IS\s+NOT\s+NULL', condition_str, re.IGNORECASE)
    if is_not_null_match:
        return NotNull(is_not_null_match.group(1))
    
    # Basic comparisons: column op value
    # Match: identifier operator literal
    comp_match = re.match(r'(\w+)\s*(=|!=|<>|<|<=|>|>=)\s*(.+)', condition_str, re.IGNORECASE)
    if comp_match:
        col, op, value = comp_match.groups()
        value = value.strip()
        
        # Try to parse value (remove quotes if string)
        if value.startswith("'") and value.endswith("'"):
            value = value[1:-1]
        elif value.isdigit():
            value = int(value)
        elif value.replace('.', '', 1).isdigit():
            value = float(value)
        
        # Map operator to PyIceberg expression
        if op == '=':
            return EqualTo(col, value)
        elif op in ('!=', '<>'):
            return NotEqualTo(col, value)
        elif op == '<':
            return LessThan(col, value)
        elif op == '<=':
            return LessThanOrEqual(col, value)
        elif op == '>':
            return GreaterThan(col, value)
        elif op == '>=':
            return GreaterThanOrEqual(col, value)
    
    # If we can't parse it, return AlwaysTrue (no filter)
    print(f"Warning: Could not parse condition '{condition_str}', skipping filter")
    return AlwaysTrue()

def sql_where_to_iceberg_filter(where_str: str):
    """
    Convert SQL WHERE clause to PyIceberg filter expression.
    Currently supports simple conditions; complex AND/OR requires more parsing.
    """
    if not where_str:
        return AlwaysTrue()
    
    # Simple case: single condition
    # For complex conditions with AND/OR, we'd need more sophisticated parsing
    # For now, handle simple cases
    
    # Try to split by AND (simplified)
    if ' AND ' in where_str.upper():
        conditions = re.split(r'\s+AND\s+', where_str, flags=re.IGNORECASE)
        filters = [parse_simple_condition(c) for c in conditions]
        # Combine with And
        result = filters[0]
        for f in filters[1:]:
            result = And(result, f)
        return result
    
    # Single condition
    return parse_simple_condition(where_str)

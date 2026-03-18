"""
spark_config_mapper/utils/list_ops.py

List and collection utilities for working with column names and field lists.
"""

from spark_config_mapper.header import re, get_logger, List
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

logger = get_logger(__name__)


def _to_col_list(obj):
    """Convert various column-like inputs to a plain list of strings.

    Handles: list, tuple, DataFrame (has .columns), Item/ExtractItem (has .df.columns
    or .columns attribute), or None.
    """
    if obj is None:
        return []
    if isinstance(obj, (list, tuple)):
        return list(obj)
    # DataFrame — has .columns as a list
    if hasattr(obj, 'columns') and isinstance(obj.columns, list):
        return list(obj.columns)
    # Item/ExtractItem — has .df with .columns
    if hasattr(obj, 'df') and obj.df is not None and hasattr(obj.df, 'columns'):
        return list(obj.df.columns)
    # Fallback: try to iterate
    try:
        return list(obj)
    except TypeError:
        logger.warning(f"noColColide: cannot convert {type(obj).__name__} to column list")
        return []


def noColColide(masterColumns, colideColumns, index, masterList=None):
    # type: (object, object, object, object) -> List[str]
    """
    Select columns from master list that don't collide with another list.

    Used when joining tables to avoid duplicate column names. Index columns
    are always included as they're used for the join.

    Accepts lists, tuples, DataFrames, Item objects, or anything with a
    .columns attribute. All inputs are normalized to plain lists of strings.

    Parameters:
        masterColumns: Columns from the primary table (list, DataFrame, or Item)
        colideColumns: Columns that could cause collisions
        index: Index columns (always included)
        masterList: Restrict to only these columns (optional)

    Returns:
        List[str]: Columns that won't collide, including index columns

    Example:
        >>> master = ['personid', 'name', 'age', 'date']
        >>> other = ['name', 'value']
        >>> noColColide(master, other, ['personid'])
        ['personid', 'age', 'date']  # 'name' excluded due to collision
    """
    masterColumns = _to_col_list(masterColumns)
    colideColumns = _to_col_list(colideColumns)
    index = _to_col_list(index)

    if masterList is None:
        masterList = masterColumns
    else:
        masterList = _to_col_list(masterList)

    result = list(index)  # always a fresh copy
    for item in masterColumns:
        if (item not in result and
            item in masterList and
            item not in colideColumns and
            item is not None):
            result.append(item)

    return result


def unique_non_none(*args) -> List:
    """
    Return unique non-None values from arguments.
    
    Parameters:
        *args: Variable arguments (can be single values or lists)
    
    Returns:
        List: Unique non-None values
    """
    result = []
    seen = set()
    
    for arg in args:
        if arg is None:
            continue
        if isinstance(arg, (list, tuple)):
            for item in arg:
                if item is not None and item not in seen:
                    seen.add(item)
                    result.append(item)
        else:
            if arg not in seen:
                seen.add(arg)
                result.append(arg)
    
    return result


def find_single_level_items(items: List[str]) -> List[str]:
    """
    Find items that are single-level (no dots in name).
    
    Parameters:
        items (List[str]): List of field names
    
    Returns:
        List[str]: Items without dots
    
    Example:
        >>> find_single_level_items(['personid', 'name.first', 'age'])
        ['personid', 'age']
    """
    return [item for item in items if '.' not in item]


def is_single_level(field: str) -> bool:
    """
    Check if a field name is single-level (no nested path).
    
    Parameters:
        field (str): Field name
    
    Returns:
        bool: True if no dot in field name
    """
    return '.' not in field


def get_element_index(element: str, elements: List[str]) -> int:
    """
    Find the index of an element in a list (case-insensitive).
    
    Parameters:
        element (str): Element to find
        elements (List[str]): List to search
    
    Returns:
        int: Index of element, or -1 if not found
    """
    element_lower = element.lower()
    for i, e in enumerate(elements):
        if e.lower() == element_lower:
            return i
    return -1


def escape_and_bound_dot(pattern: str) -> str:
    """
    Escape dots and add word boundaries for regex matching.
    
    Parameters:
        pattern (str): Pattern to escape
    
    Returns:
        str: Escaped pattern with word boundaries
    """
    escaped = re.escape(pattern)
    return rf'\b{escaped}\b'


# UDF version for use in Spark SQL
escape_and_bound_dot_udf = udf(escape_and_bound_dot, StringType())


def preprocess_string(s: str) -> str:
    """
    Preprocess a string for matching (lowercase, strip whitespace).
    
    Parameters:
        s (str): Input string
    
    Returns:
        str: Preprocessed string
    """
    if s is None:
        return ''
    return s.lower().strip()


def extractTableName(TBL: str, schemaString: str = 'iuhealth_prime') -> tuple:
    """
    Extract table name from a schema-prefixed string.
    
    Parameters:
        TBL (str): Table string (possibly with schema prefix)
        schemaString (str): Expected schema prefix
    
    Returns:
        tuple: (table_name, full_path)
    
    Example:
        >>> extractTableName('iuhealth_prime_encounter', 'iuhealth_prime')
        ('encounter', 'iuhealth_prime_encounter')
        >>> extractTableName('my_table', 'iuhealth_prime')
        ('my_table', 'iuhealth_prime.my_table')
    """
    if TBL.startswith(schemaString + '_'):
        return TBL.replace(schemaString + '_', '', 1), TBL
    else:
        return TBL, f"{schemaString}.{TBL}"


def filter_columns_by_pattern(columns: List[str], patterns: List[str], 
                               exclude: bool = False) -> List[str]:
    """
    Filter column list by regex patterns.
    
    Parameters:
        columns (List[str]): Column names to filter
        patterns (List[str]): Regex patterns
        exclude (bool): If True, exclude matches; if False, include matches
    
    Returns:
        List[str]: Filtered column names
    """
    result = []
    for col in columns:
        matches = any(re.search(p, col, re.IGNORECASE) for p in patterns)
        if exclude and not matches:
            result.append(col)
        elif not exclude and matches:
            result.append(col)
    return result

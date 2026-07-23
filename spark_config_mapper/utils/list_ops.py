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


# Collision policies for noColColide (default keeps historical behavior).
ON_COLLISION_EXCLUDE = 'exclude'  # drop non-key clashes from master (legacy)
ON_COLLISION_RAISE = 'raise'      # fail loud — feature-attach / panel joins
_ON_COLLISION_MODES = (ON_COLLISION_EXCLUDE, ON_COLLISION_RAISE)


def noColColide(masterColumns, colideColumns, index, masterList=None,
                on_collision=ON_COLLISION_EXCLUDE):
    """
    Select columns from master that are safe to keep when joining to another
    table (avoid duplicate column names).

    Used when joining tables to avoid duplicate column names. Index columns
    are always included as they're used for the join.

    Accepts lists, tuples, DataFrames, Item objects, or anything with a
    .columns attribute. All inputs are normalized to plain lists of strings.

    Parameters:
        masterColumns: Columns from the primary table (list, DataFrame, or Item)
        colideColumns: Columns that could cause collisions (the other side)
        index: Index / join-key columns (always included; may appear on both
            sides)
        masterList: Restrict to only these columns (optional)
        on_collision (str): Policy when a **non-key** master column name also
            appears in ``colideColumns``:

            * ``'exclude'`` (default) — omit the colliding name from the
              result (historical behavior; safe for general extract joins
              where one copy of a shared field is enough).
            * ``'raise'`` — raise ``ValueError`` listing the colliding
              names. Use for person-feature **attach** / panel assembly,
              where silently dropping a feature's ``index_*`` / ``entries_*``
              column is almost always a bug (e.g. two ``write_index_table``
              products with the same ``code``).

    Returns:
        List[str]: Columns to select from the master side, including index

    Raises:
        ValueError: If ``on_collision='raise'`` and non-key name clashes exist,
            or if ``on_collision`` is not a known mode.

    Example:
        >>> master = ['personid', 'name', 'age', 'date']
        >>> other = ['name', 'value']
        >>> noColColide(master, other, ['personid'])
        ['personid', 'age', 'date']  # 'name' excluded due to collision
        >>> noColColide(master, other, ['personid'], on_collision='raise')
        Traceback (most recent call last):
            ...
        ValueError: noColColide: non-key column name collision(s) ...
    """
    masterColumns = _to_col_list(masterColumns)
    colideColumns = _to_col_list(colideColumns)
    index = _to_col_list(index)

    if masterList is None:
        masterList = masterColumns
    else:
        masterList = _to_col_list(masterList)

    mode = (on_collision if on_collision is not None else ON_COLLISION_EXCLUDE)
    if isinstance(mode, str):
        mode = mode.lower().strip()
    if mode not in _ON_COLLISION_MODES:
        raise ValueError(
            f"noColColide: on_collision must be one of {_ON_COLLISION_MODES}, "
            f"got {on_collision!r}"
        )

    index_set = set(index)
    colide_set = set(colideColumns)
    master_list_set = set(masterList)

    result = list(index)  # always a fresh copy; keys may exist on both sides
    collisions = []

    for item in masterColumns:
        if item is None or item in result:
            continue
        if item not in master_list_set:
            continue
        if item in colide_set and item not in index_set:
            # Non-key name appears on both sides of the join.
            collisions.append(item)
            if mode == ON_COLLISION_EXCLUDE:
                continue  # legacy: drop from master select list
            # raise mode: still skip building a bad select list; raise below
            continue
        result.append(item)

    if mode == ON_COLLISION_RAISE and collisions:
        raise ValueError(
            "noColColide: non-key column name collision(s) with "
            f"on_collision='raise': {collisions}. "
            "Join keys (index) may share names; non-key columns must be "
            "unique on both sides. Typical fix: distinct write_index_table "
            "`code` values so index_*/last_*/entries_* do not clash, or "
            "rename before attach."
        )

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


def extractTableName(TBL: str, schemaString: str = 'rwd_prime') -> tuple:
    """
    Extract table name from a schema-prefixed string.
    
    Parameters:
        TBL (str): Table string (possibly with schema prefix)
        schemaString (str): Expected schema prefix
    
    Returns:
        tuple: (table_name, full_path)
    
    Example:
        >>> extractTableName('rwd_prime_encounter', 'rwd_prime')
        ('encounter', 'rwd_prime_encounter')
        >>> extractTableName('my_table', 'rwd_prime')
        ('my_table', 'rwd_prime.my_table')
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

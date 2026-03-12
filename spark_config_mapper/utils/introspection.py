"""
spark_config_mapper/utils/introspection.py

Introspection utilities for Spark DataFrames and schemas.
Provides tools for schema flattening, table location extraction, and field analysis.

Compatible with Python 3.6+
"""

from spark_config_mapper.header import (
    re, spark, DataFrame, List, ArrayType, StructType, get_logger
)

logger = get_logger(__name__)


def coalesce(*args):
    """
    Return the first non-None value from arguments.

    Utility function for providing default values in a clean way.

    Parameters:
        *args: Variable number of arguments

    Returns:
        First non-None argument, or None if all are None

    Example:
        >>> coalesce(None, None, 'default')
        'default'
        >>> coalesce('first', 'second')
        'first'
    """
    for arg in args:
        if arg is not None:
            return arg
    return None


def flatten_schema(schema, prefix=None, include_arrays=False):
    # type: (StructType, str, bool) -> List[str]
    """
    Flatten a nested Spark schema into dot-notation field paths.

    Recursively traverses StructType fields to produce a flat list of field
    paths like 'parent.child.grandchild'.

    IMPORTANT: For ArrayType fields:
    - If include_arrays=False (default): Unwraps array and recurses into element type.
      This gives you the PATHS but you cannot SELECT them without exploding first.
    - If include_arrays=True: Stops at array fields, returning the array column name.
      Use this when you want to know which columns are arrays.

    Parameters:
        schema: Spark StructType schema
        prefix (str, optional): Prefix for nested fields
        include_arrays (bool): If True, include array columns without recursing into them.
                               If False (default), recurse into array element types.

    Returns:
        List[str]: List of flattened field paths

    Example:
        >>> # Schema: root |-- name: struct<first, last> |-- codes: array<struct<id, display>>
        >>> flatten_schema(df.schema)
        ['name.first', 'name.last', 'codes.id', 'codes.display']

        >>> flatten_schema(df.schema, include_arrays=True)
        ['name.first', 'name.last', 'codes']

    Warning:
        The paths returned for array contents (e.g., 'codes.id') cannot be selected
        directly. You must first explode the array column. Use get_array_fields() to
        identify which fields need explosion.
    """
    fields = []
    for field in schema.fields:
        name = "{}.{}".format(prefix, field.name) if prefix else field.name
        dtype = field.dataType

        if isinstance(dtype, ArrayType):
            if include_arrays:
                # Stop at array, don't recurse
                fields.append(name)
            else:
                # Unwrap ArrayType to get element type
                element_type = dtype.elementType
                if isinstance(element_type, StructType):
                    fields += flatten_schema(element_type, prefix=name, include_arrays=include_arrays)
                else:
                    # Array of primitives
                    fields.append(name)
        elif isinstance(dtype, StructType):
            fields += flatten_schema(dtype, prefix=name, include_arrays=include_arrays)
        else:
            fields.append(name)

    return fields


def get_array_fields(schema, prefix=None):
    # type: (StructType, str) -> List[str]
    """
    Get all array-type fields from a schema.

    Useful for identifying which columns need to be exploded before their
    nested contents can be accessed.

    Parameters:
        schema: Spark StructType schema
        prefix (str, optional): Prefix for nested fields

    Returns:
        List[str]: List of array field paths

    Example:
        >>> # Schema: root |-- name: string |-- codes: array |-- meds: array
        >>> get_array_fields(df.schema)
        ['codes', 'meds']
    """
    arrays = []
    for field in schema.fields:
        name = "{}.{}".format(prefix, field.name) if prefix else field.name
        dtype = field.dataType

        if isinstance(dtype, ArrayType):
            arrays.append(name)
            # Do NOT recurse into array element types — nested arrays inside
            # array elements should not be counted as separate top-level arrays.
            # Over-counting here triggers the multi-array branch in flattenTable(),
            # which skips explosion AND drops struct subfields.
        elif isinstance(dtype, StructType):
            arrays += get_array_fields(dtype, prefix=name)

    return arrays


def get_struct_fields(schema, prefix=None):
    # type: (StructType, str) -> List[str]
    """
    Get all struct-type fields from a schema (excludes arrays of structs).

    These are fields that can be flattened without explosion.

    Parameters:
        schema: Spark StructType schema
        prefix (str, optional): Prefix for nested fields

    Returns:
        List[str]: List of struct field paths
    """
    structs = []
    for field in schema.fields:
        name = "{}.{}".format(prefix, field.name) if prefix else field.name
        dtype = field.dataType

        if isinstance(dtype, StructType):
            structs.append(name)
            structs += get_struct_fields(dtype, prefix=name)
        # Don't recurse into arrays - those need explosion first

    return structs


def flat_schema(in_table, spark_session=None, prefix=None):
    # type: (DataFrame, object, str) -> List[str]
    """
    Get flattened schema from a table name or DataFrame.

    Parameters:
        in_table: Either a DataFrame or table name string
        spark_session: SparkSession (optional, uses global if not provided)
        prefix (str, optional): Prefix for field names

    Returns:
        List[str]: Flattened field paths

    Example:
        >>> flat_schema('my_schema.my_table')
        ['personid', 'name.first', 'name.last', 'dob']
    """
    if isinstance(in_table, str):
        session = spark_session or spark
        in_table = session.table(in_table)
    return flatten_schema(in_table.schema, prefix)


def flatSchema(in_table, prefix=None):
    # type: (DataFrame, str) -> List[str]
    """
    Alias for flat_schema for backwards compatibility.

    Deprecated: Use flat_schema() instead.
    """
    return flatten_schema(in_table.schema, prefix)


def extractTableLocations(tableList, schema):
    # type: (List[str], str) -> dict
    """
    Create a mapping from table names to their full schema.table paths.

    Parameters:
        tableList (List[str]): List of table names
        schema (str): Schema/database name

    Returns:
        dict: Dictionary mapping table name -> schema.table_name

    Example:
        >>> tables = ['encounter', 'person', 'condition']
        >>> extractTableLocations(tables, 'my_schema')
        {'encounter': 'my_schema.encounter',
         'person': 'my_schema.person',
         'condition': 'my_schema.condition'}
    """
    return {table: "{}.{}".format(schema, table) for table in tableList}


def fields_reconcile(fields, inTable, delim='_'):
    # type: (List[str], DataFrame, str) -> List[str]
    """
    Reconcile field names with actual schema fields.

    Takes a list of field names (possibly with underscore delimiters) and
    returns the actual field names as they exist in the table schema.
    Handles both underscore and dot notation.

    Parameters:
        fields (List[str]): List of requested field names
        inTable: DataFrame to check fields against
        delim (str): Delimiter used in requested field names

    Returns:
        List[str]: Actual field names from the table schema

    Example:
        >>> # If table has 'name.first', 'name.last'
        >>> fields_reconcile(['name_first', 'name_last'], df)
        ['name.first', 'name.last']
    """
    fschema = flat_schema(inTable)
    fields_lower = [field.lower() for field in fields]

    actual_fields = [
        field for field in fschema if (
            field.lower() in fields_lower or
            field.replace(".", '_').lower() in fields_lower or
            field.replace("_", '.').lower() in fields_lower
        )
    ]
    return actual_fields


def translate_index(index, table, byIndex=True):
    # type: (object, DataFrame, bool) -> List[str]
    """
    Translate index field names to match table schema.

    Parameters:
        index: String or list of index field names
        table: DataFrame to check against
        byIndex (bool): Whether indexing is being used

    Returns:
        List[str]: Translated index field names
    """
    if isinstance(index, str):
        index = [index]

    if byIndex:
        return fields_reconcile(index, table)
    return index


def deduplicate_fields(table, fields):
    # type: (DataFrame, List[str]) -> List[str]
    """
    Remove duplicate fields and validate against table schema.

    Parameters:
        table: DataFrame to validate against
        fields (List[str]): List of field names

    Returns:
        List[str]: Deduplicated list of valid field names
    """
    valid_fields = fields_reconcile(fields, table)
    seen = set()
    result = []
    for field in valid_fields:
        if field not in seen:
            seen.add(field)
            result.append(field)
    return result


def get_root_columns(flat_columns):
    # type: (List[str]) -> List[str]
    """
    Extract root column names from flattened schema paths.

    Parameters:
        flat_columns (List[str]): Flattened column paths

    Returns:
        List[str]: Unique root-level column names

    Example:
        >>> get_root_columns(['name.first', 'name.last', 'age'])
        ['name', 'age']
    """
    roots = set()
    for col in flat_columns:
        root = col.split('.')[0]
        roots.add(root)
    return list(roots)


def get_standard_id_elements(columns, patterns, debug=False):
    # type: (List[str], List[str], bool) -> List[str]
    """
    Extract element names that match specified patterns.

    Used to identify standard fields like 'gender.standard.id' and extract
    the element name ('gender').

    Parameters:
        columns (List[str]): Column names to search
        patterns (List[str]): Patterns to match (e.g., 'standard.id')
        debug (bool): Enable debug output

    Returns:
        List[str]: Extracted element names

    Example:
        >>> cols = ['gender.standard.id', 'race.standard.id', 'personid']
        >>> get_standard_id_elements(cols, ['standard.id'])
        ['gender', 'race']
    """
    extracted_words = set()

    for column in columns:
        for pattern in patterns:
            # Match word before .pattern
            regex_preceding = r'(\w+)\.' + re.escape(pattern)
            # Match exact pattern
            regex_exact = r'^(' + re.escape(pattern) + ')$'

            match_preceding = re.search(regex_preceding, column, re.IGNORECASE)
            match_exact = re.search(regex_exact, column, re.IGNORECASE)

            if match_preceding:
                extracted_words.add(match_preceding.group(1))
                if debug:
                    logger.debug("Matched preceding: {}".format(match_preceding.group(1)))
            elif match_exact:
                extracted_words.add(match_exact.group(1))
                if debug:
                    logger.debug("Matched exact: {}".format(match_exact.group(1)))

    return list(extracted_words)


def describe_schema(schema, indent=0):
    # type: (StructType, int) -> str
    """
    Create a human-readable description of a schema.

    Useful for debugging and understanding complex nested schemas.

    Parameters:
        schema: Spark StructType schema
        indent (int): Indentation level

    Returns:
        str: Formatted schema description

    Example:
        >>> print(describe_schema(df.schema))
        personid: string
        name: struct
          first: string
          last: string
        codes: array<struct>
          id: string
          display: string
    """
    lines = []
    prefix = "  " * indent

    for field in schema.fields:
        dtype = field.dataType

        if isinstance(dtype, ArrayType):
            element = dtype.elementType
            if isinstance(element, StructType):
                lines.append("{}{}:  [ARRAY] (explode before accessing nested fields)".format(prefix, field.name))
                lines.append(describe_schema(element, indent + 1))
            else:
                lines.append("{}{}: array<{}>".format(prefix, field.name, element.simpleString()))
        elif isinstance(dtype, StructType):
            lines.append("{}{}: struct".format(prefix, field.name))
            lines.append(describe_schema(dtype, indent + 1))
        else:
            lines.append("{}{}: {}".format(prefix, field.name, dtype.simpleString()))

    return "\n".join(lines)

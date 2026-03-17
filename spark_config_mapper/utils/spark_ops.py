"""
spark_config_mapper/utils/spark_ops.py

Spark DataFrame operations for writing, transforming, and manipulating data.
Compatible with Python 3.6+
"""

from spark_config_mapper.header import (
    re, spark, F, DataFrame, ArrayType, StructType, List, get_logger
)

logger = get_logger(__name__)


def writeTable(df, outTable, description="", partitionBy=None, mode="overwrite"):
    # type: (DataFrame, str, str, str, str) -> None
    """
    Write a DataFrame to a Spark table with optional partitioning.

    Parameters:
        df (DataFrame): DataFrame to write
        outTable (str): Full table path (schema.table_name)
        description (str): Table description/comment
        partitionBy (str, optional): Column to partition by
        mode (str): Write mode ('overwrite', 'append', 'ignore', 'error')

    Example:
        >>> writeTable(processed_df, 'project.cohort_table',
        ...            description='Patient cohort', partitionBy='tenant')
    """
    try:
        writer = df.write.mode(mode)

        if partitionBy and partitionBy in df.columns:
            writer = writer.partitionBy(partitionBy)

        writer.saveAsTable(outTable)

        if description:
            try:
                # Escape single quotes in description to prevent SQL injection
                safe_description = description.replace("'", "''")
                # Validate table name is a legal identifier
                if not re.match(r'^[a-zA-Z_]\w*(\.[a-zA-Z_]\w*)*$', outTable):
                    logger.warning("Invalid table name format: {}".format(outTable))
                else:
                    spark.sql("ALTER TABLE {} SET TBLPROPERTIES ('comment' = '{}')".format(
                        outTable, safe_description))
            except Exception:
                pass  # Description setting is optional

        logger.info("Table written: {}".format(outTable))

    except Exception as e:
        logger.error("Failed to write table {}: {}".format(outTable, e))
        raise


def flattenTable(df, include_patterns=None, exclude_patterns=None,
                 explode_array=None, error_on_multiple_arrays=True):
    # type: (DataFrame, List[str], List[str], str, bool) -> DataFrame
    """
    Flatten a DataFrame with nested structures into flat columns.

    Converts nested struct fields to flat columns using underscore notation
    (e.g., 'name.first' becomes 'name_first').

    ARRAY HANDLING:
    - If the schema has NO arrays: flattens all structs safely
    - If the schema has ONE array: explodes it, then flattens (safe)
    - If the schema has MULTIPLE arrays:
        - If explode_array is specified: explodes only that array
        - If error_on_multiple_arrays=True (default): raises error
        - If error_on_multiple_arrays=False: skips arrays, flattens structs only

    Parameters:
        df (DataFrame): DataFrame with nested structures
        include_patterns (List[str]): Regex patterns - only include matching columns
        exclude_patterns (List[str]): Patterns to exclude from result
        explode_array (str): Specific array column to explode (for multiple arrays)
        error_on_multiple_arrays (bool): If True, raise error when multiple arrays
            exist and explode_array is not specified. Default True.

    Returns:
        DataFrame: Flattened DataFrame

    Raises:
        ValueError: If multiple arrays exist and no explode_array specified
            (when error_on_multiple_arrays=True)

    Example:
        >>> # Single array - automatically exploded
        >>> flat_df = flattenTable(df_with_one_array)

        >>> # Multiple arrays - specify which one to explode
        >>> flat_df = flattenTable(df, explode_array='medications')

        >>> # Multiple arrays - skip arrays, flatten structs only
        >>> flat_df = flattenTable(df, error_on_multiple_arrays=False)
    """
    from spark_config_mapper.utils.introspection import (
        flatten_schema, get_array_fields
    )

    # Identify arrays in schema
    array_fields = get_array_fields(df.schema)
    num_arrays = len(array_fields)

    logger.debug("Found {} array fields: {}".format(num_arrays, array_fields))

    # Handle array explosion
    result_df = df

    if num_arrays == 0:
        # No arrays - safe to flatten everything
        logger.debug("No arrays found, flattening structs only")

    elif num_arrays == 1:
        # Single array - safe to explode
        array_col = array_fields[0]
        logger.info("Exploding single array column: {}".format(array_col))
        result_df = result_df.withColumn(array_col, F.explode_outer(F.col(array_col)))

    else:
        # Multiple arrays - need explicit handling
        if explode_array:
            # User specified which array to explode
            if explode_array not in array_fields:
                raise ValueError(
                    "explode_array='{}' not found in array fields: {}".format(
                        explode_array, array_fields))
            logger.info("Exploding specified array: {} (other arrays: {})".format(
                explode_array, [a for a in array_fields if a != explode_array]))
            result_df = result_df.withColumn(
                explode_array, F.explode_outer(F.col(explode_array)))
        elif error_on_multiple_arrays:
            raise ValueError(
                "DataFrame has {} arrays: {}. "
                "Exploding multiple arrays creates cartesian product. "
                "Either specify explode_array='column_name' to explode one array, "
                "or set error_on_multiple_arrays=False to skip arrays and flatten structs only.".format(
                    num_arrays, array_fields))
        else:
            logger.warning(
                "Multiple arrays found ({}), skipping array explosion. "
                "Only flattening struct columns.".format(array_fields))

    # Get flattened column paths
    # Always use include_arrays=True so we stop at array boundaries.
    # Array columns cannot be selected via dot notation without exploding first.
    # Struct fields (even nested inside struct parents) are selectable via dot notation.
    flat_cols = flatten_schema(result_df.schema, include_arrays=True)
    logger.debug("flatten_schema returned {} paths".format(len(flat_cols)))

    # Apply include patterns (if specified)
    # Patterns use underscore notation (e.g., '^conditioncode_standard_')
    # but flat_cols uses dot notation (e.g., 'conditioncode.standard.id').
    # Match against the underscore-replaced name for compatibility.
    if include_patterns:
        filtered_cols = []
        for col in flat_cols:
            flat_name = col.replace('.', '_')
            for pattern in include_patterns:
                if re.search(pattern, flat_name, re.IGNORECASE):
                    filtered_cols.append(col)
                    break
        logger.debug("regex filtered: {} -> {} cols".format(len(flat_cols), len(filtered_cols)))
        flat_cols = filtered_cols

    # Apply exclude patterns
    if exclude_patterns:
        for pattern in exclude_patterns:
            flat_cols = [c for c in flat_cols if pattern.lower() not in c.lower()]

    if not flat_cols:
        logger.warning("No columns remain after filtering")
        return result_df

    # Build select expressions
    select_cols = []
    for col_path in flat_cols:
        new_name = col_path.replace('.', '_')
        select_cols.append(F.col(col_path).alias(new_name))

    return result_df.select(select_cols)


def explode_single_array(df, array_column, flatten=True):
    # type: (DataFrame, str, bool) -> DataFrame
    """
    Explode a single array column and optionally flatten the result.

    This is a safe operation that only multiplies rows by array length.

    Parameters:
        df (DataFrame): Input DataFrame
        array_column (str): Name of array column to explode
        flatten (bool): If True, also flatten any nested structs

    Returns:
        DataFrame: DataFrame with array exploded

    Example:
        >>> # Explode medications array
        >>> meds_df = explode_single_array(df, 'medications')
        >>> # Columns: personid, medications_code, medications_name, ...
    """
    from spark_config_mapper.utils.introspection import flatten_schema

    # Verify column exists and is an array
    if array_column not in df.columns:
        raise ValueError("Column '{}' not found in DataFrame".format(array_column))

    field_type = df.schema[array_column].dataType
    if not isinstance(field_type, ArrayType):
        raise ValueError("Column '{}' is not an array type".format(array_column))

    # Explode the array
    result = df.withColumn(array_column, F.explode_outer(F.col(array_column)))

    if flatten:
        # Flatten all columns including the exploded one
        flat_cols = flatten_schema(result.schema, include_arrays=True)
        select_cols = [F.col(c).alias(c.replace('.', '_')) for c in flat_cols]
        result = result.select(select_cols)

    return result


def distCol(cols, masterList=None):
    # type: (List[str], List[str]) -> List[str]
    """
    Get distinct columns, optionally filtered by a master list.

    Parameters:
        cols (List[str]): Column names
        masterList (List[str], optional): List to filter against

    Returns:
        List[str]: Distinct column names
    """
    seen = set()
    result = []
    for col in cols:
        if col not in seen:
            if masterList is None or col in masterList:
                seen.add(col)
                result.append(col)
    return result


def checkIndex(df, index):
    # type: (DataFrame, List[str]) -> bool
    """
    Check if index columns exist in DataFrame.

    Parameters:
        df (DataFrame): DataFrame to check
        index (List[str]): Index column names

    Returns:
        bool: True if all index columns exist
    """
    return all(col in df.columns for col in index)


def convert_date_fields(date_fields):
    # type: (List[str]) -> callable
    """
    Create a function to convert string fields to date type.

    Parameters:
        date_fields (List[str]): Fields to convert

    Returns:
        Callable: Function that converts date fields in a DataFrame
    """
    def _convert(df):
        result = df
        for field in date_fields:
            if field in result.columns:
                result = result.withColumn(field, F.to_date(F.col(field)))
        return result
    return _convert


def use_last_value(col_names, windowSpec):
    # type: (List[str], object) -> callable
    """
    Create a function to get the last non-null value for columns within a window.

    Parameters:
        col_names (List[str]): Columns to process
        windowSpec: Window specification

    Returns:
        Callable: Function that applies last value logic
    """
    def _apply(df):
        result = df
        for col_name in col_names:
            if col_name in result.columns:
                result = result.withColumn(
                    col_name,
                    F.last(col_name, ignorenulls=True).over(windowSpec)
                )
        return result
    return _apply


def explode_columns(columns):
    # type: (list) -> callable
    """
    Return a function that explodes array columns.

    Parameters:
        columns (list): List of array column names to explode.

    Returns:
        callable: A function that takes a DataFrame and returns it with exploded columns.
    """
    def inner(df):
        for column in columns:
            df = df.withColumn(column, F.explode_outer(column))
        return df
    return inner


def create_empty_df(schema):
    # type: (StructType) -> DataFrame
    """
    Create an empty DataFrame with the given schema.

    Parameters:
        schema: Spark StructType schema

    Returns:
        DataFrame: Empty DataFrame with specified schema
    """
    return spark.createDataFrame([], schema)


def getColumnMapping(df, pattern):
    # type: (DataFrame, str) -> dict
    """
    Get columns matching a pattern with their new names.

    Parameters:
        df (DataFrame): DataFrame to inspect
        pattern (str): Regex pattern for columns

    Returns:
        dict: Mapping of original names to new names
    """
    compiled = re.compile(pattern, re.IGNORECASE)
    mapping = {}
    for col in df.columns:
        if compiled.search(col):
            new_name = col.replace('.', '_')
            mapping[col] = new_name
    return mapping


def assignPropertyFromDictionary(prop, inDict, debug=False):
    # type: (str, dict, bool) -> any
    """
    Safely get a property from a dictionary.

    Parameters:
        prop (str): The property/key name.
        inDict (dict): The dictionary to search.
        debug (bool): If True, prints debug info.

    Returns:
        The value if found, None otherwise.
    """
    if prop in inDict.keys():
        return inDict[prop]
    return None

"""
spark_config_mapper/utils

Utility modules for Spark operations, introspection, and parameter handling.
"""

from spark_config_mapper.utils.introspection import (
    coalesce,
    flatten_schema,
    flat_schema,
    extractTableLocations,
    fields_reconcile,
    translate_index,
    deduplicate_fields,
    get_root_columns,
    get_standard_id_elements
)

from spark_config_mapper.utils.spark_ops import (
    writeTable,
    flattenTable,
    explode_columns,
    distCol,
    checkIndex,
    convert_date_fields,
    assignPropertyFromDictionary,
    create_empty_df,
    getColumnMapping
)

from spark_config_mapper.utils.list_ops import (
    noColColide,
    unique_non_none,
    find_single_level_items,
    is_single_level,
    get_element_index,
    escape_and_bound_dot,
    escape_and_bound_dot_udf,
    preprocess_string,
    extractTableName,
    filter_columns_by_pattern
)

from spark_config_mapper.utils.parameters import (
    get_default_args,
    missingParameters,
    get_parameters,
    getParameters,
    set_function_parameters,
    setFunctionParameters,
    set_default_params
)

__all__ = [
    # Introspection
    'coalesce',
    'flatten_schema',
    'flat_schema',
    'extractTableLocations',
    'fields_reconcile',
    'translate_index',
    'deduplicate_fields',
    'get_root_columns',
    'get_standard_id_elements',
    # Spark operations
    'writeTable',
    'flattenTable',
    'explode_columns',
    'distCol',
    'checkIndex',
    'convert_date_fields',
    'assignPropertyFromDictionary',
    'create_empty_df',
    'getColumnMapping',
    # List operations
    'noColColide',
    'unique_non_none',
    'find_single_level_items',
    'is_single_level',
    'get_element_index',
    'escape_and_bound_dot',
    'escape_and_bound_dot_udf',
    'preprocess_string',
    'extractTableName',
    'filter_columns_by_pattern',
    # Parameters
    'get_default_args',
    'missingParameters',
    'get_parameters',
    'getParameters',
    'set_function_parameters',
    'setFunctionParameters',
    'set_default_params'
]

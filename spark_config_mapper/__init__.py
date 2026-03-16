"""
spark-config-mapper

Generic configuration management for Spark projects.
Provides YAML-based configuration loading with template substitution,
dynamic schema mapping, and table discovery utilities.

This package was extracted from the lhn (HealthEIntent) package to provide
reusable configuration management capabilities without healthcare-specific code.

Usage:
    from spark_config_mapper import read_config, processDataTables

    # Load configuration with template substitution
    config = read_config('000-config.yaml', {'today': '2025-01-19'})

    # Process tables from schema
    r = processDataTables(config['RWDTables'], schema='my_schema', ...)

    # Access tables
    df = r.encounter.df

Logging:
    # Default is WARNING level (quiet, only problems shown)

    # Enable debug logging to see what's happening
    import spark_config_mapper
    spark_config_mapper.set_log_level('DEBUG')

    # Or use convenience functions
    spark_config_mapper.verbose()   # Same as DEBUG
    spark_config_mapper.silence()   # No output at all

    # Or via environment variable (before import):
    # export SPARK_CONFIG_MAPPER_LOG_LEVEL=DEBUG
"""

__version__ = '0.1.0'
__author__ = 'Harlan Nelson'

# Logging configuration (import first so other modules can use it)
from spark_config_mapper.logging_config import (
    get_logger,
    configure_logging,
    set_log_level,
    silence,
    verbose
)

# Header module exports
from spark_config_mapper.header import (
    spark,
    get_or_create_spark_session
)

# Config module exports
from spark_config_mapper.config import (
    read_config,
    recursive_template,
    merge_configs,
    validate_table_config,
    validate_tables_config,
    validate_required_keys,
    ConfigValidationError
)

# Schema module exports
from spark_config_mapper.schema import (
    database_exists,
    getTableList,
    getListOfTables,
    check_table_existence,
    get_table_columns,
    search_tables,
    Item,
    TableList,
    processDataTables,
    update_dictionary,
    ItemLoadError,
    ItemProcessError,
    ITEM_UNLOADED,
    ITEM_LOADED,
    ITEM_PROCESSED,
    ITEM_FAILED,
    ITEM_NOT_FOUND,
)

# Utils module exports
from spark_config_mapper.utils import (
    # Introspection
    coalesce,
    flatten_schema,
    flat_schema,
    extractTableLocations,
    fields_reconcile,
    translate_index,
    deduplicate_fields,
    get_root_columns,
    get_standard_id_elements,
    # Spark operations
    writeTable,
    flattenTable,
    explode_columns,
    distCol,
    checkIndex,
    convert_date_fields,
    assignPropertyFromDictionary,
    create_empty_df,
    getColumnMapping,
    # List operations
    noColColide,
    unique_non_none,
    find_single_level_items,
    is_single_level,
    get_element_index,
    escape_and_bound_dot,
    escape_and_bound_dot_udf,
    preprocess_string,
    extractTableName,
    filter_columns_by_pattern,
    # Parameters
    get_default_args,
    missingParameters,
    get_parameters,
    getParameters,
    set_function_parameters,
    setFunctionParameters,
    set_default_params
)

__all__ = [
    # Version
    '__version__',
    '__author__',
    # Logging
    'get_logger',
    'configure_logging',
    'set_log_level',
    'silence',
    'verbose',
    # Header
    'spark',
    'get_or_create_spark_session',
    # Config
    'read_config',
    'recursive_template',
    'merge_configs',
    'validate_table_config',
    'validate_tables_config',
    'validate_required_keys',
    'ConfigValidationError',
    # Schema discovery
    'database_exists',
    'getTableList',
    'getListOfTables',
    'check_table_existence',
    'get_table_columns',
    'search_tables',
    # Schema mapping
    'Item',
    'TableList',
    'processDataTables',
    'update_dictionary',
    'ItemLoadError',
    'ItemProcessError',
    'ITEM_UNLOADED',
    'ITEM_LOADED',
    'ITEM_PROCESSED',
    'ITEM_FAILED',
    'ITEM_NOT_FOUND',
    # Utils - Introspection
    'coalesce',
    'flatten_schema',
    'flat_schema',
    'extractTableLocations',
    'fields_reconcile',
    'translate_index',
    'deduplicate_fields',
    'get_root_columns',
    'get_standard_id_elements',
    # Utils - Spark operations
    'writeTable',
    'flattenTable',
    'explode_columns',
    'distCol',
    'checkIndex',
    'convert_date_fields',
    'assignPropertyFromDictionary',
    'create_empty_df',
    'getColumnMapping',
    # Utils - List operations
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
    # Utils - Parameters
    'get_default_args',
    'missingParameters',
    'get_parameters',
    'getParameters',
    'set_function_parameters',
    'setFunctionParameters',
    'set_default_params'
]

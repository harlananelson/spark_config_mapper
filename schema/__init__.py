"""
spark_config_mapper/schema

Schema discovery and mapping module.
"""

from spark_config_mapper.schema.discovery import (
    database_exists,
    getTableList,
    getListOfTables,
    check_table_existence,
    get_table_columns,
    search_tables
)

from spark_config_mapper.schema.mapper import (
    Item,
    TableList,
    processDataTables,
    update_dictionary
)

__all__ = [
    # Discovery
    'database_exists',
    'getTableList',
    'getListOfTables',
    'check_table_existence',
    'get_table_columns',
    'search_tables',
    # Mapper
    'Item',
    'TableList',
    'processDataTables',
    'update_dictionary'
]

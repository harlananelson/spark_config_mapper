"""
spark_config_mapper/schema/discovery.py

Schema and table discovery functions for Spark environments.
Provides utilities to inspect databases, list tables, and verify existence.
"""

from spark_config_mapper.header import (
    spark, F, get_logger, List, Dict
)

logger = get_logger(__name__)


def database_exists(database_name: str) -> bool:
    """
    Check if a database/schema exists in the Spark catalog.
    
    Parameters:
        database_name (str): Name of the database to check
    
    Returns:
        bool: True if database exists, False otherwise
    
    Example:
        >>> if database_exists('my_schema'):
        ...     df = spark.table('my_schema.my_table')
    """
    try:
        databases = [db.name for db in spark.catalog.listDatabases()]
        return database_name in databases
    except Exception as e:
        logger.warning(f"Error checking database existence: {e}")
        return False


def getTableList(schema: str) -> List[str]:
    """
    Get list of all table names in a schema.
    
    Parameters:
        schema (str): Schema/database name
    
    Returns:
        List[str]: List of table names (without schema prefix)
    
    Example:
        >>> tables = getTableList('iuhealth_ed_data')
        >>> 'encounter' in tables
        True
    """
    try:
        tables = spark.catalog.listTables(schema)
        return [table.name for table in tables]
    except Exception as e:
        logger.error(f"Error listing tables in {schema}: {e}")
        return []


def getListOfTables(schema: str) -> Dict[str, str]:
    """
    Get dictionary of tables with their creation dates.
    
    Parameters:
        schema (str): Schema/database name
    
    Returns:
        Dict[str, str]: Dictionary mapping table names to creation dates
    
    Example:
        >>> tables = getListOfTables('my_schema')
        >>> tables['encounter']
        '2024-01-15'
    """
    try:
        tables = spark.catalog.listTables(schema)
        result = {}
        for table in tables:
            try:
                # Try to get table properties for creation date
                desc = spark.sql(f"DESCRIBE FORMATTED {schema}.{table.name}").collect()
                created = None
                for row in desc:
                    if 'Created Time' in str(row[0]):
                        created = row[1]
                        break
                result[table.name] = created or 'unknown'
            except:
                result[table.name] = 'unknown'
        return result
    except Exception as e:
        logger.error(f"Error getting table list: {e}")
        return {}


def check_table_existence(table_path: str) -> bool:
    """
    Check if a specific table exists.
    
    Parameters:
        table_path (str): Full table path (schema.table_name)
    
    Returns:
        bool: True if table exists, False otherwise
    
    Example:
        >>> check_table_existence('my_schema.encounter')
        True
    """
    try:
        # Use single-argument form for PySpark 2.4 compatibility
        return spark.catalog.tableExists(table_path)
    except Exception as e:
        logger.debug(f"Table existence check failed for {table_path}: {e}")
        return False


def get_table_columns(table_path: str) -> List[str]:
    """
    Get column names for a table.
    
    Parameters:
        table_path (str): Full table path (schema.table_name)
    
    Returns:
        List[str]: List of column names
    """
    try:
        df = spark.table(table_path)
        return df.columns
    except Exception as e:
        logger.error(f"Error getting columns for {table_path}: {e}")
        return []


def search_tables(schema: str, pattern: str) -> List[str]:
    """
    Search for tables matching a regex pattern.
    
    Parameters:
        schema (str): Schema to search in
        pattern (str): Regex pattern to match table names
    
    Returns:
        List[str]: List of matching table names
    """
    import re
    tables = getTableList(schema)
    compiled = re.compile(pattern, re.IGNORECASE)
    return [t for t in tables if compiled.search(t)]

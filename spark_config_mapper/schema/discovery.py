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
        >>> tables = getTableList('real_world_data_ed')
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


def _flatten_fields(dtype, prefix=""):
    """Yield (dotted_name, type_string, nullable) for a Spark StructType, recursing into
    nested struct fields (and the element struct of arrays-of-struct)."""
    from pyspark.sql.types import StructType, ArrayType
    for field in dtype.fields:
        name = f"{prefix}{field.name}"
        ft = field.dataType
        yield (name, ft.simpleString(), bool(field.nullable))
        if isinstance(ft, StructType):
            for sub in _flatten_fields(ft, prefix=f"{name}."):
                yield sub
        elif isinstance(ft, ArrayType) and isinstance(ft.elementType, StructType):
            for sub in _flatten_fields(ft.elementType, prefix=f"{name}."):
                yield sub


def discover_full_catalog(schemas=None, include_counts=False, flatten_structs=True,
                          skip_schemas=None):
    """
    Build a COMPREHENSIVE catalog of catalogs/schemas -> tables -> fields (+ metadata).

    This is the all-schemas counterpart to the project-scoped ``dump_pipeline_schema``. The
    result is a single searchable dict (dump to JSON) that downstream tools — e.g. the
    notebook field-name checker's ``--schema`` option — consume to validate column
    references against the *actual* live metastore.

    Parameters:
        schemas (List[str] | None): Schemas to catalog in full (tables + fields). ``None``
            catalogs EVERY database in the metastore — this is a heavy sweep (SHOW TABLES +
            schema read per table); prefer passing an explicit list. The full database-NAME
            list is always recorded under ``all_schemas`` regardless.
        include_counts (bool): If True, add a row count per table (expensive — one Spark
            action per table). Default False.
        flatten_structs (bool): If True, also record dotted/flattened field paths for nested
            struct and array-of-struct columns (matches how lhn flattens nested columns to
            ``parent_child``). Default True.
        skip_schemas (List[str] | None): Schema names to skip even if in ``schemas``.

    Returns:
        dict: {
          "metadata": {"all_schemas": [...], "n_schemas_cataloged": int, "include_counts": bool},
          "schemas": {
             "<schema>": {"tables": {
                "<table>": {
                   "fields": [{"name","type","nullable"}, ...],   # top-level
                   "flat_fields": ["a","b","parent_child", ...],  # incl. nested (if flatten)
                   "n_rows": int (if include_counts),
                   "error": "..." (only if the table could not be read)
                }, ...}}, ...}}

    Example:
        >>> cat = discover_full_catalog(schemas=['real_world_data_ed_feb_2026'])
        >>> import json; json.dump(cat, open('hdl_full_catalog.json','w'), indent=1)
    """
    from pyspark.sql.types import StructType
    skip = set(skip_schemas or [])
    try:
        all_dbs = sorted(db.name for db in spark.catalog.listDatabases())
    except Exception as e:
        logger.error(f"Could not list databases: {e}")
        all_dbs = []

    targets = [s for s in (schemas if schemas is not None else all_dbs) if s not in skip]
    if schemas is None:
        logger.warning(f"discover_full_catalog: cataloging ALL {len(targets)} schemas in full "
                       "— heavy metastore sweep. Pass `schemas=[...]` to scope.")

    out = {"metadata": {"all_schemas": all_dbs, "n_schemas_cataloged": 0,
                        "include_counts": include_counts},
           "schemas": {}}

    for schema in targets:
        tables = getTableList(schema)
        logger.info(f"cataloging {schema}: {len(tables)} tables")
        sblock = {"tables": {}}
        for t in tables:
            fqn = f"{schema}.{t}"
            entry = {}
            try:
                sdf = spark.table(fqn).schema
                entry["fields"] = [{"name": f.name, "type": f.dataType.simpleString(),
                                    "nullable": bool(f.nullable)} for f in sdf.fields]
                if flatten_structs:
                    entry["flat_fields"] = [n for n, _t, _nl in _flatten_fields(sdf)]
                if include_counts:
                    entry["n_rows"] = spark.table(fqn).count()
            except Exception as e:
                entry["error"] = str(e)[:200]
            sblock["tables"][t] = entry
        out["schemas"][schema] = sblock
        out["metadata"]["n_schemas_cataloged"] += 1
    return out


def search_catalog(catalog: dict, pattern: str, kind: str = "field"):
    """
    Search a catalog dict (from ``discover_full_catalog``) by regex.

    Parameters:
        catalog (dict): Output of ``discover_full_catalog``.
        pattern (str): Regex (case-insensitive) to match.
        kind (str): "field" (default), "table", or "schema".

    Returns:
        List[str]: Matches as "schema.table.field" / "schema.table" / "schema".
    """
    import re
    rx = re.compile(pattern, re.IGNORECASE)
    hits = []
    for schema, sblock in catalog.get("schemas", {}).items():
        if kind == "schema":
            if rx.search(schema):
                hits.append(schema)
            continue
        for table, entry in sblock.get("tables", {}).items():
            if kind == "table":
                if rx.search(table):
                    hits.append(f"{schema}.{table}")
                continue
            for fname in entry.get("flat_fields") or [f["name"] for f in entry.get("fields", [])]:
                if rx.search(fname):
                    hits.append(f"{schema}.{table}.{fname}")
    return hits

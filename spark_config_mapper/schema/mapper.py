"""
spark_config_mapper/schema/mapper.py

Schema mapping functions for dynamic database/table discovery and processing.
Maps logical schema names to physical Spark schemas and processes table metadata.
"""

import warnings
from spark_config_mapper.header import (
    re, spark, pprint, get_logger, DataFrame, F
)
from spark_config_mapper.schema.discovery import database_exists, getTableList
from spark_config_mapper.utils.introspection import extractTableLocations, flat_schema, flatSchema

logger = get_logger(__name__)


# Item lifecycle status constants
ITEM_UNLOADED = 'UNLOADED'
ITEM_LOADED = 'LOADED'
ITEM_PROCESSED = 'PROCESSED'
ITEM_FAILED = 'FAILED'
ITEM_NOT_FOUND = 'NOT_FOUND'


class ItemLoadError(Exception):
    """Raised when an Item's DataFrame cannot be loaded."""
    pass


class ItemProcessError(Exception):
    """Raised when Item.process() fails in strict mode."""
    pass


class Item:
    """
    Represents a single data table with its metadata and configuration.

    An Item encapsulates all the metadata needed to work with a specific table
    in the Spark environment, including its location, field mappings, and
    output paths for various formats.

    Attributes:
        name (str): Table name
        location (str): Full schema.table path in Spark
        status (str): Lifecycle status — UNLOADED, LOADED, PROCESSED, FAILED, NOT_FOUND
        df (DataFrame): Spark DataFrame reference (lazy loaded)
        csv (str): Path for CSV export
        parquet (str): Path for Parquet export
        fields (dict): Field configuration from YAML
        load_error (str): Error message if loading failed, None otherwise
        process_error (str): Error message if processing failed, None otherwise
    """

    def __init__(self, TBL, dataTables, TBLLoc, schema, dataLoc, disease,
                 schemaTag, project, parquetLoc, debug=False):
        """
        Initialize an Item from configuration and discovered table metadata.

        Parameters:
            TBL (str): Table name key from dataTables
            dataTables (dict): Table configurations from YAML
            TBLLoc (dict): Discovered table locations (name -> schema.name)
            schema (str): Schema name
            dataLoc (str): Base path for CSV exports
            disease (str): Disease/project tag for naming
            schemaTag (str): Schema version tag
            project (str): Project name
            parquetLoc (str): HDFS base path for Parquet files
            debug (bool): Enable debug logging
        """
        self.name = TBL
        self.schema = schema
        self.schemaTag = schemaTag
        self.disease = disease
        self.project = project

        # Get table configuration from dataTables
        table_config = dataTables.get(TBL, {})

        # Determine actual table name - use 'source' field if present, otherwise use TBL
        # This allows config keys like 'mortalitySource' to map to actual table 'mortality'
        source_name = table_config.get('source', TBL)
        # Handle schema.table format in source
        if '.' in str(source_name):
            source_name = source_name.split('.', 1)[-1]
        self.source = source_name

        # Look up source_name in discovered tables (case-insensitive)
        TBLLoc_lower = {k.lower(): v for k, v in TBLLoc.items()}
        if source_name.lower() in TBLLoc_lower:
            self.location = TBLLoc_lower[source_name.lower()]
            self.exists = True
            self.status = ITEM_UNLOADED
        else:
            self.location = f"{schema}.{source_name}"
            self.exists = False
            self.status = ITEM_NOT_FOUND
            # Always warn about missing tables so users know what's happening
            if source_name != TBL:
                logger.warning(f"Table '{source_name}' (config: {TBL}) not found in schema {schema}")
            else:
                logger.warning(f"Table '{TBL}' not found in schema {schema}")

        # Set up export paths
        self.csv = f"{dataLoc}{TBL}_{disease}_{schemaTag}.csv"
        self.parquet = f"{parquetLoc}{TBL}_{disease}_{schemaTag}"

        # Copy configuration attributes
        for key, value in table_config.items():
            setattr(self, key, value)

        # Initialize DataFrame reference (lazy) and error tracking
        self._df = None
        self.load_error = None
        self.process_error = None

        if debug:
            logger.info(f"Created Item: {self.name} at {self.location} (status={self.status})")

    @property
    def df(self):
        """Lazy-load DataFrame when first accessed."""
        if self._df is None and self.exists:
            try:
                self._df = spark.table(self.location)
                self.status = ITEM_LOADED
                self.load_error = None
            except Exception as e:
                self.status = ITEM_FAILED
                self.load_error = str(e)
                logger.error(f"Failed to load table {self.location}: {e}")
        return self._df

    @df.setter
    def df(self, value):
        """Allow setting DataFrame directly."""
        self._df = value
        if value is not None and self.status not in (ITEM_PROCESSED,):
            self.status = ITEM_LOADED

    def process(self, strict=False):
        """
        Process this item's source table using config-driven directives.

        Applies three steps in order:
        1. inputRegex: Use flattenTable() to explode arrays AND flatten structs,
           then filter columns by regex patterns. This matches v0.1.0 behavior
           where inputRegex triggered full flattening (array explosion + struct
           flattening + column selection).
        2. insert: Apply a list of PySpark code strings (e.g.
           "withColumn('dt', F.to_timestamp(...))" ) via eval(). This matches
           the config-RWD.yaml format where insert is a list, not a dict.
        3. colsRename: Rename columns via withColumnRenamed().

        Parameters:
            strict (bool): If True, raise ItemProcessError on any step failure.
                If False (default), log warnings and continue.

        Mutates self.df in place. Returns None.
        """
        if not self.exists or self.df is None:
            return

        df = self.df

        # Step 1: Flatten nested schema + inputRegex filter
        # Uses flattenTable() which handles arrays (explode) AND structs (flatten),
        # then selects columns matching the regex patterns.
        if hasattr(self, 'inputRegex') and self.inputRegex:
            try:
                from spark_config_mapper.utils.spark_ops import flattenTable
                regex_list = self.inputRegex if isinstance(self.inputRegex, list) else [self.inputRegex]
                df = flattenTable(
                    df,
                    include_patterns=regex_list,
                    error_on_multiple_arrays=False
                )
            except Exception as ex:
                msg = f"inputRegex flatten failed for {self.name}: {ex}"
                if strict:
                    self.status = ITEM_FAILED
                    self.process_error = msg
                    raise ItemProcessError(msg) from ex
                logger.warning(msg)

        # Step 2: Apply insert directives (list of PySpark code strings)
        if hasattr(self, 'insert') and self.insert:
            for insert_code in self.insert:
                try:
                    df = eval(f"df.{insert_code}")
                except Exception as ex:
                    msg = f"insert failed for {self.name} '{insert_code}': {ex}"
                    if strict:
                        self.status = ITEM_FAILED
                        self.process_error = msg
                        raise ItemProcessError(msg) from ex
                    logger.warning(msg)

        # Step 3: Apply colsRename
        if hasattr(self, 'colsRename') and self.colsRename:
            for old_col, new_col in self.colsRename.items():
                if old_col in df.columns:
                    df = df.withColumnRenamed(old_col, new_col)

        self.df = df
        self.status = ITEM_PROCESSED
        self.process_error = None

    def flatten(self, **kwargs):
        """
        Flatten this item's DataFrame using flattenTable.

        Parameters:
            **kwargs: Passed to flattenTable (include_patterns, exclude_patterns,
                      explode_array, error_on_multiple_arrays)

        Returns:
            DataFrame: Flattened DataFrame
        """
        from spark_config_mapper.utils.spark_ops import flattenTable
        if self.df is None:
            logger.error("No DataFrame available to flatten")
            return None
        return flattenTable(self.df, **kwargs)

    def properties(self):
        """Return dictionary of all item properties."""
        return {k: v for k, v in self.__dict__.items() if not k.startswith('_')}


class TableList:
    """
    Container for multiple Item instances with dictionary-like access.

    Provides both attribute-style (tbl.encounter) and dictionary-style
    (tbl['encounter']) access to tables.
    """

    def __init__(self, items: dict):
        """
        Initialize TableList from dictionary of Items.

        Parameters:
            items (dict): Dictionary mapping table names to Item instances
        """
        self._items = items
        for name, item in items.items():
            setattr(self, name, item)

    def __getitem__(self, key):
        return self._items[key]

    def __contains__(self, key):
        return key in self._items

    def keys(self):
        return self._items.keys()

    def values(self):
        return self._items.values()

    def items(self):
        return self._items.items()

    def __iter__(self):
        return iter(self._items)

    def __len__(self):
        return len(self._items)

    def report(self):
        """
        Return a diagnostic summary of all items and their lifecycle status.

        Returns:
            list[dict]: List of dicts with keys: name, source, location,
                status, exists, load_error, process_error, columns.

        Example:
            >>> r = processDataTables(...)
            >>> for row in r.report():
            ...     print(f"{row['name']:30s} {row['status']:12s} {row['columns']}")
        """
        rows = []
        for name, item in self._items.items():
            col_count = len(item.df.columns) if item._df is not None else 0
            rows.append({
                'name': name,
                'source': getattr(item, 'source', name),
                'location': item.location,
                'status': item.status,
                'exists': item.exists,
                'load_error': item.load_error,
                'process_error': item.process_error,
                'columns': col_count,
            })
        return rows

    def report_str(self):
        """
        Return a formatted string summary of all items.

        Example:
            >>> print(r.report_str())
            Name                           Status       Cols  Errors
            encounterSource                PROCESSED      12
            conditionSource                PROCESSED       8
            labSource                      NOT_FOUND       0  Table 'lab' not found
        """
        rows = self.report()
        lines = [
            "{:<30s} {:<12s} {:>5s}  {}".format('Name', 'Status', 'Cols', 'Errors')
        ]
        for row in rows:
            error = row['load_error'] or row['process_error'] or ''
            lines.append("{:<30s} {:<12s} {:>5d}  {}".format(
                row['name'], row['status'], row['columns'], error
            ))
        return '\n'.join(lines)

    def load_into_local(self, everything=False):
        # type: (bool) -> dict
        """
        Return dictionary suitable for loading items into local namespace.

        This enables the pattern:
            locals().update(r.load_into_local())
            # Now access tables directly: death.df, encounter.df

        Parameters:
            everything (bool): If True, include all items regardless of existence.
                If False (default), only include items where the table exists.

        Returns:
            dict: Dictionary mapping table names to Item instances

        Example:
            >>> r = processDataTables(config['RWDTables'], schema='my_schema', ...)
            >>> locals().update(r.load_into_local(everything=False))
            >>> # Now you can use:
            >>> death.df.count()  # instead of r.death.df.count()
            >>> encounter.df.show()
        """
        if everything:
            return dict(self._items)
        else:
            return {name: item for name, item in self._items.items() if item.exists}


def processDataTables(dataTables, schema, dataLoc, disease, schemaTag,
                      project, parquetLoc, debug=False, strict=False):
    """
    Process table configurations and return a TableList of Item instances.

    This is the primary function for converting YAML table configurations into
    live Spark table references. It discovers tables in the schema and creates
    Item instances for each configured table.

    Parameters:
        dataTables (dict): Table configurations from YAML, keyed by table name.
            Each entry contains field mappings and other metadata.
        schema (str): Spark schema/database name where tables exist
        dataLoc (str): Base file path for CSV exports
        disease (str): Disease/project tag for naming exports
        schemaTag (str): Schema version tag
        project (str): Project name
        parquetLoc (str): HDFS base path for Parquet exports
        debug (bool): Enable debug logging
        strict (bool): If True, raise on any table processing failure.
            If False (default), log warnings and continue (lenient mode).

    Returns:
        TableList: Container of Item instances, or None if schema doesn't exist

    Example:
        >>> dataTables = config['RWDTables']
        >>> r = processDataTables(
        ...     dataTables,
        ...     schema='iuhealth_ed_data_cohort_202306',
        ...     dataLoc='/data/exports/',
        ...     disease='SCD',
        ...     schemaTag='RWD',
        ...     project='SickleCell',
        ...     parquetLoc='hdfs:///user/project/'
        ... )
        >>> r.encounter.df.count()
        >>> # Check status of all tables:
        >>> print(r.report_str())
    """
    if not database_exists(schema):
        logger.error(f"Database {schema} does not exist")
        return None

    # Discover all tables in schema
    tableList = getTableList(schema)
    TBLLoc = extractTableLocations(tableList, schema)

    logger.debug(f"Processing tables: {list(dataTables.keys())}")

    # Create Item for each configured table
    TBLSource = {}
    for TBL in dataTables.keys():
        item = Item(
            TBL, dataTables, TBLLoc, schema, dataLoc,
            disease, schemaTag, project, parquetLoc, debug=debug
        )
        if item:
            TBLSource[TBL] = item

    # Auto-process all items (flatten, insert, rename)
    errors = []
    for name, item in TBLSource.items():
        if item.exists and item.df is not None:
            try:
                item.process(strict=strict)
                logger.debug(f"Processed {name}: {len(item.df.columns)} columns")
            except ItemProcessError as e:
                errors.append(str(e))
                if strict:
                    # In strict mode, collect all errors then raise summary
                    continue

    if strict and errors:
        raise ItemProcessError(
            "processDataTables failed for {} table(s):\n  - {}".format(
                len(errors), '\n  - '.join(errors)))

    return TableList(TBLSource)


def update_dictionary(schema_dict, schema, projectSchema, schemaTag, 
                      obs, reRun, debug, personid, tableNameTemplate):
    """
    Update a configuration dictionary with discovered schema metadata.
    
    Scans a schema for tables and adds their metadata to the configuration
    dictionary, optionally filtering by a table name template pattern.
    
    Parameters:
        schema_dict (dict): Existing dictionary to update (or None for new)
        schema (str): Schema to scan for tables
        projectSchema (str): Output schema for processed tables
        schemaTag (str): Tag for naming
        obs (int): Observation limit for processing
        reRun (bool): Whether to reprocess existing tables
        debug (bool): Enable debug logging
        personid (str): Primary identifier field name
        tableNameTemplate (str): Regex pattern to filter/extract table names
    
    Returns:
        dict: Updated configuration dictionary with table metadata
    
    Example:
        >>> tables = update_dictionary(
        ...     None, 'source_schema', 'output_schema', 
        ...     'v1', 1000000, False, False, 'personid', 'cohort_'
        ... )
    """
    from spark_config_mapper.schema.discovery import getTableList
    
    # Get table list from schema
    table_list = getTableList(schema)
    TBLLoc = extractTableLocations(table_list, schema)
    
    # Start with existing dict or create new
    new_dict = schema_dict.copy() if schema_dict else {}
    
    # Compile pattern if provided
    pattern = re.compile(rf"^(.*?){tableNameTemplate}") if tableNameTemplate else None
    logger.debug(f"Using tableNameTemplate pattern: {pattern}")
    
    for key, value in TBLLoc.items():
        logger.debug(f"Processing: {key}:{value}")
        
        new_key = key
        if pattern:
            match = pattern.match(key)
            if match:
                new_key = match.group(1).rstrip('_')
            else:
                new_key = key
        
        # Ensure unique keys
        original_new_key = new_key
        counter = 1
        while new_key in new_dict:
            new_key = f"{original_new_key}_{counter}"
            counter += 1
        
        # Add table metadata
        try:
            table_schema = flat_schema(spark.table(value))
            new_dict[new_key] = {
                'source': value,
                'inputRegex': [f'^{field}$' for field in table_schema]
            }
        except Exception as e:
            logger.warning(f"Could not get schema for {value}: {e}")
            new_dict[new_key] = {
                'source': value,
                'inputRegex': []
            }
    
    return new_dict

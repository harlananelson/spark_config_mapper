"""
spark_config_mapper/schema/mapper.py

Schema mapping functions for dynamic database/table discovery and processing.
Maps logical schema names to physical Spark schemas and processes table metadata.
"""

from spark_config_mapper.header import (
    re, spark, pprint, get_logger, DataFrame
)
from spark_config_mapper.schema.discovery import database_exists, getTableList
from spark_config_mapper.utils.introspection import extractTableLocations, flat_schema

logger = get_logger(__name__)


class Item:
    """
    Represents a single data table with its metadata and configuration.
    
    An Item encapsulates all the metadata needed to work with a specific table
    in the Spark environment, including its location, field mappings, and
    output paths for various formats.
    
    Attributes:
        name (str): Table name
        location (str): Full schema.table path in Spark
        df (DataFrame): Spark DataFrame reference (lazy loaded)
        csv (str): Path for CSV export
        parquet (str): Path for Parquet export
        fields (dict): Field configuration from YAML
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
        
        # Determine table location
        if TBL in TBLLoc:
            self.location = TBLLoc[TBL]
            self.exists = True
        else:
            self.location = f"{schema}.{TBL}"
            self.exists = False
            if debug:
                logger.warning(f"Table {TBL} not found in schema {schema}")
        
        # Set up export paths
        self.csv = f"{dataLoc}{TBL}_{disease}_{schemaTag}.csv"
        self.parquet = f"{parquetLoc}{TBL}_{disease}_{schemaTag}"
        
        # Copy configuration attributes
        for key, value in table_config.items():
            setattr(self, key, value)
        
        # Initialize DataFrame reference (lazy)
        self._df = None
        
        if debug:
            logger.info(f"Created Item: {self.name} at {self.location}")
    
    @property
    def df(self):
        """Lazy-load DataFrame when first accessed."""
        if self._df is None and self.exists:
            try:
                self._df = spark.table(self.location)
            except Exception as e:
                logger.error(f"Failed to load table {self.location}: {e}")
        return self._df
    
    @df.setter
    def df(self, value):
        """Allow setting DataFrame directly."""
        self._df = value
    
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
                      project, parquetLoc, debug=False):
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

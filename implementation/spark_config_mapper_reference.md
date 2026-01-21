# Spark Config Mapper: LLM Reference Document

> **Purpose**: Machine-readable reference for LLM context injection. Provides precise definitions, resolution rules, and patterns for the spark_config_mapper package.

---

## 1. PACKAGE OVERVIEW

**spark_config_mapper** is a generic Python library for Apache Spark configuration management. It provides:

1. **YAML Configuration Loading** - Load hierarchical configs with template substitution
2. **Schema Discovery** - Discover and validate Spark catalog schemas
3. **Table Mapping** - Map logical table names to physical Spark tables
4. **Utilities** - Common Spark DataFrame operations and schema introspection

**This package has NO healthcare-specific logic** - it can be used in any Spark-based data project.

---

## 2. GLOSSARY OF TERMS

| Term | Type | Definition | Example |
|------|------|------------|---------|
| `read_config` | Function | Load YAML file with ${variable} substitution | `config = read_config('config.yaml', replace={'today': '2025-01-20'})` |
| `recursive_template` | Function | Apply cascading variable substitution to nested dicts | `recursive_template(config, {'var': 'value'})` |
| `processDataTables` | Function | Convert config table definitions to Item objects | `tables = processDataTables(spark, config, schema, data_type)` |
| `Item` | Class | Represents a single data table with metadata and DataFrame reference | `item.df`, `item.location`, `item.existsDF` |
| `TableList` | Class | Dictionary-like container of Item objects with attribute access | `table_list.my_table` or `table_list['my_table']` |
| `database_exists` | Function | Check if a Spark catalog schema exists | `database_exists(spark, 'my_schema')` |
| `getTableList` | Function | List all tables in a Spark schema | `tables = getTableList(spark, 'my_schema')` |
| `writeTable` | Function | Write DataFrame to Spark table with options | `writeTable(df, 'schema.table', partitionBy='date')` |
| `flattenTable` | Function | Flatten nested arrays/structs in DataFrame | `flat_df = flattenTable(df, inclusionRegex=['^field.*'])` |
| `setFunctionParameters` | Function | Map config dict to function parameters | `params = setFunctionParameters(config, func)` |

---

## 3. PACKAGE STRUCTURE

```
spark_config_mapper/
├── __init__.py              # Public API exports
├── header.py                # Core imports, logging, Spark session reference
├── config/
│   ├── __init__.py
│   └── loader.py            # read_config, recursive_template, merge_configs
├── schema/
│   ├── __init__.py
│   ├── discovery.py         # database_exists, getTableList, search_tables
│   └── mapper.py            # Item, TableList, processDataTables
└── utils/
    ├── __init__.py
    ├── introspection.py     # flatten_schema, fields_reconcile, deduplicate_fields
    ├── spark_ops.py         # writeTable, flattenTable, convert_date_fields
    ├── list_ops.py          # noColColide, unique_non_none, find_single_level_items
    └── parameters.py        # setFunctionParameters, getParameters
```

---

## 4. CONFIGURATION LOADING

### 4.1 read_config Function

```python
from spark_config_mapper import read_config

config = read_config(
    config_file='path/to/config.yaml',
    replace={
        'today': '2025-01-20',
        'project': 'MyProject',
        'basePath': '/home/user/work'
    },
    debug=False
)
```

### 4.2 Template Substitution Rules

Template variables use `${variable_name}` syntax:

```yaml
# In YAML config file
project: MyProject
startDate: ${today}
dataLoc: "${basePath}/data/${project}/"
outputSchema: ${project}_output
```

**Resolution order**:
1. Variables in `replace` dict (passed to read_config)
2. Variables defined earlier in same config file
3. Variables from parent/merged configs

**Important**: Variables must be defined before they are referenced.

### 4.3 Configuration Merging

When loading multiple config files, later files override earlier ones:

```python
from spark_config_mapper.config import merge_configs

base_config = read_config('base.yaml')
project_config = read_config('project.yaml')

merged = merge_configs(base_config, project_config)
# project_config values override base_config values
```

---

## 5. SCHEMA DISCOVERY

### 5.1 Check Schema Existence

```python
from spark_config_mapper.schema import database_exists

if database_exists(spark, 'my_database'):
    print("Database exists")
else:
    print("Database not found")
```

### 5.2 List Tables in Schema

```python
from spark_config_mapper.schema import getTableList

tables = getTableList(spark, 'my_database')
# Returns: ['table1', 'table2', 'table3']
```

### 5.3 Search Tables by Pattern

```python
from spark_config_mapper.schema import search_tables

# Find tables matching pattern
matching = search_tables(spark, 'my_database', pattern='patient.*')
```

---

## 6. TABLE MAPPING

### 6.1 Table Definition Structure

In YAML config, tables are defined with metadata:

```yaml
MyTables:
  conditionSource:
    source: condition              # Physical table name
    datefield: effectiveDate       # Primary date column
    inputRegex:                    # Column filtering patterns
      - ^personid
      - ^conditionCode_
      - ^effectiveDate
    insert:                        # PySpark transformations
      - "withColumn('dateCol', F.to_timestamp(F.col('effectiveDate')))"
    colsRename:                    # Column renaming
      old_name: new_name
```

### 6.2 processDataTables Function

Convert config definitions to Item objects:

```python
from spark_config_mapper.schema import processDataTables

table_list = processDataTables(
    spark=spark,
    config_dict=config,
    schema='my_database',
    data_type='MyTables',          # Key in config containing table definitions
    type_key='src',                # Attribute name for TableList
    updateDict=False               # If True, auto-discover tables from schema
)
```

### 6.3 Item Class

Each Item represents a table:

```python
# Access Item properties
item = table_list.conditionSource
item.name          # 'conditionSource' (config key)
item.source        # 'condition' (physical table name)
item.schema        # 'my_database'
item.location      # 'my_database.condition'
item.df            # Spark DataFrame
item.existsDF      # True if table exists
item.csv           # Auto-generated CSV path
item.parquet       # Auto-generated Parquet path
item.inputRegex    # Column filter patterns from config
```

### 6.4 TableList Class

Container for Items with attribute access:

```python
# Access tables
table_list.conditionSource       # Returns Item
table_list['conditionSource']    # Same as above

# Iterate
for name, item in table_list.items():
    print(f"{name}: {item.df.count()} rows")

# Load into local namespace (notebook pattern)
locals().update(table_list.load_into_local(everything=False))
# Now access directly: conditionSource.df instead of table_list.conditionSource.df
```

### 6.5 load_into_local Method

Load table Items directly into the local namespace for cleaner notebook code:

```python
# Load only tables that exist in the schema
locals().update(table_list.load_into_local(everything=False))

# Load all configured tables (even if not found)
locals().update(table_list.load_into_local(everything=True))

# Parameters:
#   everything (bool): If False (default), only include tables where item.exists is True
#                      If True, include all configured tables

# After loading, access tables directly:
death.df.count()       # instead of table_list.death.df.count()
encounter.df.filter(F.col('date') > '2024-01-01')
```

**Common pattern in Quarto/Jupyter notebooks:**

```python
#| include: false
# Hidden setup cell
from spark_config_mapper import read_config, processDataTables
config = read_config('000-config.yaml', {'today': '2025-01-20'})
e = processDataTables(config['RWDTables'], schema=config['schemas']['RWDSchema'], ...)
locals().update(e.load_into_local(everything=False))
```

---

## 7. SPARK OPERATIONS

### 7.1 Write DataFrame to Table

```python
from spark_config_mapper.utils import writeTable

result = writeTable(
    DF=my_df,
    outTable='schema.table_name',
    partitionBy='tenant',           # Optional: partition column
    description='Table description',
    removeDuplicates=True           # Optional: deduplicate before write
)
```

### 7.2 Flatten Nested Structures

```python
from spark_config_mapper.utils import flattenTable

# Flatten nested arrays and structs
flat_df = flattenTable(
    df=nested_df,
    inclusionRegex=['^code.*', '^name.*'],  # Columns to flatten
    maxTrys=5                                # Max flattening iterations
)
```

### 7.3 Date Conversion

```python
from spark_config_mapper.utils import convert_date_fields

# Convert string columns to date type
df = convert_date_fields(['date1', 'date2', 'date3'])(df)
```

---

## 8. SCHEMA INTROSPECTION

### 8.1 Flatten Schema

```python
from spark_config_mapper.utils import flatten_schema

# Get flat list of all fields in nested schema
fields = flatten_schema(df.schema, separator='_')
# Returns: ['field1', 'nested_field_subfield', ...]
```

### 8.2 Reconcile Fields

```python
from spark_config_mapper.utils import fields_reconcile

# Match user-provided field names to actual schema fields
matched = fields_reconcile(
    requested_fields=['personID', 'encounter_id'],
    actual_fields=df.columns,
    case_insensitive=True
)
```

### 8.3 Deduplicate Column Names

```python
from spark_config_mapper.utils import deduplicate_fields

# Handle duplicate column names after joins
unique_fields = deduplicate_fields(df.columns)
```

---

## 9. LIST OPERATIONS

### 9.1 Column Name Collision Handling

```python
from spark_config_mapper.utils import noColColide

# Get columns avoiding name collisions
safe_cols = noColColide(
    columns=source_df.columns,
    colideColumns=other_df.columns,
    index=['personid'],              # Always include these
    masterList=allowed_columns       # Restrict to these
)
```

### 9.2 Find Single-Level Items

```python
from spark_config_mapper.utils import find_single_level_items

# Extract simple key-value pairs from nested dict
simple_items = find_single_level_items(nested_dict)
```

---

## 10. FUNCTION PARAMETERS

### 10.1 Map Config to Function Parameters

```python
from spark_config_mapper.utils import setFunctionParameters

def my_function(start_date, end_date, filters=None):
    pass

# Config dict with matching keys
config = {
    'start_date': '2020-01-01',
    'end_date': '2024-12-31',
    'filters': {'active': True}
}

# Map config to function parameters
params = setFunctionParameters(config, my_function)
# Returns: {'start_date': '2020-01-01', 'end_date': '2024-12-31', 'filters': {'active': True}}

# Call function with mapped parameters
my_function(**params)
```

---

## 11. COMMON PATTERNS

### 11.1 Complete Configuration Loading Pattern

```python
from spark_config_mapper import read_config
from spark_config_mapper.schema import database_exists, processDataTables
from pathlib import Path

# Load configuration with variable substitution
config = read_config(
    config_file='000-config.yaml',
    replace={
        'today': '2025-01-20',
        'basePath': str(Path.home() / 'work')
    }
)

# Check schema exists
schema_name = config['schemas']['mySchema']
if database_exists(spark, schema_name):
    # Create table objects
    tables = processDataTables(
        spark=spark,
        config_dict=config,
        schema=schema_name,
        data_type='myTables'
    )

    # Access tables
    df = tables.my_table.df
```

### 11.2 Multi-Schema Configuration Pattern

```python
# Config file with multiple schemas
schemas:
  sourceSchema: source_database
  outputSchema: project_output

mySourceTables:
  table1:
    source: actual_table_name

myOutputTables:
  results:
    label: "Analysis results"
```

```python
# Process each schema
for schema_key in ['sourceSchema', 'outputSchema']:
    schema_name = config['schemas'][schema_key]
    if database_exists(spark, schema_name):
        data_type = 'mySourceTables' if 'source' in schema_key.lower() else 'myOutputTables'
        tables = processDataTables(spark, config, schema_name, data_type)
```

---

## 12. ERROR HANDLING

| Error | Cause | Resolution |
|-------|-------|------------|
| `KeyError: 'variable'` | Undefined template variable | Ensure variable is defined in `replace` dict or earlier in config |
| `Database not found` | Schema doesn't exist | Verify schema name in Spark catalog |
| `Table not found` | Table missing from schema | Check physical table name in `source` field |
| `Empty DataFrame` | No data or column regex matched nothing | Verify `inputRegex` patterns against actual columns |
| `Column not found` | Wrong column name | Check column names with `df.columns` |

---

## 13. DEPENDENCIES

```python
# Required
pyspark >= 3.0.0
pyyaml >= 5.0

# Optional (for extended functionality)
pandas >= 1.0.0
numpy >= 1.18.0
```

---

## 14. IMPORT REFERENCE

```python
# Top-level imports
from spark_config_mapper import (
    # Config loading
    read_config,
    recursive_template,
    merge_configs,

    # Schema operations
    database_exists,
    getTableList,
    search_tables,
    processDataTables,

    # Classes
    Item,
    TableList,

    # Utilities
    writeTable,
    flattenTable,
    flatten_schema,
    noColColide,
    setFunctionParameters,
    convert_date_fields
)
```

# spark-config-mapper

Generic configuration management for Apache Spark projects with YAML-based configuration loading and dynamic schema mapping.

## Overview

`spark-config-mapper` provides a reusable configuration management layer for PySpark applications. It was extracted from a healthcare analytics package to offer these capabilities without domain-specific dependencies.

### Key Features

- **YAML Configuration Loading**: Read and process hierarchical YAML configurations with cascading template substitution
- **Dynamic Schema Mapping**: Map logical schema names to physical Spark databases at runtime
- **Table Discovery**: Automatically discover and catalog tables within Spark schemas
- **Template Substitution**: Support for `$variable` and `${variable}` syntax with cascading references
- **Schema Introspection**: Flatten nested Spark schemas, reconcile field names, and analyze structure

## Installation

```bash
# From source (development mode)
pip install -e .

# From PyPI (when published)
pip install spark-config-mapper
```

## Quick Start

### Basic Configuration Loading

```python
from spark_config_mapper import read_config

# Define template substitutions
replace = {
    'today': '2025-01-19',
    'project': 'MyProject',
    'projectSchema': 'myproject_data'
}

# Load configuration with substitutions
config = read_config('000-config.yaml', replace)

# Access configuration values
schema = config['schemas']['RWDSchema']
tables = config['RWDTables']
```

### Processing Data Tables

```python
from spark_config_mapper import processDataTables, read_config

# Load configuration
config = read_config('000-config.yaml', {'today': '2025-01-19'})

# Process tables from schema into accessible objects
r = processDataTables(
    dataTables=config['RWDTables'],
    schema=config['schemas']['RWDSchema'],
    dataLoc='/data/exports/',
    disease='MyDisease',
    schemaTag='v1',
    project='MyProject',
    parquetLoc='hdfs:///user/myproject/'
)

# Access tables by name
encounter_df = r.encounter.df
person_df = r.person.df

# Tables have metadata
print(r.encounter.location)  # schema.table_name
print(r.encounter.csv)       # export path
```

### Schema Discovery

```python
from spark_config_mapper import database_exists, getTableList, search_tables

# Check if database exists
if database_exists('my_schema'):
    # List all tables
    tables = getTableList('my_schema')
    
    # Search for specific tables
    encounter_tables = search_tables('my_schema', 'encounter.*')
```

## Configuration File Structure

### Project Configuration (`000-config.yaml`)

```yaml
project: MyProject
schemaTag: RWD

# Schema mappings: logical name -> physical schema
schemas:
  RWDSchema: my_data_schema_202401
  projectSchema: myproject_output
  omopSchema: omop_cdm_v5

# Table configurations
RWDTables:
  encounter:
    label: "Patient Encounters"
    indexFields: ["personid", "encounterid"]
    inputRegex:
      - "^personid$"
      - "^encounterid$"
      - "^servicedate$"
  
  person:
    label: "Person Demographics"
    indexFields: ["personid"]
```

### Template Substitution

Templates use Python's `string.Template` syntax:

```yaml
# Earlier values can be referenced by later ones
baseSchema: my_data
dataSchema: ${baseSchema}_v1
outputPath: /data/${project}/${today}/
```

## API Reference

### Configuration

| Function | Description |
|----------|-------------|
| `read_config(yaml_file, replace, debug=False)` | Load YAML with template substitution |
| `recursive_template(d, replace, top_call=True)` | Recursively apply template substitutions |
| `merge_configs(*configs)` | Deep merge multiple configuration dicts |

### Schema Discovery

| Function | Description |
|----------|-------------|
| `database_exists(database_name)` | Check if database exists |
| `getTableList(schema)` | Get list of table names |
| `check_table_existence(table_path)` | Check if specific table exists |
| `search_tables(schema, pattern)` | Search tables by regex pattern |

### Schema Mapping

| Function | Description |
|----------|-------------|
| `processDataTables(...)` | Process YAML config into TableList |
| `update_dictionary(...)` | Update config with discovered tables |

### Utilities

| Function | Description |
|----------|-------------|
| `flatten_schema(schema)` | Flatten nested schema to dot notation |
| `flat_schema(table)` | Get flat schema from table/DataFrame |
| `writeTable(df, outTable, ...)` | Write DataFrame to Spark table |
| `noColColide(master, other, index)` | Resolve column name collisions |
| `setFunctionParameters(func, params, config)` | Map config to function params |

## Architecture

```
spark_config_mapper/
├── __init__.py          # Package exports
├── header.py            # Core imports, logging, Spark session
├── config/
│   ├── __init__.py
│   └── loader.py        # YAML loading, template substitution
├── schema/
│   ├── __init__.py
│   ├── discovery.py     # Database/table discovery
│   └── mapper.py        # Item, TableList, processDataTables
└── utils/
    ├── __init__.py
    ├── introspection.py # Schema introspection utilities
    ├── spark_ops.py     # DataFrame operations
    ├── list_ops.py      # List/collection utilities
    └── parameters.py    # Function parameter handling
```

## Dependencies

- PySpark >= 3.0.0
- PyYAML >= 5.0
- Pandas >= 1.0.0
- NumPy >= 1.18.0

## License

MIT License

## Contributing

Contributions welcome! Please submit issues and pull requests to the GitHub repository.

# Spark Config Mapper: Tutorial

> **Purpose**: Step-by-step guide for human developers. Learn the configuration system through concrete examples and practical workflows.

---

## Introduction: What Problem Does This Solve?

Working with Apache Spark often involves:
- Managing multiple database schemas (dev, staging, production)
- Keeping configuration separate from code
- Applying consistent transformations across tables
- Handling schema discovery and validation

**spark_config_mapper** provides:
1. **Configuration-driven table access** - Switch schemas by changing YAML, not code
2. **Template substitution** - Use variables like `${today}` or `${project}` in config
3. **Consistent patterns** - Same code works across different schemas
4. **Metadata-rich table objects** - Know where data comes from and what it contains

---

## Part 1: Configuration Files

### 1.1 Basic Configuration Structure

Create a YAML configuration file (`config.yaml`):

```yaml
# Project identification
project: SalesAnalysis
outputSchema: sales_analysis_2025

# Variable definitions (available for ${} substitution)
startDate: "2020-01-01"
stopDate: ${today}

# Schema mappings: logical name -> physical Spark schema
schemas:
  sourceSchema: company_data_warehouse
  outputSchema: ${outputSchema}

# Table definitions
sourceTables:
  orders:
    source: fact_orders           # Physical table name
    datefield: order_date
    inputRegex:                   # Only include columns matching these patterns
      - ^order_id
      - ^customer_id
      - ^order_date
      - ^total_amount

  customers:
    source: dim_customers
    inputRegex:
      - ^customer_id
      - ^name
      - ^region
      - ^segment

outputTables:
  sales_summary:
    label: "Monthly sales summary by region"
    indexFields: ["region", "month"]
```

### 1.2 Template Variables

Use `${variable}` syntax for substitution:

```yaml
project: MyProject
dataLoc: "/data/${project}/"          # Becomes: /data/MyProject/
startDate: "2020-01-01"
analysisStart: ${startDate}           # Becomes: 2020-01-01
```

**Built-in variables** (passed at load time):
- `${today}` - Current date (you provide this)
- `${basePath}` - Base path for files (you provide this)

**Rules**:
- Variables must be defined before use
- Variables can reference other variables
- String values with variables should be quoted: `"${var}/path"`

### 1.3 Multiple Configuration Files

For larger projects, split configuration into multiple files:

```
configuration/
├── config-global.yaml      # Shared settings
├── config-source.yaml      # Source table definitions
└── 000-config.yaml         # Project-specific settings
```

Load and merge them:

```python
from spark_config_mapper import read_config
from spark_config_mapper.config import merge_configs

# Load each config
global_config = read_config('config-global.yaml', replace={'today': '2025-01-20'})
source_config = read_config('config-source.yaml')
project_config = read_config('000-config.yaml')

# Merge (later configs override earlier ones)
config = merge_configs(global_config, source_config, project_config)
```

---

## Part 2: Loading Configuration

### 2.1 Basic Loading

```python
from spark_config_mapper import read_config

# Load with variable substitution
config = read_config(
    config_file='config.yaml',
    replace={
        'today': '2025-01-20',
        'basePath': '/home/user/work'
    }
)

# Access configuration values
project = config['project']
schemas = config['schemas']
source_schema = schemas['sourceSchema']
```

### 2.2 Debugging Configuration Loading

```python
# Enable debug mode to see substitution details
config = read_config('config.yaml', replace={'today': '2025-01-20'}, debug=True)
```

This prints:
- Original values before substitution
- Substituted values
- Any unresolved variables

---

## Part 3: Schema Discovery

### 3.1 Check If Schema Exists

Always verify a schema exists before trying to use it:

```python
from spark_config_mapper.schema import database_exists

schema_name = config['schemas']['sourceSchema']

if database_exists(spark, schema_name):
    print(f"Schema '{schema_name}' exists")
else:
    print(f"Schema '{schema_name}' not found - check your configuration")
```

### 3.2 List Tables in a Schema

```python
from spark_config_mapper.schema import getTableList

tables = getTableList(spark, 'company_data_warehouse')
print(f"Found {len(tables)} tables:")
for table in tables:
    print(f"  - {table}")
```

### 3.3 Search for Tables

```python
from spark_config_mapper.schema import search_tables

# Find tables matching a pattern
order_tables = search_tables(spark, 'company_data_warehouse', pattern='order.*')
print(f"Order-related tables: {order_tables}")
```

---

## Part 4: Table Mapping

### 4.1 Create Table Objects from Configuration

```python
from spark_config_mapper.schema import database_exists, processDataTables

# Get schema name from config
source_schema = config['schemas']['sourceSchema']

# Verify schema exists
if not database_exists(spark, source_schema):
    raise ValueError(f"Schema not found: {source_schema}")

# Create table objects
source_tables = processDataTables(
    spark=spark,
    config_dict=config,
    schema=source_schema,
    data_type='sourceTables'    # Key in config with table definitions
)
```

### 4.2 Access Tables

```python
# Access table by name (attribute style)
orders = source_tables.orders

# Access table by name (dictionary style)
customers = source_tables['customers']

# Get the DataFrame
orders_df = orders.df
customers_df = customers.df

# Check table metadata
print(f"Table: {orders.name}")
print(f"Location: {orders.location}")
print(f"Exists: {orders.existsDF}")
print(f"Date field: {orders.datefield}")
```

### 4.3 Load Tables into Local Namespace (Notebook Pattern)

For cleaner notebook code, load table objects directly into the local namespace:

```python
# Instead of repeatedly typing: source_tables.orders.df, source_tables.customers.df

# Load all existing tables into local namespace
locals().update(source_tables.load_into_local(everything=False))

# Now access tables directly:
orders.df.count()      # instead of source_tables.orders.df.count()
customers.df.show()    # instead of source_tables.customers.df.show()
death.df.filter(...)   # direct access to any table

# Parameters:
#   everything=False (default): Only load tables that exist in the schema
#   everything=True: Load all configured tables, even if not found
```

This pattern is especially useful in Quarto/Jupyter notebooks where you want clean, readable code:

```python
#| include: false
# Setup cell - hidden from output
from spark_config_mapper import read_config, processDataTables
config = read_config('000-config.yaml', {'today': '2025-01-20'})
e = processDataTables(config['RWDTables'], schema=config['schemas']['RWDSchema'], ...)
locals().update(e.load_into_local(everything=False))
```

```python
# Analysis cell - clean, readable code
encounter.df.filter(F.col('admission_date') > '2024-01-01').count()
```

### 4.4 Iterate Over All Tables

```python
# List all tables
for name, item in source_tables.items():
    if item.existsDF:
        count = item.df.count()
        print(f"{name}: {count:,} rows")
    else:
        print(f"{name}: TABLE NOT FOUND")
```

---

## Part 5: Working with DataFrames

### 5.1 Basic Queries

```python
from pyspark.sql import functions as F

# Get DataFrame from table object
orders_df = source_tables.orders.df

# Filter by date
recent_orders = orders_df.filter(
    F.col('order_date') >= config['startDate']
)

# Aggregate
monthly_sales = recent_orders.groupBy(
    F.year('order_date').alias('year'),
    F.month('order_date').alias('month')
).agg(
    F.sum('total_amount').alias('total_sales'),
    F.countDistinct('customer_id').alias('unique_customers')
)

monthly_sales.show()
```

### 5.2 Write Results to Table

```python
from spark_config_mapper.utils import writeTable

# Get output schema
output_schema = config['schemas']['outputSchema']

# Write DataFrame to Spark table
writeTable(
    DF=monthly_sales,
    outTable=f'{output_schema}.monthly_sales',
    description='Monthly sales aggregation',
    removeDuplicates=True
)
```

### 5.3 Flatten Nested Data

If your data has nested structures (common in JSON/FHIR data):

```python
from spark_config_mapper.utils import flattenTable

# Flatten nested arrays and structs
flat_df = flattenTable(
    df=nested_df,
    inclusionRegex=['^code.*', '^value.*'],  # Only flatten columns matching patterns
    maxTrys=5                                  # Max recursion depth
)
```

---

## Part 6: Column Filtering with inputRegex

### 6.1 How inputRegex Works

The `inputRegex` config option filters which columns are included:

```yaml
orders:
  source: fact_orders
  inputRegex:
    - ^order_id           # Columns starting with 'order_id'
    - ^customer           # Columns starting with 'customer'
    - ^total_amount$      # Exact match: 'total_amount'
    - date                # Columns containing 'date' anywhere
```

Only columns matching **at least one** pattern are included.

### 6.2 Why Use inputRegex?

1. **Reduce data transfer** - Don't load columns you don't need
2. **Avoid name conflicts** - Exclude columns that would collide in joins
3. **Simplify schema** - Focus on relevant columns

### 6.3 Testing Patterns

```python
import re

# Test which columns would match
table_columns = ['order_id', 'customer_id', 'product_id', 'order_date', 'ship_date', 'total_amount']
patterns = ['^order_id', '^customer', 'date']

for col in table_columns:
    matches = [p for p in patterns if re.search(p, col)]
    if matches:
        print(f"  {col} - matches: {matches}")
    else:
        print(f"  {col} - EXCLUDED")
```

---

## Part 7: Complete Example

### 7.1 Project Setup

```python
# setup_project.py
from spark_config_mapper import read_config
from spark_config_mapper.schema import database_exists, processDataTables
from datetime import datetime
from pathlib import Path

def initialize_project(spark, config_file='config.yaml'):
    """Initialize project configuration and table objects."""

    # Load configuration
    config = read_config(
        config_file=config_file,
        replace={
            'today': datetime.now().strftime('%Y-%m-%d'),
            'basePath': str(Path.home() / 'work')
        }
    )

    result = {
        'config': config,
        'tables': {}
    }

    # Process each schema
    for schema_key, schema_name in config.get('schemas', {}).items():
        if not database_exists(spark, schema_name):
            print(f"Warning: Schema '{schema_name}' not found, skipping")
            continue

        # Determine which table definitions to use
        data_type = f"{schema_key.replace('Schema', '')}Tables"
        if data_type not in config:
            continue

        # Create table objects
        tables = processDataTables(
            spark=spark,
            config_dict=config,
            schema=schema_name,
            data_type=data_type
        )

        result['tables'][schema_key] = tables
        print(f"Loaded {len(list(tables.items()))} tables from {schema_name}")

    return result
```

### 7.2 Analysis Script

```python
# analyze_sales.py
from setup_project import initialize_project
from spark_config_mapper.utils import writeTable
from pyspark.sql import functions as F

# Initialize
project = initialize_project(spark, 'config.yaml')
config = project['config']
source = project['tables'].get('sourceSchema')

if source is None:
    raise RuntimeError("Source schema not available")

# Get DataFrames
orders = source.orders.df
customers = source.customers.df

# Join orders with customers
order_details = orders.join(
    customers,
    on='customer_id',
    how='left'
)

# Aggregate by region
regional_sales = order_details.groupBy('region').agg(
    F.sum('total_amount').alias('total_sales'),
    F.countDistinct('order_id').alias('order_count'),
    F.countDistinct('customer_id').alias('customer_count')
).orderBy(F.desc('total_sales'))

# Display results
regional_sales.show()

# Write to output table
output_schema = config['schemas']['outputSchema']
writeTable(
    DF=regional_sales,
    outTable=f'{output_schema}.regional_sales_summary',
    description='Regional sales summary'
)
```

---

## Part 8: Troubleshooting

### 8.1 "Schema not found"

**Symptom**: `database_exists` returns False
**Check**:
```python
# List all databases
spark.sql("SHOW DATABASES").show(100, truncate=False)
```

**Common causes**:
- Typo in schema name
- Schema not yet created
- Insufficient permissions

### 8.2 "KeyError in config"

**Symptom**: `KeyError: 'myTables'`
**Check**:
```python
# Print available keys
print(config.keys())
```

**Common causes**:
- Config file not loaded
- Key name doesn't match config file
- Template variable not resolved

### 8.3 "Empty DataFrame"

**Symptom**: `item.df.count()` returns 0
**Check**:
```python
# Check if table exists
print(f"Table exists: {item.existsDF}")

# Check inputRegex matches
import re
actual_columns = spark.table(item.location).columns
patterns = item.inputRegex or []
for col in actual_columns[:10]:
    matches = [p for p in patterns if re.search(p, col)]
    print(f"{col}: {'INCLUDED' if matches else 'EXCLUDED'}")
```

**Common causes**:
- Table doesn't exist
- `inputRegex` patterns match no columns
- Date filters exclude all data

### 8.4 "Variable not substituted"

**Symptom**: Config value still contains `${variable}`
**Check**:
```python
# Print unresolved variables
import re
def find_unresolved(obj, path=''):
    if isinstance(obj, dict):
        for k, v in obj.items():
            find_unresolved(v, f"{path}.{k}")
    elif isinstance(obj, str) and '${' in obj:
        print(f"Unresolved at {path}: {obj}")

find_unresolved(config)
```

**Common causes**:
- Variable not in `replace` dict
- Variable referenced before definition
- Typo in variable name

---

## Quick Reference

### Configuration Loading
```python
config = read_config('config.yaml', replace={'today': '2025-01-20'})
```

### Schema Discovery
```python
exists = database_exists(spark, 'schema_name')
tables = getTableList(spark, 'schema_name')
```

### Table Mapping
```python
table_list = processDataTables(spark, config, 'schema_name', 'tableDefs')
df = table_list.my_table.df
```

### Writing Results
```python
writeTable(df, 'schema.table_name', description='My table')
```

---

## Next Steps

- See `spark_config_mapper_reference.md` for complete API documentation
- See `the_config-files/` for example configuration templates
- See the `lhn` package for healthcare-specific workflows built on this foundation

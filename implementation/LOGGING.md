# Logging Guide for spark_config_mapper

## Quick Reference

```python
import spark_config_mapper

# Default: WARNING level (only problems shown)

# Debug mode - see everything
spark_config_mapper.verbose()
# or
spark_config_mapper.set_log_level('DEBUG')

# Silence all output
spark_config_mapper.silence()

# Back to normal
spark_config_mapper.set_log_level('WARNING')
```

## Environment Variable

Set before importing the package:

```bash
# In terminal
export SPARK_CONFIG_MAPPER_LOG_LEVEL=DEBUG
python my_script.py

# Or in notebook (first cell)
import os
os.environ['SPARK_CONFIG_MAPPER_LOG_LEVEL'] = 'DEBUG'
import spark_config_mapper
```

---

## Log Levels Explained

| Level | When to Use | Example Output |
|-------|-------------|----------------|
| **CRITICAL** | Package is broken | (rarely used) |
| **ERROR** | Operation failed | `ERROR - Failed to write table: permission denied` |
| **WARNING** | Unexpected but recoverable | `WARNING - Column 'tenant' not found, skipping partitioning` |
| **INFO** | Normal milestones | `INFO - Table written: schema.table_name` |
| **DEBUG** | Diagnostic details | `DEBUG - Found 2 array fields: ['medications', 'diagnoses']` |

**Default level: WARNING** - You only see problems.

---

## Common Scenarios

### Scenario 1: "My code isn't working, I need to debug"

```python
import spark_config_mapper
spark_config_mapper.verbose()  # Enable DEBUG

# Now run your code - you'll see detailed output
from spark_config_mapper import read_config, flattenTable
config = read_config('config.yaml', {'today': '2025-01-20'})
# DEBUG - Starting recursive_template
# DEBUG - Loaded configuration from: config.yaml

flat_df = flattenTable(my_df)
# DEBUG - Found 1 array fields: ['medications']
# INFO - Exploding single array column: medications
```

### Scenario 2: "Production code, I want silence unless something breaks"

```python
import spark_config_mapper
# Default is WARNING - already quiet

# Or explicitly silence everything
spark_config_mapper.silence()
```

### Scenario 3: "I want INFO messages but not DEBUG noise"

```python
import spark_config_mapper
spark_config_mapper.set_log_level('INFO')

# Now you'll see:
# INFO - Table written: project.cohort
# INFO - Exploding single array column: medications

# But NOT:
# DEBUG - Found 2 array fields: [...]
```

### Scenario 4: "Running in a notebook, want clean output"

```python
# First cell
import spark_config_mapper
spark_config_mapper.set_log_level('WARNING')  # Quiet by default

# When debugging a specific cell
spark_config_mapper.verbose()
# ... problematic code ...
spark_config_mapper.set_log_level('WARNING')  # Back to quiet
```

---

## For Package Developers

### Getting a Logger in Your Module

```python
# In spark_config_mapper/utils/my_module.py
from spark_config_mapper.logging_config import get_logger

logger = get_logger(__name__)
# Creates: spark_config_mapper.utils.my_module

def my_function(df, table_name):
    logger.debug("my_function called with table: {}".format(table_name))

    if df.count() == 0:
        logger.warning("Empty DataFrame provided to my_function")
        return None

    try:
        result = do_something(df)
        logger.info("Successfully processed: {}".format(table_name))
        return result
    except Exception as e:
        logger.error("Failed to process {}: {}".format(table_name, e))
        raise
```

### When to Use Each Level

```python
# DEBUG - Detailed diagnostic information
logger.debug("Processing {} rows from {}".format(df.count(), table_name))
logger.debug("Found {} array fields: {}".format(len(arrays), arrays))
logger.debug("Config keys: {}".format(list(config.keys())))

# INFO - Normal operation milestones users care about
logger.info("Table written: {}".format(out_table))
logger.info("Configuration loaded from: {}".format(config_path))
logger.info("Exploding array column: {}".format(array_col))

# WARNING - Unexpected but we can continue
logger.warning("Column '{}' not found, skipping".format(col_name))
logger.warning("Empty DataFrame, no rows written")
logger.warning("Multiple arrays found ({}), skipping explosion".format(arrays))

# ERROR - Operation failed (usually before raising exception)
logger.error("Failed to write table {}: {}".format(table_name, error))
logger.error("Schema '{}' not found in catalog".format(schema_name))

# CRITICAL - Package cannot function (rarely used)
logger.critical("Spark session not available")
```

### Best Practices

1. **Include context in messages**
   ```python
   # Good
   logger.error("Failed to write table {}: {}".format(table_name, error))

   # Bad
   logger.error("Write failed")
   ```

2. **Don't log AND raise with the same message**
   ```python
   # Good - log with context, raise with type
   logger.error("Schema '{}' not found".format(schema))
   raise ValueError("Schema not found: {}".format(schema))

   # Or just raise (exception will be logged by caller if needed)
   raise ValueError("Schema not found: {}".format(schema))
   ```

3. **Use DEBUG liberally - it's free when disabled**
   ```python
   logger.debug("Entering process_table with {} rows".format(df.count()))
   logger.debug("Columns: {}".format(df.columns))
   logger.debug("Schema: {}".format(df.schema))
   ```

4. **Use .format() not f-strings (Python 3.6 compatibility)**
   ```python
   # Good
   logger.info("Processing {}".format(name))

   # Avoid (Python 3.6 might have issues in some contexts)
   logger.info(f"Processing {name}")
   ```

---

## Output Formats

The format changes based on log level:

**WARNING and above (simple):**
```
WARNING - Column 'tenant' not found, skipping partitioning
ERROR - Failed to write table: permission denied
```

**INFO (detailed):**
```
2025-01-20 10:30:45,123 - spark_config_mapper.utils.spark_ops - INFO - Table written: project.cohort
```

**DEBUG (full diagnostic):**
```
2025-01-20 10:30:45,123 - spark_config_mapper.utils.spark_ops - DEBUG - spark_ops.py:115 - Found 2 array fields: ['medications', 'diagnoses']
```

---

## Troubleshooting

### "I'm not seeing any log output"

1. Check if logging was silenced:
   ```python
   spark_config_mapper.set_log_level('INFO')
   ```

2. Check if another library configured the root logger:
   ```python
   import logging
   logging.getLogger('spark_config_mapper').setLevel(logging.DEBUG)
   ```

### "I'm seeing duplicate log messages"

This can happen if the root logger also has handlers. The package is configured to NOT propagate to root, but if you manually added handlers:

```python
import logging
pkg_logger = logging.getLogger('spark_config_mapper')
pkg_logger.handlers = []  # Clear handlers
spark_config_mapper.configure_logging('INFO')  # Reconfigure
```

### "I want to log to a file"

```python
import logging

# Get the package logger
pkg_logger = logging.getLogger('spark_config_mapper')

# Add file handler
fh = logging.FileHandler('spark_config_mapper.log')
fh.setLevel(logging.DEBUG)
fh.setFormatter(logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
))
pkg_logger.addHandler(fh)
```

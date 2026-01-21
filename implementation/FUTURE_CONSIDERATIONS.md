# Future Considerations

This document tracks potential improvements and alternatives to consider when constraints change (e.g., Python version upgrade).

---

## Configuration Template System

### Current Implementation

The `recursive_template` function in `config/loader.py` provides:
- Template substitution using Python's `string.Template` (`$key` or `${key}` syntax)
- Cascading references (later keys can reference earlier keys)
- Recursive processing of nested dicts and lists
- Zero external dependencies (stdlib only)
- Python 3.6+ compatibility

### When to Reconsider: Python 3.8+ Available

**Recommended Alternative: OmegaConf**

[OmegaConf](https://github.com/omry/omegaconf) is the industry standard for YAML configuration with interpolation, used by Facebook/Meta's [Hydra](https://hydra.cc/) framework.

**Requirements**: Python 3.8+

**Benefits over current implementation**:
- Circular reference detection
- Type safety and validation
- Custom resolvers (functions in config)
- Structured configs from dataclasses
- Extensive test coverage
- Active maintenance by Meta

**Migration Example**:

```python
# Current (recursive_template)
from spark_config_mapper.config.loader import read_config
config = read_config('config.yaml', replace={'today': '2025-01-20'})

# Future (OmegaConf)
from omegaconf import OmegaConf

# Load with built-in interpolation
config = OmegaConf.load('config.yaml')

# Add runtime values
OmegaConf.update(config, 'today', '2025-01-20')

# Resolve all interpolations
OmegaConf.resolve(config)
```

**Note on syntax**: OmegaConf uses `${key}` syntax (same as current), so YAML files would be compatible.

**Installation**:
```bash
pip install omegaconf>=2.3.0
```

---

## Python Version Upgrade Path

### Python 3.6 → 3.8 Migration Checklist

When upgrading to Python 3.8+:

1. **Replace `recursive_template` with OmegaConf**
   - Remove `config/loader.py` custom template code
   - Update `read_config` to use OmegaConf
   - Run existing tests to verify compatibility

2. **Use f-strings instead of `.format()`**
   - Current code uses `.format()` for Python 3.6 compatibility
   - Can switch to f-strings (available since 3.6 but walrus operator in 3.8)

3. **Type hints improvements**
   - Current: `# type: (str, Dict, bool) -> Dict` comments
   - Future: Native type hints with `from __future__ import annotations`

4. **Dict ordering**
   - Current behavior relies on Python 3.7+ dict ordering
   - Document this requirement or add explicit OrderedDict for 3.6

---

## Other Libraries Evaluated

### Jinja2

**Not recommended** for this use case.

- Full templating engine (overkill for config interpolation)
- Different syntax (`{{ key }}` vs `${key}`)
- Would require rewriting all config files
- Better suited for generating output files, not config loading

### Dynaconf

**Potential alternative** if multi-environment support needed.

- Supports multiple environments (dev/staging/prod)
- Multiple file formats (YAML, TOML, JSON, INI)
- Environment variable overrides
- Python 3.8+ required

### python-dotenv + PyYAML

**Simpler alternative** if only environment variable injection needed.

- Load `.env` file into environment
- Use `os.environ.get()` in Python
- Doesn't support config-to-config references

---

## Schema Flattening and Array Handling

### The Cross-Product Problem

When flattening nested data structures with multiple arrays, exploding all arrays creates a cartesian product:

```
Patient with:
- 3 medications
- 4 diagnoses

After exploding both arrays:
3 × 4 = 12 rows per patient (cartesian explosion!)
```

### Current Approach (Recommended)

The current `flatten_schema` function is designed for **controlled flattening**:

1. **`flatten_schema(schema)`** - Returns dot-notation paths, recursing INTO array element types
   - Use this to see all possible field paths
   - WARNING: Paths inside arrays cannot be selected without exploding first

2. **`flatten_schema(schema, include_arrays=True)`** - Stops at array columns
   - Use this to see which columns are "safe" to select directly

3. **`get_array_fields(schema)`** - Returns only array columns
   - Use this to identify what needs explosion

### Safe Workflow for Complex Nested Data

```python
# 1. Identify arrays
arrays = get_array_fields(df.schema)
# ['medications', 'diagnoses']

# 2. For each array you need, explode ONE at a time
meds_df = df.select('personid', F.explode('medications').alias('med'))
# Now you can access: meds_df.select('personid', 'med.code', 'med.name')

# 3. If you need multiple arrays, keep them in separate DataFrames
diag_df = df.select('personid', F.explode('diagnoses').alias('diag'))

# 4. Join on personid if needed (still creates cartesian, but explicit)
```

### Why Not Auto-Flatten Everything?

The original `flattenTable` in lhn-original attempted to:
1. Identify all arrays
2. Explode them all
3. Flatten all structs

This was problematic because:
- Unpredictable row explosion
- Memory issues with large datasets
- Implicit cartesian products hidden from user

The current approach requires **explicit explosion**, making the data transformation visible and controllable.

### Future Improvement: Spark DataFrame Nested Column Support

Spark 3.0+ has improved support for nested columns:

```python
# Direct access to nested struct (no explosion needed)
df.select('personid', 'name.first', 'name.last')

# Transform within array (no explosion)
df.withColumn('med_codes', F.transform('medications', lambda m: m.code))

# Filter within array
df.withColumn('diabetes_meds',
    F.filter('medications', lambda m: m.code.startswith('E11')))
```

Consider adding utility functions for these patterns when Spark version allows.

---

## Testing Improvements

When Python version allows:

1. **Property-based testing with Hypothesis**
   - Generate random nested structures
   - Verify substitution invariants

2. **Type checking with mypy**
   - Add `py.typed` marker
   - Full type annotations

3. **Benchmark tests**
   - Compare performance with OmegaConf
   - Profile deeply nested configs

---

## References

- [OmegaConf GitHub](https://github.com/omry/omegaconf)
- [OmegaConf Documentation](https://omegaconf.readthedocs.io/)
- [Hydra Framework](https://hydra.cc/docs/intro/)
- [Python string.Template](https://docs.python.org/3/library/string.html#template-strings)

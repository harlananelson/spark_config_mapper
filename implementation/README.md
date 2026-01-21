# spark_config_mapper Implementation Guide

This directory contains documentation and examples for understanding and using the `spark_config_mapper` package.

## Directory Contents

| File | Purpose | Audience |
|------|---------|----------|
| `spark_config_mapper_reference.md` | Complete API reference with precise definitions | LLM context injection, developers |
| `spark_config_mapper_tutorial.md` | Step-by-step learning guide with examples | Human developers |
| `AI_ASSISTED_CONFIG_GENERATION.md` | Using LLMs to generate config files from schemas | ETL developers, data engineers |
| `LOGGING.md` | Logging configuration guide | All users |
| `FUTURE_CONSIDERATIONS.md` | Future improvements and alternatives | Maintainers |
| `the_config-files/` | Example configuration files | All users |

## The Big Picture

**spark_config_mapper** is a config-driven data pipeline framework. The core insight:

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│  Source Schema  │     │  Target Schema  │     │  Config Format  │
│    Metadata     │     │   Definition    │     │  Specification  │
└────────┬────────┘     └────────┬────────┘     └────────┬────────┘
         │                       │                       │
         └───────────────────────┼───────────────────────┘
                                 ▼
                          ┌─────────────┐
                          │  LLM or     │
                          │   Human     │
                          └──────┬──────┘
                                 ▼
                          ┌─────────────┐
                          │ Config YAML │ ← Reviewable, version-controlled
                          └──────┬──────┘
                                 ▼
                          ┌─────────────┐
                          │config_mapper│ ← Deterministic execution
                          └─────────────┘
```

**Key use cases:**
- **OMOP ETL** - Map proprietary schemas (Epic, Cerner, custom) to OMOP CDM
- **Data warehouse migration** - Legacy → modern lakehouse
- **Multi-source integration** - Unify disparate data sources
- **Schema evolution** - Manage schema changes over time

**See `AI_ASSISTED_CONFIG_GENERATION.md`** for using LLMs to generate config files automatically.

## Three-Tier Configuration System

The package uses a three-tier configuration hierarchy:

```
┌─────────────────────────────────────────────────────────────────────────────┐
│  TIER 1: 000-config.yaml (PROJECT-SPECIFIC)                                 │
│  Location: {basePath}/Projects/{project}/000-config.yaml                    │
│  Changes: YES - different for each project                                  │
│  Contains: project name, dates, schema mappings, projectTables              │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  TIER 2: config-global.yaml (SHARED)                                        │
│  Location: {basePath}/configuration/config-global.yaml                      │
│  Changes: NO - same for all projects                                        │
│  Contains: callFunProcessDataTables mappings, config_table_locations        │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  TIER 3: config-RWD.yaml / config-OMOP.yaml (SCHEMA STRUCTURE)              │
│  Location: {basePath}/configuration/config-{type}.yaml                      │
│  Changes: NO - one file per data model                                      │
│  Contains: source table definitions (source, inputRegex, insert)            │
└─────────────────────────────────────────────────────────────────────────────┘
```

### What Goes Where

| Configuration | File | Changes Per Project? |
|--------------|------|---------------------|
| Project name, investigators | `000-config.yaml` | YES |
| Study dates (start, stop) | `000-config.yaml` | YES |
| Schema mappings (logical → physical) | `000-config.yaml` | YES |
| `projectTables` (output tables) | `000-config.yaml` | YES |
| `callFunProcessDataTables` rules | `config-global.yaml` | NO |
| Config file locations | `config-global.yaml` | NO |
| Source table definitions | `config-RWD.yaml` | NO |
| `inputRegex`, `insert` transforms | `config-RWD.yaml` | NO |

## Example Configuration Files

```
the_config-files/
├── 000-config-example.yaml    # Project-specific (ONLY file that changes)
├── config-global.yaml         # Shared across all projects
└── config-RWD.yaml            # RWD schema structure definitions
```

## Quick Start

### For Humans

1. Read `spark_config_mapper_tutorial.md` for step-by-step guidance
2. Copy `000-config-example.yaml` to your project directory
3. Modify only the project-specific values
4. The `config-global.yaml` and `config-RWD.yaml` are shared infrastructure

### For LLM Context

Include `spark_config_mapper_reference.md` in your prompt context. Key points:
- Only generate `000-config.yaml` for new projects
- Never modify `config-global.yaml` or `config-RWD.yaml` per project
- `projectTables` define OUTPUT tables the project creates
- Source tables are defined in `config-RWD.yaml` (not project-specific)

## Key Concepts

### Schema Mapping Flow

```
000-config.yaml                 config-global.yaml              config-RWD.yaml
───────────────                 ──────────────────              ───────────────
schemas:                        callFunProcessDataTables:       RWDTables:
  RWDSchema: my_database  ───▶    RWDcallFunc:             ───▶   conditionSource:
                                    schema_type: 'RWDSchema'        source: condition
                                    data_type: 'RWDTables'          inputRegex: [...]
```

1. Project config maps `RWDSchema` → physical database name
2. Global config says "for RWDSchema, use RWDTables definitions"
3. RWD config defines how to read source tables

### Project Tables vs Source Tables

| Type | Defined In | Purpose |
|------|-----------|---------|
| Source Tables | `config-RWD.yaml` | READ from source schemas (shared definitions) |
| Project Tables | `000-config.yaml` | CREATED by this project (project-specific) |

## Dependencies

- `pyspark >= 3.0.0`
- `pyyaml >= 5.0`
- `pandas >= 1.0.0` (optional)

## Related Packages

- **lhn**: Healthcare data workflows built on spark_config_mapper
- **txtarchive**: Archive format for sharing code with LLMs

## Future: Framework-Agnostic Architecture

The core config_mapper patterns are not Spark-specific. Future architecture:

```
┌─────────────────────────────────────────────────────────────┐
│  config_mapper (core)                                        │
│  - Template substitution (recursive_template)                │
│  - Config hierarchy (merge_configs)                          │
│  - Schema discovery interface                                │
│  - Mapping rules engine                                      │
│  - callFunProcessDataTables pattern                          │
└─────────────────────────────────────────────────────────────┘
            │              │              │              │
            ▼              ▼              ▼              ▼
     ┌──────────┐   ┌──────────┐   ┌──────────┐   ┌──────────┐
     │  Spark   │   │  Pandas  │   │  DuckDB  │   │   SQL    │
     │ Adapter  │   │ Adapter  │   │ Adapter  │   │Generator │
     └──────────┘   └──────────┘   └──────────┘   └──────────┘
```

Each adapter implements:
- `discover_schema(connection)` → table/column metadata
- `execute_transform(df, transform_string)` → apply transform
- `write_table(df, location)` → persist result

This enables the same config files to drive ETL on different platforms.

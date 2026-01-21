# AI-Assisted Configuration Generation

> **Purpose**: Guide for using LLMs to generate config_mapper YAML files from schema metadata. Enables rapid ETL pipeline creation with human review checkpoints.

---

## The Core Concept

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│  Source Schema  │     │  Target Schema  │     │  Config Format  │
│    Metadata     │     │   Definition    │     │  Specification  │
└────────┬────────┘     └────────┬────────┘     └────────┬────────┘
         │                       │                       │
         └───────────────────────┼───────────────────────┘
                                 ▼
                          ┌─────────────┐
                          │     LLM     │
                          └──────┬──────┘
                                 ▼
                          ┌─────────────┐
                          │ Config YAML │ ← Human review checkpoint
                          └──────┬──────┘
                                 ▼
                          ┌─────────────┐
                          │config_mapper│ ← Deterministic execution
                          └──────┬──────┘
                                 ▼
                          ┌─────────────┐
                          │ Target Data │
                          └─────────────┘
```

**Key insight**: LLMs excel at semantic mapping and YAML generation. Deterministic engines excel at reliable execution. This architecture uses each for what it's good at.

---

## What the LLM Needs

### 1. Source Schema Metadata

Extract and provide table/column information from your source system:

```yaml
# source_schema.yaml (extracted from source database)
tables:
  PATIENT:
    columns:
      - name: PAT_ID
        type: VARCHAR(50)
        nullable: false
        description: "Primary patient identifier"
      - name: BIRTH_DATE
        type: DATE
        nullable: true
        description: "Patient date of birth"
      - name: SEX_C
        type: INTEGER
        nullable: true
        description: "Gender code: 1=Male, 2=Female, 3=Unknown"
      - name: ETHNIC_GROUP_C
        type: INTEGER
        nullable: true
        description: "Ethnicity code"

  ENCOUNTER:
    columns:
      - name: PAT_ENC_CSN_ID
        type: VARCHAR(50)
        nullable: false
      - name: PAT_ID
        type: VARCHAR(50)
        nullable: false
        foreign_key: PATIENT.PAT_ID
      - name: CONTACT_DATE
        type: DATETIME
        nullable: true
```

### 2. Target Schema Definition

For OMOP CDM or other standard models:

```yaml
# target_schema_OMOP.yaml
tables:
  person:
    description: "One record per person"
    columns:
      - name: person_id
        type: INTEGER
        primary_key: true
        description: "Unique person identifier"
      - name: birth_datetime
        type: DATETIME
        description: "Date and time of birth"
      - name: gender_concept_id
        type: INTEGER
        required: true
        description: "OMOP concept ID for gender"
        valid_concepts: [8507, 8532, 8551, 0]  # Male, Female, Unknown, No matching

  visit_occurrence:
    description: "Healthcare encounters"
    columns:
      - name: visit_occurrence_id
        type: INTEGER
        primary_key: true
      - name: person_id
        type: INTEGER
        foreign_key: person.person_id
      - name: visit_start_datetime
        type: DATETIME
```

### 3. Config Format Specification

Provide the config_mapper reference document or a condensed version:

```yaml
# config_format_spec.yaml
description: "config_mapper YAML format specification"

structure:
  mappings:
    description: "Dictionary of target_table -> mapping definition"
    per_table:
      source_table: "Full source table path"
      target_table: "Full target table path"
      join: "Optional: join clause if multiple source tables"
      fields:
        description: "Dictionary of source_field -> target mapping"
        per_field:
          target: "Target column name"
          transform: "Optional: PySpark transform expression"
          note: "Optional: explanation of mapping logic"
      filters:
        description: "Optional: list of filter conditions"

example: |
  mappings:
    person:
      source_table: source_db.PATIENT
      target_table: cdm.person
      fields:
        PAT_ID:
          target: person_id
          transform: "cast_to_int"
        BIRTH_DATE:
          target: birth_datetime
          transform: "to_timestamp"
```

---

## The Prompt Template

```
You are a healthcare data engineer creating ETL configurations.

## Task
Generate a config_mapper YAML file to map the SOURCE schema to the TARGET schema.

## Source Schema
{source_schema_yaml}

## Target Schema (OMOP CDM v5.4)
{target_schema_yaml}

## Config Format Specification
{config_format_spec}

## Requirements
1. Map all source fields that have clear target equivalents
2. Include transform expressions for type conversions
3. Add notes explaining non-obvious mappings
4. Flag fields with "confidence: low" if the mapping is uncertain
5. Use OMOP standard concept IDs where applicable
6. Include a header comment with generation metadata

## Output
Generate the complete config YAML file.
```

---

## Example: Epic to OMOP

### Input to LLM

**Source (Epic Clarity excerpt):**
```yaml
tables:
  PATIENT:
    columns:
      - {name: PAT_ID, type: VARCHAR(50), description: "Patient ID"}
      - {name: BIRTH_DATE, type: DATE}
      - {name: SEX_C, type: INT, description: "1=Male, 2=Female"}
      - {name: ETHNIC_GROUP_C, type: INT}
      - {name: DEATH_DATE, type: DATE}
```

**Target (OMOP CDM):**
```yaml
tables:
  person:
    columns:
      - {name: person_id, type: INT, primary_key: true}
      - {name: birth_datetime, type: DATETIME}
      - {name: gender_concept_id, type: INT, concepts: [8507, 8532]}
      - {name: ethnicity_concept_id, type: INT}
      - {name: death_datetime, type: DATETIME}
```

### LLM Output

```yaml
# =============================================================================
# OMOP CDM ETL Configuration
# =============================================================================
# Generated by: Claude
# Generated on: 2025-01-20
# Source system: Epic Clarity
# Target: OMOP CDM v5.4
# Status: DRAFT - requires human review
# =============================================================================

mappings:
  person:
    source_table: clarity.PATIENT
    target_table: cdm.person

    fields:
      PAT_ID:
        target: person_id
        transform: "F.col('PAT_ID').cast('int')"
        confidence: high

      BIRTH_DATE:
        target: birth_datetime
        transform: "F.to_timestamp(F.col('BIRTH_DATE'))"
        confidence: high

      SEX_C:
        target: gender_concept_id
        transform: |
          F.when(F.col('SEX_C') == 1, 8507)
           .when(F.col('SEX_C') == 2, 8532)
           .otherwise(0)
        note: "Epic: 1=Male→8507, 2=Female→8532, else→0 (unknown)"
        confidence: high

      ETHNIC_GROUP_C:
        target: ethnicity_concept_id
        transform: null  # TODO: Need Epic ethnicity code mappings
        confidence: low
        note: "REVIEW REQUIRED: Need ethnicity code reference table"

      DEATH_DATE:
        target: death_datetime
        transform: "F.to_timestamp(F.col('DEATH_DATE'))"
        confidence: high

# =============================================================================
# REVIEW CHECKLIST
# =============================================================================
# [ ] Verify PAT_ID uniqueness for person_id
# [ ] Confirm Epic SEX_C codes match assumed values
# [ ] Obtain ETHNIC_GROUP_C → ethnicity_concept_id mapping
# [ ] Validate date formats in source system
# =============================================================================
```

---

## The Human Review Workflow

### Step 1: Generate Initial Config

```python
import anthropic

def generate_config(source_schema_path, target_schema_path):
    client = anthropic.Anthropic()

    source_schema = open(source_schema_path).read()
    target_schema = open(target_schema_path).read()
    config_spec = open('config_format_spec.yaml').read()

    prompt = PROMPT_TEMPLATE.format(
        source_schema_yaml=source_schema,
        target_schema_yaml=target_schema,
        config_format_spec=config_spec
    )

    response = client.messages.create(
        model="claude-sonnet-4-20250514",
        max_tokens=4096,
        messages=[{"role": "user", "content": prompt}]
    )

    return response.content[0].text
```

### Step 2: Review and Refine

1. **Check confidence: low items** - These need human decision
2. **Validate transforms** - Ensure PySpark syntax is correct
3. **Verify concept mappings** - Confirm OMOP concept IDs
4. **Add missing mappings** - LLM may miss domain-specific fields
5. **Test on sample data** - Run config on small dataset first

### Step 3: Iterate

```python
# If issues found, ask LLM to fix specific problems
followup_prompt = """
The generated config has these issues:
1. ETHNIC_GROUP_C mapping is missing. Here are the Epic codes: {codes}
2. The SEX_C transform syntax has an error

Please regenerate just the affected field mappings.
"""
```

### Step 4: Execute

```python
from spark_config_mapper import Resources

# Config is now human-reviewed and ready
resource = Resources(
    config_file='000-epic-to-omop.yaml',
    spark=spark,
    process_all=True
)

# Deterministic execution from reviewed config
```

---

## Best Practices

### 1. Provide Rich Schema Metadata

**Better:**
```yaml
- name: SEX_C
  type: INT
  description: "Gender code: 1=Male, 2=Female, 3=Non-binary, 4=Unknown"
  values: {1: "Male", 2: "Female", 3: "Non-binary", 4: "Unknown"}
```

**Worse:**
```yaml
- name: SEX_C
  type: INT
```

### 2. Include Reference Tables

```yaml
# Provide concept mappings the LLM can use
reference_tables:
  gender_mapping:
    source_system: Epic
    mappings:
      1: {omop_concept_id: 8507, omop_name: "Male"}
      2: {omop_concept_id: 8532, omop_name: "Female"}
```

### 3. Use Confidence Levels

Ask the LLM to self-assess:
```yaml
confidence: high   # Clear 1:1 mapping
confidence: medium # Reasonable inference
confidence: low    # Uncertain, needs review
```

### 4. Request Review Checklists

The LLM should generate a checklist of items requiring human verification.

### 5. Version Control Everything

```
project/
├── schemas/
│   ├── source_epic_v1.yaml      # Extracted schema
│   └── target_omop_v5.4.yaml    # Target definition
├── configs/
│   ├── 000-epic-omop-v1.yaml    # Generated config
│   └── 000-epic-omop-v2.yaml    # After human review
└── prompts/
    └── generation_prompt.md      # Prompt used for generation
```

---

## Schema Extraction Helpers

### From Spark Catalog

```python
def extract_spark_schema(spark, database_name):
    """Extract schema metadata from Spark catalog."""
    tables = spark.catalog.listTables(database_name)

    schema = {"tables": {}}
    for table in tables:
        df = spark.table(f"{database_name}.{table.name}")
        schema["tables"][table.name] = {
            "columns": [
                {
                    "name": field.name,
                    "type": str(field.dataType),
                    "nullable": field.nullable
                }
                for field in df.schema.fields
            ]
        }

    return yaml.dump(schema)
```

### From SQL Database

```python
def extract_sql_schema(connection, schema_name):
    """Extract schema from SQL database information_schema."""
    query = """
        SELECT table_name, column_name, data_type, is_nullable
        FROM information_schema.columns
        WHERE table_schema = %s
        ORDER BY table_name, ordinal_position
    """
    # ... build YAML structure
```

---

## Use Cases Beyond OMOP

### 1. Data Warehouse Migration

```
Legacy Warehouse  →  LLM  →  Config  →  Modern Lakehouse
```

### 2. API Response Mapping

```
External API Schema  →  LLM  →  Config  →  Internal Data Model
```

### 3. Multi-Source Integration

```
Source A Schema ─┐
Source B Schema ─┼→  LLM  →  Config  →  Unified Schema
Source C Schema ─┘
```

### 4. Schema Evolution

```
Old Schema v1  ─┐
New Schema v2  ─┴→  LLM  →  Migration Config  →  Data Migration
```

---

## Limitations and Caveats

1. **LLMs make mistakes** - Always review generated configs before production use

2. **Domain knowledge gaps** - LLMs may not know organization-specific codes or conventions

3. **Complex transforms** - Multi-step transformations may need human authoring

4. **Performance considerations** - LLM-generated transforms may not be optimized

5. **Hallucinated mappings** - LLMs may invent plausible but incorrect mappings

**The config file is a checkpoint, not a final product.** Human review is essential.

---

## Future Directions

1. **Validation layer** - Automatically validate LLM-generated configs against schema rules

2. **Learning from corrections** - Fine-tune prompts based on human edits

3. **Mapping library** - Build reusable mapping patterns (Epic→OMOP, Cerner→OMOP)

4. **Interactive refinement** - Chat-based config editing with LLM assistance

5. **Automated testing** - Generate test cases alongside configs

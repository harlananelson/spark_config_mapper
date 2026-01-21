# Proposal: omop_concept_mapper Package

> **Purpose**: Standalone package for OMOP concept mapping with pluggable backends. Integrates Siddharth Rajesh's semantic mapping innovation with flexible backend options.

---

## The Problem

OMOP CDM conversions require mapping source values to standard OMOP concept IDs:

```
Source Value              →    OMOP Concept ID
─────────────────────          ─────────────────
"K35.9"                   →    4023928 (Acute appendicitis)
"ER visit"                →    9201 (Emergency Dept Visit)
"Pediatric Cardiology"    →    45756805 (specialty, no ICD code)
"heart attack"            →    4329847 (Myocardial infarction)
```

**Challenges:**
1. Not all values have direct code matches (specialties, free text)
2. Source systems use different naming conventions
3. Abbreviations and synonyms are common
4. Need confidence scores for review

---

## Proposed Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│  omop_concept_mapper (core)                                      │
│  ─────────────────────────                                       │
│  - ConceptMapper: Main orchestrator                              │
│  - MappingStrategy: Interface for backends                       │
│  - MappingResult: Standardized output with confidence            │
│  - VocabularyLoader: Load OMOP vocabularies                      │
│  - MedicalPreprocessor: Abbreviation expansion                   │
└─────────────────────────────────────────────────────────────────┘
            │              │              │              │
            ▼              ▼              ▼              ▼
     ┌──────────┐   ┌──────────┐   ┌──────────┐   ┌──────────┐
     │  Exact   │   │  Fuzzy   │   │Transformer│   │   LLM    │
     │  Match   │   │  Match   │   │ (Local)   │   │  (API)   │
     └──────────┘   └──────────┘   └──────────┘   └──────────┘
     No deps        fuzzywuzzy     HuggingFace    Anthropic/
                                   sentence-      OpenAI API
                                   transformers
            │              │              │              │
            ▼              ▼              ▼              ▼
     ┌──────────┐   ┌──────────┐   ┌──────────┐   ┌──────────┐
     │  Spark   │   │  Pandas  │   │ John Snow│   │  Future  │
     │  Adapter │   │  Adapter │   │   Labs   │   │ Adapters │
     └──────────┘   └──────────┘   └──────────┘   └──────────┘
```

---

## Backend Options

### 1. Exact Match (No Dependencies)
```python
from omop_concept_mapper import ConceptMapper
from omop_concept_mapper.strategies import ExactMatchStrategy

mapper = ConceptMapper(strategy=ExactMatchStrategy())
result = mapper.map(source_df, 'source_code', concept_df)
```
- Direct string/code matching
- Fastest, no external dependencies
- Best for coded values (ICD, CPT, LOINC)

### 2. Fuzzy Match (fuzzywuzzy)
```python
from omop_concept_mapper.strategies import FuzzyMatchStrategy

mapper = ConceptMapper(strategy=FuzzyMatchStrategy(threshold=80))
```
- Levenshtein distance matching
- Handles typos and minor variations
- Lightweight dependency

### 3. Transformer-Based (HuggingFace)
```python
from omop_concept_mapper.strategies import TransformerStrategy

mapper = ConceptMapper(
    strategy=TransformerStrategy(
        model_name='sentence-transformers/all-MiniLM-L6-v2',
        threshold=0.7,
        device='cuda'  # or 'cpu'
    )
)
```
- Semantic understanding of medical terms
- Handles synonyms, abbreviations, free text
- Requires transformers, torch

### 4. John Snow Labs (Spark NLP) - Future
```python
from omop_concept_mapper.strategies import JohnSnowStrategy

mapper = ConceptMapper(
    strategy=JohnSnowStrategy(
        model='en_clinical_ner_large'
    )
)
```
- Spark-native, distributed processing
- Medical NER and entity linking
- Requires Spark NLP license for healthcare models

### 5. LLM-Based (API) - Future
```python
from omop_concept_mapper.strategies import LLMStrategy

mapper = ConceptMapper(
    strategy=LLMStrategy(
        provider='anthropic',  # or 'openai'
        model='claude-sonnet-4-20250514'
    )
)
```
- Most flexible, understands context
- Can handle complex descriptions
- Requires API key, has cost per request

---

## Multi-Strategy Matching

The key innovation from srajesh_OMOP: try multiple strategies in sequence.

```python
from omop_concept_mapper import ConceptMapper
from omop_concept_mapper.strategies import MultiStrategy, ExactMatchStrategy, FuzzyMatchStrategy, TransformerStrategy

mapper = ConceptMapper(
    strategy=MultiStrategy([
        ExactMatchStrategy(),                    # Try exact first
        FuzzyMatchStrategy(threshold=85),        # Then fuzzy
        TransformerStrategy(threshold=0.7)       # Finally semantic
    ])
)

result = mapper.map(source_df, 'source_value', concept_df)

# Result includes which strategy matched each record
# result.df['matching_strategy'] = 'exact_match' | 'fuzzy_match' | 'semantic_match' | None
# result.df['confidence'] = 1.0 | 0.85 | 0.73 | None
```

---

## Medical Preprocessing

From srajesh_OMOP's `preprocess_text`:

```python
from omop_concept_mapper import MedicalPreprocessor

preprocessor = MedicalPreprocessor()

# Expands medical abbreviations
preprocessor.preprocess("ED visit for chest pain")
# → "emergency department visit for chest pain"

preprocessor.preprocess("ICU admission post-op CABG")
# → "intensive care unit admission post-operative coronary artery bypass graft"
```

**Built-in abbreviation dictionary:**
- `ED` → `emergency department`
- `ER` → `emergency room`
- `ICU` → `intensive care unit`
- `OR` → `operating room`
- `dx` → `diagnosis`
- `tx` → `treatment`
- `hx` → `history`
- `pt` → `patient`
- ... (extensible)

---

## Integration with spark_config_mapper

```yaml
# 000-config.yaml
concept_mapping:
  strategy: multi_strategy
  strategies:
    - type: exact_match
    - type: fuzzy_match
      threshold: 85
    - type: transformer
      model: sentence-transformers/all-MiniLM-L6-v2
      threshold: 0.7

  preprocess: true
  medical_abbreviations: true
```

```python
from spark_config_mapper import Resources
from omop_concept_mapper import ConceptMapper

resource = Resources(project='OMOP_ETL', spark=spark, process_all=True)

# ConceptMapper reads config and auto-configures
mapper = ConceptMapper.from_config(resource.config_dict['concept_mapping'])

# Map source values to OMOP concepts
mapped_df = mapper.map_spark(
    source_df=source_df,
    source_column='condition_source_value',
    concept_df=omop_concepts_df,
    concept_column='concept_name'
)
```

---

## Vocabulary Loading

```python
from omop_concept_mapper import VocabularyLoader

# Load from Athena CSV exports
loader = VocabularyLoader(path='./vocabulary_csvs/')
concepts_df = loader.load_concepts(domain='Condition')

# Or from Spark table
loader = VocabularyLoader.from_spark_table(spark, 'omop_cdm.concept')
concepts_df = loader.load_concepts(domain='Condition')
```

---

## Output Format

All strategies return standardized `MappingResult`:

```python
result = mapper.map(source_df, 'source_value', concept_df)

result.df.columns:
  - <original source columns>
  - concept_id: int              # Mapped OMOP concept ID (or None)
  - concept_name: str            # Matched concept name
  - matching_strategy: str       # 'exact_match', 'fuzzy_match', 'semantic_match'
  - confidence: float            # 0.0 - 1.0
  - domain_id: str               # 'Condition', 'Procedure', 'Provider', etc.

result.stats:
  - total_records: int
  - mapped_records: int
  - unmapped_records: int
  - mapping_rate: float
  - by_strategy: dict            # Counts per strategy
```

---

## Package Structure

```
omop_concept_mapper/
├── __init__.py
├── mapper.py                    # ConceptMapper main class
├── result.py                    # MappingResult dataclass
├── vocabulary.py                # VocabularyLoader
├── preprocessor.py              # MedicalPreprocessor
├── strategies/
│   ├── __init__.py
│   ├── base.py                  # MappingStrategy interface
│   ├── exact.py                 # ExactMatchStrategy
│   ├── fuzzy.py                 # FuzzyMatchStrategy
│   ├── transformer.py           # TransformerStrategy
│   ├── multi.py                 # MultiStrategy
│   └── llm.py                   # LLMStrategy (future)
├── adapters/
│   ├── __init__.py
│   ├── pandas_adapter.py        # Pandas DataFrame operations
│   ├── spark_adapter.py         # PySpark DataFrame operations
│   └── johnsnow_adapter.py      # John Snow Labs (future)
└── data/
    └── medical_abbreviations.json
```

---

## Installation Options

```bash
# Core only (exact match)
pip install omop-concept-mapper

# With fuzzy matching
pip install omop-concept-mapper[fuzzy]

# With transformer support
pip install omop-concept-mapper[transformer]

# With Spark support
pip install omop-concept-mapper[spark]

# Full installation
pip install omop-concept-mapper[all]
```

---

## Credits

- **Core concept mapping patterns**: Siddharth Rajesh (srajesh_OMOP)
- **Config-driven architecture**: Harlan Nelson (spark_config_mapper)
- **OMOP CDM**: OHDSI community

---

## Implementation Plan

### Phase 1: Core Package
1. Extract ConceptMapper and TransformerConceptMapper from srajesh_OMOP
2. Refactor into strategy pattern
3. Add MedicalPreprocessor
4. Create Pandas adapter
5. Write tests

### Phase 2: Spark Integration
1. Create Spark adapter
2. Integrate with spark_config_mapper config system
3. Add PySpark UDFs for distributed mapping

### Phase 3: Additional Backends
1. John Snow Labs adapter
2. LLM API adapter
3. Performance optimization

### Phase 4: AI-Assisted Mapping
1. Use LLM to suggest mappings for unmapped values
2. Human review workflow
3. Learning from corrections

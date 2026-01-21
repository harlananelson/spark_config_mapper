"""
Tests for flatten_schema and related introspection functions.

These tests use PySpark schema types directly without requiring a SparkSession.
Run with: pytest tests/test_flatten_schema.py -v
"""

import pytest
import os
import sys

# Handle import
try:
    from spark_config_mapper.utils.introspection import (
        flatten_schema, get_array_fields, get_struct_fields,
        get_root_columns, describe_schema
    )
except ImportError:
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from utils.introspection import (
        flatten_schema, get_array_fields, get_struct_fields,
        get_root_columns, describe_schema
    )

from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, ArrayType,
    LongType, DoubleType, BooleanType, DateType, TimestampType
)


# =============================================================================
# TEST SCHEMAS
# =============================================================================

def simple_flat_schema():
    """Simple flat schema with primitive types."""
    return StructType([
        StructField("personid", StringType(), True),
        StructField("name", StringType(), True),
        StructField("age", IntegerType(), True),
        StructField("active", BooleanType(), True)
    ])


def nested_struct_schema():
    """Schema with nested struct (no arrays)."""
    return StructType([
        StructField("personid", StringType(), True),
        StructField("name", StructType([
            StructField("first", StringType(), True),
            StructField("last", StringType(), True)
        ]), True),
        StructField("address", StructType([
            StructField("street", StringType(), True),
            StructField("city", StringType(), True),
            StructField("zip", StringType(), True)
        ]), True)
    ])


def deeply_nested_struct_schema():
    """Schema with deeply nested structs."""
    return StructType([
        StructField("id", StringType(), True),
        StructField("level1", StructType([
            StructField("level2", StructType([
                StructField("level3", StructType([
                    StructField("value", StringType(), True)
                ]), True)
            ]), True)
        ]), True)
    ])


def array_of_primitives_schema():
    """Schema with array of primitive types."""
    return StructType([
        StructField("personid", StringType(), True),
        StructField("tags", ArrayType(StringType()), True),
        StructField("scores", ArrayType(IntegerType()), True)
    ])


def array_of_structs_schema():
    """Schema with array of structs (common in FHIR data)."""
    return StructType([
        StructField("personid", StringType(), True),
        StructField("medications", ArrayType(StructType([
            StructField("code", StringType(), True),
            StructField("name", StringType(), True),
            StructField("dosage", DoubleType(), True)
        ])), True)
    ])


def multiple_arrays_schema():
    """Schema with multiple arrays (causes cartesian product on explode)."""
    return StructType([
        StructField("personid", StringType(), True),
        StructField("medications", ArrayType(StructType([
            StructField("code", StringType(), True),
            StructField("name", StringType(), True)
        ])), True),
        StructField("diagnoses", ArrayType(StructType([
            StructField("icd", StringType(), True),
            StructField("description", StringType(), True)
        ])), True)
    ])


def nested_arrays_schema():
    """Schema with nested arrays (array within struct within array)."""
    return StructType([
        StructField("personid", StringType(), True),
        StructField("encounters", ArrayType(StructType([
            StructField("encounterid", StringType(), True),
            StructField("codes", ArrayType(StructType([
                StructField("system", StringType(), True),
                StructField("code", StringType(), True)
            ])), True)
        ])), True)
    ])


def healthcare_fhir_schema():
    """Realistic FHIR-like schema for healthcare data."""
    return StructType([
        StructField("personid", StringType(), True),
        StructField("tenant", StringType(), True),
        StructField("conditioncode", StructType([
            StructField("standard", StructType([
                StructField("id", StringType(), True),
                StructField("primaryDisplay", StringType(), True),
                StructField("codingSystemId", StringType(), True)
            ]), True)
        ]), True),
        StructField("effectiveDate", DateType(), True)
    ])


def mixed_complex_schema():
    """Complex schema mixing structs and arrays."""
    return StructType([
        StructField("personid", StringType(), True),
        StructField("demographics", StructType([
            StructField("name", StructType([
                StructField("first", StringType(), True),
                StructField("last", StringType(), True)
            ]), True),
            StructField("dob", DateType(), True)
        ]), True),
        StructField("encounters", ArrayType(StructType([
            StructField("id", StringType(), True),
            StructField("date", TimestampType(), True)
        ])), True)
    ])


# =============================================================================
# TESTS: flatten_schema
# =============================================================================

class TestFlattenSchemaBasic:
    """Basic tests for flatten_schema."""

    def test_flat_schema(self):
        """Simple flat schema returns column names."""
        schema = simple_flat_schema()
        result = flatten_schema(schema)
        assert result == ['personid', 'name', 'age', 'active']

    def test_empty_schema(self):
        """Empty schema returns empty list."""
        schema = StructType([])
        result = flatten_schema(schema)
        assert result == []

    def test_single_column(self):
        """Single column schema."""
        schema = StructType([StructField("id", StringType(), True)])
        result = flatten_schema(schema)
        assert result == ['id']


class TestFlattenSchemaStructs:
    """Tests for nested struct handling."""

    def test_nested_struct(self):
        """Nested structs produce dot-notation paths."""
        schema = nested_struct_schema()
        result = flatten_schema(schema)
        expected = [
            'personid',
            'name.first', 'name.last',
            'address.street', 'address.city', 'address.zip'
        ]
        assert result == expected

    def test_deeply_nested_struct(self):
        """Deeply nested structs work correctly."""
        schema = deeply_nested_struct_schema()
        result = flatten_schema(schema)
        assert result == ['id', 'level1.level2.level3.value']

    def test_with_prefix(self):
        """Prefix is prepended to all paths."""
        schema = simple_flat_schema()
        result = flatten_schema(schema, prefix='root')
        expected = ['root.personid', 'root.name', 'root.age', 'root.active']
        assert result == expected

    def test_healthcare_fhir_nested(self):
        """FHIR-like nested struct produces correct paths."""
        schema = healthcare_fhir_schema()
        result = flatten_schema(schema)
        expected = [
            'personid', 'tenant',
            'conditioncode.standard.id',
            'conditioncode.standard.primaryDisplay',
            'conditioncode.standard.codingSystemId',
            'effectiveDate'
        ]
        assert result == expected


class TestFlattenSchemaArrays:
    """Tests for array handling."""

    def test_array_of_primitives_default(self):
        """Array of primitives is kept as column name (default behavior)."""
        schema = array_of_primitives_schema()
        result = flatten_schema(schema)
        # Default: recurses into array, but primitives just return the path
        assert 'tags' in result
        assert 'scores' in result

    def test_array_of_structs_default(self):
        """Array of structs: default behavior recurses into struct."""
        schema = array_of_structs_schema()
        result = flatten_schema(schema)
        # Default: unwraps array and recurses into struct element
        expected = ['personid', 'medications.code', 'medications.name', 'medications.dosage']
        assert result == expected

    def test_array_of_structs_include_arrays(self):
        """Array of structs with include_arrays=True stops at array."""
        schema = array_of_structs_schema()
        result = flatten_schema(schema, include_arrays=True)
        # With include_arrays=True: stops at array level
        expected = ['personid', 'medications']
        assert result == expected

    def test_multiple_arrays_default(self):
        """Multiple arrays: default recurses into both."""
        schema = multiple_arrays_schema()
        result = flatten_schema(schema)
        expected = [
            'personid',
            'medications.code', 'medications.name',
            'diagnoses.icd', 'diagnoses.description'
        ]
        assert result == expected

    def test_multiple_arrays_include_arrays(self):
        """Multiple arrays with include_arrays=True."""
        schema = multiple_arrays_schema()
        result = flatten_schema(schema, include_arrays=True)
        expected = ['personid', 'medications', 'diagnoses']
        assert result == expected

    def test_nested_arrays(self):
        """Nested arrays (array inside struct inside array)."""
        schema = nested_arrays_schema()
        result = flatten_schema(schema)
        # Recurses through: encounters -> encounterid, codes -> system, code
        expected = [
            'personid',
            'encounters.encounterid',
            'encounters.codes.system',
            'encounters.codes.code'
        ]
        assert result == expected

    def test_nested_arrays_include_arrays(self):
        """Nested arrays with include_arrays=True."""
        schema = nested_arrays_schema()
        result = flatten_schema(schema, include_arrays=True)
        expected = ['personid', 'encounters']
        assert result == expected


class TestFlattenSchemaMixed:
    """Tests for mixed struct/array schemas."""

    def test_mixed_complex(self):
        """Mixed structs and arrays."""
        schema = mixed_complex_schema()
        result = flatten_schema(schema)
        expected = [
            'personid',
            'demographics.name.first',
            'demographics.name.last',
            'demographics.dob',
            'encounters.id',
            'encounters.date'
        ]
        assert result == expected

    def test_mixed_complex_include_arrays(self):
        """Mixed with include_arrays=True."""
        schema = mixed_complex_schema()
        result = flatten_schema(schema, include_arrays=True)
        expected = [
            'personid',
            'demographics.name.first',
            'demographics.name.last',
            'demographics.dob',
            'encounters'
        ]
        assert result == expected


# =============================================================================
# TESTS: get_array_fields
# =============================================================================

class TestGetArrayFields:
    """Tests for get_array_fields function."""

    def test_no_arrays(self):
        """Schema without arrays returns empty list."""
        schema = nested_struct_schema()
        result = get_array_fields(schema)
        assert result == []

    def test_array_of_primitives(self):
        """Array of primitives are identified."""
        schema = array_of_primitives_schema()
        result = get_array_fields(schema)
        assert set(result) == {'tags', 'scores'}

    def test_array_of_structs(self):
        """Array of structs is identified."""
        schema = array_of_structs_schema()
        result = get_array_fields(schema)
        assert result == ['medications']

    def test_multiple_arrays(self):
        """Multiple arrays are all identified."""
        schema = multiple_arrays_schema()
        result = get_array_fields(schema)
        assert set(result) == {'medications', 'diagnoses'}

    def test_nested_arrays(self):
        """Nested arrays are identified at all levels."""
        schema = nested_arrays_schema()
        result = get_array_fields(schema)
        # Should find both encounters and encounters.codes
        assert 'encounters' in result
        assert 'encounters.codes' in result


# =============================================================================
# TESTS: get_struct_fields
# =============================================================================

class TestGetStructFields:
    """Tests for get_struct_fields function."""

    def test_no_structs(self):
        """Flat schema returns empty list."""
        schema = simple_flat_schema()
        result = get_struct_fields(schema)
        assert result == []

    def test_nested_structs(self):
        """Nested structs are identified."""
        schema = nested_struct_schema()
        result = get_struct_fields(schema)
        assert 'name' in result
        assert 'address' in result

    def test_deeply_nested(self):
        """Deeply nested structs are all found."""
        schema = deeply_nested_struct_schema()
        result = get_struct_fields(schema)
        assert 'level1' in result
        assert 'level1.level2' in result
        assert 'level1.level2.level3' in result

    def test_ignores_arrays(self):
        """Arrays are not included even if they contain structs."""
        schema = array_of_structs_schema()
        result = get_struct_fields(schema)
        # Should NOT include 'medications' since it's an array
        assert 'medications' not in result


# =============================================================================
# TESTS: get_root_columns
# =============================================================================

class TestGetRootColumns:
    """Tests for get_root_columns function."""

    def test_flat_columns(self):
        """Flat column names return themselves."""
        result = get_root_columns(['personid', 'name', 'age'])
        assert set(result) == {'personid', 'name', 'age'}

    def test_nested_columns(self):
        """Nested paths return root only."""
        result = get_root_columns(['name.first', 'name.last', 'address.city'])
        assert set(result) == {'name', 'address'}

    def test_mixed(self):
        """Mix of flat and nested."""
        result = get_root_columns(['personid', 'name.first', 'name.last'])
        assert set(result) == {'personid', 'name'}

    def test_empty_list(self):
        """Empty list returns empty list."""
        result = get_root_columns([])
        assert result == []


# =============================================================================
# TESTS: describe_schema
# =============================================================================

class TestDescribeSchema:
    """Tests for describe_schema function."""

    def test_simple_schema(self):
        """Simple schema produces readable output."""
        schema = simple_flat_schema()
        result = describe_schema(schema)
        assert 'personid: string' in result
        assert 'age: int' in result
        assert 'active: boolean' in result

    def test_nested_struct_in_description(self):
        """Nested structs show indentation."""
        schema = nested_struct_schema()
        result = describe_schema(schema)
        assert 'name: struct' in result
        assert '  first: string' in result  # Indented

    def test_array_warning(self):
        """Arrays show warning about explosion."""
        schema = array_of_structs_schema()
        result = describe_schema(schema)
        assert '[ARRAY]' in result
        assert 'explode' in result.lower()


# =============================================================================
# INTEGRATION TESTS
# =============================================================================

class TestIntegration:
    """Integration tests with realistic schemas."""

    def test_fhir_condition_flattening(self):
        """Realistic FHIR condition schema."""
        schema = healthcare_fhir_schema()

        # Get flat fields (for SELECT after any needed explosion)
        flat = flatten_schema(schema)
        assert 'conditioncode.standard.id' in flat

        # Get arrays (to know what to explode)
        arrays = get_array_fields(schema)
        assert arrays == []  # This schema has no arrays

        # Get structs (safe to flatten directly)
        structs = get_struct_fields(schema)
        assert 'conditioncode' in structs
        assert 'conditioncode.standard' in structs

    def test_complex_schema_workflow(self):
        """Workflow for handling complex nested schema."""
        schema = multiple_arrays_schema()

        # Step 1: Identify arrays (these need explosion)
        arrays = get_array_fields(schema)
        assert 'medications' in arrays
        assert 'diagnoses' in arrays

        # Step 2: Get fields if we stop at arrays
        safe_fields = flatten_schema(schema, include_arrays=True)
        assert safe_fields == ['personid', 'medications', 'diagnoses']

        # Step 3: Get all nested paths (for reference)
        all_paths = flatten_schema(schema, include_arrays=False)
        assert 'medications.code' in all_paths
        assert 'diagnoses.icd' in all_paths

        # The workflow would be:
        # 1. Select personid and explode ONE array at a time
        # 2. After exploding, the nested struct paths become valid
        # 3. Never explode multiple arrays in the same query (cartesian product!)


# =============================================================================
# EDGE CASES AND REGRESSION TESTS
# =============================================================================

class TestEdgeCases:
    """Edge cases and potential failure scenarios."""

    def test_struct_with_same_field_names_at_different_levels(self):
        """Struct with 'id' at multiple levels."""
        schema = StructType([
            StructField("id", StringType(), True),
            StructField("child", StructType([
                StructField("id", StringType(), True),
                StructField("grandchild", StructType([
                    StructField("id", StringType(), True)
                ]), True)
            ]), True)
        ])
        result = flatten_schema(schema)
        assert result == ['id', 'child.id', 'child.grandchild.id']

    def test_empty_struct(self):
        """Struct with no fields."""
        schema = StructType([
            StructField("id", StringType(), True),
            StructField("empty", StructType([]), True)
        ])
        result = flatten_schema(schema)
        assert result == ['id']  # Empty struct contributes nothing

    def test_array_of_empty_struct(self):
        """Array containing empty struct."""
        schema = StructType([
            StructField("id", StringType(), True),
            StructField("items", ArrayType(StructType([])), True)
        ])
        result = flatten_schema(schema)
        assert result == ['id']  # Empty struct in array contributes nothing

    def test_special_characters_in_field_names(self):
        """Field names with special characters."""
        schema = StructType([
            StructField("person_id", StringType(), True),
            StructField("first-name", StringType(), True),
            StructField("data.point", StringType(), True)  # Dot in field name
        ])
        result = flatten_schema(schema)
        # These are the actual field names, not paths
        assert result == ['person_id', 'first-name', 'data.point']


if __name__ == '__main__':
    pytest.main([__file__, '-v'])

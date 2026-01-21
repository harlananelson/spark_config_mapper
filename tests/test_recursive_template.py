"""
Tests for recursive_template function.

Compatible with Python 3.6+ and pytest.
Run with: pytest tests/test_recursive_template.py -v
"""

import pytest
import os
import tempfile

# Handle import for both installed package and direct testing
try:
    from spark_config_mapper.config.loader import (
        recursive_template, read_config, merge_configs, _process_list
    )
except ImportError:
    import sys
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from config.loader import (
        recursive_template, read_config, merge_configs, _process_list
    )


class TestRecursiveTemplateBasic:
    """Basic functionality tests."""

    def test_simple_string_substitution(self):
        """Basic $key substitution."""
        d = {'name': 'test', 'greeting': 'Hello $name'}
        result = recursive_template(d, {})
        assert result['greeting'] == 'Hello test'

    def test_braced_substitution(self):
        """${key} syntax works."""
        d = {'base': '/data', 'path': '${base}/project'}
        result = recursive_template(d, {})
        assert result['path'] == '/data/project'

    def test_external_replace_dict(self):
        """Values from external replace dict are used."""
        d = {'path': '/data/$today'}
        result = recursive_template(d, {'today': '2025-01-20'})
        assert result['path'] == '/data/2025-01-20'

    def test_cascading_substitution(self):
        """Later keys can reference earlier keys (top_call=True)."""
        d = {
            'base': '/home/user',
            'project': 'test',
            'full_path': '$base/projects/$project'
        }
        result = recursive_template(d, {})
        assert result['full_path'] == '/home/user/projects/test'

    def test_missing_key_left_as_is(self):
        """Missing keys are not replaced (safe_substitute behavior)."""
        d = {'path': '/data/$undefined'}
        result = recursive_template(d, {})
        assert result['path'] == '/data/$undefined'

    def test_replace_overrides_dict_value(self):
        """If key exists in replace dict, use that value."""
        d = {'name': 'original'}
        result = recursive_template(d, {'name': 'overridden'})
        assert result['name'] == 'overridden'


class TestRecursiveTemplateNested:
    """Tests for nested structures."""

    def test_nested_dict(self):
        """Nested dictionaries are processed."""
        d = {
            'base': '/data',
            'paths': {
                'input': '$base/input',
                'output': '$base/output'
            }
        }
        result = recursive_template(d, {})
        assert result['paths']['input'] == '/data/input'
        assert result['paths']['output'] == '/data/output'

    def test_deeply_nested_structure(self):
        """Multiple levels of nesting work."""
        d = {
            'root': '/data',
            'level1': {
                'level2': {
                    'level3': {
                        'path': '$root/deep'
                    }
                }
            }
        }
        result = recursive_template(d, {})
        assert result['level1']['level2']['level3']['path'] == '/data/deep'

    def test_list_of_strings(self):
        """Lists of strings are substituted."""
        d = {
            'prefix': 'test',
            'items': ['${prefix}_one', '${prefix}_two']
        }
        result = recursive_template(d, {})
        assert result['items'] == ['test_one', 'test_two']

    def test_list_of_dicts(self):
        """Lists containing dictionaries are processed."""
        d = {
            'base': '/data',
            'tables': [
                {'name': 'table1', 'path': '$base/table1'},
                {'name': 'table2', 'path': '$base/table2'}
            ]
        }
        result = recursive_template(d, {})
        assert result['tables'][0]['path'] == '/data/table1'
        assert result['tables'][1]['path'] == '/data/table2'

    def test_nested_list_with_strings(self):
        """Nested lists of strings are processed."""
        d = {
            'prefix': 'item',
            'matrix': [
                ['${prefix}_a', '${prefix}_b'],
                ['${prefix}_c', '${prefix}_d']
            ]
        }
        result = recursive_template(d, {})
        assert result['matrix'][0] == ['item_a', 'item_b']
        assert result['matrix'][1] == ['item_c', 'item_d']

    def test_nested_list_with_dicts(self):
        """Nested lists containing dicts are processed (bug fix test)."""
        d = {
            'base': '/data',
            'matrix': [
                [{'path': '$base/a'}, {'path': '$base/b'}],
                [{'path': '$base/c'}]
            ]
        }
        result = recursive_template(d, {})
        assert result['matrix'][0][0]['path'] == '/data/a'
        assert result['matrix'][0][1]['path'] == '/data/b'
        assert result['matrix'][1][0]['path'] == '/data/c'

    def test_mixed_list(self):
        """Lists with mixed types are handled correctly."""
        d = {
            'base': '/data',
            'mixed': [
                'string: $base',
                42,
                {'nested': '$base/nested'},
                ['$base/list'],
                None,
                True
            ]
        }
        result = recursive_template(d, {})
        assert result['mixed'][0] == 'string: /data'
        assert result['mixed'][1] == 42
        assert result['mixed'][2]['nested'] == '/data/nested'
        assert result['mixed'][3] == ['/data/list']
        assert result['mixed'][4] is None
        assert result['mixed'][5] is True


class TestRecursiveTemplateTypes:
    """Tests for type preservation."""

    def test_non_string_values_preserved(self):
        """Integers, floats, booleans pass through unchanged."""
        d = {
            'count': 42,
            'ratio': 3.14,
            'enabled': True,
            'disabled': False,
            'nothing': None
        }
        result = recursive_template(d, {})
        assert result['count'] == 42
        assert result['ratio'] == 3.14
        assert result['enabled'] is True
        assert result['disabled'] is False
        assert result['nothing'] is None

    def test_empty_dict(self):
        """Empty dict returns empty dict."""
        result = recursive_template({}, {})
        assert result == {}

    def test_none_input(self):
        """None input returns empty dict."""
        result = recursive_template(None, {})
        assert result == {}

    def test_empty_string_value(self):
        """Empty string is preserved."""
        d = {'empty': ''}
        result = recursive_template(d, {})
        assert result['empty'] == ''


class TestRecursiveTemplateEdgeCases:
    """Edge cases and special characters."""

    def test_dollar_sign_escape(self):
        """$$ produces literal $."""
        d = {'price': '$$100'}
        result = recursive_template(d, {})
        assert result['price'] == '$100'

    def test_special_characters_in_value(self):
        """Special regex characters in replacement values work."""
        d = {'pattern': 'E11\\.[0-9]', 'search': '$pattern'}
        result = recursive_template(d, {})
        assert result['search'] == 'E11\\.[0-9]'

    def test_top_call_false_no_cascade(self):
        """With top_call=False, don't update replace dict for cascading."""
        d = {'a': 'value_a', 'b': '$a'}
        replace = {}
        result = recursive_template(d, replace, top_call=False)
        # With top_call=False, 'a' isn't added to replace, so $a isn't substituted
        assert result['b'] == '$a'

    def test_multiple_substitutions_in_one_string(self):
        """Multiple variables in one string."""
        d = {
            'host': 'localhost',
            'port': '5432',
            'url': 'postgres://$host:$port/db'
        }
        result = recursive_template(d, {})
        assert result['url'] == 'postgres://localhost:5432/db'

    def test_adjacent_substitutions(self):
        """Adjacent variables without separator."""
        d = {
            'a': 'hello',
            'b': 'world',
            'combined': '${a}${b}'
        }
        result = recursive_template(d, {})
        assert result['combined'] == 'helloworld'


class TestRecursiveTemplateIntegration:
    """Integration tests mimicking real config usage."""

    def test_healthcare_config_pattern(self):
        """Simulates typical lhn/healthcare config structure."""
        d = {
            'project': 'DiabetesStudy',
            'disease': 'DM2',
            'schemaTag': 'RWD',
            'historyStart': '2015-01-01',
            'historyStop': '${today}',
            'search_string': 'E11\\.[0-9]',
            'projectTables': {
                'cohort': {
                    'label': '${disease} cohort',
                    'histStart': '${historyStart}',
                    'histEnd': '${historyStop}'
                }
            }
        }
        replace = {'today': '2025-01-20'}
        result = recursive_template(d, replace)

        assert result['historyStop'] == '2025-01-20'
        assert result['projectTables']['cohort']['label'] == 'DM2 cohort'
        assert result['projectTables']['cohort']['histStart'] == '2015-01-01'
        assert result['projectTables']['cohort']['histEnd'] == '2025-01-20'

    def test_schema_mapping_pattern(self):
        """Config pattern for schema mappings."""
        d = {
            'project': 'SickleCell',
            'outputSchema': '${project}_rwd',
            'schemas': {
                'RWDSchema': 'source_data_2024',
                'projectSchema': '${outputSchema}'
            }
        }
        result = recursive_template(d, {})
        assert result['outputSchema'] == 'SickleCell_rwd'
        assert result['schemas']['projectSchema'] == 'SickleCell_rwd'

    def test_list_of_table_definitions(self):
        """Config pattern with list of table definitions."""
        d = {
            'base': '/data',
            'disease': 'SCD',
            'tables': [
                {
                    'name': 'cohort',
                    'path': '$base/${disease}/cohort.parquet'
                },
                {
                    'name': 'demographics',
                    'path': '$base/${disease}/demographics.parquet'
                }
            ]
        }
        result = recursive_template(d, {})
        assert result['tables'][0]['path'] == '/data/SCD/cohort.parquet'
        assert result['tables'][1]['path'] == '/data/SCD/demographics.parquet'


class TestProcessList:
    """Direct tests for _process_list helper function."""

    def test_empty_list(self):
        """Empty list returns empty list."""
        result = _process_list([], {})
        assert result == []

    def test_list_preserves_order(self):
        """Order is preserved."""
        result = _process_list(['a', 'b', 'c'], {})
        assert result == ['a', 'b', 'c']

    def test_list_with_none(self):
        """None in list is preserved."""
        result = _process_list([None, 'a', None], {})
        assert result == [None, 'a', None]


class TestMergeConfigs:
    """Tests for merge_configs function."""

    def test_simple_merge(self):
        """Later config overrides earlier."""
        a = {'x': 1, 'y': 2}
        b = {'y': 3, 'z': 4}
        result = merge_configs(a, b)
        assert result == {'x': 1, 'y': 3, 'z': 4}

    def test_deep_merge(self):
        """Nested dicts are merged, not replaced."""
        a = {'settings': {'timeout': 30, 'retries': 3}}
        b = {'settings': {'timeout': 60}}
        result = merge_configs(a, b)
        assert result == {'settings': {'timeout': 60, 'retries': 3}}

    def test_three_configs(self):
        """Multiple configs merged in order."""
        a = {'a': 1}
        b = {'b': 2}
        c = {'c': 3}
        result = merge_configs(a, b, c)
        assert result == {'a': 1, 'b': 2, 'c': 3}

    def test_none_configs_ignored(self):
        """None configs are skipped."""
        a = {'x': 1}
        result = merge_configs(a, None, {'y': 2})
        assert result == {'x': 1, 'y': 2}

    def test_empty_configs(self):
        """Empty configs work."""
        result = merge_configs({}, {})
        assert result == {}


class TestReadConfig:
    """Tests for read_config function with actual YAML files."""

    def test_read_simple_yaml(self):
        """Read a simple YAML file."""
        yaml_content = """
project: TestProject
version: 1
"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            f.write(yaml_content)
            f.flush()
            try:
                result = read_config(f.name, {})
                assert result['project'] == 'TestProject'
                assert result['version'] == 1
            finally:
                os.unlink(f.name)

    def test_read_yaml_with_substitution(self):
        """Read YAML with template substitution."""
        yaml_content = """
project: TestProject
dataPath: /data/${project}
"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            f.write(yaml_content)
            f.flush()
            try:
                result = read_config(f.name, {})
                assert result['dataPath'] == '/data/TestProject'
            finally:
                os.unlink(f.name)

    def test_read_yaml_with_external_replace(self):
        """External replace dict values are used."""
        yaml_content = """
date: ${today}
"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            f.write(yaml_content)
            f.flush()
            try:
                result = read_config(f.name, {'today': '2025-01-20'})
                assert result['date'] == '2025-01-20'
            finally:
                os.unlink(f.name)

    def test_read_empty_yaml(self):
        """Empty YAML file returns empty dict."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            f.write('')
            f.flush()
            try:
                result = read_config(f.name, {})
                assert result == {}
            finally:
                os.unlink(f.name)


if __name__ == '__main__':
    pytest.main([__file__, '-v'])

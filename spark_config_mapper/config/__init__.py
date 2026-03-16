"""
spark_config_mapper/config

Configuration loading and template substitution module.
"""

from spark_config_mapper.config.loader import (
    read_config,
    recursive_template,
    merge_configs
)

from spark_config_mapper.config.validation import (
    validate_table_config,
    validate_tables_config,
    validate_required_keys,
    ConfigValidationError
)

__all__ = [
    'read_config',
    'recursive_template',
    'merge_configs',
    'validate_table_config',
    'validate_tables_config',
    'validate_required_keys',
    'ConfigValidationError',
]

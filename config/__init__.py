"""
spark_config_mapper/config

Configuration loading and template substitution module.
"""

from spark_config_mapper.config.loader import (
    read_config,
    recursive_template,
    merge_configs
)

__all__ = [
    'read_config',
    'recursive_template',
    'merge_configs'
]

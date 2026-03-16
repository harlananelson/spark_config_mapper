"""
spark_config_mapper/config/validation.py

Configuration validation for YAML-based table configurations.
Catches common configuration errors before any Spark work begins.
"""

from spark_config_mapper.header import get_logger

logger = get_logger(__name__)


class ConfigValidationError(Exception):
    """Raised when configuration validation fails."""
    pass


def validate_table_config(table_name, table_config):
    """
    Validate a single table configuration entry.

    Checks for common issues:
    - inputRegex must be a list of strings (not a bare string)
    - insert must be a list of strings (not a bare string)
    - colsRename must be a dict
    - source, if present, must be a string

    Parameters:
        table_name (str): Configuration key for this table
        table_config: Configuration value (should be dict or None)

    Returns:
        list[str]: List of warning/error messages (empty if valid)
    """
    issues = []

    if table_config is None:
        issues.append(f"{table_name}: config is None (expected dict)")
        return issues

    if not isinstance(table_config, dict):
        issues.append(f"{table_name}: config is {type(table_config).__name__} (expected dict)")
        return issues

    # source should be a string
    if 'source' in table_config:
        if not isinstance(table_config['source'], str):
            issues.append(
                f"{table_name}.source: expected string, got {type(table_config['source']).__name__}")

    # inputRegex should be a list
    if 'inputRegex' in table_config:
        val = table_config['inputRegex']
        if isinstance(val, str):
            issues.append(
                f"{table_name}.inputRegex: is a bare string (should be a list of strings)")
        elif isinstance(val, list):
            for i, pat in enumerate(val):
                if not isinstance(pat, str):
                    issues.append(
                        f"{table_name}.inputRegex[{i}]: expected string, got {type(pat).__name__}")
        elif val is not None:
            issues.append(
                f"{table_name}.inputRegex: expected list, got {type(val).__name__}")

    # insert should be a list
    if 'insert' in table_config:
        val = table_config['insert']
        if isinstance(val, str):
            issues.append(
                f"{table_name}.insert: is a bare string (should be a list of strings)")
        elif isinstance(val, list):
            for i, code in enumerate(val):
                if not isinstance(code, str):
                    issues.append(
                        f"{table_name}.insert[{i}]: expected string, got {type(code).__name__}")
        elif val is not None:
            issues.append(
                f"{table_name}.insert: expected list, got {type(val).__name__}")

    # colsRename should be a dict
    if 'colsRename' in table_config:
        val = table_config['colsRename']
        if not isinstance(val, dict) and val is not None:
            issues.append(
                f"{table_name}.colsRename: expected dict, got {type(val).__name__}")

    return issues


def validate_tables_config(tables_dict, section_name='RWDTables', strict=False):
    """
    Validate an entire tables configuration section.

    Parameters:
        tables_dict (dict): Dictionary of table configurations
            (e.g., config['RWDTables'])
        section_name (str): Name of the config section (for error messages)
        strict (bool): If True, raise ConfigValidationError on any issue.
            If False (default), log warnings and return issues list.

    Returns:
        list[str]: All validation issues found

    Raises:
        ConfigValidationError: If strict=True and any issues found
    """
    all_issues = []

    if tables_dict is None:
        msg = f"{section_name}: is None (expected dict of table configs)"
        if strict:
            raise ConfigValidationError(msg)
        logger.warning(msg)
        return [msg]

    if not isinstance(tables_dict, dict):
        msg = f"{section_name}: is {type(tables_dict).__name__} (expected dict)"
        if strict:
            raise ConfigValidationError(msg)
        logger.warning(msg)
        return [msg]

    for table_name, table_config in tables_dict.items():
        issues = validate_table_config(table_name, table_config)
        all_issues.extend(issues)

    if all_issues:
        for issue in all_issues:
            logger.warning(f"Config validation: {issue}")
        if strict:
            raise ConfigValidationError(
                "{} config has {} issue(s):\n  - {}".format(
                    section_name, len(all_issues), '\n  - '.join(all_issues)))

    return all_issues


def validate_required_keys(config, required_keys, section_name='config'):
    """
    Validate that required top-level keys exist in a config dict.

    Parameters:
        config (dict): Configuration dictionary
        required_keys (list[str]): Keys that must be present
        section_name (str): Name for error messages

    Returns:
        list[str]: List of missing keys

    Example:
        >>> missing = validate_required_keys(config, ['schema', 'disease', 'schemaTag'])
        >>> if missing:
        ...     print(f"Missing: {missing}")
    """
    if not isinstance(config, dict):
        return required_keys

    missing = [k for k in required_keys if k not in config]
    if missing:
        logger.warning(f"{section_name}: missing required keys: {missing}")
    return missing

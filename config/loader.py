"""
spark_config_mapper/config/loader.py

Configuration loading and template substitution for YAML-based configurations.
Supports hierarchical configuration with cascading template substitution.

Compatible with Python 3.6+
"""

from spark_config_mapper.header import (
    yaml, Template, Dict, pprint, deepcopy, get_logger
)

logger = get_logger(__name__)


def read_config(yaml_file, replace, debug=False):
    # type: (str, Dict, bool) -> Dict
    """
    Read a YAML configuration file and apply template substitutions.

    This function opens a YAML file, reads its contents into a dictionary, and applies
    template replacements recursively. This enables hierarchical configuration where
    values can reference other configuration keys.

    Parameters:
        yaml_file (str or Path): Path to the YAML configuration file
        replace (Dict): Dictionary of key-value pairs for template substitution.
            Keys are placeholders (e.g., '$projectSchema'), values are replacements.
        debug (bool): If True, prints debug information about the loading process

    Returns:
        Dict: Configuration dictionary with all template substitutions applied

    Example:
        >>> replace = {'today': '2025-01-19', 'projectSchema': 'sicklecell_rwd'}
        >>> config = read_config('000-config.yaml', replace)
        >>> config['schemas']['projectSchema']
        'sicklecell_rwd'

    Notes:
        - Template syntax uses Python's string.Template: $key or ${key}
        - Substitutions cascade: earlier keys can be referenced by later ones
        - Missing keys are left as-is (safe_substitute behavior)
    """
    replace = deepcopy(replace)  # Don't modify caller's dict

    with open(str(yaml_file)) as cf:
        raw_dict = yaml.safe_load(cf)

    if raw_dict is None:
        raw_dict = {}

    dictionary = recursive_template(raw_dict, replace=replace)

    if debug:
        logger.info("Loaded configuration from: {}".format(yaml_file))
        pprint.pprint(dictionary)

    return dictionary


def recursive_template(d, replace, top_call=True, debug=False):
    # type: (Dict, Dict, bool, bool) -> Dict
    """
    Recursively apply template substitutions to a dictionary.

    Traverses a nested dictionary structure and replaces $key or ${key} patterns
    with corresponding values from the replace dictionary. When top_call is True,
    new substitutions are added to the replace dict, enabling cascading references
    where later keys can reference earlier ones.

    Parameters:
        d (Dict): Dictionary to process. If None or empty, returns empty dict.
        replace (Dict): Substitution mappings
        top_call (bool): If True, propagate new substitutions back to replace dict.
            This enables cascading where 'path: $base/data' can reference
            'base: /home' defined earlier in the same dict.
        debug (bool): Enable debug logging

    Returns:
        Dict: Dictionary with all template substitutions applied

    Example:
        >>> d = {'base': '/data', 'path': '$base/project'}
        >>> recursive_template(d, {})
        {'base': '/data', 'path': '/data/project'}

    Notes:
        - Uses Python's string.Template with safe_substitute (missing keys left as-is)
        - Handles: strings, lists (of strings/dicts/lists), nested dicts, pass-through others
        - Order matters: in Python 3.7+ dict order is preserved, enabling cascading

    See Also:
        For Python 3.8+ projects, consider using OmegaConf instead:
        https://github.com/omry/omegaconf
    """
    if debug:
        logger.debug("Starting recursive_template")

    # Handle None or non-dict input
    if d is None:
        return {}
    if not isinstance(d, dict):
        return d

    new_dict = {}

    for k, v in d.items():
        # If key exists in replace, use that value instead
        if k in replace:
            v = replace[k]

        if isinstance(v, str):
            # Apply template substitution to string values
            new_dict[k] = Template(v).safe_substitute(**replace)
            if top_call:
                # Update replace dict so later keys can reference this one
                replace[k] = new_dict[k]

        elif isinstance(v, list):
            # Process list items recursively
            new_dict[k] = _process_list(v, replace)

        elif isinstance(v, dict):
            # Recursively process nested dictionaries
            new_dict[k] = recursive_template(v, replace, top_call=False, debug=debug)

        else:
            # Pass through non-string, non-container values unchanged
            # (int, float, bool, None, etc.)
            new_dict[k] = v

    return new_dict


def _process_list(lst, replace):
    # type: (list, Dict) -> list
    """
    Process a list, applying template substitution to its elements.

    Handles:
        - Strings: apply Template substitution
        - Dicts: recursively process with recursive_template
        - Lists: recursively process (nested lists)
        - Other types: pass through unchanged

    Parameters:
        lst (list): List to process
        replace (Dict): Substitution mappings

    Returns:
        list: Processed list with substitutions applied
    """
    new_list = []
    for item in lst:
        if isinstance(item, str):
            new_list.append(Template(item).safe_substitute(replace))
        elif isinstance(item, dict):
            new_list.append(recursive_template(item, replace, top_call=False))
        elif isinstance(item, list):
            # Recursively process nested lists (fixes previous bug)
            new_list.append(_process_list(item, replace))
        else:
            # Pass through int, float, bool, None, etc.
            new_list.append(item)
    return new_list


def merge_configs(*configs):
    # type: (*Dict) -> Dict
    """
    Merge multiple configuration dictionaries with later ones taking precedence.

    Performs a deep merge: nested dictionaries are merged recursively rather than
    replaced entirely.

    Parameters:
        *configs: Variable number of configuration dictionaries

    Returns:
        Dict: Merged configuration dictionary

    Example:
        >>> global_cfg = {'db': 'default', 'settings': {'timeout': 30}}
        >>> project_cfg = {'db': 'project_db', 'settings': {'retries': 3}}
        >>> merge_configs(global_cfg, project_cfg)
        {'db': 'project_db', 'settings': {'timeout': 30, 'retries': 3}}
    """
    result = {}
    for config in configs:
        if config:
            _deep_merge(result, config)
    return result


def _deep_merge(base, override):
    # type: (Dict, Dict) -> None
    """
    Deep merge override into base dictionary in-place.

    Parameters:
        base: Base dictionary (modified in-place)
        override: Dictionary with values to override
    """
    for key, value in override.items():
        if key in base and isinstance(base[key], dict) and isinstance(value, dict):
            _deep_merge(base[key], value)
        else:
            base[key] = value

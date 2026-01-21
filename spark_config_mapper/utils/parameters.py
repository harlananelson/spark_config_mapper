"""
spark_config_mapper/utils/parameters.py

Function parameter handling utilities.
Provides tools for mapping configuration dictionaries to function parameters.
"""

from spark_config_mapper.header import (
    inspect, OrderedDict, pprint, get_logger
)

logger = get_logger(__name__)


def get_default_args(func) -> dict:
    """
    Get default argument values from a function signature.
    
    Parameters:
        func: Function to inspect
    
    Returns:
        dict: Dictionary of parameter names to default values
    
    Example:
        >>> def my_func(a, b=10, c='hello'):
        ...     pass
        >>> get_default_args(my_func)
        {'b': 10, 'c': 'hello'}
    """
    signature = inspect.signature(func)
    return {
        k: v.default
        for k, v in signature.parameters.items()
        if v.default is not inspect.Parameter.empty
    }


def missingParameters(config_dict: dict, function) -> list:
    """
    Find function parameters that are missing from the config dictionary.
    
    Parameters:
        config_dict (dict): Configuration dictionary
        function: Function to check against
    
    Returns:
        list: Parameter names not in config_dict that have no defaults
    """
    fn_args = inspect.getfullargspec(function).args
    defaults = get_default_args(function)
    
    missing = []
    for arg in fn_args:
        if arg not in config_dict and arg not in defaults:
            if arg != 'self':  # Ignore self parameter
                missing.append(arg)
    
    return missing


def get_parameters(config_dict: dict, function, config_dictt: dict = None,
                   debug: bool = False) -> OrderedDict:
    """
    Extract parameters for a function from a configuration dictionary.
    
    Maps keys from config_dict to the function's parameter names,
    using defaults where available.
    
    Parameters:
        config_dict (dict): Configuration values
        function: Target function
        config_dictt (dict): Additional config (deprecated, use config_dict)
        debug (bool): Enable debug output
    
    Returns:
        OrderedDict: Parameters in function signature order
    """
    # Get function arguments in order
    fn_args = inspect.getfullargspec(function).args
    
    # Get defaults
    args_default = get_default_args(function)
    
    # Filter config to only matching arguments
    new_values = {k: v for k, v in config_dict.items() if k in fn_args}
    
    # Build ordered result
    result = OrderedDict.fromkeys(fn_args, None)
    result.update(args_default)
    result.update(new_values)
    
    if debug:
        logger.debug(f"Function args: {fn_args}")
        logger.debug(f"Defaults: {args_default}")
        logger.debug(f"New values: {new_values}")
    
    return result


def getParameters(config_dict: dict, function, config_dictt: dict = None,
                  debug: bool = False) -> OrderedDict:
    """Alias for get_parameters for backward compatibility."""
    return get_parameters(config_dict, function, config_dictt, debug)


def set_function_parameters(function, attributes_param: dict, 
                            config_dict: dict = None,
                            debug: bool = False) -> OrderedDict:
    """
    Combine attribute parameters with config to create function parameters.
    
    Parameters:
        function: Target function
        attributes_param (dict): Direct parameter values
        config_dict (dict): Base configuration
        debug (bool): Enable debug output
    
    Returns:
        OrderedDict: Combined parameters for function call
    """
    config_dict = config_dict or {}
    config_dict_local = config_dict.copy()
    config_dict_local.update(attributes_param)
    
    return get_parameters(config_dict_local, function, debug=debug)


def setFunctionParameters(function, funParam: dict = None, 
                          config_dict: dict = None,
                          update: bool = False,
                          debug: bool = False) -> dict:
    """
    Create a parameter dictionary suitable for calling a function.
    
    This is the primary interface for converting configuration dictionaries
    into function call parameters. It handles parameter filtering, default
    values, and type coercion.
    
    Parameters:
        function: Target function to call
        funParam (dict): Function-specific parameters (take precedence)
        config_dict (dict): Base configuration dictionary
        update (bool): If True, update config_dict with funParam
        debug (bool): Enable debug output
    
    Returns:
        dict: Parameters ready for **kwargs unpacking
    
    Example:
        >>> def process_data(df, schema, cohort=None, debug=False):
        ...     pass
        >>> config = {'schema': 'my_schema', 'debug': True}
        >>> params = setFunctionParameters(process_data, 
        ...     {'df': my_df}, config)
        >>> process_data(**params)
    """
    funParam = funParam or {}
    config_dict = config_dict or {}
    
    if update:
        config_dict.update(funParam)
    
    config_dict2 = config_dict.copy()
    config_dict2.update(funParam)
    
    if debug:
        missing = missingParameters(config_dict2, function)
        if missing:
            logger.warning(f"Missing parameters: {missing}")
    
    addCall = get_parameters(config_dict2, function, debug=debug)
    addCall.update(funParam)
    
    # Remove 'self' parameter
    filtered = {k: v for k, v in addCall.items() if k != 'self'}
    
    if debug:
        pprint.pprint({k: v for k, v in filtered.items() if k != 'DF'})
    
    return filtered


def set_default_params(obj, params: list, default_values: dict, **kwargs) -> dict:
    """
    Set default parameter values from an object's attributes.
    
    Checks if the object has attributes matching parameter names and uses
    those values as defaults if they exist.
    
    Parameters:
        obj: Object to get attributes from
        params (list): Parameter names to check
        default_values (dict): Fallback default values
        **kwargs: Existing parameter values
    
    Returns:
        dict: kwargs with defaults applied
    
    Example:
        >>> class Config:
        ...     schema = 'my_schema'
        >>> defaults = {'schema': 'default', 'limit': 100}
        >>> set_default_params(Config(), ['schema', 'limit'], defaults)
        {'schema': 'my_schema', 'limit': 100}
    """
    for param in params:
        if hasattr(obj, param):
            kwargs.setdefault(param, getattr(obj, param))
        else:
            kwargs.setdefault(param, default_values.get(param))
    return kwargs

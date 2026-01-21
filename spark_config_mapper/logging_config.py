"""
spark_config_mapper/logging_config.py

Centralized logging configuration for spark_config_mapper.

QUICK START:
    # Production (default) - only warnings and errors
    import spark_config_mapper

    # Debug mode - see what's happening
    import spark_config_mapper
    spark_config_mapper.set_log_level('DEBUG')

    # Or via environment variable (before import):
    # export SPARK_CONFIG_MAPPER_LOG_LEVEL=DEBUG

LOGGING LEVELS (from least to most verbose):
    CRITICAL - Package is broken, cannot continue
    ERROR    - Operation failed, but package can continue
    WARNING  - Something unexpected, user should know (default for production)
    INFO     - Normal operation milestones (table written, config loaded)
    DEBUG    - Detailed diagnostic information

Compatible with Python 3.6+
"""

import logging
import os
import sys

# Package-wide logger name prefix
PACKAGE_NAME = 'spark_config_mapper'

# Environment variable for log level
LOG_LEVEL_ENV_VAR = 'SPARK_CONFIG_MAPPER_LOG_LEVEL'

# Default log level (production = WARNING, only show problems)
DEFAULT_LOG_LEVEL = 'WARNING'

# Format strings
SIMPLE_FORMAT = '%(levelname)s - %(message)s'
DETAILED_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
DEBUG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s'

# Track if logging has been configured
_logging_configured = False


def get_log_level_from_env():
    # type: () -> str
    """Get log level from environment variable or return default."""
    level = os.environ.get(LOG_LEVEL_ENV_VAR, DEFAULT_LOG_LEVEL).upper()
    valid_levels = ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']
    if level not in valid_levels:
        print("Warning: Invalid log level '{}', using '{}'".format(level, DEFAULT_LOG_LEVEL),
              file=sys.stderr)
        return DEFAULT_LOG_LEVEL
    return level


def configure_logging(level=None, format_style='auto', stream=None):
    # type: (str, str, object) -> None
    """
    Configure logging for the spark_config_mapper package.

    This function should be called once at the start of your script/notebook
    if you want to customize logging. If not called, default configuration
    (WARNING level) is used automatically.

    Parameters:
        level (str): Log level - 'DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'
                     If None, uses environment variable or default (WARNING)
        format_style (str): 'simple', 'detailed', 'debug', or 'auto'
                           'auto' uses simple for WARNING+, detailed for INFO/DEBUG
        stream: Output stream (default: sys.stderr)

    Example:
        # At start of notebook
        from spark_config_mapper import configure_logging
        configure_logging('DEBUG')  # See everything

        # Or for production
        configure_logging('WARNING')  # Only problems
    """
    global _logging_configured

    # Determine level
    if level is None:
        level = get_log_level_from_env()
    level = level.upper()

    # Get numeric level
    numeric_level = getattr(logging, level, logging.WARNING)

    # Determine format
    if format_style == 'auto':
        if numeric_level <= logging.DEBUG:
            format_str = DEBUG_FORMAT
        elif numeric_level <= logging.INFO:
            format_str = DETAILED_FORMAT
        else:
            format_str = SIMPLE_FORMAT
    elif format_style == 'simple':
        format_str = SIMPLE_FORMAT
    elif format_style == 'detailed':
        format_str = DETAILED_FORMAT
    elif format_style == 'debug':
        format_str = DEBUG_FORMAT
    else:
        format_str = SIMPLE_FORMAT

    # Get the package logger
    package_logger = logging.getLogger(PACKAGE_NAME)

    # Remove existing handlers to avoid duplicates
    package_logger.handlers = []

    # Create handler
    handler = logging.StreamHandler(stream or sys.stderr)
    handler.setLevel(numeric_level)
    handler.setFormatter(logging.Formatter(format_str))

    # Configure package logger
    package_logger.setLevel(numeric_level)
    package_logger.addHandler(handler)

    # Don't propagate to root logger (avoids duplicate messages)
    package_logger.propagate = False

    _logging_configured = True

    # Log that we configured (only visible if DEBUG)
    package_logger.debug("Logging configured: level={}, format={}".format(level, format_style))


def set_log_level(level):
    # type: (str) -> None
    """
    Change the log level for spark_config_mapper.

    This is a convenience function for quickly changing verbosity.

    Parameters:
        level (str): 'DEBUG', 'INFO', 'WARNING', 'ERROR', or 'CRITICAL'

    Example:
        import spark_config_mapper
        spark_config_mapper.set_log_level('DEBUG')  # See details
        # ... do work ...
        spark_config_mapper.set_log_level('WARNING')  # Back to quiet
    """
    level = level.upper()
    numeric_level = getattr(logging, level, logging.WARNING)

    package_logger = logging.getLogger(PACKAGE_NAME)
    package_logger.setLevel(numeric_level)

    for handler in package_logger.handlers:
        handler.setLevel(numeric_level)

    # Update format if switching to/from DEBUG
    if numeric_level <= logging.DEBUG:
        for handler in package_logger.handlers:
            handler.setFormatter(logging.Formatter(DEBUG_FORMAT))
    elif numeric_level <= logging.INFO:
        for handler in package_logger.handlers:
            handler.setFormatter(logging.Formatter(DETAILED_FORMAT))
    else:
        for handler in package_logger.handlers:
            handler.setFormatter(logging.Formatter(SIMPLE_FORMAT))


def get_logger(name):
    # type: (str) -> logging.Logger
    """
    Get a logger for a module within spark_config_mapper.

    This creates a child logger under the package namespace, ensuring
    all loggers respect the package-wide configuration.

    Parameters:
        name (str): Module name (usually __name__)

    Returns:
        logging.Logger: Configured logger instance

    Example:
        # In spark_config_mapper/utils/spark_ops.py
        from spark_config_mapper.logging_config import get_logger
        logger = get_logger(__name__)
        # Creates logger: spark_config_mapper.utils.spark_ops

        logger.debug("Processing started")  # Only shown if DEBUG level
        logger.info("Table written")        # Shown if INFO or DEBUG
        logger.warning("Missing column")    # Shown if WARNING or above
        logger.error("Failed to write")     # Always shown (unless CRITICAL only)
    """
    global _logging_configured

    # Auto-configure with defaults if not yet configured
    if not _logging_configured:
        configure_logging()

    # Ensure name is under package namespace
    if not name.startswith(PACKAGE_NAME):
        name = "{}.{}".format(PACKAGE_NAME, name)

    return logging.getLogger(name)


def silence():
    # type: () -> None
    """
    Silence all spark_config_mapper logging (set to CRITICAL only).

    Useful when you want no output at all.

    Example:
        import spark_config_mapper
        spark_config_mapper.silence()
    """
    set_log_level('CRITICAL')


def verbose():
    # type: () -> None
    """
    Enable verbose/debug logging for spark_config_mapper.

    Useful for troubleshooting.

    Example:
        import spark_config_mapper
        spark_config_mapper.verbose()
    """
    set_log_level('DEBUG')


# Guidance for when to use each level
LOG_LEVEL_GUIDANCE = """
LOGGING LEVEL GUIDANCE FOR DEVELOPERS:

logger.debug(msg)
    - Function entry/exit with parameters
    - Loop iterations
    - Variable values during processing
    - "Found 5 array fields: ['a', 'b', ...]"
    - Configuration details being applied

logger.info(msg)
    - Operation milestones users care about
    - "Table written: schema.table_name"
    - "Exploding single array column: medications"
    - "Loaded configuration from: config.yaml"

logger.warning(msg)
    - Unexpected but recoverable situations
    - "Column 'tenant' not found, skipping partitioning"
    - "Multiple arrays found, skipping explosion"
    - "Empty DataFrame, no rows written"
    - Things users should probably know about

logger.error(msg)
    - Operation failures (usually before raising exception)
    - "Failed to write table: permission denied"
    - "Schema not found: my_database"
    - Include enough context to diagnose

logger.critical(msg)
    - Package cannot function
    - Rarely used
    - "Spark session not available"

BEST PRACTICES:
1. Include context: logger.error("Failed to write {}: {}".format(table, error))
2. Don't log AND raise with same message (choose one)
3. Use debug() liberally - it's free when not enabled
4. Use warning() for "user should know but we can continue"
5. Format with .format() not f-strings (Python 3.6 compat)
"""

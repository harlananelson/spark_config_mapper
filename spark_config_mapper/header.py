"""
spark_config_mapper/header.py

Central header module for spark-config-mapper package.
Contains all external imports and Spark session initialization.

Logging is handled by logging_config.py - import get_logger from there.

Compatible with Python 3.6+
"""

# Standard library imports
import os
import sys
import re
from pathlib import Path, PosixPath
from string import Template
from datetime import date, datetime
import time
import yaml
from copy import copy, deepcopy
import json
from importlib import reload
import pprint

# Type hints (Python 3.6 compatible)
from typing import Any, List, Tuple, Type, Dict, Optional

# Third-party library imports
import pandas as pd
import numpy as np
from collections import OrderedDict
import inspect
from functools import reduce

# Pyspark imports
from pyspark import SparkConf, SparkContext
from pyspark.sql.window import Window
from pyspark.sql import SparkSession, DataFrame, functions as F, types
from pyspark.sql.types import (
    StructField, FloatType, StringType, TimestampType,
    ArrayType, StructType, DateType, IntegerType
)
from pyspark.sql.utils import AnalysisException

# Import logging from our logging_config module
from spark_config_mapper.logging_config import get_logger

# Constants
JOIN_INNER = 'inner'

# Module logger
logger = get_logger(__name__)


def get_or_create_spark_session():
    # type: () -> SparkSession
    """
    Get existing Spark session or create a new one with standard configurations.

    Includes Hive metastore support for IU Health Datalab environment.
    Set HADOOP_CONF_DIR=/etc/jupyter/configs before importing for metastore access.

    Returns:
        SparkSession: The active Spark session
    """
    # Ensure HADOOP_CONF_DIR is set for metastore access (IU Health Datalab)
    if 'HADOOP_CONF_DIR' not in os.environ:
        hadoop_conf = '/etc/jupyter/configs'
        if os.path.isdir(hadoop_conf):
            os.environ['HADOOP_CONF_DIR'] = hadoop_conf
            logger.debug(f"Set HADOOP_CONF_DIR={hadoop_conf}")

    # Build session with Hive support for metastore access
    builder = (SparkSession.builder
        .config("spark.sql.catalogImplementation", "hive")
        .config("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation", "true")
        .config("spark.sql.autoBroadcastJoinThreshold", 200 * 1024 * 1024)  # 200MB
        .config("spark.sql.broadcastTimeout", 3600)
        .config("spark.driver.maxResultSize", "8g")
        .config("spark.driver.memory", "8g")
        .config("spark.executor.memory", "8g")
        .config("spark.executor.cores", "5")
        .config("hive.exec.dynamic.partition.mode", "nonstrict")
        .enableHiveSupport()
    )

    spark = builder.getOrCreate()

    return spark


# Initialize SparkSession on module load
spark = get_or_create_spark_session()
logger.debug("Spark Session initialized by spark_config_mapper.header")

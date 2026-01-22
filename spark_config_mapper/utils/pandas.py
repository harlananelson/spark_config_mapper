"""
spark_config_mapper/utils/pandas.py

Pandas utility functions for data conversion and manipulation.
"""

from spark_config_mapper.header import pd


def dict2Pandas(dict, columnname='codes'):
    """
    Convert a dictionary to a Pandas DataFrame.

    Parameters:
        dict: Dictionary to convert (keys become index, values become column)
        columnname (str): Name for the values column

    Returns:
        pd.DataFrame: DataFrame with dictionary keys as index
    """
    return pd.DataFrame.from_dict(dict, orient='index', columns=[columnname])

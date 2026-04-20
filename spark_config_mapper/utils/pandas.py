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
        columnname (str or list): Name for the values column. A single
            string is wrapped into a one-element list; an existing list
            is passed through. Historically some callers passed a list
            like ['codes'], which was then re-wrapped to [['codes']] and
            produced pandas MultiIndex columns with tuple names (e.g.
            ('codes',)) that broke downstream Spark conversion.

    Returns:
        pd.DataFrame: DataFrame with dictionary keys as index
    """
    cols = columnname if isinstance(columnname, list) else [columnname]
    return pd.DataFrame.from_dict(dict, orient='index', columns=cols)

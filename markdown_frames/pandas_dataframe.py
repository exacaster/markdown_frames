"""Functions that parse markdown table or CSV file to Pandas DataFrame."""

import pandas as pd

from markdown_frames.table import Table
from markdown_frames.utils import parse_file, parse_markdown


def pandas_df(markdown_table: str) -> pd.DataFrame:
    """Returns a Pandas DataFrame with specified types.

    Args:
        markdown_table (str): Markdown representation of input data.

    Returns:
        DataFrame: Pandas Dataframe with data.
    """

    table = Table(parse_markdown(markdown_table))
    return pd.DataFrame(data=table.to_pandas_data())


def pandas_df_from_csv(path: str, delimiter: str = ",") -> pd.DataFrame:
    """Returns a Pandas DataFrame built from CSV. Second line in CSV must represent data types.

    CSV file format
    ```
    column_name1, column_name2, column_name3
    string, int, float
    hellow, 1, 1.1
    goodbye, 2, 2.2
    ```
    Args:
        path (str): Path where `.csv` file is placed.

    Returns:
        DataFrame: Pandas DataFrame for provided path.
    """

    table = Table(parse_file(path, delimiter))
    return pd.DataFrame(data=table.to_pandas_data())

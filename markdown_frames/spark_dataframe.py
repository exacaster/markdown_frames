"""Functions that parse markdown table or CSV file to Apache Spark (PySpark)
DataFrame.
"""
from pyspark.sql import DataFrame, SparkSession

from markdown_frames.table import Table
from markdown_frames.utils import parse_file, parse_markdown


def spark_df(markdown: str, spark: SparkSession) -> DataFrame:
    """Returns a Spark DataFrame with specified types.

    Args:
        markdown (str): Markdown representation of input data.
        spark (SparkSession): Spark Session object.

    Returns:
        DataFrame: DataFrame with data and schema specified.
    """

    table = Table(parse_markdown(markdown))
    data, schema = table.to_spark_data()
    return spark.createDataFrame(data, schema)


def spark_df_from_csv(path: str, spark: SparkSession, delimiter: str = ",") -> DataFrame:
    """Returns a Spark DataFrame built from CSV. Second line in CSV must represent data types.

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
        DataFrame: Spark DataFrame for provided path.
    """

    table = Table(parse_file(path, delimiter))
    data, schema = table.to_spark_data()
    return spark.createDataFrame(data, schema)

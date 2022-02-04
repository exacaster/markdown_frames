from datetime import datetime
from decimal import Decimal

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    MapType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from markdown_frames.spark_dataframe import spark_df, spark_df_from_csv


def test_spark_df_markdown(spark_session: SparkSession) -> None:
    input_table1 = """
        | column1 | column2 | column3 | column4 |
        |   int   |  string |  float  |  bigint |
        | ******* | ******* | ******* | ******* |
        |   1     |   user1 |   3.14  |  111111 |
        |   2     |   null  |   1.618 |  222222 |
        |   3     |   ''    |   2.718 |  333333 |
        """
    input_table2 = """
        | column1      |        column2      | column3 |  column4   |
        | decimal(4,2) |       timestamp     |  double |   string   |
        |     2.2      | 2017-01-01 23:59:59 |  2.333  | 2017-01-01 |
        |     3.33     |         None        |  3.222  | 2017-12-31 |
        |     na       | 2017-12-31 23:59:59 |  4.444  | 2017-12-31 |
        """
    input_table3 = """
        |   column1    |        column2       | column3     |  column4   |
        | decimal(4,2) | map<str, array<int>> |  array<int> |   string   |
        |     2.22     | {'key': [1, 2, 3]}   |  [1, 1, 1]  | 2017-01-01 |
        |     3.33     | {'key': [3, 1, 2]}   |  [1, 2, 2]  | 2017-12-31 |
        |      na      |         None         |  [5, 5, 5]  | 2017-12-31 |
        """
    expected_schema1 = StructType(
        [
            StructField("column1", IntegerType()),
            StructField("column2", StringType()),
            StructField("column3", FloatType()),
            StructField("column4", LongType()),
        ]
    )
    expected_table1 = spark_session.createDataFrame(
        [(1, "user1", 3.14, 111_111), (2, None, 1.618, 222_222), (3, "", 2.718, 333_333)],
        expected_schema1,
    )
    expected_schema2 = StructType(
        [
            StructField("column1", DecimalType(4, 2)),
            StructField("column2", TimestampType()),
            StructField("column3", DoubleType()),
            StructField("column4", StringType()),
        ]
    )
    expected_table2 = spark_session.createDataFrame(
        [
            (Decimal("2.22"), datetime(2017, 1, 1, 23, 59, 59), 2.333, "2017-01-01"),
            (Decimal("3.33"), None, 3.222, "2017-12-31"),
            (None, datetime(2017, 12, 31, 23, 59, 59), 4.444, "2017-12-31"),
        ],
        expected_schema2,
    )
    epxected_schema3 = StructType(
        [
            StructField("column1", DecimalType(4, 2)),
            StructField("column2", MapType(StringType(), ArrayType(IntegerType()))),
            StructField("column3", ArrayType(IntegerType())),
            StructField("column4", StringType()),
        ]
    )
    expected_table3 = spark_session.createDataFrame(
        [
            (Decimal("2.22"), {"key": [1, 2, 3]}, [1, 1, 1], "2017-01-01"),
            (Decimal("3.33"), {"key": [3, 1, 2]}, [1, 2, 2], "2017-12-31"),
            (None, None, [5, 5, 5], "2017-12-31"),
        ],
        epxected_schema3,
    )

    output_table1 = spark_df(input_table1, spark_session)
    output_table2 = spark_df(input_table2, spark_session)
    output_table3 = spark_df(input_table3, spark_session)

    output_table1.toPandas().equals(expected_table1.toPandas())
    output_table2.toPandas().equals(expected_table2.toPandas())
    output_table3.toPandas().equals(expected_table3.toPandas())


def test_spark_df_csv_file(spark_session: SparkSession) -> None:
    input_file_1 = "tests/data/test.csv"
    input_file_2 = "tests/data/test2.csv"

    output_1 = spark_df_from_csv(input_file_1, spark_session)
    output_2 = spark_df_from_csv(input_file_2, spark_session)

    expected_1 = spark_session.createDataFrame(
        [["haha", 1, 1.1]],
        StructType(
            [
                StructField("name1", StringType()),
                StructField("name2", IntegerType()),
                StructField("Name3", FloatType()),
            ]
        ),
    )
    expected_2 = spark_session.createDataFrame(
        [["HaHa"]], StructType([StructField("name1", StringType())])
    )

    assert output_1.toPandas().equals(expected_1.toPandas())
    assert output_2.toPandas().equals(expected_2.toPandas())


def test_spark_df_should_correctly_work_with_boolean_values(spark_session: SparkSession) -> None:
    markdown_table = """
    | is_sms_active |
    |    boolean    |
    | ------------- |
    | true          |
    | False         |
    """

    expected_table_schema = StructType([StructField("sub_billing_id", BooleanType())])
    expected_table = spark_session.createDataFrame([(True,), (False,)], expected_table_schema)
    output = spark_df(markdown_table, spark_session)
    output.show()
    assert output.collect() == expected_table.collect()

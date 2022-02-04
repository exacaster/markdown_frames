from datetime import datetime
from decimal import Decimal

import pandas as pd

from markdown_frames.pandas_dataframe import pandas_df, pandas_df_from_csv


def test_pandas_df() -> None:
    input_table1 = """
        | column1 | column2 | column3 | column4 |
        |   int   |  string |  float  |  bigint |
        | ******* | ******* | ******* | ******* |
        |   1     |   user1 |   3.14  |  111111 |
        |   2     |   None  |   1.618 |  222222 |
        |   3     |   user4 |   2.718 |  333333 |
        """
    input_table2 = """
        |    column1   |        column2      | column3 |  column4   |
        | decimal<4.2> |       timestamp     |  double |   string   |
        |     2.22     | 2017-01-01 23:59:59 |  2.333  | 2017-01-01 |
        |     3.33     |         None        |  3.222  | 2017-12-31 |
        |     None     | 2017-12-31 23:59:59 |  4.444  | 2017-12-31 |
        """

    expected_table1 = pd.DataFrame(
        [(1, "user1", 3.14, 111_111), (2, None, 1.618, 222_222), (3, "user4", 2.718, 333_333)],
        columns=["column1", "column2", "column3", "column4"],
    )
    expected_table2 = pd.DataFrame(
        [
            (Decimal("2.22"), datetime(2017, 1, 1, 23, 59, 59), 2.333, "2017-01-01"),
            (Decimal("3.33"), None, 3.222, "2017-12-31"),
            (None, datetime(2017, 12, 31, 23, 59, 59), 4.444, "2017-12-31"),
        ],
        columns=["column1", "column2", "column3", "column4"],
    )

    output1 = pandas_df(input_table1)
    output2 = pandas_df(input_table2)

    assert output1.equals(expected_table1)
    assert output2.equals(expected_table2)


def test_pandas_df_csv_file() -> None:
    input_file_1 = "tests/data/test.csv"
    output_1 = pandas_df_from_csv(input_file_1)
    expected_1 = pd.DataFrame([("haha", 1, 1.1)], columns=["name1", "name2", "Name3"])

    assert output_1.equals(expected_1)

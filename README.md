# Markdown Frames

Helper package for testing Apache Spark and Pandas DataFrames.
It makes your data-related unit tests more readable.

## History

While working at [Exacaster](https://exacaster.com/) [Vaidas Armonas](https://github.com/Va1da2) came up with the idea to make testing data more representable. And with the help of his team, he implemented the initial version of this package.

Before that, we had to define our testing data as follows:
```python
schema = ["user_id", "even_type", "item_id", "event_time", "country", "dt"]
input_df = spark.createDataFrame([
    (123456, 'page_view', None, datetime(2017,12,31,23,50,50), "uk", "2017-12-31"),
    (123456, 'item_view', 68471513, datetime(2017,12,31,23,50,55), "uk", "2017-12-31")], 
    schema)
```

And with this library you can define same data like this:
```python
input_data = """ 
    |  user_id   |  even_type  | item_id  |    event_time       | country  |     dt      |
    |   bigint   |   string    |  bigint  |    timestamp        |  string  |   string    |
    | ---------- | ----------- | -------- | ------------------- | -------- | ----------- |
    |   123456   |  page_view  |   None   | 2017-12-31 23:50:50 |   uk     | 2017-12-31  |
    |   123456   |  item_view  | 68471513 | 2017-12-31 23:50:55 |   uk     | 2017-12-31  |
"""
input_df = spark_df(input_data, spark)
```

## Installation
To install this package, run this command on your python environment:
```bash
pip install markdown_frames[pyspark]
```

## Usage

When you have this package installed, you can use it in your unit tests as follows (assuming you are using `pytest-spark` ang have Spark Session available):

```python
from pyspark.sql import SparkSession
from markdown_frames.spark_dataframe import spark_df

def test_your_use_case(spark: SpakSession): -> None
    expected_data = """
        | column1 | column2 | column3 | column4 |
        |   int   |  string |  float  |  bigint |
        | ------- | ------- | ------- | ------- |
        |   1     |   user1 |   3.14  |  111111 |
        |   2     |   null  |   1.618 |  222222 |
        |   3     |   ''    |   2.718 |  333333 |
        """
    expected_df = spark_df(expected_data, spark)

    actaual_df = your_use_case(spark)

    assert expected_df.collect()) == actaual_df.collect())
```

## Supported data types

This package supports all major datatypes, use these type names in your table definitions:
- `int`
- `bigint`
- `float`
- `double`
- `string`
- `boolean`
- `timestamp`
- `decimal(precision,scale)` (scale and precision must be integers)
- `array<int>` (int can be replaced by  any of mentioned types)
- `map<string,int>` (string and int can be replaced by any of mentioned types)

## License

This project is [MIT](./LICENSE) licensed.

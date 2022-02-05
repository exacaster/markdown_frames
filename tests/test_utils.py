from markdown_frames.utils import parse_file, parse_markdown


def test_parse_markdown() -> None:
    table1 = """
        | column1 | column2 | column3 |
        |   str   |  int    |  float  |
        | ------- | ------- | ------- |
        |  hello  |   1     |  1.11   |
        """
    table2 = """
        | column1 | column2 | column3 |
        |   str   |  int    |  float  |
        | ------- | ------- | ------- |
        |  hello  |   1     |  1.11   |
        """
    table3 = """
        | column1 | column2 |    column3         |
        |   str   |  int    |  map<string, int>  |
        |  hello  |   1     |  {'asdd': 1}       |
        """

    expected_table1 = [
        ["column1", "column2", "column3"],
        ["str", "int", "float"],
        ["hello", "1", "1.11"],
    ]
    expected_table2 = [
        ["column1", "column2", "column3"],
        ["str", "int", "float"],
        ["hello", "1", "1.11"],
    ]
    expected_table3 = [
        ["column1", "column2", "column3"],
        ["str", "int", "map<string, int>"],
        ["hello", "1", "{'asdd': 1}"],
    ]

    output_table1 = parse_markdown(table1)
    output_table2 = parse_markdown(table2)
    output_table3 = parse_markdown(table3)

    assert output_table1 == expected_table1
    assert output_table2 == expected_table2
    assert output_table3 == expected_table3


def test_parse_file() -> None:
    output_table = parse_file("tests/data/test.csv")
    expected_table = [
        ["name1", "name2", "Name3"],
        ["string", "int", "float"],
        ["haha", "1", "1.1"],
    ]

    assert output_table == expected_table

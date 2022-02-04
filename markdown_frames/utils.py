"""Functions that are used for parsing."""

import csv
from typing import List

_SEPARATOR_SYMBOLS = ("#", "-", "+", "=", "*")


def _parse_row(row_string: str) -> List[str]:
    """Returns a formatted values for a row.

    Args:
        row_string (str): String that represent one row for input table.

    Returns:
        List[str]: :ist of formated values in the table row.
    """
    row_values = filter(lambda s: s.strip() != "", row_string.split("|"))
    return list(map(lambda s: s.strip(), row_values))


def parse_markdown(markdown_table: str) -> List[List[str]]:
    """Given markdown table produces a list of lists - table.

    Args:
        markdown_table (str): Table in markdown format.

    Returns:
        List[List[str]]: List of lists with rows of data (still in str format).
    """
    rows = map(_parse_row, markdown_table.split("\n"))
    # Remove empty rows
    filtered_table = list(filter(lambda x: x, rows))

    # Remove separator, if present
    if len(filtered_table) > 2:
        first_symbol = filtered_table[2][0][0]
        if first_symbol in _SEPARATOR_SYMBOLS and all(
            not len(val.replace(first_symbol, "")) for val in filtered_table[2]
        ):
            del filtered_table[2]

    return filtered_table


def parse_file(path: str, delimiter: str = ",") -> List[List[str]]:
    """Reads CSV file and returns Table representation for it.

    Args:
        path (str): File path.
        delimiter (str): CSV delimiter (default: `,`)

    Returns:
        List[List[str]]: List of lists with rows of data (still in str format).
    """
    with open(path, encoding="utf-8") as file:
        csv_file = csv.reader(file, delimiter=delimiter)
        return [list(map(lambda s: s.strip(), row)) for row in csv_file]

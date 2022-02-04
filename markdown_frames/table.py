"""Module dendicated for list conversions to data structures, needed for DataFrame construction"""
from typing import Any, Dict, List, Tuple

from markdown_frames.type_definitions import get_type


class InvalidDataException(Exception):
    """Exception thrown when Invalid table input is provided."""

    pass


class Column:
    """DataFrame column representation."""

    def __init__(self, name: str, dtype: str):
        self.name = name
        self._converter = get_type(dtype)
        self.dtype = self._converter.dtype if self._converter else dtype

    def value(self, val: str) -> Any:
        """Converts provided value to column data type.

        Args:
            val (str): String representation of the value.

        Returns:
            Any: Value converted to columns' data type.
        """
        if val is None or val.lower() in ("none", "null", "na"):
            return None

        return self._converter.convert(val)


class Table:
    """Table representation for DataFrame"""

    def __init__(self, table_rows: List[List[str]]):
        if len(table_rows) < 1:
            raise InvalidDataException("Table must contain at least 2 rows - header and types")
        self._column_names = table_rows[0]
        self._column_types = table_rows[1]
        self._data = table_rows[2:]

    def _columns(self) -> List[Column]:
        return [Column(name, dtype) for name, dtype in zip(self._column_names, self._column_types)]

    def to_pandas_data(self) -> Dict[str, List[Any]]:
        """Converts Table to data needed for Pandas DataFrame.

        Returns:
            dict: Dict, where each key is column value, and value - list of values.
        """
        result: Dict[str, List[Any]] = {}
        columns = self._columns()
        for row in self._data:
            for idx, val in enumerate(row):
                col = columns[idx]
                value = col.value(val)
                if col.name in result:
                    result[col.name].append(value)
                else:
                    result[col.name] = [value]
        return result

    def to_spark_data(self) -> Tuple[List[Tuple[Any, ...]], str]:
        """Converts Table to data needed for Spark DataFrame.

        Returns:
            Tuple: Tuple where first element is list of tuples representing row,
                and second element is Spark schema representation string.
        """
        columns = self._columns()
        data = []
        columns = self._columns()
        for row in self._data:
            data.append(tuple([columns[idx].value(val) for idx, val in enumerate(row)]))

        schema = ", ".join([f"`{col.name}` {col.dtype}" for col in columns])
        return (data, schema)

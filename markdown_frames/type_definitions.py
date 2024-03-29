"""Module dedicated to type conversions."""

import ast
import re
from datetime import datetime
from decimal import Decimal, localcontext
from typing import Any, Callable, Optional, Type, Union


class InvalidValueException(Exception):
    """Exception thrown when value cannot be parsed to specified data type."""

    pass


class BaseType:
    """Base class for data type."""

    subclasses: list = []
    convert: Union[Callable[..., Any], Type[Any]]
    dtype: str

    def __init_subclass__(cls: Any, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        cls.subclasses.append(cls)


def apply_converter(converter: BaseType, val: Optional[str]) -> Any:
    if val is None or val == "None":
        return None

    return converter.convert(val)


def get_type(dtype: str) -> BaseType:
    """Gets Data Type object for provided string representation.

    Args:
        dtype (str): String representation of data type.

    Returns:
        Optional[BaseType]: Object representing data type.
    """
    for factory in BaseType.subclasses:
        current = factory.get_for_type(dtype)
        if current:
            return current
    raise InvalidValueException(f"Not supported: {dtype}")


class IntType(BaseType):
    convert = int

    @staticmethod
    def get_for_type(dtype: str) -> Optional[BaseType]:
        return IntType(dtype.lower()) if dtype.lower() in ("int", "bigint") else None

    def __init__(self, dtype: str):
        self.dtype = dtype


class FloatType(BaseType):
    convert = float

    def __init__(self, dtype: str):
        self.dtype = dtype

    @staticmethod
    def get_for_type(dtype: str) -> Optional[BaseType]:
        return (
            FloatType(dtype.lower()) if dtype.lower() in ("float", "double") else None
        )


class StringType(BaseType):
    convert = str
    dtype = "string"

    @staticmethod
    def get_for_type(dtype: str) -> Optional[BaseType]:
        return StringType() if dtype.lower() in ("string", "str") else None


class BooleanType(BaseType):
    dtype = "boolean"

    @staticmethod
    def get_for_type(dtype: str) -> Optional[BaseType]:
        return BooleanType() if dtype.lower() in ("boolean", "bool") else None

    def convert(self, value: str) -> bool:
        return value.lower() == "true"


class DateTimeType(BaseType):
    dtype = "timestamp"

    @staticmethod
    def get_for_type(dtype: str) -> Optional[BaseType]:
        return DateTimeType() if dtype.lower() == "timestamp" else None

    def convert(self, value: str) -> datetime:
        return datetime.strptime(value, "%Y-%m-%d %H:%M:%S")

class DateType(BaseType):
    dtype = "date"

    @staticmethod
    def get_for_type(dtype: str) -> Optional[BaseType]:
        return DateType() if dtype.lower() == "date" else None

    def convert(self, value: str) -> datetime:
        return datetime.strptime(value, "%Y-%m-%d")


class DateType(BaseType):
    dtype = "date"

    @staticmethod
    def get_for_type(dtype: str) -> Optional[BaseType]:
        return DateType() if dtype.lower() == "date" else None

    def convert(self, value: str) -> datetime:
        return datetime.strptime(value, "%Y-%m-%d")


class DecimalType(BaseType):
    @staticmethod
    def get_for_type(dtype: str) -> Optional[BaseType]:
        lower_type = dtype.lower()
        if lower_type.startswith("decimal"):
            precision, scale = map(
                int, re.split(r"\.|,", lower_type.replace("decimal", "")[1:-1])
            )
            return DecimalType(precision, scale)
        return None

    def __init__(self, precision: int, scale: int):
        self.precision = precision
        self.scale = scale
        self.dtype = f"decimal({precision},{scale})"

    def convert(self, value: str) -> Decimal:
        if len(str(int(float(value)))) <= (self.precision - self.scale):
            with localcontext() as ctx:
                ctx.prec = self.precision
                return Decimal(value)
        else:
            raise InvalidValueException(
                "{} not in range {}".format(value, (self.precision - self.scale) * "9")
            )


class MapType(BaseType):
    @staticmethod
    def get_for_type(dtype: str) -> Optional[BaseType]:
        lower_type = dtype.lower()
        if lower_type.startswith("map"):
            matches = re.match(r"map<(\w+),\s?(.+)>", dtype)
            if not matches:
                return None

            key_type = matches.group(1)
            value_type = matches.group(2)
            return MapType(key_type, value_type)
        return None

    def __init__(self, key_type: str, value_type: str):
        self.key_type = get_type(key_type)
        self.value_type = get_type(value_type)
        self.dtype = f"map<{self.key_type.dtype},{self.value_type.dtype}>"

    def convert(self, value: str) -> dict:
        items = ast.literal_eval(value)
        result = {}
        for key, val in items.items():
            result[apply_converter(self.key_type, key)] = apply_converter(
                self.value_type, str(val)
            )
        return result


class ArrayType(BaseType):
    @staticmethod
    def get_for_type(dtype: str) -> Optional[BaseType]:
        lower_type = dtype.lower()
        if lower_type.startswith("array"):
            matches = re.match(r"array<(.+)>", dtype)
            if not matches:
                return None
            val_type = matches.group(1)
            return ArrayType(val_type)
        return None

    def __init__(self, value_type: str):
        self.value_type = get_type(value_type)
        self.dtype = f"array<{self.value_type.dtype}>"

    def convert(self, value: str) -> list:
        items = ast.literal_eval(value)
        result = []
        for val in items:
            result.append(apply_converter(self.value_type, str(val)))
        return result

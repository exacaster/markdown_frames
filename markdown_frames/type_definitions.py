"""Module dedicated to type conversions."""

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


def get_type(dtype: str) -> Optional[BaseType]:
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
    return None


class IntType(BaseType):
    convert = int
    dtype = "int"

    @staticmethod
    def get_for_type(dtype: str) -> Optional[BaseType]:
        return IntType() if dtype.lower() in ("int", "integer") else None


class FloatType(BaseType):
    convert = float

    def __init__(self, dtype: str):
        self.dtype = dtype

    @staticmethod
    def get_for_type(dtype: str) -> Optional[BaseType]:
        return FloatType(dtype) if dtype.lower() in ("float", "double") else None


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


class DecimalType(BaseType):
    @staticmethod
    def get_for_type(dtype: str) -> Optional[BaseType]:
        lower_type = dtype.lower()
        if lower_type.startswith("decimal"):
            precision, scale = map(int, re.split(r"\.|,", lower_type.replace("decimal", "")[1:-1]))
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

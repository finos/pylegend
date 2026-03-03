# Copyright 2026 Goldman Sachs
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from pylegend._typing import (
    PyLegendSequence,
    PyLegendUnion,
    PyLegendTuple,
)
import builtins
from pylegend.core.tds.tds_column import PrimitiveType

CastTarget = PyLegendUnion[PrimitiveType, PyLegendTuple[PrimitiveType, ...]]

__all__: PyLegendSequence[str] = [
    # Simple types
    "boolean",
    "string",
    "integer",
    "float",
    "number",
    "decimal",
    "date",
    "datetime",
    "strictdate",

    # Precise integer types
    "tinyint",
    "utinyint",
    "smallint",
    "usmallint",
    "int",
    "uint",
    "bigint",
    "ubigint",

    # Precise float types
    "float4",
    "double",

    # Precise date types
    "timestamp",

    # Parameterized types
    "varchar",
    "numeric",
]


def boolean() -> CastTarget:
    """Cast to Boolean."""
    return PrimitiveType.Boolean


def string() -> CastTarget:
    """Cast to String."""
    return PrimitiveType.String


def integer() -> CastTarget:
    """Cast to Integer."""
    return PrimitiveType.Integer


def float() -> CastTarget:
    """Cast to Float."""
    return PrimitiveType.Float


def number() -> CastTarget:
    """Cast to Number."""
    return PrimitiveType.Number


def decimal() -> CastTarget:
    """Cast to Decimal."""
    return PrimitiveType.Decimal


def date() -> CastTarget:
    """Cast to Date."""
    return PrimitiveType.Date


def datetime() -> CastTarget:
    """Cast to DateTime."""
    return PrimitiveType.DateTime


def strictdate() -> CastTarget:
    """Cast to StrictDate."""
    return PrimitiveType.StrictDate


def tinyint() -> CastTarget:
    """Cast to TinyInt."""
    return PrimitiveType.TinyInt


def utinyint() -> CastTarget:
    """Cast to UTinyInt."""
    return PrimitiveType.UTinyInt


def smallint() -> CastTarget:
    """Cast to SmallInt."""
    return PrimitiveType.SmallInt


def usmallint() -> CastTarget:
    """Cast to USmallInt."""
    return PrimitiveType.USmallInt


def int() -> CastTarget:
    """Cast to Int."""
    return PrimitiveType.Int


def uint() -> CastTarget:
    """Cast to UInt."""
    return PrimitiveType.UInt


def bigint() -> CastTarget:
    """Cast to BigInt."""
    return PrimitiveType.BigInt


def ubigint() -> CastTarget:
    """Cast to UBigInt."""
    return PrimitiveType.UBigInt


def float4() -> CastTarget:
    """Cast to Float4."""
    return PrimitiveType.Float4


def double() -> CastTarget:
    """Cast to Double."""
    return PrimitiveType.Double


def timestamp() -> CastTarget:
    """Cast to Timestamp."""
    return PrimitiveType.Timestamp


def varchar(max_length: builtins.int) -> CastTarget:  # noqa: F821
    """Cast to Varchar with a given max length.

    Args:
        max_length: Maximum character length for the Varchar column.

    Example::

        frame.cast({"name": pylegend.type_factory.varchar(200)})
    """
    return (PrimitiveType.Varchar, max_length)


def numeric(precision: builtins.int, scale: builtins.int) -> CastTarget:  # noqa: F821
    """Cast to Numeric with given precision and scale.

    Args:
        precision: Total number of digits.
        scale: Number of digits after the decimal point.

    Example::

        frame.cast({"amount": pylegend.type_factory.numeric(10, 2)})
    """
    return (PrimitiveType.Numeric, precision, scale)

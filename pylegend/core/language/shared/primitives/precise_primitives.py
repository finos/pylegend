# Copyright 2025 Goldman Sachs
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
    PyLegendDict,
)
from pylegend.core.language.shared.primitives.integer import PyLegendInteger
from pylegend.core.language.shared.primitives.string import PyLegendString
from pylegend.core.language.shared.primitives.float import PyLegendFloat
from pylegend.core.language.shared.primitives.datetime import PyLegendDateTime
from pylegend.core.language.shared.primitives.decimal import PyLegendDecimal
from pylegend.core.language.shared.expression import (
    PyLegendExpressionIntegerReturn,
    PyLegendExpressionStringReturn,
    PyLegendExpressionFloatReturn,
    PyLegendExpressionDateTimeReturn,
    PyLegendExpressionDecimalReturn,
)
from pylegend.core.sql.metamodel import (
    Expression,
    QuerySpecification,
)
from pylegend.core.tds.tds_frame import FrameToSqlConfig


__all__: PyLegendSequence[str] = [
    "PyLegendTinyInt",
    "PyLegendUTinyInt",
    "PyLegendSmallInt",
    "PyLegendUSmallInt",
    "PyLegendInt",
    "PyLegendUInt",
    "PyLegendBigInt",
    "PyLegendUBigInt",
    "PyLegendVarchar",
    "PyLegendTimestamp",
    "PyLegendFloat4",
    "PyLegendDouble",
    "PyLegendNumeric",
]


class PyLegendTinyInt(PyLegendInteger):
    """Precise primitive: TinyInt – signed 8-bit integer (-128 .. 127)."""

    def __init__(self, value: PyLegendExpressionIntegerReturn) -> None:
        super().__init__(value)

    def to_sql_expression(
            self,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        return super().to_sql_expression(frame_name_to_base_query_map, config)


class PyLegendUTinyInt(PyLegendInteger):
    """Precise primitive: UTinyInt – unsigned 8-bit integer (0 .. 255)."""

    def __init__(self, value: PyLegendExpressionIntegerReturn) -> None:
        super().__init__(value)

    def to_sql_expression(
            self,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        return super().to_sql_expression(frame_name_to_base_query_map, config)


class PyLegendSmallInt(PyLegendInteger):
    """Precise primitive: SmallInt – signed 16-bit integer (-32768 .. 32767)."""

    def __init__(self, value: PyLegendExpressionIntegerReturn) -> None:
        super().__init__(value)

    def to_sql_expression(
            self,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        return super().to_sql_expression(frame_name_to_base_query_map, config)


class PyLegendUSmallInt(PyLegendInteger):
    """Precise primitive: USmallInt – unsigned 16-bit integer (0 .. 65535)."""

    def __init__(self, value: PyLegendExpressionIntegerReturn) -> None:
        super().__init__(value)

    def to_sql_expression(
            self,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        return super().to_sql_expression(frame_name_to_base_query_map, config)


class PyLegendInt(PyLegendInteger):
    """Precise primitive: Int – signed 32-bit integer."""

    def __init__(self, value: PyLegendExpressionIntegerReturn) -> None:
        super().__init__(value)

    def to_sql_expression(
            self,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        return super().to_sql_expression(frame_name_to_base_query_map, config)


class PyLegendUInt(PyLegendInteger):
    """Precise primitive: UInt – unsigned 32-bit integer."""

    def __init__(self, value: PyLegendExpressionIntegerReturn) -> None:
        super().__init__(value)

    def to_sql_expression(
            self,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        return super().to_sql_expression(frame_name_to_base_query_map, config)


class PyLegendBigInt(PyLegendInteger):
    """Precise primitive: BigInt – signed 64-bit integer."""

    def __init__(self, value: PyLegendExpressionIntegerReturn) -> None:
        super().__init__(value)

    def to_sql_expression(
            self,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        return super().to_sql_expression(frame_name_to_base_query_map, config)


class PyLegendUBigInt(PyLegendInteger):
    """Precise primitive: UBigInt – unsigned 64-bit integer."""

    def __init__(self, value: PyLegendExpressionIntegerReturn) -> None:
        super().__init__(value)

    def to_sql_expression(
            self,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        return super().to_sql_expression(frame_name_to_base_query_map, config)


class PyLegendVarchar(PyLegendString):
    """Precise primitive: Varchar(max_length) – variable-length string with max length constraint."""
    __max_length: int

    def __init__(self, value: PyLegendExpressionStringReturn, max_length: int) -> None:
        super().__init__(value)
        self.__max_length = max_length

    @property
    def max_length(self) -> int:
        return self.__max_length

    def to_sql_expression(
            self,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        return super().to_sql_expression(frame_name_to_base_query_map, config)


class PyLegendTimestamp(PyLegendDateTime):
    """Precise primitive: Timestamp – extends DateTime."""

    def __init__(self, value: PyLegendExpressionDateTimeReturn) -> None:
        super().__init__(value)

    def to_sql_expression(
            self,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        return super().to_sql_expression(frame_name_to_base_query_map, config)


class PyLegendFloat4(PyLegendFloat):
    """Precise primitive: Float4 – single-precision float."""

    def __init__(self, value: PyLegendExpressionFloatReturn) -> None:
        super().__init__(value)

    def to_sql_expression(
            self,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        return super().to_sql_expression(frame_name_to_base_query_map, config)


class PyLegendDouble(PyLegendFloat):
    """Precise primitive: Double – double-precision float."""

    def __init__(self, value: PyLegendExpressionFloatReturn) -> None:
        super().__init__(value)

    def to_sql_expression(
            self,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        return super().to_sql_expression(frame_name_to_base_query_map, config)


class PyLegendNumeric(PyLegendDecimal):
    """Precise primitive: Numeric(precision, scale) – fixed-point decimal with precision and scale constraints."""
    __precision: int
    __scale: int

    def __init__(self, value: PyLegendExpressionDecimalReturn, precision: int, scale: int) -> None:
        super().__init__(value)
        if precision < scale:
            raise ValueError(
                f"Numeric precision ({precision}) must be >= scale ({scale})"
            )
        self.__precision = precision
        self.__scale = scale

    @property
    def precision(self) -> int:
        return self.__precision

    @property
    def scale(self) -> int:
        return self.__scale

    def to_sql_expression(
            self,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        return super().to_sql_expression(frame_name_to_base_query_map, config)

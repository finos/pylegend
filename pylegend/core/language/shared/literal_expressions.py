# Copyright 2023 Goldman Sachs
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


from datetime import date, datetime
from pylegend._typing import (
    PyLegendSequence,
    PyLegendDict,
    PyLegendUnion,
)
from pylegend.core.language.shared.expression import (
    PyLegendExpression,
    PyLegendExpressionBooleanReturn,
    PyLegendExpressionStringReturn,
    PyLegendExpressionIntegerReturn,
    PyLegendExpressionFloatReturn,
    PyLegendExpressionDateTimeReturn,
    PyLegendExpressionStrictDateReturn,
)
from pylegend.core.sql.metamodel import (
    Expression,
    BooleanLiteral,
    StringLiteral,
    IntegerLiteral,
    DoubleLiteral,
    QuerySpecification,
    Cast,
    ColumnType,
)
from pylegend.core.tds.tds_frame import FrameToSqlConfig
from pylegend.core.tds.tds_frame import FrameToPureConfig


__all__: PyLegendSequence[str] = [
    "PyLegendBooleanLiteralExpression",
    "PyLegendStringLiteralExpression",
    "PyLegendIntegerLiteralExpression",
    "PyLegendFloatLiteralExpression",
    "PyLegendDateTimeLiteralExpression",
    "PyLegendStrictDateLiteralExpression",
    "convert_literal_to_literal_expression",
]


class PyLegendBooleanLiteralExpression(PyLegendExpressionBooleanReturn):
    __value: bool

    def __init__(self, value: bool) -> None:
        self.__value = value

    def to_sql_expression(
            self,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        return BooleanLiteral(value=self.__value)

    def to_pure_expression(self, config: FrameToPureConfig) -> str:
        return "true" if self.__value else "false"

    def is_non_nullable(self) -> bool:
        return True


class PyLegendStringLiteralExpression(PyLegendExpressionStringReturn):
    __value: str

    def __init__(self, value: str) -> None:
        self.__value = value

    def to_sql_expression(
            self,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        return StringLiteral(value=self.__value, quoted=False)

    def to_pure_expression(self, config: FrameToPureConfig) -> str:
        escaped = self.__value.replace("'", "\\'")
        return f"'{escaped}'"

    def is_non_nullable(self) -> bool:
        return True


class PyLegendIntegerLiteralExpression(PyLegendExpressionIntegerReturn):
    __value: int

    def __init__(self, value: int) -> None:
        self.__value = value

    def to_sql_expression(
            self,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        return IntegerLiteral(value=self.__value)

    def to_pure_expression(self, config: FrameToPureConfig) -> str:
        return str(self.__value)

    def is_non_nullable(self) -> bool:
        return True


class PyLegendFloatLiteralExpression(PyLegendExpressionFloatReturn):
    __value: float

    def __init__(self, value: float) -> None:
        self.__value = value

    def to_sql_expression(
            self,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        return DoubleLiteral(value=self.__value)

    def to_pure_expression(self, config: FrameToPureConfig) -> str:
        return str(self.__value)

    def is_non_nullable(self) -> bool:
        return True


class PyLegendDateTimeLiteralExpression(PyLegendExpressionDateTimeReturn):
    __value: datetime

    def __init__(self, value: datetime) -> None:
        self.__value = value

    def to_sql_expression(
            self,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        return Cast(
            expression=StringLiteral(value=self.__value.isoformat(), quoted=False),
            type_=ColumnType(name="TIMESTAMP", parameters=[])
        )

    def to_pure_expression(self, config: FrameToPureConfig) -> str:
        return f"%{self.__value.isoformat()}"

    def is_non_nullable(self) -> bool:
        return True


class PyLegendStrictDateLiteralExpression(PyLegendExpressionStrictDateReturn):
    __value: date

    def __init__(self, value: date) -> None:
        self.__value = value

    def to_sql_expression(
            self,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        return Cast(
            expression=StringLiteral(value=self.__value.isoformat(), quoted=False),
            type_=ColumnType(name="DATE", parameters=[])
        )

    def to_pure_expression(self, config: FrameToPureConfig) -> str:
        return f"%{self.__value.isoformat()}"

    def is_non_nullable(self) -> bool:
        return True


def convert_literal_to_literal_expression(
        literal: PyLegendUnion[int, float, bool, str, datetime, date]
) -> PyLegendExpression:
    if isinstance(literal, bool):
        return PyLegendBooleanLiteralExpression(literal)
    if isinstance(literal, int):
        return PyLegendIntegerLiteralExpression(literal)
    if isinstance(literal, float):
        return PyLegendFloatLiteralExpression(literal)
    if isinstance(literal, str):
        return PyLegendStringLiteralExpression(literal)
    if isinstance(literal, datetime):
        return PyLegendDateTimeLiteralExpression(literal)
    if isinstance(literal, date):
        return PyLegendStrictDateLiteralExpression(literal)

    raise TypeError(f"Cannot convert value - {literal} of type {type(literal)} to literal expression")

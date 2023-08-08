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


from pylegend._typing import (
    PyLegendSequence,
    PyLegendDict,
    PyLegendUnion,
)
from pylegend.core.language.expression import (
    PyLegendExpression,
    PyLegendExpressionBooleanReturn,
    PyLegendExpressionStringReturn,
    PyLegendExpressionIntegerReturn,
    PyLegendExpressionFloatReturn,
)
from pylegend.core.sql.metamodel import (
    Expression,
    BooleanLiteral,
    StringLiteral,
    IntegerLiteral,
    DoubleLiteral,
    QuerySpecification,
)
from pylegend.core.tds.tds_frame import FrameToSqlConfig


__all__: PyLegendSequence[str] = [
    "PyLegendBooleanLiteralExpression",
    "PyLegendStringLiteralExpression",
    "PyLegendIntegerLiteralExpression",
    "PyLegendFloatLiteralExpression",
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


def convert_literal_to_literal_expression(literal: PyLegendUnion[int, float, bool, str]) -> PyLegendExpression:
    if isinstance(literal, bool):
        return PyLegendBooleanLiteralExpression(literal)
    if isinstance(literal, int):
        return PyLegendIntegerLiteralExpression(literal)
    if isinstance(literal, float):
        return PyLegendFloatLiteralExpression(literal)
    if isinstance(literal, str):
        return PyLegendStringLiteralExpression(literal)

    raise TypeError("Cannot convert value - {v} of type {t} to literal expression".format(v=literal, t=type(literal)))

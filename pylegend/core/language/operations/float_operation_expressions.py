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
)
from pylegend.core.language.expression import (
    PyLegendExpressionFloatReturn,
)
from pylegend.core.language.operations.binary_expression import PyLegendBinaryExpression
from pylegend.core.language.operations.unary_expression import PyLegendUnaryExpression
from pylegend.core.sql.metamodel import (
    Expression,
    QuerySpecification,
    ArithmeticType,
    ArithmeticExpression,
    NegativeExpression,
)
from pylegend.core.sql.metamodel_extension import (
    AbsoluteExpression,
)
from pylegend.core.tds.tds_frame import FrameToSqlConfig


__all__: PyLegendSequence[str] = [
    "PyLegendFloatAddExpression",
    "PyLegendFloatAbsoluteExpression",
    "PyLegendFloatNegativeExpression",
    "PyLegendFloatSubtractExpression",
    "PyLegendFloatMultiplyExpression",
]


class PyLegendFloatAddExpression(PyLegendBinaryExpression, PyLegendExpressionFloatReturn):

    @staticmethod
    def __to_sql_func(
            expression1: Expression,
            expression2: Expression,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        return ArithmeticExpression(ArithmeticType.ADD, expression1, expression2)

    def __init__(self, operand1: PyLegendExpressionFloatReturn, operand2: PyLegendExpressionFloatReturn) -> None:
        PyLegendExpressionFloatReturn.__init__(self)
        PyLegendBinaryExpression.__init__(
            self,
            operand1,
            operand2,
            PyLegendFloatAddExpression.__to_sql_func
        )


class PyLegendFloatSubtractExpression(PyLegendBinaryExpression, PyLegendExpressionFloatReturn):

    @staticmethod
    def __to_sql_func(
            expression1: Expression,
            expression2: Expression,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        return ArithmeticExpression(ArithmeticType.SUBTRACT, expression1, expression2)

    def __init__(self, operand1: PyLegendExpressionFloatReturn, operand2: PyLegendExpressionFloatReturn) -> None:
        PyLegendExpressionFloatReturn.__init__(self)
        PyLegendBinaryExpression.__init__(
            self,
            operand1,
            operand2,
            PyLegendFloatSubtractExpression.__to_sql_func
        )


class PyLegendFloatMultiplyExpression(PyLegendBinaryExpression, PyLegendExpressionFloatReturn):

    @staticmethod
    def __to_sql_func(
            expression1: Expression,
            expression2: Expression,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        return ArithmeticExpression(ArithmeticType.MULTIPLY, expression1, expression2)

    def __init__(self, operand1: PyLegendExpressionFloatReturn, operand2: PyLegendExpressionFloatReturn) -> None:
        PyLegendExpressionFloatReturn.__init__(self)
        PyLegendBinaryExpression.__init__(
            self,
            operand1,
            operand2,
            PyLegendFloatMultiplyExpression.__to_sql_func
        )


class PyLegendFloatAbsoluteExpression(PyLegendUnaryExpression, PyLegendExpressionFloatReturn):

    @staticmethod
    def __to_sql_func(
            expression: Expression,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        return AbsoluteExpression(expression)

    def __init__(self, operand: PyLegendExpressionFloatReturn) -> None:
        PyLegendExpressionFloatReturn.__init__(self)
        PyLegendUnaryExpression.__init__(
            self,
            operand,
            PyLegendFloatAbsoluteExpression.__to_sql_func
        )


class PyLegendFloatNegativeExpression(PyLegendUnaryExpression, PyLegendExpressionFloatReturn):

    @staticmethod
    def __to_sql_func(
            expression: Expression,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        return NegativeExpression(expression)

    def __init__(self, operand: PyLegendExpressionFloatReturn) -> None:
        PyLegendExpressionFloatReturn.__init__(self)
        PyLegendUnaryExpression.__init__(
            self,
            operand,
            PyLegendFloatNegativeExpression.__to_sql_func
        )

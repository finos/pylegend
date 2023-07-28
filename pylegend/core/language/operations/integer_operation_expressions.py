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
    PyLegendExpressionIntegerReturn,
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
    "PyLegendIntegerAddExpression",
    "PyLegendIntegerAbsoluteExpression",
    "PyLegendIntegerNegativeExpression",
    "PyLegendIntegerSubtractExpression",
    "PyLegendIntegerMultiplyExpression",
    "PyLegendIntegerModuloExpression",
]


class PyLegendIntegerAddExpression(PyLegendBinaryExpression, PyLegendExpressionIntegerReturn):

    @staticmethod
    def __to_sql_func(
            expression1: Expression,
            expression2: Expression,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        return ArithmeticExpression(ArithmeticType.ADD, expression1, expression2)

    def __init__(self, operand1: PyLegendExpressionIntegerReturn, operand2: PyLegendExpressionIntegerReturn) -> None:
        PyLegendExpressionIntegerReturn.__init__(self)
        PyLegendBinaryExpression.__init__(
            self,
            operand1,
            operand2,
            PyLegendIntegerAddExpression.__to_sql_func
        )


class PyLegendIntegerSubtractExpression(PyLegendBinaryExpression, PyLegendExpressionIntegerReturn):

    @staticmethod
    def __to_sql_func(
            expression1: Expression,
            expression2: Expression,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        return ArithmeticExpression(ArithmeticType.SUBTRACT, expression1, expression2)

    def __init__(self, operand1: PyLegendExpressionIntegerReturn, operand2: PyLegendExpressionIntegerReturn) -> None:
        PyLegendExpressionIntegerReturn.__init__(self)
        PyLegendBinaryExpression.__init__(
            self,
            operand1,
            operand2,
            PyLegendIntegerSubtractExpression.__to_sql_func
        )


class PyLegendIntegerMultiplyExpression(PyLegendBinaryExpression, PyLegendExpressionIntegerReturn):

    @staticmethod
    def __to_sql_func(
            expression1: Expression,
            expression2: Expression,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        return ArithmeticExpression(ArithmeticType.MULTIPLY, expression1, expression2)

    def __init__(self, operand1: PyLegendExpressionIntegerReturn, operand2: PyLegendExpressionIntegerReturn) -> None:
        PyLegendExpressionIntegerReturn.__init__(self)
        PyLegendBinaryExpression.__init__(
            self,
            operand1,
            operand2,
            PyLegendIntegerMultiplyExpression.__to_sql_func
        )


class PyLegendIntegerModuloExpression(PyLegendBinaryExpression, PyLegendExpressionIntegerReturn):

    @staticmethod
    def __to_sql_func(
            expression1: Expression,
            expression2: Expression,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        return ArithmeticExpression(ArithmeticType.MODULUS, expression1, expression2)

    def __init__(self, operand1: PyLegendExpressionIntegerReturn, operand2: PyLegendExpressionIntegerReturn) -> None:
        PyLegendExpressionIntegerReturn.__init__(self)
        PyLegendBinaryExpression.__init__(
            self,
            operand1,
            operand2,
            PyLegendIntegerModuloExpression.__to_sql_func
        )


class PyLegendIntegerAbsoluteExpression(PyLegendUnaryExpression, PyLegendExpressionIntegerReturn):

    @staticmethod
    def __to_sql_func(
            expression: Expression,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        return AbsoluteExpression(expression)

    def __init__(self, operand: PyLegendExpressionIntegerReturn) -> None:
        PyLegendExpressionIntegerReturn.__init__(self)
        PyLegendUnaryExpression.__init__(
            self,
            operand,
            PyLegendIntegerAbsoluteExpression.__to_sql_func
        )


class PyLegendIntegerNegativeExpression(PyLegendUnaryExpression, PyLegendExpressionIntegerReturn):

    @staticmethod
    def __to_sql_func(
            expression: Expression,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        return NegativeExpression(expression)

    def __init__(self, operand: PyLegendExpressionIntegerReturn) -> None:
        PyLegendExpressionIntegerReturn.__init__(self)
        PyLegendUnaryExpression.__init__(
            self,
            operand,
            PyLegendIntegerNegativeExpression.__to_sql_func
        )

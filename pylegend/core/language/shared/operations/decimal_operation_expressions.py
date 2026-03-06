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
    PyLegendDict,
)
from pylegend.core.language.shared.expression import (
    PyLegendExpressionDecimalReturn,
)
from pylegend.core.language.shared.operations.binary_expression import PyLegendBinaryExpression
from pylegend.core.language.shared.operations.unary_expression import PyLegendUnaryExpression
from pylegend.core.language.shared.helpers import generate_pure_functional_call
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
from pylegend.core.tds.tds_frame import FrameToPureConfig


__all__: PyLegendSequence[str] = [
    "PyLegendDecimalAddExpression",
    "PyLegendDecimalAbsoluteExpression",
    "PyLegendDecimalNegativeExpression",
    "PyLegendDecimalSubtractExpression",
    "PyLegendDecimalMultiplyExpression",
]


class PyLegendDecimalAddExpression(PyLegendBinaryExpression, PyLegendExpressionDecimalReturn):

    @staticmethod
    def __to_sql_func(
            expression1: Expression,
            expression2: Expression,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        return ArithmeticExpression(ArithmeticType.ADD, expression1, expression2)

    @staticmethod
    def __to_pure_func(op1_expr: str, op2_expr: str, config: FrameToPureConfig) -> str:
        return f"({op1_expr} + {op2_expr})"

    def __init__(self, operand1: PyLegendExpressionDecimalReturn, operand2: PyLegendExpressionDecimalReturn) -> None:
        PyLegendExpressionDecimalReturn.__init__(self)
        PyLegendBinaryExpression.__init__(
            self,
            operand1,
            operand2,
            PyLegendDecimalAddExpression.__to_sql_func,
            PyLegendDecimalAddExpression.__to_pure_func,
            non_nullable=True,
            first_operand_needs_to_be_non_nullable=True,
            second_operand_needs_to_be_non_nullable=True
        )


class PyLegendDecimalSubtractExpression(PyLegendBinaryExpression, PyLegendExpressionDecimalReturn):

    @staticmethod
    def __to_sql_func(
            expression1: Expression,
            expression2: Expression,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        return ArithmeticExpression(ArithmeticType.SUBTRACT, expression1, expression2)

    @staticmethod
    def __to_pure_func(op1_expr: str, op2_expr: str, config: FrameToPureConfig) -> str:
        return f"({op1_expr} - {op2_expr})"

    def __init__(self, operand1: PyLegendExpressionDecimalReturn, operand2: PyLegendExpressionDecimalReturn) -> None:
        PyLegendExpressionDecimalReturn.__init__(self)
        PyLegendBinaryExpression.__init__(
            self,
            operand1,
            operand2,
            PyLegendDecimalSubtractExpression.__to_sql_func,
            PyLegendDecimalSubtractExpression.__to_pure_func,
            non_nullable=True,
            first_operand_needs_to_be_non_nullable=True,
            second_operand_needs_to_be_non_nullable=True
        )


class PyLegendDecimalMultiplyExpression(PyLegendBinaryExpression, PyLegendExpressionDecimalReturn):

    @staticmethod
    def __to_sql_func(
            expression1: Expression,
            expression2: Expression,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        return ArithmeticExpression(ArithmeticType.MULTIPLY, expression1, expression2)

    @staticmethod
    def __to_pure_func(op1_expr: str, op2_expr: str, config: FrameToPureConfig) -> str:
        return f"({op1_expr} * {op2_expr})"

    def __init__(self, operand1: PyLegendExpressionDecimalReturn, operand2: PyLegendExpressionDecimalReturn) -> None:
        PyLegendExpressionDecimalReturn.__init__(self)
        PyLegendBinaryExpression.__init__(
            self,
            operand1,
            operand2,
            PyLegendDecimalMultiplyExpression.__to_sql_func,
            PyLegendDecimalMultiplyExpression.__to_pure_func,
            non_nullable=True,
            first_operand_needs_to_be_non_nullable=True,
            second_operand_needs_to_be_non_nullable=True
        )


class PyLegendDecimalAbsoluteExpression(PyLegendUnaryExpression, PyLegendExpressionDecimalReturn):

    @staticmethod
    def __to_sql_func(
            expression: Expression,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        return AbsoluteExpression(expression)

    @staticmethod
    def __to_pure_func(op_expr: str, config: FrameToPureConfig) -> str:
        return generate_pure_functional_call("abs", [op_expr])

    def __init__(self, operand: PyLegendExpressionDecimalReturn) -> None:
        PyLegendExpressionDecimalReturn.__init__(self)
        PyLegendUnaryExpression.__init__(
            self,
            operand,
            PyLegendDecimalAbsoluteExpression.__to_sql_func,
            PyLegendDecimalAbsoluteExpression.__to_pure_func,
            non_nullable=True,
            operand_needs_to_be_non_nullable=True
        )


class PyLegendDecimalNegativeExpression(PyLegendUnaryExpression, PyLegendExpressionDecimalReturn):

    @staticmethod
    def __to_sql_func(
            expression: Expression,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        return NegativeExpression(expression)

    @staticmethod
    def __to_pure_func(op_expr: str, config: FrameToPureConfig) -> str:
        return generate_pure_functional_call("minus", [op_expr])

    def __init__(self, operand: PyLegendExpressionDecimalReturn) -> None:
        PyLegendExpressionDecimalReturn.__init__(self)
        PyLegendUnaryExpression.__init__(
            self,
            operand,
            PyLegendDecimalNegativeExpression.__to_sql_func,
            PyLegendDecimalNegativeExpression.__to_pure_func,
            non_nullable=True,
            operand_needs_to_be_non_nullable=True,
        )

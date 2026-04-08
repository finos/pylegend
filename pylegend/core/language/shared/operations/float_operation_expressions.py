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
from pylegend.core.language.shared.expression import (
    PyLegendExpressionFloatReturn,
)
from pylegend.core.language.shared.operations.binary_expression import PyLegendBinaryExpression
from pylegend.core.language.shared.operations.nullary_expression import PyLegendNullaryExpression
from pylegend.core.language.shared.operations.unary_expression import PyLegendUnaryExpression
from pylegend.core.language.shared.helpers import generate_pure_functional_call
from pylegend.core.sql.metamodel import (
    Expression,
    QuerySpecification,
    ArithmeticType,
    ArithmeticExpression,
    NegativeExpression,
    FunctionCall,
    QualifiedName,
)
from pylegend.core.sql.metamodel_extension import (
    AbsoluteExpression,
)
from pylegend.core.tds.tds_frame import FrameToSqlConfig
from pylegend.core.tds.tds_frame import FrameToPureConfig


__all__: PyLegendSequence[str] = [
    "PyLegendFloatAddExpression",
    "PyLegendFloatAbsoluteExpression",
    "PyLegendFloatNegativeExpression",
    "PyLegendFloatSubtractExpression",
    "PyLegendFloatMultiplyExpression",
    "PyLegendFloatPiExpression",
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

    @staticmethod
    def __to_pure_func(op1_expr: str, op2_expr: str, config: FrameToPureConfig) -> str:
        return f"({op1_expr} + {op2_expr})"

    def __init__(self, operand1: PyLegendExpressionFloatReturn, operand2: PyLegendExpressionFloatReturn) -> None:
        PyLegendExpressionFloatReturn.__init__(self)
        PyLegendBinaryExpression.__init__(
            self,
            operand1,
            operand2,
            PyLegendFloatAddExpression.__to_sql_func,
            PyLegendFloatAddExpression.__to_pure_func,
            non_nullable=True,
            first_operand_needs_to_be_non_nullable=True,
            second_operand_needs_to_be_non_nullable=True
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

    @staticmethod
    def __to_pure_func(op1_expr: str, op2_expr: str, config: FrameToPureConfig) -> str:
        return f"({op1_expr} - {op2_expr})"

    def __init__(self, operand1: PyLegendExpressionFloatReturn, operand2: PyLegendExpressionFloatReturn) -> None:
        PyLegendExpressionFloatReturn.__init__(self)
        PyLegendBinaryExpression.__init__(
            self,
            operand1,
            operand2,
            PyLegendFloatSubtractExpression.__to_sql_func,
            PyLegendFloatSubtractExpression.__to_pure_func,
            non_nullable=True,
            first_operand_needs_to_be_non_nullable=True,
            second_operand_needs_to_be_non_nullable=True
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

    @staticmethod
    def __to_pure_func(op1_expr: str, op2_expr: str, config: FrameToPureConfig) -> str:
        return f"({op1_expr} * {op2_expr})"

    def __init__(self, operand1: PyLegendExpressionFloatReturn, operand2: PyLegendExpressionFloatReturn) -> None:
        PyLegendExpressionFloatReturn.__init__(self)
        PyLegendBinaryExpression.__init__(
            self,
            operand1,
            operand2,
            PyLegendFloatMultiplyExpression.__to_sql_func,
            PyLegendFloatMultiplyExpression.__to_pure_func,
            non_nullable=True,
            first_operand_needs_to_be_non_nullable=True,
            second_operand_needs_to_be_non_nullable=True
        )


class PyLegendFloatAbsoluteExpression(PyLegendUnaryExpression, PyLegendExpressionFloatReturn):

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

    def __init__(self, operand: PyLegendExpressionFloatReturn) -> None:
        PyLegendExpressionFloatReturn.__init__(self)
        PyLegendUnaryExpression.__init__(
            self,
            operand,
            PyLegendFloatAbsoluteExpression.__to_sql_func,
            PyLegendFloatAbsoluteExpression.__to_pure_func,
            non_nullable=True,
            operand_needs_to_be_non_nullable=True
        )


class PyLegendFloatNegativeExpression(PyLegendUnaryExpression, PyLegendExpressionFloatReturn):

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

    def __init__(self, operand: PyLegendExpressionFloatReturn) -> None:
        PyLegendExpressionFloatReturn.__init__(self)
        PyLegendUnaryExpression.__init__(
            self,
            operand,
            PyLegendFloatNegativeExpression.__to_sql_func,
            PyLegendFloatNegativeExpression.__to_pure_func,
            non_nullable=True,
            operand_needs_to_be_non_nullable=True,
        )


class PyLegendFloatPiExpression(PyLegendNullaryExpression, PyLegendExpressionFloatReturn):

    @staticmethod
    def __to_sql_func(
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        return FunctionCall(
            name=QualifiedName(parts=["PI"]),
            distinct=False,
            arguments=[],
            filter_=None,
            window=None
        )

    @staticmethod
    def __to_pure_func(config: FrameToPureConfig) -> str:
        return "pi()"

    def __init__(self) -> None:
        PyLegendExpressionFloatReturn.__init__(self)
        PyLegendNullaryExpression.__init__(
            self,
            PyLegendFloatPiExpression.__to_sql_func,
            PyLegendFloatPiExpression.__to_pure_func,
            non_nullable=True
        )

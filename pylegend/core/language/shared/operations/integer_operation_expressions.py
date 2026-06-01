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
)
from pylegend.core.language.shared.expression import (
    PyLegendExpressionIntegerReturn,
    PyLegendExpressionStringReturn,
)
from pylegend.core.language.shared.operations.binary_expression import PyLegendBinaryExpression
from pylegend.core.language.shared.operations.unary_expression import PyLegendUnaryExpression
from pylegend.core.language.shared.helpers import generate_pure_functional_call
from pylegend.core.tds.tds_frame import FrameToPureConfig


__all__: PyLegendSequence[str] = [
    "PyLegendIntegerAddExpression",
    "PyLegendIntegerAbsoluteExpression",
    "PyLegendIntegerNegativeExpression",
    "PyLegendIntegerSubtractExpression",
    "PyLegendIntegerMultiplyExpression",
    "PyLegendIntegerModuloExpression",
    "PyLegendIntegerCharExpression",
    "PyLegendIntegerBitAndExpression",
    "PyLegendIntegerBitOrExpression",
    "PyLegendIntegerBitXorExpression",
    "PyLegendIntegerBitShiftLeftExpression",
    "PyLegendIntegerBitShiftRightExpression",
    "PyLegendIntegerBitNotExpression"
]


class PyLegendIntegerAddExpression(PyLegendBinaryExpression, PyLegendExpressionIntegerReturn):

    @staticmethod
    def __to_pure_func(op1_expr: str, op2_expr: str, config: FrameToPureConfig) -> str:
        return f"({op1_expr} + {op2_expr})"

    def __init__(self, operand1: PyLegendExpressionIntegerReturn, operand2: PyLegendExpressionIntegerReturn) -> None:
        PyLegendExpressionIntegerReturn.__init__(self)
        PyLegendBinaryExpression.__init__(
            self,
            operand1,
            operand2,
            PyLegendIntegerAddExpression.__to_pure_func,
            non_nullable=True,
            first_operand_needs_to_be_non_nullable=True,
            second_operand_needs_to_be_non_nullable=True
        )


class PyLegendIntegerSubtractExpression(PyLegendBinaryExpression, PyLegendExpressionIntegerReturn):

    @staticmethod
    def __to_pure_func(op1_expr: str, op2_expr: str, config: FrameToPureConfig) -> str:
        return f"({op1_expr} - {op2_expr})"

    def __init__(self, operand1: PyLegendExpressionIntegerReturn, operand2: PyLegendExpressionIntegerReturn) -> None:
        PyLegendExpressionIntegerReturn.__init__(self)
        PyLegendBinaryExpression.__init__(
            self,
            operand1,
            operand2,
            PyLegendIntegerSubtractExpression.__to_pure_func,
            non_nullable=True,
            first_operand_needs_to_be_non_nullable=True,
            second_operand_needs_to_be_non_nullable=True
        )


class PyLegendIntegerMultiplyExpression(PyLegendBinaryExpression, PyLegendExpressionIntegerReturn):

    @staticmethod
    def __to_pure_func(op1_expr: str, op2_expr: str, config: FrameToPureConfig) -> str:
        return f"({op1_expr} * {op2_expr})"

    def __init__(self, operand1: PyLegendExpressionIntegerReturn, operand2: PyLegendExpressionIntegerReturn) -> None:
        PyLegendExpressionIntegerReturn.__init__(self)
        PyLegendBinaryExpression.__init__(
            self,
            operand1,
            operand2,
            PyLegendIntegerMultiplyExpression.__to_pure_func,
            non_nullable=True,
            first_operand_needs_to_be_non_nullable=True,
            second_operand_needs_to_be_non_nullable=True
        )


class PyLegendIntegerModuloExpression(PyLegendBinaryExpression, PyLegendExpressionIntegerReturn):

    @staticmethod
    def __to_pure_func(op1_expr: str, op2_expr: str, config: FrameToPureConfig) -> str:
        return generate_pure_functional_call("mod", [op1_expr, op2_expr])

    def __init__(self, operand1: PyLegendExpressionIntegerReturn, operand2: PyLegendExpressionIntegerReturn) -> None:
        PyLegendExpressionIntegerReturn.__init__(self)
        PyLegendBinaryExpression.__init__(
            self,
            operand1,
            operand2,
            PyLegendIntegerModuloExpression.__to_pure_func,
            non_nullable=True,
            first_operand_needs_to_be_non_nullable=True,
            second_operand_needs_to_be_non_nullable=True
        )


class PyLegendIntegerAbsoluteExpression(PyLegendUnaryExpression, PyLegendExpressionIntegerReturn):

    @staticmethod
    def __to_pure_func(op_expr: str, config: FrameToPureConfig) -> str:
        return generate_pure_functional_call("abs", [op_expr])

    def __init__(self, operand: PyLegendExpressionIntegerReturn) -> None:
        PyLegendExpressionIntegerReturn.__init__(self)
        PyLegendUnaryExpression.__init__(
            self,
            operand,
            PyLegendIntegerAbsoluteExpression.__to_pure_func,
            non_nullable=True,
            operand_needs_to_be_non_nullable=True,
        )


class PyLegendIntegerNegativeExpression(PyLegendUnaryExpression, PyLegendExpressionIntegerReturn):

    @staticmethod
    def __to_pure_func(op_expr: str, config: FrameToPureConfig) -> str:
        return generate_pure_functional_call("minus", [op_expr])

    def __init__(self, operand: PyLegendExpressionIntegerReturn) -> None:
        PyLegendExpressionIntegerReturn.__init__(self)
        PyLegendUnaryExpression.__init__(
            self,
            operand,
            PyLegendIntegerNegativeExpression.__to_pure_func,
            non_nullable=True,
            operand_needs_to_be_non_nullable=True,
        )


class PyLegendIntegerCharExpression(PyLegendUnaryExpression, PyLegendExpressionStringReturn):

    @staticmethod
    def __to_pure_func(op_expr: str, config: FrameToPureConfig) -> str:
        return generate_pure_functional_call("char", [op_expr])

    def __init__(self, operand: PyLegendExpressionIntegerReturn) -> None:
        PyLegendExpressionStringReturn.__init__(self)
        PyLegendUnaryExpression.__init__(
            self,
            operand,
            PyLegendIntegerCharExpression.__to_pure_func,
            non_nullable=True,
            operand_needs_to_be_non_nullable=True,
        )


class PyLegendIntegerBitAndExpression(PyLegendBinaryExpression, PyLegendExpressionIntegerReturn):

    @staticmethod
    def __to_pure_func(op1_expr: str, op2_expr: str, config: FrameToPureConfig) -> str:
        return generate_pure_functional_call("bitAnd", [op1_expr, op2_expr])

    def __init__(self, operand1: PyLegendExpressionIntegerReturn, operand2: PyLegendExpressionIntegerReturn) -> None:
        PyLegendExpressionIntegerReturn.__init__(self)
        PyLegendBinaryExpression.__init__(
            self,
            operand1,
            operand2,
            PyLegendIntegerBitAndExpression.__to_pure_func,
            non_nullable=True,
            first_operand_needs_to_be_non_nullable=True,
            second_operand_needs_to_be_non_nullable=True
        )


class PyLegendIntegerBitOrExpression(PyLegendBinaryExpression, PyLegendExpressionIntegerReturn):

    @staticmethod
    def __to_pure_func(op1_expr: str, op2_expr: str, config: FrameToPureConfig) -> str:
        return generate_pure_functional_call("bitOr", [op1_expr, op2_expr])

    def __init__(self, operand1: PyLegendExpressionIntegerReturn, operand2: PyLegendExpressionIntegerReturn) -> None:
        PyLegendExpressionIntegerReturn.__init__(self)
        PyLegendBinaryExpression.__init__(
            self,
            operand1,
            operand2,
            PyLegendIntegerBitOrExpression.__to_pure_func,
            non_nullable=True,
            first_operand_needs_to_be_non_nullable=True,
            second_operand_needs_to_be_non_nullable=True
        )


class PyLegendIntegerBitXorExpression(PyLegendBinaryExpression, PyLegendExpressionIntegerReturn):

    @staticmethod
    def __to_pure_func(op1_expr: str, op2_expr: str, config: FrameToPureConfig) -> str:
        return generate_pure_functional_call("bitXor", [op1_expr, op2_expr])

    def __init__(self, operand1: PyLegendExpressionIntegerReturn, operand2: PyLegendExpressionIntegerReturn) -> None:
        PyLegendExpressionIntegerReturn.__init__(self)
        PyLegendBinaryExpression.__init__(
            self,
            operand1,
            operand2,
            PyLegendIntegerBitXorExpression.__to_pure_func,
            non_nullable=True,
            first_operand_needs_to_be_non_nullable=True,
            second_operand_needs_to_be_non_nullable=True
        )


class PyLegendIntegerBitShiftLeftExpression(PyLegendBinaryExpression, PyLegendExpressionIntegerReturn):

    @staticmethod
    def __to_pure_func(op1_expr: str, op2_expr: str, config: FrameToPureConfig) -> str:
        return generate_pure_functional_call("bitShiftLeft", [op1_expr, op2_expr])

    def __init__(self, operand1: PyLegendExpressionIntegerReturn, operand2: PyLegendExpressionIntegerReturn) -> None:
        PyLegendExpressionIntegerReturn.__init__(self)
        PyLegendBinaryExpression.__init__(
            self,
            operand1,
            operand2,
            PyLegendIntegerBitShiftLeftExpression.__to_pure_func,
            non_nullable=True,
            first_operand_needs_to_be_non_nullable=True,
            second_operand_needs_to_be_non_nullable=True
        )


class PyLegendIntegerBitShiftRightExpression(PyLegendBinaryExpression, PyLegendExpressionIntegerReturn):

    @staticmethod
    def __to_pure_func(op1_expr: str, op2_expr: str, config: FrameToPureConfig) -> str:
        return generate_pure_functional_call("bitShiftRight", [op1_expr, op2_expr])

    def __init__(self, operand1: PyLegendExpressionIntegerReturn, operand2: PyLegendExpressionIntegerReturn) -> None:
        PyLegendExpressionIntegerReturn.__init__(self)
        PyLegendBinaryExpression.__init__(
            self,
            operand1,
            operand2,
            PyLegendIntegerBitShiftRightExpression.__to_pure_func,
            non_nullable=True,
            first_operand_needs_to_be_non_nullable=True,
            second_operand_needs_to_be_non_nullable=True
        )


class PyLegendIntegerBitNotExpression(PyLegendUnaryExpression, PyLegendExpressionIntegerReturn):

    @staticmethod
    def __to_pure_func(op_expr: str, config: FrameToPureConfig) -> str:
        return generate_pure_functional_call("bitNot", [op_expr])

    def __init__(self, operand: PyLegendExpressionIntegerReturn) -> None:
        PyLegendExpressionIntegerReturn.__init__(self)
        PyLegendUnaryExpression.__init__(
            self,
            operand,
            PyLegendIntegerBitNotExpression.__to_pure_func,
            non_nullable=True,
            operand_needs_to_be_non_nullable=True,
        )

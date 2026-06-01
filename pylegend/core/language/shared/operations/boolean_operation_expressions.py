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
    PyLegendExpression,
    PyLegendExpressionBooleanReturn,
    PyLegendExpressionStringReturn,
    PyLegendExpressionNumberReturn,
    PyLegendExpressionIntegerReturn,
    PyLegendExpressionFloatReturn,
    PyLegendExpressionDecimalReturn,
    PyLegendExpressionDateReturn,
    PyLegendExpressionDateTimeReturn,
    PyLegendExpressionStrictDateReturn,
)
from pylegend.core.language.shared.helpers import generate_pure_functional_call
from pylegend.core.language.shared.operations.binary_expression import PyLegendBinaryExpression
from pylegend.core.language.shared.operations.unary_expression import PyLegendUnaryExpression
from pylegend.core.language.shared.operations.nary_expression import PyLegendNaryExpression
from pylegend.core.tds.tds_frame import FrameToPureConfig


__all__: PyLegendSequence[str] = [
    "PyLegendBooleanOrExpression",
    "PyLegendBooleanAndExpression",
    "PyLegendBooleanNotExpression",
    "PyLegendBooleanLessThanExpression",
    "PyLegendBooleanLessThanEqualExpression",
    "PyLegendBooleanGreaterThanExpression",
    "PyLegendBooleanGreaterThanEqualExpression",
    "PyLegendBooleanXorExpression",
    "PyLegendBooleanCaseExpression",
    "PyLegendStringCaseExpression",
    "PyLegendNumberCaseExpression",
    "PyLegendIntegerCaseExpression",
    "PyLegendFloatCaseExpression",
    "PyLegendDecimalCaseExpression",
    "PyLegendDateCaseExpression",
    "PyLegendDateTimeCaseExpression",
    "PyLegendStrictDateCaseExpression",
]


class PyLegendBooleanOrExpression(PyLegendBinaryExpression, PyLegendExpressionBooleanReturn):

    @staticmethod
    def __to_pure_func(op1_expr: str, op2_expr: str, config: FrameToPureConfig) -> str:
        return f"({op1_expr} || {op2_expr})"

    def __init__(self, operand1: PyLegendExpressionBooleanReturn, operand2: PyLegendExpressionBooleanReturn) -> None:
        PyLegendExpressionBooleanReturn.__init__(self)
        PyLegendBinaryExpression.__init__(
            self,
            operand1,
            operand2,
            PyLegendBooleanOrExpression.__to_pure_func,
            non_nullable=True,
            first_operand_needs_to_be_non_nullable=True,
            second_operand_needs_to_be_non_nullable=True
        )


class PyLegendBooleanAndExpression(PyLegendBinaryExpression, PyLegendExpressionBooleanReturn):

    @staticmethod
    def __to_pure_func(op1_expr: str, op2_expr: str, config: FrameToPureConfig) -> str:
        return f"({op1_expr} && {op2_expr})"

    def __init__(self, operand1: PyLegendExpressionBooleanReturn, operand2: PyLegendExpressionBooleanReturn) -> None:
        PyLegendExpressionBooleanReturn.__init__(self)
        PyLegendBinaryExpression.__init__(
            self,
            operand1,
            operand2,
            PyLegendBooleanAndExpression.__to_pure_func,
            non_nullable=True,
            first_operand_needs_to_be_non_nullable=True,
            second_operand_needs_to_be_non_nullable=True
        )


class PyLegendBooleanLessThanExpression(PyLegendBinaryExpression, PyLegendExpressionBooleanReturn):

    @staticmethod
    def __to_pure_func(op1_expr: str, op2_expr: str, config: FrameToPureConfig) -> str:
        return f"({op1_expr} < {op2_expr})"

    def __init__(self, operand1: PyLegendExpressionBooleanReturn, operand2: PyLegendExpressionBooleanReturn) -> None:
        PyLegendExpressionBooleanReturn.__init__(self)
        PyLegendBinaryExpression.__init__(
            self,
            operand1,
            operand2,
            PyLegendBooleanLessThanExpression.__to_pure_func,
            non_nullable=True
        )


class PyLegendBooleanLessThanEqualExpression(PyLegendBinaryExpression, PyLegendExpressionBooleanReturn):

    @staticmethod
    def __to_pure_func(op1_expr: str, op2_expr: str, config: FrameToPureConfig) -> str:
        return f"({op1_expr} <= {op2_expr})"

    def __init__(self, operand1: PyLegendExpressionBooleanReturn, operand2: PyLegendExpressionBooleanReturn) -> None:
        PyLegendExpressionBooleanReturn.__init__(self)
        PyLegendBinaryExpression.__init__(
            self,
            operand1,
            operand2,
            PyLegendBooleanLessThanEqualExpression.__to_pure_func,
            non_nullable=True
        )


class PyLegendBooleanGreaterThanExpression(PyLegendBinaryExpression, PyLegendExpressionBooleanReturn):

    @staticmethod
    def __to_pure_func(op1_expr: str, op2_expr: str, config: FrameToPureConfig) -> str:
        return f"({op1_expr} > {op2_expr})"

    def __init__(self, operand1: PyLegendExpressionBooleanReturn, operand2: PyLegendExpressionBooleanReturn) -> None:
        PyLegendExpressionBooleanReturn.__init__(self)
        PyLegendBinaryExpression.__init__(
            self,
            operand1,
            operand2,
            PyLegendBooleanGreaterThanExpression.__to_pure_func,
            non_nullable=True
        )


class PyLegendBooleanGreaterThanEqualExpression(PyLegendBinaryExpression, PyLegendExpressionBooleanReturn):

    @staticmethod
    def __to_pure_func(op1_expr: str, op2_expr: str, config: FrameToPureConfig) -> str:
        return f"({op1_expr} >= {op2_expr})"

    def __init__(self, operand1: PyLegendExpressionBooleanReturn, operand2: PyLegendExpressionBooleanReturn) -> None:
        PyLegendExpressionBooleanReturn.__init__(self)
        PyLegendBinaryExpression.__init__(
            self,
            operand1,
            operand2,
            PyLegendBooleanGreaterThanEqualExpression.__to_pure_func,
            non_nullable=True
        )


class PyLegendBooleanXorExpression(PyLegendBinaryExpression, PyLegendExpressionBooleanReturn):

    @staticmethod
    def __to_pure_func(op1_expr: str, op2_expr: str, config: FrameToPureConfig) -> str:
        return generate_pure_functional_call("xor", [op1_expr, op2_expr])

    def __init__(self, operand1: PyLegendExpressionBooleanReturn, operand2: PyLegendExpressionBooleanReturn) -> None:
        PyLegendExpressionBooleanReturn.__init__(self)
        PyLegendBinaryExpression.__init__(
            self,
            operand1,
            operand2,
            PyLegendBooleanXorExpression.__to_pure_func,
            non_nullable=True,
            first_operand_needs_to_be_non_nullable=True,
            second_operand_needs_to_be_non_nullable=True
        )


class PyLegendBooleanNotExpression(PyLegendUnaryExpression, PyLegendExpressionBooleanReturn):

    @staticmethod
    def __to_pure_func(op_expr: str, config: FrameToPureConfig) -> str:
        return generate_pure_functional_call("not", [op_expr])

    def __init__(self, operand: PyLegendExpressionBooleanReturn) -> None:
        PyLegendExpressionBooleanReturn.__init__(self)
        PyLegendUnaryExpression.__init__(
            self,
            operand,
            PyLegendBooleanNotExpression.__to_pure_func,
            non_nullable=True,
            operand_needs_to_be_non_nullable=True,
        )


class PyLegendCaseExpressionBase(PyLegendNaryExpression):

    @staticmethod
    def __to_pure_func(op_exprs: list, config: FrameToPureConfig) -> str:
        return f"if({op_exprs[0]}, |{op_exprs[1]}, |{op_exprs[2]})"

    def __init__(
            self,
            condition: PyLegendExpressionBooleanReturn,
            if_true: PyLegendExpression,
            if_false: PyLegendExpression,
    ) -> None:
        PyLegendNaryExpression.__init__(
            self,
            [condition, if_true, if_false],
            PyLegendCaseExpressionBase.__to_pure_func,
            non_nullable=False,
            operands_non_nullable_flags=[True, False, False]
        )


class PyLegendBooleanCaseExpression(PyLegendCaseExpressionBase, PyLegendExpressionBooleanReturn):
    def __init__(self, condition: PyLegendExpressionBooleanReturn,
                 if_true: PyLegendExpressionBooleanReturn, if_false: PyLegendExpressionBooleanReturn) -> None:
        PyLegendExpressionBooleanReturn.__init__(self)
        PyLegendCaseExpressionBase.__init__(self, condition, if_true, if_false)


class PyLegendStringCaseExpression(PyLegendCaseExpressionBase, PyLegendExpressionStringReturn):
    def __init__(self, condition: PyLegendExpressionBooleanReturn,
                 if_true: PyLegendExpressionStringReturn, if_false: PyLegendExpressionStringReturn) -> None:
        PyLegendExpressionStringReturn.__init__(self)
        PyLegendCaseExpressionBase.__init__(self, condition, if_true, if_false)


class PyLegendNumberCaseExpression(PyLegendCaseExpressionBase, PyLegendExpressionNumberReturn):
    def __init__(self, condition: PyLegendExpressionBooleanReturn,
                 if_true: PyLegendExpressionNumberReturn, if_false: PyLegendExpressionNumberReturn) -> None:
        PyLegendExpressionNumberReturn.__init__(self)
        PyLegendCaseExpressionBase.__init__(self, condition, if_true, if_false)


class PyLegendIntegerCaseExpression(PyLegendCaseExpressionBase, PyLegendExpressionIntegerReturn):
    def __init__(self, condition: PyLegendExpressionBooleanReturn,
                 if_true: PyLegendExpressionIntegerReturn, if_false: PyLegendExpressionIntegerReturn) -> None:
        PyLegendExpressionIntegerReturn.__init__(self)
        PyLegendCaseExpressionBase.__init__(self, condition, if_true, if_false)


class PyLegendFloatCaseExpression(PyLegendCaseExpressionBase, PyLegendExpressionFloatReturn):
    def __init__(self, condition: PyLegendExpressionBooleanReturn,
                 if_true: PyLegendExpressionFloatReturn, if_false: PyLegendExpressionFloatReturn) -> None:
        PyLegendExpressionFloatReturn.__init__(self)
        PyLegendCaseExpressionBase.__init__(self, condition, if_true, if_false)


class PyLegendDecimalCaseExpression(PyLegendCaseExpressionBase, PyLegendExpressionDecimalReturn):
    def __init__(self, condition: PyLegendExpressionBooleanReturn,
                 if_true: PyLegendExpressionDecimalReturn, if_false: PyLegendExpressionDecimalReturn) -> None:
        PyLegendExpressionDecimalReturn.__init__(self)
        PyLegendCaseExpressionBase.__init__(self, condition, if_true, if_false)


class PyLegendDateCaseExpression(PyLegendCaseExpressionBase, PyLegendExpressionDateReturn):
    def __init__(self, condition: PyLegendExpressionBooleanReturn,
                 if_true: PyLegendExpressionDateReturn, if_false: PyLegendExpressionDateReturn) -> None:
        PyLegendExpressionDateReturn.__init__(self)
        PyLegendCaseExpressionBase.__init__(self, condition, if_true, if_false)


class PyLegendDateTimeCaseExpression(PyLegendCaseExpressionBase, PyLegendExpressionDateTimeReturn):
    def __init__(self, condition: PyLegendExpressionBooleanReturn,
                 if_true: PyLegendExpressionDateTimeReturn, if_false: PyLegendExpressionDateTimeReturn) -> None:
        PyLegendExpressionDateTimeReturn.__init__(self)
        PyLegendCaseExpressionBase.__init__(self, condition, if_true, if_false)


class PyLegendStrictDateCaseExpression(PyLegendCaseExpressionBase, PyLegendExpressionStrictDateReturn):
    def __init__(self, condition: PyLegendExpressionBooleanReturn,
                 if_true: PyLegendExpressionStrictDateReturn, if_false: PyLegendExpressionStrictDateReturn) -> None:
        PyLegendExpressionStrictDateReturn.__init__(self)
        PyLegendCaseExpressionBase.__init__(self, condition, if_true, if_false)

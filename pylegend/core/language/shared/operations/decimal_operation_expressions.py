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
    PyLegendExpressionFloatReturn,
    PyLegendExpressionIntegerReturn,
    PyLegendExpressionNumberReturn,
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
    Cast,
    ColumnType,
)
from pylegend.core.sql.metamodel_extension import (
    AbsoluteExpression,
    RoundExpression,
)
from pylegend.core.tds.tds_frame import FrameToSqlConfig
from pylegend.core.tds.tds_frame import FrameToPureConfig


__all__: PyLegendSequence[str] = [
    "PyLegendDecimalAddExpression",
    "PyLegendDecimalAbsoluteExpression",
    "PyLegendDecimalNegativeExpression",
    "PyLegendDecimalSubtractExpression",
    "PyLegendDecimalMultiplyExpression",
    "PyLegendDecimalDivideExpression",
    "PyLegendDecimalDivideScaledExpression",
    "PyLegendDecimalRoundExpression",
    "PyLegendNumberToDecimalExpression",
    "PyLegendNumberToFloatExpression",
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


class PyLegendDecimalDivideExpression(PyLegendBinaryExpression, PyLegendExpressionDecimalReturn):

    @staticmethod
    def __to_sql_func(
            expression1: Expression,
            expression2: Expression,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        return ArithmeticExpression(ArithmeticType.DIVIDE, expression1, expression2)

    @staticmethod
    def __to_pure_func(op1_expr: str, op2_expr: str, config: FrameToPureConfig) -> str:
        return f"({op1_expr} / {op2_expr})"

    def __init__(self, operand1: PyLegendExpressionNumberReturn, operand2: PyLegendExpressionNumberReturn) -> None:
        PyLegendExpressionDecimalReturn.__init__(self)
        PyLegendBinaryExpression.__init__(
            self,
            operand1,
            operand2,
            PyLegendDecimalDivideExpression.__to_sql_func,
            PyLegendDecimalDivideExpression.__to_pure_func,
            non_nullable=True,
            first_operand_needs_to_be_non_nullable=True,
            second_operand_needs_to_be_non_nullable=True
        )


class PyLegendDecimalDivideScaledExpression(PyLegendExpressionDecimalReturn):
    __operand1: PyLegendExpressionDecimalReturn
    __operand2: PyLegendExpressionDecimalReturn
    __scale: PyLegendExpressionIntegerReturn

    def __init__(
            self,
            operand1: PyLegendExpressionDecimalReturn,
            operand2: PyLegendExpressionDecimalReturn,
            scale: PyLegendExpressionIntegerReturn,
    ) -> None:
        PyLegendExpressionDecimalReturn.__init__(self)
        self.__operand1 = operand1
        self.__operand2 = operand2
        self.__scale = scale

    def to_sql_expression(
            self,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        op1 = self.__operand1.to_sql_expression(frame_name_to_base_query_map, config)
        op2 = self.__operand2.to_sql_expression(frame_name_to_base_query_map, config)
        scale = self.__scale.to_sql_expression(frame_name_to_base_query_map, config)
        return RoundExpression(ArithmeticExpression(ArithmeticType.DIVIDE, op1, op2), scale)

    def to_pure_expression(self, config: FrameToPureConfig) -> str:
        from pylegend.core.language.shared.helpers import expr_has_matching_start_and_end_parentheses
        from pylegend.core.language.pandas_api.pandas_api_series import Series
        from pylegend.core.language.pandas_api.pandas_api_groupby_series import GroupbySeries
        op1 = self.__operand1.to_pure_expression(config)
        if not (self.__operand1.is_non_nullable()
                or (isinstance(self.__operand1, (Series, GroupbySeries)) and self.__operand1.expr is not None)):
            op1 = f"toOne({op1[1:-1] if expr_has_matching_start_and_end_parentheses(op1) else op1})"
        op2 = self.__operand2.to_pure_expression(config)
        if not (self.__operand2.is_non_nullable()
                or (isinstance(self.__operand2, (Series, GroupbySeries)) and self.__operand2.expr is not None)):
            op2 = f"toOne({op2[1:-1] if expr_has_matching_start_and_end_parentheses(op2) else op2})"
        scale = self.__scale.to_pure_expression(config)
        return generate_pure_functional_call("divide", [op1, op2, scale])

    def is_non_nullable(self) -> bool:
        return True


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


class PyLegendDecimalRoundExpression(PyLegendBinaryExpression, PyLegendExpressionDecimalReturn):

    @staticmethod
    def __to_sql_func(
            expression1: Expression,
            expression2: Expression,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        return RoundExpression(expression1, expression2)

    @staticmethod
    def __to_pure_func(op1_expr: str, op2_expr: str, config: FrameToPureConfig) -> str:
        if op2_expr == "0":
            return generate_pure_functional_call("round", [op1_expr])
        return generate_pure_functional_call("round", [op1_expr, op2_expr])

    def __init__(self, operand1: PyLegendExpressionDecimalReturn, operand2: PyLegendExpressionIntegerReturn) -> None:
        PyLegendExpressionDecimalReturn.__init__(self)
        PyLegendBinaryExpression.__init__(
            self,
            operand1,
            operand2,
            PyLegendDecimalRoundExpression.__to_sql_func,
            PyLegendDecimalRoundExpression.__to_pure_func,
            non_nullable=True,
            first_operand_needs_to_be_non_nullable=True,
            second_operand_needs_to_be_non_nullable=True
        )

    def is_non_nullable(self) -> bool:
        return True


class PyLegendNumberToDecimalExpression(PyLegendUnaryExpression, PyLegendExpressionDecimalReturn):

    @staticmethod
    def __to_sql_func(
            expression: Expression,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        return Cast(expression, ColumnType(name="DECIMAL", parameters=[]))

    @staticmethod
    def __to_pure_func(op_expr: str, config: FrameToPureConfig) -> str:
        return generate_pure_functional_call("toDecimal", [op_expr])

    def __init__(self, operand: PyLegendExpressionNumberReturn) -> None:
        PyLegendExpressionDecimalReturn.__init__(self)
        PyLegendUnaryExpression.__init__(
            self,
            operand,
            PyLegendNumberToDecimalExpression.__to_sql_func,
            PyLegendNumberToDecimalExpression.__to_pure_func,
            non_nullable=True,
            operand_needs_to_be_non_nullable=True,
        )


class PyLegendNumberToFloatExpression(PyLegendUnaryExpression, PyLegendExpressionFloatReturn):

    @staticmethod
    def __to_sql_func(
            expression: Expression,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        return Cast(expression, ColumnType(name="DOUBLE PRECISION", parameters=[]))

    @staticmethod
    def __to_pure_func(op_expr: str, config: FrameToPureConfig) -> str:
        return generate_pure_functional_call("toFloat", [op_expr])

    def __init__(self, operand: PyLegendExpressionNumberReturn) -> None:
        PyLegendExpressionFloatReturn.__init__(self)
        PyLegendUnaryExpression.__init__(
            self,
            operand,
            PyLegendNumberToFloatExpression.__to_sql_func,
            PyLegendNumberToFloatExpression.__to_pure_func,
            non_nullable=True,
            operand_needs_to_be_non_nullable=True,
        )

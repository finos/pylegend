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
    PyLegendExpression,
    PyLegendExpressionBooleanReturn,
    PyLegendExpressionStringReturn
)
from pylegend.core.language.shared.helpers import generate_pure_functional_call
from pylegend.core.language.shared.operations.binary_expression import PyLegendBinaryExpression
from pylegend.core.language.shared.operations.unary_expression import PyLegendUnaryExpression
from pylegend.core.sql.metamodel import (
    Expression,
    QuerySpecification,
    ComparisonExpression,
    ComparisonOperator,
    IsNullPredicate,
    IsNotNullPredicate,
    Cast,
    ColumnType,
    StringLiteral
)
from pylegend.core.tds.tds_frame import FrameToSqlConfig
from pylegend.core.tds.tds_frame import FrameToPureConfig


__all__: PyLegendSequence[str] = [
    "PyLegendPrimitiveEqualsExpression",
    "PyLegendPrimitiveNotEqualsExpression",
    "PyLegendIsEmptyExpression",
    "PyLegendIsNotEmptyExpression",
    "PyLegendPrimitiveToStringExpression",
    "PyLegendPrimitiveCastExpression"
]


class PyLegendPrimitiveEqualsExpression(PyLegendBinaryExpression, PyLegendExpressionBooleanReturn):

    @staticmethod
    def __to_sql_func(
            expression1: Expression,
            expression2: Expression,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        return ComparisonExpression(expression1, expression2, ComparisonOperator.EQUAL)

    @staticmethod
    def __to_pure_func(op1_expr: str, op2_expr: str, config: FrameToPureConfig) -> str:
        return f"({op1_expr} == {op2_expr})"

    def __init__(self, operand1: PyLegendExpression, operand2: PyLegendExpression) -> None:
        PyLegendExpressionBooleanReturn.__init__(self)
        PyLegendBinaryExpression.__init__(
            self,
            operand1,
            operand2,
            PyLegendPrimitiveEqualsExpression.__to_sql_func,
            PyLegendPrimitiveEqualsExpression.__to_pure_func,
            non_nullable=True,
        )


class PyLegendPrimitiveNotEqualsExpression(PyLegendBinaryExpression, PyLegendExpressionBooleanReturn):

    @staticmethod
    def __to_sql_func(
            expression1: Expression,
            expression2: Expression,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        return ComparisonExpression(expression1, expression2, ComparisonOperator.NOT_EQUAL)

    @staticmethod
    def __to_pure_func(op1_expr: str, op2_expr: str, config: FrameToPureConfig) -> str:
        return f"({op1_expr} != {op2_expr})"

    def __init__(self, operand1: PyLegendExpression, operand2: PyLegendExpression) -> None:
        PyLegendExpressionBooleanReturn.__init__(self)
        PyLegendBinaryExpression.__init__(
            self,
            operand1,
            operand2,
            PyLegendPrimitiveNotEqualsExpression.__to_sql_func,
            PyLegendPrimitiveNotEqualsExpression.__to_pure_func,
            non_nullable=True,
        )


class PyLegendIsEmptyExpression(PyLegendUnaryExpression, PyLegendExpressionBooleanReturn):

    @staticmethod
    def __to_sql_func(
            expression: Expression,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        return IsNullPredicate(expression)

    @staticmethod
    def __to_pure_func(op_expr: str, config: FrameToPureConfig) -> str:
        return generate_pure_functional_call("isEmpty", [op_expr])

    def __init__(self, operand: PyLegendExpression) -> None:
        PyLegendExpressionBooleanReturn.__init__(self)
        PyLegendUnaryExpression.__init__(
            self,
            operand,
            PyLegendIsEmptyExpression.__to_sql_func,
            PyLegendIsEmptyExpression.__to_pure_func,
            non_nullable=True,
        )


class PyLegendIsNotEmptyExpression(PyLegendUnaryExpression, PyLegendExpressionBooleanReturn):

    @staticmethod
    def __to_sql_func(
            expression: Expression,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        return IsNotNullPredicate(expression)

    @staticmethod
    def __to_pure_func(op_expr: str, config: FrameToPureConfig) -> str:
        return generate_pure_functional_call("isNotEmpty", [op_expr])

    def __init__(self, operand: PyLegendExpression) -> None:
        PyLegendExpressionBooleanReturn.__init__(self)
        PyLegendUnaryExpression.__init__(
            self,
            operand,
            PyLegendIsNotEmptyExpression.__to_sql_func,
            PyLegendIsNotEmptyExpression.__to_pure_func,
            non_nullable=True,
        )


class PyLegendPrimitiveToStringExpression(PyLegendUnaryExpression, PyLegendExpressionStringReturn):

    @staticmethod
    def __to_sql_func(
            expression: Expression,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        return Cast(expression, ColumnType(name="TEXT", parameters=[]))

    @staticmethod
    def __to_pure_func(op_expr: str, config: FrameToPureConfig) -> str:
        return generate_pure_functional_call("toString", [op_expr])

    def __init__(self, operand: PyLegendExpression) -> None:
        PyLegendExpressionStringReturn.__init__(self)
        PyLegendUnaryExpression.__init__(
            self,
            operand,
            PyLegendPrimitiveToStringExpression.__to_sql_func,
            PyLegendPrimitiveToStringExpression.__to_pure_func,
            non_nullable=True,
            operand_needs_to_be_non_nullable=True,
        )


_PRIMITIVE_TYPE_TO_SQL_TYPE: PyLegendDict[str, str] = {
    "Boolean": "BOOLEAN",
    "String": "TEXT",
    "Number": "DOUBLE PRECISION",
    "Integer": "INTEGER",
    "Float": "DOUBLE PRECISION",
    "Decimal": "DECIMAL",
    "Date": "DATE",
    "DateTime": "TIMESTAMP",
    "StrictDate": "DATE",
    "LatestDate": "DATE",
    "TinyInt": "SMALLINT",
    "UTinyInt": "SMALLINT",
    "SmallInt": "SMALLINT",
    "USmallInt": "INTEGER",
    "Int": "INTEGER",
    "UInt": "BIGINT",
    "BigInt": "BIGINT",
    "UBigInt": "NUMERIC",
    "Varchar": "VARCHAR",
    "Timestamp": "TIMESTAMP",
    "Float4": "REAL",
    "Double": "DOUBLE PRECISION",
    "Numeric": "NUMERIC",
}


class PyLegendPrimitiveCastExpression(PyLegendBinaryExpression, PyLegendExpression):

    @staticmethod
    def __to_sql_func(
            expression1: Expression,
            expression2: Expression,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        assert isinstance(expression2, StringLiteral)
        type_name = expression2.value
        sql_type = _PRIMITIVE_TYPE_TO_SQL_TYPE.get(type_name, type_name.upper())
        return Cast(expression1, ColumnType(name=sql_type, parameters=[]))

    @staticmethod
    def __to_pure_func(op1_expr: str, op2_expr: str, config: FrameToPureConfig) -> str:
        type_name = op2_expr[1:-1] if op2_expr.startswith("'") and op2_expr.endswith("'") else op2_expr
        return generate_pure_functional_call("cast", [op1_expr, f"@{type_name}"])

    def __init__(self, operand1: PyLegendExpression, operand2: PyLegendExpression) -> None:
        PyLegendExpression.__init__(self)
        PyLegendBinaryExpression.__init__(
            self,
            operand1,
            operand2,
            PyLegendPrimitiveCastExpression.__to_sql_func,
            PyLegendPrimitiveCastExpression.__to_pure_func,
            non_nullable=True,
        )

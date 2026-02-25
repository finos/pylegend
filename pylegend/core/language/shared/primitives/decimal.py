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
    PyLegendUnion,
    TYPE_CHECKING,
)
from pylegend.core.language.shared.primitives.number import PyLegendNumber
from pylegend.core.language.shared.expression import PyLegendExpressionDecimalReturn
from pylegend.core.language.shared.literal_expressions import PyLegendDecimalLiteralExpression
from pylegend.core.sql.metamodel import (
    Expression,
    QuerySpecification
)
from pylegend.core.tds.pandas_api.frames.helpers.series_helper import grammar_method
from pylegend.core.tds.tds_frame import FrameToSqlConfig
from pylegend.core.language.shared.operations.decimal_operation_expressions import (
    PyLegendDecimalAbsoluteExpression,
    PyLegendDecimalAddExpression,
    PyLegendDecimalNegativeExpression,
    PyLegendDecimalSubtractExpression,
    PyLegendDecimalMultiplyExpression,
)
if TYPE_CHECKING:
    from pylegend.core.language.shared.primitives import PyLegendInteger, PyLegendFloat

__all__: PyLegendSequence[str] = [
    "PyLegendDecimal"
]


class PyLegendDecimal(PyLegendNumber):
    __value_copy: PyLegendExpressionDecimalReturn

    def __init__(
            self,
            value: PyLegendExpressionDecimalReturn
    ) -> None:
        self.__value_copy = value
        super().__init__(value)

    @grammar_method
    def __add__(
            self,
            other: PyLegendUnion[int, float, "PyLegendInteger", "PyLegendFloat", "PyLegendDecimal", "PyLegendNumber"]
    ) -> "PyLegendUnion[PyLegendNumber, PyLegendDecimal]":
        PyLegendNumber.validate_param_to_be_number(other, "Decimal plus (+) parameter")
        if isinstance(other, (float, PyLegendDecimal)):
            other_op = PyLegendDecimal.__convert_to_decimal_expr(other)
            return PyLegendDecimal(PyLegendDecimalAddExpression(self.__value_copy, other_op))
        else:
            return super().__add__(other)

    @grammar_method
    def __radd__(
            self,
            other: PyLegendUnion[int, float, "PyLegendInteger", "PyLegendFloat", "PyLegendDecimal", "PyLegendNumber"]
    ) -> "PyLegendUnion[PyLegendNumber, PyLegendDecimal]":
        PyLegendNumber.validate_param_to_be_number(other, "Decimal plus (+) parameter")
        if isinstance(other, (float, PyLegendDecimal)):
            other_op = PyLegendDecimal.__convert_to_decimal_expr(other)
            return PyLegendDecimal(PyLegendDecimalAddExpression(other_op, self.__value_copy))
        else:
            return super().__radd__(other)

    @grammar_method
    def __sub__(
            self,
            other: PyLegendUnion[int, float, "PyLegendInteger", "PyLegendFloat", "PyLegendDecimal", "PyLegendNumber"]
    ) -> "PyLegendUnion[PyLegendNumber, PyLegendDecimal]":
        PyLegendNumber.validate_param_to_be_number(other, "Decimal minus (-) parameter")
        if isinstance(other, (float, PyLegendDecimal)):
            other_op = PyLegendDecimal.__convert_to_decimal_expr(other)
            return PyLegendDecimal(PyLegendDecimalSubtractExpression(self.__value_copy, other_op))
        else:
            return super().__sub__(other)

    @grammar_method
    def __rsub__(
            self,
            other: PyLegendUnion[int, float, "PyLegendInteger", "PyLegendFloat", "PyLegendDecimal", "PyLegendNumber"]
    ) -> "PyLegendUnion[PyLegendNumber, PyLegendDecimal]":
        PyLegendNumber.validate_param_to_be_number(other, "Decimal minus (-) parameter")
        if isinstance(other, (float, PyLegendDecimal)):
            other_op = PyLegendDecimal.__convert_to_decimal_expr(other)
            return PyLegendDecimal(PyLegendDecimalSubtractExpression(other_op, self.__value_copy))
        else:
            return super().__rsub__(other)

    @grammar_method
    def __mul__(
            self,
            other: PyLegendUnion[int, float, "PyLegendInteger", "PyLegendFloat", "PyLegendDecimal", "PyLegendNumber"]
    ) -> "PyLegendUnion[PyLegendNumber, PyLegendDecimal]":
        PyLegendNumber.validate_param_to_be_number(other, "Decimal multiply (*) parameter")
        if isinstance(other, (float, PyLegendDecimal)):
            other_op = PyLegendDecimal.__convert_to_decimal_expr(other)
            return PyLegendDecimal(PyLegendDecimalMultiplyExpression(self.__value_copy, other_op))
        else:
            return super().__mul__(other)

    @grammar_method
    def __rmul__(
            self,
            other: PyLegendUnion[int, float, "PyLegendInteger", "PyLegendFloat", "PyLegendDecimal", "PyLegendNumber"]
    ) -> "PyLegendUnion[PyLegendNumber, PyLegendDecimal]":
        PyLegendNumber.validate_param_to_be_number(other, "Decimal multiply (*) parameter")
        if isinstance(other, (float, PyLegendDecimal)):
            other_op = PyLegendDecimal.__convert_to_decimal_expr(other)
            return PyLegendDecimal(PyLegendDecimalMultiplyExpression(other_op, self.__value_copy))
        else:
            return super().__rmul__(other)

    @grammar_method
    def __abs__(self) -> "PyLegendDecimal":
        return PyLegendDecimal(PyLegendDecimalAbsoluteExpression(self.__value_copy))

    @grammar_method
    def __neg__(self) -> "PyLegendDecimal":
        return PyLegendDecimal(PyLegendDecimalNegativeExpression(self.__value_copy))

    @grammar_method
    def __pos__(self) -> "PyLegendDecimal":
        return self

    @staticmethod
    def __convert_to_decimal_expr(
            val: PyLegendUnion[float, "PyLegendDecimal"]
    ) -> PyLegendExpressionDecimalReturn:
        if isinstance(val, float):
            return PyLegendDecimalLiteralExpression(val)
        return val.__value_copy

    def to_sql_expression(
            self,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        return super().to_sql_expression(frame_name_to_base_query_map, config)

    def value(self) -> PyLegendExpressionDecimalReturn:
        return self.__value_copy

    @staticmethod
    def __validate__param_to_be_decimal(params, desc):  # type: ignore
        PyLegendNumber.validate_param_to_be_number(params, desc)


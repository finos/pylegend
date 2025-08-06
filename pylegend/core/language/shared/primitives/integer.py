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
    TYPE_CHECKING,
)
from pylegend.core.language.shared.primitives.number import PyLegendNumber
from pylegend.core.language.shared.expression import PyLegendExpressionIntegerReturn
from pylegend.core.language.shared.literal_expressions import PyLegendIntegerLiteralExpression
from pylegend.core.sql.metamodel import (
    Expression,
    QuerySpecification
)
from pylegend.core.tds.tds_frame import FrameToSqlConfig
from pylegend.core.language.shared.operations.integer_operation_expressions import (
    PyLegendIntegerAddExpression,
    PyLegendIntegerAbsoluteExpression,
    PyLegendIntegerNegativeExpression,
    PyLegendIntegerSubtractExpression,
    PyLegendIntegerMultiplyExpression,
    PyLegendIntegerModuloExpression,
)
if TYPE_CHECKING:
    from pylegend.core.language.shared.primitives import PyLegendFloat

__all__: PyLegendSequence[str] = [
    "PyLegendInteger"
]


class PyLegendInteger(PyLegendNumber):
    __value_copy: PyLegendExpressionIntegerReturn

    def __init__(
            self,
            value: PyLegendExpressionIntegerReturn
    ) -> None:
        self.__value_copy = value
        super().__init__(value)

    def to_sql_expression(
            self,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        return super().to_sql_expression(frame_name_to_base_query_map, config)

    def value(self) -> PyLegendExpressionIntegerReturn:
        return self.__value_copy

    def __add__(
            self,
            other: PyLegendUnion[int, float, "PyLegendInteger", "PyLegendFloat", "PyLegendNumber"]
    ) -> "PyLegendUnion[PyLegendNumber, PyLegendInteger]":
        PyLegendNumber.validate_param_to_be_number(other, "Integer plus (+) parameter")
        if isinstance(other, (int, PyLegendInteger)):
            other_op = PyLegendInteger.__convert_to_integer_expr(other)
            return PyLegendInteger(PyLegendIntegerAddExpression(self.__value_copy, other_op))
        else:
            return super().__add__(other)

    def __radd__(
            self,
            other: PyLegendUnion[int, float, "PyLegendInteger", "PyLegendFloat", "PyLegendNumber"]
    ) -> "PyLegendUnion[PyLegendNumber, PyLegendInteger]":
        PyLegendNumber.validate_param_to_be_number(other, "Integer plus (+) parameter")
        if isinstance(other, (int, PyLegendInteger)):
            other_op = PyLegendInteger.__convert_to_integer_expr(other)
            return PyLegendInteger(PyLegendIntegerAddExpression(other_op, self.__value_copy))
        else:
            return super().__radd__(other)

    def __sub__(
            self,
            other: PyLegendUnion[int, float, "PyLegendInteger", "PyLegendFloat", "PyLegendNumber"]
    ) -> "PyLegendUnion[PyLegendNumber, PyLegendInteger]":
        PyLegendNumber.validate_param_to_be_number(other, "Integer minus (-) parameter")
        if isinstance(other, (int, PyLegendInteger)):
            other_op = PyLegendInteger.__convert_to_integer_expr(other)
            return PyLegendInteger(PyLegendIntegerSubtractExpression(self.__value_copy, other_op))
        else:
            return super().__sub__(other)

    def __rsub__(
            self,
            other: PyLegendUnion[int, float, "PyLegendInteger", "PyLegendFloat", "PyLegendNumber"]
    ) -> "PyLegendUnion[PyLegendNumber, PyLegendInteger]":
        PyLegendNumber.validate_param_to_be_number(other, "Integer minus (-) parameter")
        if isinstance(other, (int, PyLegendInteger)):
            other_op = PyLegendInteger.__convert_to_integer_expr(other)
            return PyLegendInteger(PyLegendIntegerSubtractExpression(other_op, self.__value_copy))
        else:
            return super().__rsub__(other)

    def __mul__(
            self,
            other: PyLegendUnion[int, float, "PyLegendInteger", "PyLegendFloat", "PyLegendNumber"]
    ) -> "PyLegendUnion[PyLegendNumber, PyLegendInteger]":
        PyLegendNumber.validate_param_to_be_number(other, "Integer multiply (*) parameter")
        if isinstance(other, (int, PyLegendInteger)):
            other_op = PyLegendInteger.__convert_to_integer_expr(other)
            return PyLegendInteger(PyLegendIntegerMultiplyExpression(self.__value_copy, other_op))
        else:
            return super().__mul__(other)

    def __rmul__(
            self,
            other: PyLegendUnion[int, float, "PyLegendInteger", "PyLegendFloat", "PyLegendNumber"]
    ) -> "PyLegendUnion[PyLegendNumber, PyLegendInteger]":
        PyLegendNumber.validate_param_to_be_number(other, "Integer multiply (*) parameter")
        if isinstance(other, (int, PyLegendInteger)):
            other_op = PyLegendInteger.__convert_to_integer_expr(other)
            return PyLegendInteger(PyLegendIntegerMultiplyExpression(other_op, self.__value_copy))
        else:
            return super().__rmul__(other)

    def __mod__(
            self,
            other: PyLegendUnion[int, "PyLegendInteger"]
    ) -> "PyLegendUnion[PyLegendNumber, PyLegendInteger]":
        PyLegendInteger.__validate__param_to_be_integer(other, "Integer modulo (%) parameter")
        other_op = PyLegendInteger.__convert_to_integer_expr(other)
        return PyLegendInteger(PyLegendIntegerModuloExpression(self.__value_copy, other_op))

    def __rmod__(
            self,
            other: PyLegendUnion[int, "PyLegendInteger"]
    ) -> "PyLegendUnion[PyLegendNumber, PyLegendInteger]":
        PyLegendNumber.validate_param_to_be_number(other, "Integer modulo (%) parameter")
        other_op = PyLegendInteger.__convert_to_integer_expr(other)
        return PyLegendInteger(PyLegendIntegerModuloExpression(other_op, self.__value_copy))

    def __abs__(self) -> "PyLegendInteger":
        return PyLegendInteger(PyLegendIntegerAbsoluteExpression(self.__value_copy))

    def __neg__(self) -> "PyLegendInteger":
        return PyLegendInteger(PyLegendIntegerNegativeExpression(self.__value_copy))

    def __pos__(self) -> "PyLegendInteger":
        return self

    @staticmethod
    def __convert_to_integer_expr(
            val: PyLegendUnion[int, "PyLegendInteger"]
    ) -> PyLegendExpressionIntegerReturn:
        if isinstance(val, int):
            return PyLegendIntegerLiteralExpression(val)
        return val.__value_copy

    @staticmethod
    def __validate__param_to_be_integer(param: PyLegendUnion[int, "PyLegendInteger"], desc: str) -> None:
        if not isinstance(param, (int, PyLegendInteger)):
            raise TypeError(desc + " should be a int or an integer expression (PyLegendInteger)."
                                   " Got value " + str(param) + " of type: " + str(type(param)))

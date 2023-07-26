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
from pylegend.core.language.primitives.primitive import PyLegendPrimitive
from pylegend.core.language.primitives.boolean import PyLegendBoolean
from pylegend.core.language.expression import (
    PyLegendExpressionNumberReturn,
)
from pylegend.core.language.literal_expressions import (
    PyLegendIntegerLiteralExpression,
    PyLegendFloatLiteralExpression,
)
from pylegend.core.language.operations.number_operation_expressions import (
    PyLegendNumberAddExpression,
    PyLegendNumberMultiplyExpression,
    PyLegendNumberDivideExpression,
    PyLegendNumberSubtractExpression,
    PyLegendNumberLessThanExpression,
    PyLegendNumberLessThanEqualExpression,
    PyLegendNumberGreaterThanExpression,
    PyLegendNumberGreaterThanEqualExpression,
    PyLegendNumberNegativeExpression,
)
from pylegend.core.sql.metamodel import (
    Expression,
    QuerySpecification
)
from pylegend.core.tds.tds_frame import FrameToSqlConfig
if TYPE_CHECKING:
    from pylegend.core.language.primitives.integer import PyLegendInteger
    from pylegend.core.language.primitives.float import PyLegendFloat


__all__: PyLegendSequence[str] = [
    "PyLegendNumber"
]


class PyLegendNumber(PyLegendPrimitive):
    __value: PyLegendExpressionNumberReturn

    def __init__(
            self,
            value: PyLegendExpressionNumberReturn
    ) -> None:
        self.__value = value

    def to_sql_expression(
            self,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        return self.__value.to_sql_expression(frame_name_to_base_query_map, config)

    def __add__(
            self,
            other: PyLegendUnion[int, float, "PyLegendInteger", "PyLegendFloat", "PyLegendNumber"]
    ) -> "PyLegendNumber":
        PyLegendNumber.validate_param_to_be_number(other, "Number plus (+) parameter")
        other_op = PyLegendNumber.__convert_to_number_expr(other)
        return PyLegendNumber(PyLegendNumberAddExpression(self.__value, other_op))

    def __radd__(
            self,
            other: PyLegendUnion[int, float, "PyLegendInteger", "PyLegendFloat", "PyLegendNumber"]
    ) -> "PyLegendNumber":
        PyLegendNumber.validate_param_to_be_number(other, "Number plus (+) parameter")
        other_op = PyLegendNumber.__convert_to_number_expr(other)
        return PyLegendNumber(PyLegendNumberAddExpression(other_op, self.__value))

    def __mul__(
            self,
            other: PyLegendUnion[int, float, "PyLegendInteger", "PyLegendFloat", "PyLegendNumber"]
    ) -> "PyLegendNumber":
        PyLegendNumber.validate_param_to_be_number(other, "Number multiply (*) parameter")
        other_op = PyLegendNumber.__convert_to_number_expr(other)
        return PyLegendNumber(PyLegendNumberMultiplyExpression(self.__value, other_op))

    def __rmul__(
            self,
            other: PyLegendUnion[int, float, "PyLegendInteger", "PyLegendFloat", "PyLegendNumber"]
    ) -> "PyLegendNumber":
        PyLegendNumber.validate_param_to_be_number(other, "Number multiply (*) parameter")
        other_op = PyLegendNumber.__convert_to_number_expr(other)
        return PyLegendNumber(PyLegendNumberMultiplyExpression(other_op, self.__value))

    def __truediv__(
            self,
            other: PyLegendUnion[int, float, "PyLegendInteger", "PyLegendFloat", "PyLegendNumber"]
    ) -> "PyLegendNumber":
        PyLegendNumber.validate_param_to_be_number(other, "Number divide (/) parameter")
        other_op = PyLegendNumber.__convert_to_number_expr(other)
        return PyLegendNumber(PyLegendNumberDivideExpression(self.__value, other_op))

    def __rtruediv__(
            self,
            other: PyLegendUnion[int, float, "PyLegendInteger", "PyLegendFloat", "PyLegendNumber"]
    ) -> "PyLegendNumber":
        PyLegendNumber.validate_param_to_be_number(other, "Number divide (/) parameter")
        other_op = PyLegendNumber.__convert_to_number_expr(other)
        return PyLegendNumber(PyLegendNumberDivideExpression(other_op, self.__value))

    def __sub__(
            self,
            other: PyLegendUnion[int, float, "PyLegendInteger", "PyLegendFloat", "PyLegendNumber"]
    ) -> "PyLegendNumber":
        PyLegendNumber.validate_param_to_be_number(other, "Number subtract (-) parameter")
        other_op = PyLegendNumber.__convert_to_number_expr(other)
        return PyLegendNumber(PyLegendNumberSubtractExpression(self.__value, other_op))

    def __rsub__(
            self,
            other: PyLegendUnion[int, float, "PyLegendInteger", "PyLegendFloat", "PyLegendNumber"]
    ) -> "PyLegendNumber":
        PyLegendNumber.validate_param_to_be_number(other, "Number subtract (-) parameter")
        other_op = PyLegendNumber.__convert_to_number_expr(other)
        return PyLegendNumber(PyLegendNumberSubtractExpression(other_op, self.__value))

    def __lt__(
            self,
            other: PyLegendUnion[int, float, "PyLegendInteger", "PyLegendFloat", "PyLegendNumber"]
    ) -> "PyLegendBoolean":
        PyLegendNumber.validate_param_to_be_number(other, "Number less than (<) parameter")
        other_op = PyLegendNumber.__convert_to_number_expr(other)
        return PyLegendBoolean(PyLegendNumberLessThanExpression(self.__value, other_op))

    def __le__(
            self,
            other: PyLegendUnion[int, float, "PyLegendInteger", "PyLegendFloat", "PyLegendNumber"]
    ) -> "PyLegendBoolean":
        PyLegendNumber.validate_param_to_be_number(other, "Number less than equal (<=) parameter")
        other_op = PyLegendNumber.__convert_to_number_expr(other)
        return PyLegendBoolean(PyLegendNumberLessThanEqualExpression(self.__value, other_op))

    def __gt__(
            self,
            other: PyLegendUnion[int, float, "PyLegendInteger", "PyLegendFloat", "PyLegendNumber"]
    ) -> "PyLegendBoolean":
        PyLegendNumber.validate_param_to_be_number(other, "Number greater than (>) parameter")
        other_op = PyLegendNumber.__convert_to_number_expr(other)
        return PyLegendBoolean(PyLegendNumberGreaterThanExpression(self.__value, other_op))

    def __ge__(
            self,
            other: PyLegendUnion[int, float, "PyLegendInteger", "PyLegendFloat", "PyLegendNumber"]
    ) -> "PyLegendBoolean":
        PyLegendNumber.validate_param_to_be_number(other, "Number greater than equal (>=) parameter")
        other_op = PyLegendNumber.__convert_to_number_expr(other)
        return PyLegendBoolean(PyLegendNumberGreaterThanEqualExpression(self.__value, other_op))

    def __pos__(self) -> "PyLegendNumber":
        return self

    def __neg__(self) -> "PyLegendNumber":
        return PyLegendNumber(PyLegendNumberNegativeExpression(self.__value))

    @staticmethod
    def __convert_to_number_expr(
            val: PyLegendUnion[int, float, "PyLegendInteger", "PyLegendFloat", "PyLegendNumber"]
    ) -> PyLegendExpressionNumberReturn:
        if isinstance(val, int):
            return PyLegendIntegerLiteralExpression(val)
        if isinstance(val, float):
            return PyLegendFloatLiteralExpression(val)
        return val.__value

    @staticmethod
    def validate_param_to_be_number(
            param: PyLegendUnion[int, float, "PyLegendInteger", "PyLegendFloat", "PyLegendNumber"],
            desc: str
    ) -> None:
        from pylegend.core.language.primitives.integer import PyLegendInteger
        from pylegend.core.language.primitives.float import PyLegendFloat
        if not isinstance(param, (int, float, PyLegendInteger, PyLegendFloat, PyLegendNumber)):
            raise TypeError(desc + " should be a int/float or a int/float/number expression"
                                   " (PyLegendInteger/PyLegendFloat/PyLegendNumber)."
                                   " Got value " + str(param) + " of type: " + str(type(param)))

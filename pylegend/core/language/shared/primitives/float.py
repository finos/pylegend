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
from pylegend.core.language.shared.expression import PyLegendExpressionFloatReturn
from pylegend.core.language.shared.literal_expressions import PyLegendFloatLiteralExpression
from pylegend.core.sql.metamodel import (
    Expression,
    QuerySpecification
)
from pylegend.core.tds.tds_frame import FrameToSqlConfig
from pylegend.core.language.shared.operations.float_operation_expressions import (
    PyLegendFloatAbsoluteExpression,
    PyLegendFloatAddExpression,
    PyLegendFloatNegativeExpression,
    PyLegendFloatSubtractExpression,
    PyLegendFloatMultiplyExpression,
)
if TYPE_CHECKING:
    from pylegend.core.language.shared.primitives import PyLegendInteger

__all__: PyLegendSequence[str] = [
    "PyLegendFloat"
]


class PyLegendFloat(PyLegendNumber):
    __value_copy: PyLegendExpressionFloatReturn

    def __init__(
            self,
            value: PyLegendExpressionFloatReturn
    ) -> None:
        self.__value_copy = value
        super().__init__(value)

    def __add__(
            self,
            other: PyLegendUnion[int, float, "PyLegendInteger", "PyLegendFloat", "PyLegendNumber"]
    ) -> "PyLegendUnion[PyLegendNumber, PyLegendFloat]":
        PyLegendNumber.validate_param_to_be_number(other, "Float plus (+) parameter")
        if isinstance(other, (float, PyLegendFloat)):
            other_op = PyLegendFloat.__convert_to_float_expr(other)
            return PyLegendFloat(PyLegendFloatAddExpression(self.__value_copy, other_op))
        else:
            return super().__add__(other)

    def __radd__(
            self,
            other: PyLegendUnion[int, float, "PyLegendInteger", "PyLegendFloat", "PyLegendNumber"]
    ) -> "PyLegendUnion[PyLegendNumber, PyLegendFloat]":
        PyLegendNumber.validate_param_to_be_number(other, "Float plus (+) parameter")
        if isinstance(other, (float, PyLegendFloat)):
            other_op = PyLegendFloat.__convert_to_float_expr(other)
            return PyLegendFloat(PyLegendFloatAddExpression(other_op, self.__value_copy))
        else:
            return super().__radd__(other)

    def __sub__(
            self,
            other: PyLegendUnion[int, float, "PyLegendInteger", "PyLegendFloat", "PyLegendNumber"]
    ) -> "PyLegendUnion[PyLegendNumber, PyLegendFloat]":
        PyLegendNumber.validate_param_to_be_number(other, "Float minus (-) parameter")
        if isinstance(other, (float, PyLegendFloat)):
            other_op = PyLegendFloat.__convert_to_float_expr(other)
            return PyLegendFloat(PyLegendFloatSubtractExpression(self.__value_copy, other_op))
        else:
            return super().__sub__(other)

    def __rsub__(
            self,
            other: PyLegendUnion[int, float, "PyLegendInteger", "PyLegendFloat", "PyLegendNumber"]
    ) -> "PyLegendUnion[PyLegendNumber, PyLegendFloat]":
        PyLegendNumber.validate_param_to_be_number(other, "Float minus (-) parameter")
        if isinstance(other, (float, PyLegendFloat)):
            other_op = PyLegendFloat.__convert_to_float_expr(other)
            return PyLegendFloat(PyLegendFloatSubtractExpression(other_op, self.__value_copy))
        else:
            return super().__rsub__(other)

    def __mul__(
            self,
            other: PyLegendUnion[int, float, "PyLegendInteger", "PyLegendFloat", "PyLegendNumber"]
    ) -> "PyLegendUnion[PyLegendNumber, PyLegendFloat]":
        PyLegendNumber.validate_param_to_be_number(other, "Float multiply (*) parameter")
        if isinstance(other, (float, PyLegendFloat)):
            other_op = PyLegendFloat.__convert_to_float_expr(other)
            return PyLegendFloat(PyLegendFloatMultiplyExpression(self.__value_copy, other_op))
        else:
            return super().__mul__(other)

    def __rmul__(
            self,
            other: PyLegendUnion[int, float, "PyLegendInteger", "PyLegendFloat", "PyLegendNumber"]
    ) -> "PyLegendUnion[PyLegendNumber, PyLegendFloat]":
        PyLegendNumber.validate_param_to_be_number(other, "Float multiply (*) parameter")
        if isinstance(other, (float, PyLegendFloat)):
            other_op = PyLegendFloat.__convert_to_float_expr(other)
            return PyLegendFloat(PyLegendFloatMultiplyExpression(other_op, self.__value_copy))
        else:
            return super().__rmul__(other)

    def __abs__(self) -> "PyLegendFloat":
        return PyLegendFloat(PyLegendFloatAbsoluteExpression(self.__value_copy))

    def __neg__(self) -> "PyLegendFloat":
        return PyLegendFloat(PyLegendFloatNegativeExpression(self.__value_copy))

    def __pos__(self) -> "PyLegendFloat":
        return self

    @staticmethod
    def __convert_to_float_expr(
            val: PyLegendUnion[float, "PyLegendFloat"]
    ) -> PyLegendExpressionFloatReturn:
        if isinstance(val, float):
            return PyLegendFloatLiteralExpression(val)
        return val.__value_copy

    def to_sql_expression(
            self,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        return super().to_sql_expression(frame_name_to_base_query_map, config)

    def value(self) -> PyLegendExpressionFloatReturn:
        return self.__value_copy

    @staticmethod
    def __validate__param_to_be_float(params, desc):  # type: ignore
        PyLegendNumber.validate_param_to_be_number(params, desc)

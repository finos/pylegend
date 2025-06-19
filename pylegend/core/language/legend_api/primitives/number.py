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
    PyLegendOptional,
    TYPE_CHECKING,
)
from pylegend.core.language.legend_api.primitives.primitive import LegendApiPrimitive
from pylegend.core.language.legend_api.primitives.boolean import LegendApiBoolean
from pylegend.core.language.shared.expression import (
    PyLegendExpression,
    PyLegendExpressionNumberReturn,
)
from pylegend.core.language.shared.literal_expressions import (
    PyLegendIntegerLiteralExpression,
    PyLegendFloatLiteralExpression,
)
from pylegend.core.language.shared.operations.number_operation_expressions import (
    PyLegendNumberAddExpression,
    PyLegendNumberMultiplyExpression,
    PyLegendNumberDivideExpression,
    PyLegendNumberSubtractExpression,
    PyLegendNumberLessThanExpression,
    PyLegendNumberLessThanEqualExpression,
    PyLegendNumberGreaterThanExpression,
    PyLegendNumberGreaterThanEqualExpression,
    PyLegendNumberNegativeExpression,
    PyLegendNumberAbsoluteExpression,
    PyLegendNumberPowerExpression,
    PyLegendNumberCeilExpression,
    PyLegendNumberFloorExpression,
    PyLegendNumberSqrtExpression,
    PyLegendNumberCbrtExpression,
    PyLegendNumberExpExpression,
    PyLegendNumberLogExpression,
    PyLegendNumberRemainderExpression,
    PyLegendNumberRoundExpression,
    PyLegendNumberSineExpression,
    PyLegendNumberArcSineExpression,
    PyLegendNumberCosineExpression,
    PyLegendNumberArcCosineExpression,
    PyLegendNumberTanExpression,
    PyLegendNumberArcTanExpression,
    PyLegendNumberArcTan2Expression,
    PyLegendNumberCotExpression,
)
from pylegend.core.sql.metamodel import (
    Expression,
    QuerySpecification
)
from pylegend.core.tds.tds_frame import FrameToSqlConfig
if TYPE_CHECKING:
    from pylegend.core.language.legend_api.primitives.integer import LegendApiInteger
    from pylegend.core.language.legend_api.primitives.float import LegendApiFloat


__all__: PyLegendSequence[str] = [
    "LegendApiNumber"
]


class LegendApiNumber(LegendApiPrimitive):
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

    def to_pure_expression(self) -> str:
        return self.__value.to_pure_expression()

    def value(self) -> PyLegendExpression:
        return self.__value

    def __add__(
            self,
            other: PyLegendUnion[int, float, "LegendApiInteger", "LegendApiFloat", "LegendApiNumber"]
    ) -> "LegendApiNumber":
        LegendApiNumber.validate_param_to_be_number(other, "Number plus (+) parameter")
        other_op = LegendApiNumber.__convert_to_number_expr(other)
        return LegendApiNumber(PyLegendNumberAddExpression(self.__value, other_op))

    def __radd__(
            self,
            other: PyLegendUnion[int, float, "LegendApiInteger", "LegendApiFloat", "LegendApiNumber"]
    ) -> "LegendApiNumber":
        LegendApiNumber.validate_param_to_be_number(other, "Number plus (+) parameter")
        other_op = LegendApiNumber.__convert_to_number_expr(other)
        return LegendApiNumber(PyLegendNumberAddExpression(other_op, self.__value))

    def __mul__(
            self,
            other: PyLegendUnion[int, float, "LegendApiInteger", "LegendApiFloat", "LegendApiNumber"]
    ) -> "LegendApiNumber":
        LegendApiNumber.validate_param_to_be_number(other, "Number multiply (*) parameter")
        other_op = LegendApiNumber.__convert_to_number_expr(other)
        return LegendApiNumber(PyLegendNumberMultiplyExpression(self.__value, other_op))

    def __rmul__(
            self,
            other: PyLegendUnion[int, float, "LegendApiInteger", "LegendApiFloat", "LegendApiNumber"]
    ) -> "LegendApiNumber":
        LegendApiNumber.validate_param_to_be_number(other, "Number multiply (*) parameter")
        other_op = LegendApiNumber.__convert_to_number_expr(other)
        return LegendApiNumber(PyLegendNumberMultiplyExpression(other_op, self.__value))

    def __truediv__(
            self,
            other: PyLegendUnion[int, float, "LegendApiInteger", "LegendApiFloat", "LegendApiNumber"]
    ) -> "LegendApiNumber":
        LegendApiNumber.validate_param_to_be_number(other, "Number divide (/) parameter")
        other_op = LegendApiNumber.__convert_to_number_expr(other)
        return LegendApiNumber(PyLegendNumberDivideExpression(self.__value, other_op))

    def __rtruediv__(
            self,
            other: PyLegendUnion[int, float, "LegendApiInteger", "LegendApiFloat", "LegendApiNumber"]
    ) -> "LegendApiNumber":
        LegendApiNumber.validate_param_to_be_number(other, "Number divide (/) parameter")
        other_op = LegendApiNumber.__convert_to_number_expr(other)
        return LegendApiNumber(PyLegendNumberDivideExpression(other_op, self.__value))

    def __sub__(
            self,
            other: PyLegendUnion[int, float, "LegendApiInteger", "LegendApiFloat", "LegendApiNumber"]
    ) -> "LegendApiNumber":
        LegendApiNumber.validate_param_to_be_number(other, "Number subtract (-) parameter")
        other_op = LegendApiNumber.__convert_to_number_expr(other)
        return LegendApiNumber(PyLegendNumberSubtractExpression(self.__value, other_op))

    def __rsub__(
            self,
            other: PyLegendUnion[int, float, "LegendApiInteger", "LegendApiFloat", "LegendApiNumber"]
    ) -> "LegendApiNumber":
        LegendApiNumber.validate_param_to_be_number(other, "Number subtract (-) parameter")
        other_op = LegendApiNumber.__convert_to_number_expr(other)
        return LegendApiNumber(PyLegendNumberSubtractExpression(other_op, self.__value))

    def __lt__(
            self,
            other: PyLegendUnion[int, float, "LegendApiInteger", "LegendApiFloat", "LegendApiNumber"]
    ) -> "LegendApiBoolean":
        LegendApiNumber.validate_param_to_be_number(other, "Number less than (<) parameter")
        other_op = LegendApiNumber.__convert_to_number_expr(other)
        return LegendApiBoolean(PyLegendNumberLessThanExpression(self.__value, other_op))

    def __le__(
            self,
            other: PyLegendUnion[int, float, "LegendApiInteger", "LegendApiFloat", "LegendApiNumber"]
    ) -> "LegendApiBoolean":
        LegendApiNumber.validate_param_to_be_number(other, "Number less than equal (<=) parameter")
        other_op = LegendApiNumber.__convert_to_number_expr(other)
        return LegendApiBoolean(PyLegendNumberLessThanEqualExpression(self.__value, other_op))

    def __gt__(
            self,
            other: PyLegendUnion[int, float, "LegendApiInteger", "LegendApiFloat", "LegendApiNumber"]
    ) -> "LegendApiBoolean":
        LegendApiNumber.validate_param_to_be_number(other, "Number greater than (>) parameter")
        other_op = LegendApiNumber.__convert_to_number_expr(other)
        return LegendApiBoolean(PyLegendNumberGreaterThanExpression(self.__value, other_op))

    def __ge__(
            self,
            other: PyLegendUnion[int, float, "LegendApiInteger", "LegendApiFloat", "LegendApiNumber"]
    ) -> "LegendApiBoolean":
        LegendApiNumber.validate_param_to_be_number(other, "Number greater than equal (>=) parameter")
        other_op = LegendApiNumber.__convert_to_number_expr(other)
        return LegendApiBoolean(PyLegendNumberGreaterThanEqualExpression(self.__value, other_op))

    def __pos__(self) -> "LegendApiNumber":
        return self

    def __neg__(self) -> "LegendApiNumber":
        return LegendApiNumber(PyLegendNumberNegativeExpression(self.__value))

    def __abs__(self) -> "LegendApiNumber":
        return LegendApiNumber(PyLegendNumberAbsoluteExpression(self.__value))

    def __pow__(
            self,
            other: PyLegendUnion[int, float, "LegendApiInteger", "LegendApiFloat", "LegendApiNumber"]
    ) -> "LegendApiNumber":
        LegendApiNumber.validate_param_to_be_number(other, "Number power (**) parameter")
        other_op = LegendApiNumber.__convert_to_number_expr(other)
        return LegendApiNumber(PyLegendNumberPowerExpression(self.__value, other_op))

    def __rpow__(
            self,
            other: PyLegendUnion[int, float, "LegendApiInteger", "LegendApiFloat", "LegendApiNumber"]
    ) -> "LegendApiNumber":
        LegendApiNumber.validate_param_to_be_number(other, "Number power (**) parameter")
        other_op = LegendApiNumber.__convert_to_number_expr(other)
        return LegendApiNumber(PyLegendNumberPowerExpression(other_op, self.__value))

    def ceil(self) -> "LegendApiInteger":
        from pylegend.core.language.legend_api.primitives.integer import LegendApiInteger
        return LegendApiInteger(PyLegendNumberCeilExpression(self.__value))

    def __ceil__(self) -> "LegendApiInteger":
        return self.ceil()

    def floor(self) -> "LegendApiInteger":
        from pylegend.core.language.legend_api.primitives.integer import LegendApiInteger
        return LegendApiInteger(PyLegendNumberFloorExpression(self.__value))

    def __floor__(self) -> "LegendApiInteger":
        return self.floor()

    def sqrt(self) -> "LegendApiNumber":
        return LegendApiNumber(PyLegendNumberSqrtExpression(self.__value))

    def cbrt(self) -> "LegendApiNumber":
        return LegendApiNumber(PyLegendNumberCbrtExpression(self.__value))

    def exp(self) -> "LegendApiNumber":
        return LegendApiNumber(PyLegendNumberExpExpression(self.__value))

    def log(self) -> "LegendApiNumber":
        return LegendApiNumber(PyLegendNumberLogExpression(self.__value))

    def rem(
            self,
            other: PyLegendUnion[int, float, "LegendApiInteger", "LegendApiFloat", "LegendApiNumber"]
    ) -> "LegendApiNumber":
        LegendApiNumber.validate_param_to_be_number(other, "Number remainder (rem) parameter")
        other_op = LegendApiNumber.__convert_to_number_expr(other)
        return LegendApiNumber(PyLegendNumberRemainderExpression(self.__value, other_op))

    def sin(self) -> "LegendApiNumber":
        return LegendApiNumber(PyLegendNumberSineExpression(self.__value))

    def asin(self) -> "LegendApiNumber":
        return LegendApiNumber(PyLegendNumberArcSineExpression(self.__value))

    def cos(self) -> "LegendApiNumber":
        return LegendApiNumber(PyLegendNumberCosineExpression(self.__value))

    def acos(self) -> "LegendApiNumber":
        return LegendApiNumber(PyLegendNumberArcCosineExpression(self.__value))

    def tan(self) -> "LegendApiNumber":
        return LegendApiNumber(PyLegendNumberTanExpression(self.__value))

    def atan(self) -> "LegendApiNumber":
        return LegendApiNumber(PyLegendNumberArcTanExpression(self.__value))

    def atan2(
            self,
            other: PyLegendUnion[int, float, "LegendApiInteger", "LegendApiFloat", "LegendApiNumber"]
    ) -> "LegendApiNumber":
        LegendApiNumber.validate_param_to_be_number(other, "Number atan2 parameter")
        other_op = LegendApiNumber.__convert_to_number_expr(other)
        return LegendApiNumber(PyLegendNumberArcTan2Expression(self.__value, other_op))

    def cot(self) -> "LegendApiNumber":
        return LegendApiNumber(PyLegendNumberCotExpression(self.__value))

    def round(
            self,
            n: PyLegendOptional[int] = None
    ) -> "LegendApiNumber":
        if n is None:
            return LegendApiNumber(PyLegendNumberRoundExpression(self.__value, PyLegendIntegerLiteralExpression(0)))
        else:
            if not isinstance(n, int):
                raise TypeError("Round parameter should be an int. Passed - " + str(type(n)))
            return LegendApiNumber(PyLegendNumberRoundExpression(self.__value, PyLegendIntegerLiteralExpression(n)))

    def __round__(self, n: PyLegendOptional[int] = None) -> "LegendApiNumber":
        return self.round(n)

    @staticmethod
    def __convert_to_number_expr(
            val: PyLegendUnion[int, float, "LegendApiInteger", "LegendApiFloat", "LegendApiNumber"]
    ) -> PyLegendExpressionNumberReturn:
        if isinstance(val, int):
            return PyLegendIntegerLiteralExpression(val)
        if isinstance(val, float):
            return PyLegendFloatLiteralExpression(val)
        return val.__value

    @staticmethod
    def validate_param_to_be_number(
            param: PyLegendUnion[int, float, "LegendApiInteger", "LegendApiFloat", "LegendApiNumber"],
            desc: str
    ) -> None:
        from pylegend.core.language.legend_api.primitives.integer import LegendApiInteger
        from pylegend.core.language.legend_api.primitives.float import LegendApiFloat
        if not isinstance(param, (int, float, LegendApiInteger, LegendApiFloat, LegendApiNumber)):
            raise TypeError(desc + " should be a int/float or a int/float/number expression"
                                   " (PyLegendInteger/PyLegendFloat/PyLegendNumber)."
                                   " Got value " + str(param) + " of type: " + str(type(param)))

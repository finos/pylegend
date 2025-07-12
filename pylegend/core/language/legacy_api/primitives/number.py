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
from pylegend.core.language.legacy_api.primitives.primitive import LegacyApiPrimitive
from pylegend.core.language.legacy_api.primitives.boolean import LegacyApiBoolean
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
from pylegend.core.tds.tds_frame import FrameToPureConfig
if TYPE_CHECKING:
    from pylegend.core.language.legacy_api.primitives.integer import LegacyApiInteger
    from pylegend.core.language.legacy_api.primitives.float import LegacyApiFloat


__all__: PyLegendSequence[str] = [
    "LegacyApiNumber"
]


class LegacyApiNumber(LegacyApiPrimitive):
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

    def to_pure_expression(self, config: FrameToPureConfig) -> str:
        return self.__value.to_pure_expression(config)

    def value(self) -> PyLegendExpression:
        return self.__value

    def __add__(
            self,
            other: PyLegendUnion[int, float, "LegacyApiInteger", "LegacyApiFloat", "LegacyApiNumber"]
    ) -> "LegacyApiNumber":
        LegacyApiNumber.validate_param_to_be_number(other, "Number plus (+) parameter")
        other_op = LegacyApiNumber.__convert_to_number_expr(other)
        return LegacyApiNumber(PyLegendNumberAddExpression(self.__value, other_op))

    def __radd__(
            self,
            other: PyLegendUnion[int, float, "LegacyApiInteger", "LegacyApiFloat", "LegacyApiNumber"]
    ) -> "LegacyApiNumber":
        LegacyApiNumber.validate_param_to_be_number(other, "Number plus (+) parameter")
        other_op = LegacyApiNumber.__convert_to_number_expr(other)
        return LegacyApiNumber(PyLegendNumberAddExpression(other_op, self.__value))

    def __mul__(
            self,
            other: PyLegendUnion[int, float, "LegacyApiInteger", "LegacyApiFloat", "LegacyApiNumber"]
    ) -> "LegacyApiNumber":
        LegacyApiNumber.validate_param_to_be_number(other, "Number multiply (*) parameter")
        other_op = LegacyApiNumber.__convert_to_number_expr(other)
        return LegacyApiNumber(PyLegendNumberMultiplyExpression(self.__value, other_op))

    def __rmul__(
            self,
            other: PyLegendUnion[int, float, "LegacyApiInteger", "LegacyApiFloat", "LegacyApiNumber"]
    ) -> "LegacyApiNumber":
        LegacyApiNumber.validate_param_to_be_number(other, "Number multiply (*) parameter")
        other_op = LegacyApiNumber.__convert_to_number_expr(other)
        return LegacyApiNumber(PyLegendNumberMultiplyExpression(other_op, self.__value))

    def __truediv__(
            self,
            other: PyLegendUnion[int, float, "LegacyApiInteger", "LegacyApiFloat", "LegacyApiNumber"]
    ) -> "LegacyApiNumber":
        LegacyApiNumber.validate_param_to_be_number(other, "Number divide (/) parameter")
        other_op = LegacyApiNumber.__convert_to_number_expr(other)
        return LegacyApiNumber(PyLegendNumberDivideExpression(self.__value, other_op))

    def __rtruediv__(
            self,
            other: PyLegendUnion[int, float, "LegacyApiInteger", "LegacyApiFloat", "LegacyApiNumber"]
    ) -> "LegacyApiNumber":
        LegacyApiNumber.validate_param_to_be_number(other, "Number divide (/) parameter")
        other_op = LegacyApiNumber.__convert_to_number_expr(other)
        return LegacyApiNumber(PyLegendNumberDivideExpression(other_op, self.__value))

    def __sub__(
            self,
            other: PyLegendUnion[int, float, "LegacyApiInteger", "LegacyApiFloat", "LegacyApiNumber"]
    ) -> "LegacyApiNumber":
        LegacyApiNumber.validate_param_to_be_number(other, "Number subtract (-) parameter")
        other_op = LegacyApiNumber.__convert_to_number_expr(other)
        return LegacyApiNumber(PyLegendNumberSubtractExpression(self.__value, other_op))

    def __rsub__(
            self,
            other: PyLegendUnion[int, float, "LegacyApiInteger", "LegacyApiFloat", "LegacyApiNumber"]
    ) -> "LegacyApiNumber":
        LegacyApiNumber.validate_param_to_be_number(other, "Number subtract (-) parameter")
        other_op = LegacyApiNumber.__convert_to_number_expr(other)
        return LegacyApiNumber(PyLegendNumberSubtractExpression(other_op, self.__value))

    def __lt__(
            self,
            other: PyLegendUnion[int, float, "LegacyApiInteger", "LegacyApiFloat", "LegacyApiNumber"]
    ) -> "LegacyApiBoolean":
        LegacyApiNumber.validate_param_to_be_number(other, "Number less than (<) parameter")
        other_op = LegacyApiNumber.__convert_to_number_expr(other)
        return LegacyApiBoolean(PyLegendNumberLessThanExpression(self.__value, other_op))

    def __le__(
            self,
            other: PyLegendUnion[int, float, "LegacyApiInteger", "LegacyApiFloat", "LegacyApiNumber"]
    ) -> "LegacyApiBoolean":
        LegacyApiNumber.validate_param_to_be_number(other, "Number less than equal (<=) parameter")
        other_op = LegacyApiNumber.__convert_to_number_expr(other)
        return LegacyApiBoolean(PyLegendNumberLessThanEqualExpression(self.__value, other_op))

    def __gt__(
            self,
            other: PyLegendUnion[int, float, "LegacyApiInteger", "LegacyApiFloat", "LegacyApiNumber"]
    ) -> "LegacyApiBoolean":
        LegacyApiNumber.validate_param_to_be_number(other, "Number greater than (>) parameter")
        other_op = LegacyApiNumber.__convert_to_number_expr(other)
        return LegacyApiBoolean(PyLegendNumberGreaterThanExpression(self.__value, other_op))

    def __ge__(
            self,
            other: PyLegendUnion[int, float, "LegacyApiInteger", "LegacyApiFloat", "LegacyApiNumber"]
    ) -> "LegacyApiBoolean":
        LegacyApiNumber.validate_param_to_be_number(other, "Number greater than equal (>=) parameter")
        other_op = LegacyApiNumber.__convert_to_number_expr(other)
        return LegacyApiBoolean(PyLegendNumberGreaterThanEqualExpression(self.__value, other_op))

    def __pos__(self) -> "LegacyApiNumber":
        return self

    def __neg__(self) -> "LegacyApiNumber":
        return LegacyApiNumber(PyLegendNumberNegativeExpression(self.__value))

    def __abs__(self) -> "LegacyApiNumber":
        return LegacyApiNumber(PyLegendNumberAbsoluteExpression(self.__value))

    def __pow__(
            self,
            other: PyLegendUnion[int, float, "LegacyApiInteger", "LegacyApiFloat", "LegacyApiNumber"]
    ) -> "LegacyApiNumber":
        LegacyApiNumber.validate_param_to_be_number(other, "Number power (**) parameter")
        other_op = LegacyApiNumber.__convert_to_number_expr(other)
        return LegacyApiNumber(PyLegendNumberPowerExpression(self.__value, other_op))

    def __rpow__(
            self,
            other: PyLegendUnion[int, float, "LegacyApiInteger", "LegacyApiFloat", "LegacyApiNumber"]
    ) -> "LegacyApiNumber":
        LegacyApiNumber.validate_param_to_be_number(other, "Number power (**) parameter")
        other_op = LegacyApiNumber.__convert_to_number_expr(other)
        return LegacyApiNumber(PyLegendNumberPowerExpression(other_op, self.__value))

    def ceil(self) -> "LegacyApiInteger":
        from pylegend.core.language.legacy_api.primitives.integer import LegacyApiInteger
        return LegacyApiInteger(PyLegendNumberCeilExpression(self.__value))

    def __ceil__(self) -> "LegacyApiInteger":
        return self.ceil()

    def floor(self) -> "LegacyApiInteger":
        from pylegend.core.language.legacy_api.primitives.integer import LegacyApiInteger
        return LegacyApiInteger(PyLegendNumberFloorExpression(self.__value))

    def __floor__(self) -> "LegacyApiInteger":
        return self.floor()

    def sqrt(self) -> "LegacyApiNumber":
        return LegacyApiNumber(PyLegendNumberSqrtExpression(self.__value))

    def cbrt(self) -> "LegacyApiNumber":
        return LegacyApiNumber(PyLegendNumberCbrtExpression(self.__value))

    def exp(self) -> "LegacyApiNumber":
        return LegacyApiNumber(PyLegendNumberExpExpression(self.__value))

    def log(self) -> "LegacyApiNumber":
        return LegacyApiNumber(PyLegendNumberLogExpression(self.__value))

    def rem(
            self,
            other: PyLegendUnion[int, float, "LegacyApiInteger", "LegacyApiFloat", "LegacyApiNumber"]
    ) -> "LegacyApiNumber":
        LegacyApiNumber.validate_param_to_be_number(other, "Number remainder (rem) parameter")
        other_op = LegacyApiNumber.__convert_to_number_expr(other)
        return LegacyApiNumber(PyLegendNumberRemainderExpression(self.__value, other_op))

    def sin(self) -> "LegacyApiNumber":
        return LegacyApiNumber(PyLegendNumberSineExpression(self.__value))

    def asin(self) -> "LegacyApiNumber":
        return LegacyApiNumber(PyLegendNumberArcSineExpression(self.__value))

    def cos(self) -> "LegacyApiNumber":
        return LegacyApiNumber(PyLegendNumberCosineExpression(self.__value))

    def acos(self) -> "LegacyApiNumber":
        return LegacyApiNumber(PyLegendNumberArcCosineExpression(self.__value))

    def tan(self) -> "LegacyApiNumber":
        return LegacyApiNumber(PyLegendNumberTanExpression(self.__value))

    def atan(self) -> "LegacyApiNumber":
        return LegacyApiNumber(PyLegendNumberArcTanExpression(self.__value))

    def atan2(
            self,
            other: PyLegendUnion[int, float, "LegacyApiInteger", "LegacyApiFloat", "LegacyApiNumber"]
    ) -> "LegacyApiNumber":
        LegacyApiNumber.validate_param_to_be_number(other, "Number atan2 parameter")
        other_op = LegacyApiNumber.__convert_to_number_expr(other)
        return LegacyApiNumber(PyLegendNumberArcTan2Expression(self.__value, other_op))

    def cot(self) -> "LegacyApiNumber":
        return LegacyApiNumber(PyLegendNumberCotExpression(self.__value))

    def round(
            self,
            n: PyLegendOptional[int] = None
    ) -> "LegacyApiNumber":
        if n is None:
            return LegacyApiNumber(PyLegendNumberRoundExpression(self.__value, PyLegendIntegerLiteralExpression(0)))
        else:
            if not isinstance(n, int):
                raise TypeError("Round parameter should be an int. Passed - " + str(type(n)))
            return LegacyApiNumber(PyLegendNumberRoundExpression(self.__value, PyLegendIntegerLiteralExpression(n)))

    def __round__(self, n: PyLegendOptional[int] = None) -> "LegacyApiNumber":
        return self.round(n)

    @staticmethod
    def __convert_to_number_expr(
            val: PyLegendUnion[int, float, "LegacyApiInteger", "LegacyApiFloat", "LegacyApiNumber"]
    ) -> PyLegendExpressionNumberReturn:
        if isinstance(val, int):
            return PyLegendIntegerLiteralExpression(val)
        if isinstance(val, float):
            return PyLegendFloatLiteralExpression(val)
        return val.__value

    @staticmethod
    def validate_param_to_be_number(
            param: PyLegendUnion[int, float, "LegacyApiInteger", "LegacyApiFloat", "LegacyApiNumber"],
            desc: str
    ) -> None:
        from pylegend.core.language.legacy_api.primitives.integer import LegacyApiInteger
        from pylegend.core.language.legacy_api.primitives.float import LegacyApiFloat
        if not isinstance(param, (int, float, LegacyApiInteger, LegacyApiFloat, LegacyApiNumber)):
            raise TypeError(desc + " should be a int/float or a int/float/number expression"
                                   " (PyLegendInteger/PyLegendFloat/PyLegendNumber)."
                                   " Got value " + str(param) + " of type: " + str(type(param)))

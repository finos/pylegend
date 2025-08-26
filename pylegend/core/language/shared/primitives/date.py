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

from datetime import date, datetime
from pylegend._typing import (
    PyLegendSequence,
    PyLegendDict,
    PyLegendUnion,
    TYPE_CHECKING,
)
from pylegend.core.language.shared.primitives.primitive import PyLegendPrimitive
from pylegend.core.language.shared.primitives.integer import PyLegendInteger
from pylegend.core.language.shared.primitives.boolean import PyLegendBoolean
from pylegend.core.language.shared.expression import (
    PyLegendExpressionDateReturn,
)
from pylegend.core.language.shared.literal_expressions import (
    PyLegendDateTimeLiteralExpression,
    PyLegendStrictDateLiteralExpression,
)
from pylegend.core.language.shared.operations.date_operation_expressions import (
    PyLegendFirstDayOfYearExpression,
    PyLegendFirstDayOfQuarterExpression,
    PyLegendFirstDayOfMonthExpression,
    PyLegendFirstDayOfWeekExpression,
    PyLegendFirstHourOfDayExpression,
    PyLegendFirstMinuteOfHourExpression,
    PyLegendFirstSecondOfMinuteExpression,
    PyLegendFirstMillisecondOfSecondExpression,
    PyLegendYearExpression,
    PyLegendQuarterExpression,
    PyLegendMonthExpression,
    PyLegendWeekOfYearExpression,
    PyLegendDayOfYearExpression,
    PyLegendDayOfMonthExpression,
    PyLegendDayOfWeekExpression,
    PyLegendHourExpression,
    PyLegendMinuteExpression,
    PyLegendSecondExpression,
    PyLegendEpochExpression,
    PyLegendDatePartExpression,
    PyLegendDateLessThanExpression,
    PyLegendDateLessThanEqualExpression,
    PyLegendDateGreaterThanExpression,
    PyLegendDateGreaterThanEqualExpression,
)
from pylegend.core.sql.metamodel import (
    Expression,
    QuerySpecification
)
from pylegend.core.tds.tds_frame import FrameToSqlConfig
from pylegend.core.tds.tds_frame import FrameToPureConfig
if TYPE_CHECKING:
    from pylegend.core.language.shared.primitives.datetime import PyLegendDateTime
    from pylegend.core.language.shared.primitives.strictdate import PyLegendStrictDate


__all__: PyLegendSequence[str] = [
    "PyLegendDate"
]


class PyLegendDate(PyLegendPrimitive):
    __value: PyLegendExpressionDateReturn

    def __init__(
            self,
            value: PyLegendExpressionDateReturn
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

    def value(self) -> PyLegendExpressionDateReturn:
        return self.__value

    def first_day_of_year(self) -> "PyLegendDate":
        return PyLegendDate(PyLegendFirstDayOfYearExpression(self.__value))

    def first_day_of_quarter(self) -> "PyLegendDate":
        return PyLegendDate(PyLegendFirstDayOfQuarterExpression(self.__value))

    def first_day_of_month(self) -> "PyLegendDate":
        return PyLegendDate(PyLegendFirstDayOfMonthExpression(self.__value))

    def first_day_of_week(self) -> "PyLegendDate":
        return PyLegendDate(PyLegendFirstDayOfWeekExpression(self.__value))

    def first_hour_of_day(self) -> "PyLegendDateTime":
        from pylegend.core.language.shared.primitives.datetime import PyLegendDateTime
        return PyLegendDateTime(PyLegendFirstHourOfDayExpression(self.__value))

    def first_minute_of_hour(self) -> "PyLegendDateTime":
        from pylegend.core.language.shared.primitives.datetime import PyLegendDateTime
        return PyLegendDateTime(PyLegendFirstMinuteOfHourExpression(self.__value))

    def first_second_of_minute(self) -> "PyLegendDateTime":
        from pylegend.core.language.shared.primitives.datetime import PyLegendDateTime
        return PyLegendDateTime(PyLegendFirstSecondOfMinuteExpression(self.__value))

    def first_millisecond_of_second(self) -> "PyLegendDateTime":
        from pylegend.core.language.shared.primitives.datetime import PyLegendDateTime
        return PyLegendDateTime(PyLegendFirstMillisecondOfSecondExpression(self.__value))

    def year(self) -> "PyLegendInteger":
        return PyLegendInteger(PyLegendYearExpression(self.__value))

    def month(self) -> "PyLegendInteger":
        return PyLegendInteger(PyLegendMonthExpression(self.__value))

    def day(self) -> "PyLegendInteger":
        return PyLegendInteger(PyLegendDayOfMonthExpression(self.__value))

    def hour(self) -> "PyLegendInteger":
        return PyLegendInteger(PyLegendHourExpression(self.__value))

    def minute(self) -> "PyLegendInteger":
        return PyLegendInteger(PyLegendMinuteExpression(self.__value))

    def second(self) -> "PyLegendInteger":
        return PyLegendInteger(PyLegendSecondExpression(self.__value))

    def epoch_value(self) -> "PyLegendInteger":
        return PyLegendInteger(PyLegendEpochExpression(self.__value))

    def quarter(self) -> "PyLegendInteger":
        return PyLegendInteger(PyLegendQuarterExpression(self.__value))

    def week_of_year(self) -> "PyLegendInteger":
        return PyLegendInteger(PyLegendWeekOfYearExpression(self.__value))

    def day_of_year(self) -> "PyLegendInteger":
        return PyLegendInteger(PyLegendDayOfYearExpression(self.__value))

    def day_of_week(self) -> "PyLegendInteger":
        return PyLegendInteger(PyLegendDayOfWeekExpression(self.__value))

    def date_part(self) -> "PyLegendStrictDate":
        from pylegend.core.language.shared.primitives.strictdate import PyLegendStrictDate
        return PyLegendStrictDate(PyLegendDatePartExpression(self.__value))

    def __lt__(
            self,
            other: PyLegendUnion[date, datetime, "PyLegendStrictDate", "PyLegendDateTime", "PyLegendDate"]
    ) -> "PyLegendBoolean":
        PyLegendDate.validate_param_to_be_date(other, "Date less than (<) parameter")
        other_op = PyLegendDate.__convert_to_date_expr(other)
        return PyLegendBoolean(PyLegendDateLessThanExpression(self.__value, other_op))

    def __le__(
            self,
            other: PyLegendUnion[date, datetime, "PyLegendStrictDate", "PyLegendDateTime", "PyLegendDate"]
    ) -> "PyLegendBoolean":
        PyLegendDate.validate_param_to_be_date(other, "Date less than equal (<=) parameter")
        other_op = PyLegendDate.__convert_to_date_expr(other)
        return PyLegendBoolean(PyLegendDateLessThanEqualExpression(self.__value, other_op))

    def __gt__(
            self,
            other: PyLegendUnion[date, datetime, "PyLegendStrictDate", "PyLegendDateTime", "PyLegendDate"]
    ) -> "PyLegendBoolean":
        PyLegendDate.validate_param_to_be_date(other, "Date greater than (>) parameter")
        other_op = PyLegendDate.__convert_to_date_expr(other)
        return PyLegendBoolean(PyLegendDateGreaterThanExpression(self.__value, other_op))

    def __ge__(
            self,
            other: PyLegendUnion[date, datetime, "PyLegendStrictDate", "PyLegendDateTime", "PyLegendDate"]
    ) -> "PyLegendBoolean":
        PyLegendDate.validate_param_to_be_date(other, "Date greater than equal (>=) parameter")
        other_op = PyLegendDate.__convert_to_date_expr(other)
        return PyLegendBoolean(PyLegendDateGreaterThanEqualExpression(self.__value, other_op))

    @staticmethod
    def __convert_to_date_expr(
            val: PyLegendUnion[date, datetime, "PyLegendDateTime", "PyLegendStrictDate", "PyLegendDate"]
    ) -> PyLegendExpressionDateReturn:
        if isinstance(val, datetime):
            return PyLegendDateTimeLiteralExpression(val)
        if isinstance(val, date):
            return PyLegendStrictDateLiteralExpression(val)
        return val.__value

    @staticmethod
    def validate_param_to_be_date(
            param: PyLegendUnion[date, datetime, "PyLegendDateTime", "PyLegendStrictDate", "PyLegendDate"],
            desc: str
    ) -> None:
        from pylegend.core.language.shared.primitives.datetime import PyLegendDateTime
        from pylegend.core.language.shared.primitives.strictdate import PyLegendStrictDate
        if not isinstance(param, (date, datetime, PyLegendDateTime, PyLegendStrictDate, PyLegendDate)):
            raise TypeError(desc + " should be a datetime.date/datetime.datetime or a Date expression"
                                   " (PyLegendDateTime/PyLegendStrictDate/PyLegendDate)."
                                   " Got value " + str(param) + " of type: " + str(type(param)))

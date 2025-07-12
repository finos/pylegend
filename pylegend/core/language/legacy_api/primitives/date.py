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
from pylegend.core.language.legacy_api.primitives.primitive import LegacyApiPrimitive
from pylegend.core.language.legacy_api.primitives.integer import LegacyApiInteger
from pylegend.core.language.shared.expression import (
    PyLegendExpression,
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
)
from pylegend.core.sql.metamodel import (
    Expression,
    QuerySpecification
)
from pylegend.core.tds.tds_frame import FrameToSqlConfig
from pylegend.core.tds.tds_frame import FrameToPureConfig
if TYPE_CHECKING:
    from pylegend.core.language.legacy_api.primitives.datetime import LegacyApiDateTime
    from pylegend.core.language.legacy_api.primitives.strictdate import LegacyApiStrictDate


__all__: PyLegendSequence[str] = [
    "LegacyApiDate"
]


class LegacyApiDate(LegacyApiPrimitive):
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

    def value(self) -> PyLegendExpression:
        return self.__value

    def first_day_of_year(self) -> "LegacyApiDate":
        return LegacyApiDate(PyLegendFirstDayOfYearExpression(self.__value))

    def first_day_of_quarter(self) -> "LegacyApiDate":
        return LegacyApiDate(PyLegendFirstDayOfQuarterExpression(self.__value))

    def first_day_of_month(self) -> "LegacyApiDate":
        return LegacyApiDate(PyLegendFirstDayOfMonthExpression(self.__value))

    def first_day_of_week(self) -> "LegacyApiDate":
        return LegacyApiDate(PyLegendFirstDayOfWeekExpression(self.__value))

    def first_hour_of_day(self) -> "LegacyApiDateTime":
        from pylegend.core.language.legacy_api.primitives.datetime import LegacyApiDateTime
        return LegacyApiDateTime(PyLegendFirstHourOfDayExpression(self.__value))

    def first_minute_of_hour(self) -> "LegacyApiDateTime":
        from pylegend.core.language.legacy_api.primitives.datetime import LegacyApiDateTime
        return LegacyApiDateTime(PyLegendFirstMinuteOfHourExpression(self.__value))

    def first_second_of_minute(self) -> "LegacyApiDateTime":
        from pylegend.core.language.legacy_api.primitives.datetime import LegacyApiDateTime
        return LegacyApiDateTime(PyLegendFirstSecondOfMinuteExpression(self.__value))

    def first_millisecond_of_second(self) -> "LegacyApiDateTime":
        from pylegend.core.language.legacy_api.primitives.datetime import LegacyApiDateTime
        return LegacyApiDateTime(PyLegendFirstMillisecondOfSecondExpression(self.__value))

    def year(self) -> "LegacyApiInteger":
        return LegacyApiInteger(PyLegendYearExpression(self.__value))

    def month(self) -> "LegacyApiInteger":
        return LegacyApiInteger(PyLegendMonthExpression(self.__value))

    def day(self) -> "LegacyApiInteger":
        return LegacyApiInteger(PyLegendDayOfMonthExpression(self.__value))

    def hour(self) -> "LegacyApiInteger":
        return LegacyApiInteger(PyLegendHourExpression(self.__value))

    def minute(self) -> "LegacyApiInteger":
        return LegacyApiInteger(PyLegendMinuteExpression(self.__value))

    def second(self) -> "LegacyApiInteger":
        return LegacyApiInteger(PyLegendSecondExpression(self.__value))

    def epoch_value(self) -> "LegacyApiInteger":
        return LegacyApiInteger(PyLegendEpochExpression(self.__value))

    def quarter(self) -> "LegacyApiInteger":
        return LegacyApiInteger(PyLegendQuarterExpression(self.__value))

    def week_of_year(self) -> "LegacyApiInteger":
        return LegacyApiInteger(PyLegendWeekOfYearExpression(self.__value))

    def day_of_year(self) -> "LegacyApiInteger":
        return LegacyApiInteger(PyLegendDayOfYearExpression(self.__value))

    def day_of_week(self) -> "LegacyApiInteger":
        return LegacyApiInteger(PyLegendDayOfWeekExpression(self.__value))

    @staticmethod
    def __convert_to_date_expr(
            val: PyLegendUnion[date, datetime, "LegacyApiDateTime", "LegacyApiStrictDate", "LegacyApiDate"]
    ) -> PyLegendExpressionDateReturn:
        if isinstance(val, datetime):
            return PyLegendDateTimeLiteralExpression(val)
        if isinstance(val, date):
            return PyLegendStrictDateLiteralExpression(val)
        return val.__value

    @staticmethod
    def validate_param_to_be_date(
            param: PyLegendUnion[date, datetime, "LegacyApiDateTime", "LegacyApiStrictDate", "LegacyApiDate"],
            desc: str
    ) -> None:
        from pylegend.core.language.legacy_api.primitives.datetime import LegacyApiDateTime
        from pylegend.core.language.legacy_api.primitives.strictdate import LegacyApiStrictDate
        if not isinstance(param, (date, datetime, LegacyApiDateTime, LegacyApiStrictDate, LegacyApiDate)):
            raise TypeError(desc + " should be a datetime.date/datetime.datetime or a Date expression"
                                   " (PyLegendDateTime/PyLegendStrictDate/PyLegendDate)."
                                   " Got value " + str(param) + " of type: " + str(type(param)))

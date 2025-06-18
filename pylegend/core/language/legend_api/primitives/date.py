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
from pylegend.core.language.legend_api.primitives.primitive import LegendApiPrimitive
from pylegend.core.language.legend_api.primitives.integer import LegendApiInteger
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
if TYPE_CHECKING:
    from pylegend.core.language.legend_api.primitives.datetime import LegendApiDateTime
    from pylegend.core.language.legend_api.primitives.strictdate import LegendApiStrictDate


__all__: PyLegendSequence[str] = [
    "LegendApiDate"
]


class LegendApiDate(LegendApiPrimitive):
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

    def value(self) -> PyLegendExpression:
        return self.__value

    def first_day_of_year(self) -> "LegendApiDate":
        return LegendApiDate(PyLegendFirstDayOfYearExpression(self.__value))

    def first_day_of_quarter(self) -> "LegendApiDate":
        return LegendApiDate(PyLegendFirstDayOfQuarterExpression(self.__value))

    def first_day_of_month(self) -> "LegendApiDate":
        return LegendApiDate(PyLegendFirstDayOfMonthExpression(self.__value))

    def first_day_of_week(self) -> "LegendApiDate":
        return LegendApiDate(PyLegendFirstDayOfWeekExpression(self.__value))

    def first_hour_of_day(self) -> "LegendApiDateTime":
        from pylegend.core.language.legend_api.primitives.datetime import LegendApiDateTime
        return LegendApiDateTime(PyLegendFirstHourOfDayExpression(self.__value))

    def first_minute_of_hour(self) -> "LegendApiDateTime":
        from pylegend.core.language.legend_api.primitives.datetime import LegendApiDateTime
        return LegendApiDateTime(PyLegendFirstMinuteOfHourExpression(self.__value))

    def first_second_of_minute(self) -> "LegendApiDateTime":
        from pylegend.core.language.legend_api.primitives.datetime import LegendApiDateTime
        return LegendApiDateTime(PyLegendFirstSecondOfMinuteExpression(self.__value))

    def first_millisecond_of_second(self) -> "LegendApiDateTime":
        from pylegend.core.language.legend_api.primitives.datetime import LegendApiDateTime
        return LegendApiDateTime(PyLegendFirstMillisecondOfSecondExpression(self.__value))

    def year(self) -> "LegendApiInteger":
        return LegendApiInteger(PyLegendYearExpression(self.__value))

    def month(self) -> "LegendApiInteger":
        return LegendApiInteger(PyLegendMonthExpression(self.__value))

    def day(self) -> "LegendApiInteger":
        return LegendApiInteger(PyLegendDayOfMonthExpression(self.__value))

    def hour(self) -> "LegendApiInteger":
        return LegendApiInteger(PyLegendHourExpression(self.__value))

    def minute(self) -> "LegendApiInteger":
        return LegendApiInteger(PyLegendMinuteExpression(self.__value))

    def second(self) -> "LegendApiInteger":
        return LegendApiInteger(PyLegendSecondExpression(self.__value))

    def epoch_value(self) -> "LegendApiInteger":
        return LegendApiInteger(PyLegendEpochExpression(self.__value))

    def quarter(self) -> "LegendApiInteger":
        return LegendApiInteger(PyLegendQuarterExpression(self.__value))

    def week_of_year(self) -> "LegendApiInteger":
        return LegendApiInteger(PyLegendWeekOfYearExpression(self.__value))

    def day_of_year(self) -> "LegendApiInteger":
        return LegendApiInteger(PyLegendDayOfYearExpression(self.__value))

    def day_of_week(self) -> "LegendApiInteger":
        return LegendApiInteger(PyLegendDayOfWeekExpression(self.__value))

    @staticmethod
    def __convert_to_date_expr(
            val: PyLegendUnion[date, datetime, "LegendApiDateTime", "LegendApiStrictDate", "LegendApiDate"]
    ) -> PyLegendExpressionDateReturn:
        if isinstance(val, datetime):
            return PyLegendDateTimeLiteralExpression(val)
        if isinstance(val, date):
            return PyLegendStrictDateLiteralExpression(val)
        return val.__value

    @staticmethod
    def validate_param_to_be_date(
            param: PyLegendUnion[date, datetime, "LegendApiDateTime", "LegendApiStrictDate", "LegendApiDate"],
            desc: str
    ) -> None:
        from pylegend.core.language.legend_api.primitives.datetime import LegendApiDateTime
        from pylegend.core.language.legend_api.primitives.strictdate import LegendApiStrictDate
        if not isinstance(param, (date, datetime, LegendApiDateTime, LegendApiStrictDate, LegendApiDate)):
            raise TypeError(desc + " should be a datetime.date/datetime.datetime or a Date expression"
                                   " (PyLegendDateTime/PyLegendStrictDate/PyLegendDate)."
                                   " Got value " + str(param) + " of type: " + str(type(param)))

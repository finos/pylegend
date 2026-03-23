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
)
from pylegend.core.language import PyLegendNumber
from pylegend.core.language.shared.expression import PyLegendExpressionStringReturn
from pylegend.core.language.shared.literal_expressions import PyLegendStringLiteralExpression
from pylegend.core.language.shared.operations.number_operation_expressions import PyLegendNumberPiExpression
from pylegend.core.language.shared.primitives.strictdate import PyLegendStrictDate
from pylegend.core.language.shared.primitives.datetime import PyLegendDateTime
from pylegend.core.language.shared.primitives.string import PyLegendString
from pylegend.core.language.shared.operations.date_operation_expressions import (
    PyLegendTodayExpression,
    PyLegendNowExpression,
    PyLegendTodayDateTimeExpression,
    PyLegendMostRecentDayOfWeekExpression,
    PyLegendPreviousDayOfWeekExpression,
)
from pylegend.core.language.shared.operations.string_operation_expressions import (
    PyLegendCurrentUserExpression,
)
from pylegend.core.language.shared.primitives.float import PyLegendFloat
from pylegend.core.language.shared.operations.float_operation_expressions import PyLegendFloatPiExpression


__all__: PyLegendSequence[str] = [
    "today",
    "today_datetime",
    "now",
    "current_user",
    "pi",
    "pi_float",
    "most_recent_day_of_week",
    "previous_day_of_week",
]


def today() -> PyLegendStrictDate:
    return PyLegendStrictDate(PyLegendTodayExpression())


def today_datetime() -> PyLegendDateTime:
    return PyLegendDateTime(PyLegendTodayDateTimeExpression())


def now() -> PyLegendDateTime:
    return PyLegendDateTime(PyLegendNowExpression())


def current_user() -> PyLegendString:
    return PyLegendString(PyLegendCurrentUserExpression())


def pi() -> PyLegendNumber:
    return PyLegendNumber(PyLegendNumberPiExpression())


def pi_float() -> PyLegendFloat:
    return PyLegendFloat(PyLegendFloatPiExpression())


def _validate_day_of_week(day_of_week: str) -> PyLegendExpressionStringReturn:
    if not isinstance(day_of_week, str):
        raise TypeError(f"day_of_week must be a string, got {type(day_of_week).__name__}")
    normalized = day_of_week.strip().capitalize()
    if normalized.lower() not in ("monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"):
        raise ValueError(
            f"Invalid day of week: '{day_of_week}'. "
            f"Must be one of: monday, tuesday, wednesday, thursday, friday, saturday, sunday"
        )
    return PyLegendStringLiteralExpression(normalized)


def most_recent_day_of_week(day_of_week: str) -> PyLegendStrictDate:
    validated_day = _validate_day_of_week(day_of_week)
    return PyLegendStrictDate(PyLegendMostRecentDayOfWeekExpression(validated_day))


def previous_day_of_week(day_of_week: str) -> PyLegendStrictDate:
    validated_day = _validate_day_of_week(day_of_week)
    return PyLegendStrictDate(PyLegendPreviousDayOfWeekExpression(validated_day))

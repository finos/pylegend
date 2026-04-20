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
    PyLegendIntegerLiteralExpression,
    PyLegendStringLiteralExpression,
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
    PyLegendDateAdjustExpression,
    PyLegendDateDiffExpression,
    DurationUnit,
)
from pylegend.core.sql.metamodel import (
    Expression,
    QuerySpecification
)
from pylegend.core.tds.pandas_api.frames.helpers.series_helper import grammar_method
from pylegend.core.tds.tds_frame import FrameToSqlConfig
from pylegend.core.tds.tds_frame import FrameToPureConfig
if TYPE_CHECKING:
    from pylegend.core.language.shared.primitives.datetime import PyLegendDateTime
    from pylegend.core.language.shared.primitives.strictdate import PyLegendStrictDate


__all__: PyLegendSequence[str] = [
    "PyLegendDate"
]

"""
Date expression type used in the PyLegend DataFrame API.

``PyLegendDate`` is the abstract base for all date/time expressions.
Concrete sub-types :class:`~pylegend.core.language.shared.primitives.strictdate.PyLegendStrictDate`
(date-only) and
:class:`~pylegend.core.language.shared.primitives.datetime.PyLegendDateTime`
(date + time) inherit every method defined here.

Use these methods to extract calendar parts, compute differences, shift
dates by a duration, or compare two dates.  Columns whose Legend type is
``Date``, ``StrictDate``, or ``DateTime`` are surfaced as
``PyLegendDate`` (or the appropriate sub-type) when accessed on a
DataFrame.
"""


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

    @grammar_method
    def first_day_of_year(self) -> "PyLegendDate":
        """
        First day of the year for each date.

        Returns
        -------
        PyLegendDate
            A date set to January 1 of the same year.

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            frame["yr_start"] = frame["Shipped Date"].first_day_of_year()
            frame[["Shipped Date", "yr_start"]].head(3).to_pandas()

        """
        return PyLegendDate(PyLegendFirstDayOfYearExpression(self.__value))

    @grammar_method
    def first_day_of_quarter(self) -> "PyLegendDate":
        """
        First day of the quarter for each date.

        Returns
        -------
        PyLegendDate
            A date set to the first day of the quarter.

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            frame["qtr_start"] = frame["Shipped Date"].first_day_of_quarter()
            frame[["Shipped Date", "qtr_start"]].head(3).to_pandas()

        """
        return PyLegendDate(PyLegendFirstDayOfQuarterExpression(self.__value))

    @grammar_method
    def first_day_of_month(self) -> "PyLegendDate":
        """
        First day of the month for each date.

        Returns
        -------
        PyLegendDate
            A date set to the 1st of the same month.

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            frame["mo_start"] = frame["Shipped Date"].first_day_of_month()
            frame[["Shipped Date", "mo_start"]].head(3).to_pandas()

        """
        return PyLegendDate(PyLegendFirstDayOfMonthExpression(self.__value))

    @grammar_method
    def first_day_of_week(self) -> "PyLegendDate":
        """
        First day of the ISO week for each date.

        Returns
        -------
        PyLegendDate
            A date set to the Monday of the same ISO week.

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            frame["wk_start"] = frame["Shipped Date"].first_day_of_week()
            frame[["Shipped Date", "wk_start"]].head(3).to_pandas()

        """
        return PyLegendDate(PyLegendFirstDayOfWeekExpression(self.__value))

    @grammar_method
    def first_hour_of_day(self) -> "PyLegendDateTime":
        """
        First hour (midnight) of the day as a datetime.

        Returns
        -------
        PyLegendDateTime
            A datetime set to 00:00:00 of the same day.

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            frame["ts"] = frame["Shipped Date"].first_hour_of_day().timedelta(14, "HOURS").timedelta(35, "MINUTES").timedelta(22, "SECONDS")
            frame["midnight"] = frame["ts"].first_hour_of_day()
            frame[["ts", "midnight"]].head(3).to_pandas()

        """  # noqa: E501
        from pylegend.core.language.shared.primitives.datetime import PyLegendDateTime
        return PyLegendDateTime(PyLegendFirstHourOfDayExpression(self.__value))

    @grammar_method
    def first_minute_of_hour(self) -> "PyLegendDateTime":
        """
        First minute of the hour as a datetime.

        Returns
        -------
        PyLegendDateTime
            A datetime with minutes and seconds set to zero.

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            frame["ts"] = frame["Shipped Date"].first_hour_of_day().timedelta(14, "HOURS").timedelta(35, "MINUTES").timedelta(22, "SECONDS")
            frame["hr_start"] = frame["ts"].first_minute_of_hour()
            frame[["ts", "hr_start"]].head(3).to_pandas()

        """  # noqa: E501
        from pylegend.core.language.shared.primitives.datetime import PyLegendDateTime
        return PyLegendDateTime(PyLegendFirstMinuteOfHourExpression(self.__value))

    @grammar_method
    def first_second_of_minute(self) -> "PyLegendDateTime":
        """
        First second of the minute as a datetime.

        Returns
        -------
        PyLegendDateTime
            A datetime with seconds set to zero.

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            frame["ts"] = frame["Shipped Date"].first_hour_of_day().timedelta(14, "HOURS").timedelta(35, "MINUTES").timedelta(22, "SECONDS")
            frame["min_start"] = frame["ts"].first_second_of_minute()
            frame[["ts", "min_start"]].head(3).to_pandas()

        """  # noqa: E501
        from pylegend.core.language.shared.primitives.datetime import PyLegendDateTime
        return PyLegendDateTime(PyLegendFirstSecondOfMinuteExpression(self.__value))

    @grammar_method
    def first_millisecond_of_second(self) -> "PyLegendDateTime":
        """
        First millisecond of the second as a datetime.

        Returns
        -------
        PyLegendDateTime
            A datetime with milliseconds set to zero.

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            frame["ts"] = frame["Shipped Date"].first_hour_of_day().timedelta(14, "HOURS").timedelta(35, "MINUTES").timedelta(22, "SECONDS")
            frame["sec_start"] = frame["ts"].first_millisecond_of_second()
            frame[["ts", "sec_start"]].head(3).to_pandas()

        """  # noqa: E501
        from pylegend.core.language.shared.primitives.datetime import PyLegendDateTime
        return PyLegendDateTime(PyLegendFirstMillisecondOfSecondExpression(self.__value))

    @grammar_method
    def year(self) -> "PyLegendInteger":
        """
        Extract the year.

        Returns
        -------
        PyLegendInteger

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            frame["yr"] = frame["Shipped Date"].year()
            frame[["Shipped Date", "yr"]].head(3).to_pandas()

        """
        return PyLegendInteger(PyLegendYearExpression(self.__value))

    @grammar_method
    def month(self) -> "PyLegendInteger":
        """
        Extract the month (1–12).

        Returns
        -------
        PyLegendInteger

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            frame["mo"] = frame["Shipped Date"].month()
            frame[["Shipped Date", "mo"]].head(3).to_pandas()

        """
        return PyLegendInteger(PyLegendMonthExpression(self.__value))

    @grammar_method
    def day(self) -> "PyLegendInteger":
        """
        Extract the day of the month (1–31).

        Returns
        -------
        PyLegendInteger

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            frame["d"] = frame["Shipped Date"].day()
            frame[["Shipped Date", "d"]].head(3).to_pandas()

        """
        return PyLegendInteger(PyLegendDayOfMonthExpression(self.__value))

    @grammar_method
    def hour(self) -> "PyLegendInteger":
        """
        Extract the hour (0–23).

        Returns
        -------
        PyLegendInteger

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            frame["ts"] = frame["Shipped Date"].first_hour_of_day().timedelta(14, "HOURS").timedelta(35, "MINUTES").timedelta(22, "SECONDS")
            frame["hr"] = frame["ts"].hour()
            frame[["ts", "hr"]].head(3).to_pandas()

        """  # noqa: E501
        return PyLegendInteger(PyLegendHourExpression(self.__value))

    @grammar_method
    def minute(self) -> "PyLegendInteger":
        """
        Extract the minute (0–59).

        Returns
        -------
        PyLegendInteger

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            frame["ts"] = frame["Shipped Date"].first_hour_of_day().timedelta(14, "HOURS").timedelta(35, "MINUTES").timedelta(22, "SECONDS")
            frame["min"] = frame["ts"].minute()
            frame[["ts", "min"]].head(3).to_pandas()

        """  # noqa: E501
        return PyLegendInteger(PyLegendMinuteExpression(self.__value))

    @grammar_method
    def second(self) -> "PyLegendInteger":
        """
        Extract the second (0–59).

        Returns
        -------
        PyLegendInteger

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            frame["ts"] = frame["Shipped Date"].first_hour_of_day().timedelta(14, "HOURS").timedelta(35, "MINUTES").timedelta(22, "SECONDS")
            frame["sec"] = frame["ts"].second()
            frame[["ts", "sec"]].head(3).to_pandas()

        """  # noqa: E501
        return PyLegendInteger(PyLegendSecondExpression(self.__value))

    @grammar_method
    def epoch_value(self) -> "PyLegendInteger":
        """
        Seconds since the Unix epoch (1970-01-01 00:00:00 UTC).

        Returns
        -------
        PyLegendInteger

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            frame["epoch"] = frame["Shipped Date"].epoch_value()
            frame[["Shipped Date", "epoch"]].head(3).to_pandas()

        """
        return PyLegendInteger(PyLegendEpochExpression(self.__value))

    @grammar_method
    def quarter(self) -> "PyLegendInteger":
        """
        Extract the quarter (1–4).

        Returns
        -------
        PyLegendInteger

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            frame["qtr"] = frame["Shipped Date"].quarter()
            frame[["Shipped Date", "qtr"]].head(3).to_pandas()

        """
        return PyLegendInteger(PyLegendQuarterExpression(self.__value))

    @grammar_method
    def week_of_year(self) -> "PyLegendInteger":
        """
        Extract the ISO week number (1–53).

        Returns
        -------
        PyLegendInteger

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            frame["wk"] = frame["Shipped Date"].week_of_year()
            frame[["Shipped Date", "wk"]].head(3).to_pandas()

        """
        return PyLegendInteger(PyLegendWeekOfYearExpression(self.__value))

    @grammar_method
    def day_of_year(self) -> "PyLegendInteger":
        """
        Extract the day of the year (1–366).

        Returns
        -------
        PyLegendInteger

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            frame["doy"] = frame["Shipped Date"].day_of_year()
            frame[["Shipped Date", "doy"]].head(3).to_pandas()

        """
        return PyLegendInteger(PyLegendDayOfYearExpression(self.__value))

    @grammar_method
    def day_of_week(self) -> "PyLegendInteger":
        """
        Extract the day of the week (1 = Monday … 7 = Sunday).

        Returns
        -------
        PyLegendInteger

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            frame["dow"] = frame["Shipped Date"].day_of_week()
            frame[["Shipped Date", "dow"]].head(3).to_pandas()

        """
        return PyLegendInteger(PyLegendDayOfWeekExpression(self.__value))

    @grammar_method
    def date_part(self) -> "PyLegendStrictDate":
        """
        Extract the date portion, discarding any time component.

        Returns
        -------
        PyLegendStrictDate

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            frame["ts"] = frame["Shipped Date"].first_hour_of_day().timedelta(14, "HOURS").timedelta(35, "MINUTES")
            frame["dt"] = frame["ts"].date_part()
            frame[["ts", "dt"]].head(3).to_pandas()

        """
        from pylegend.core.language.shared.primitives.strictdate import PyLegendStrictDate
        return PyLegendStrictDate(PyLegendDatePartExpression(self.__value))

    @grammar_method
    def timedelta(self, number: PyLegendUnion[int, "PyLegendInteger"], duration_unit: str) -> "PyLegendDate":
        """
        Shift the date by a given amount.

        Parameters
        ----------
        number : int or PyLegendInteger
            How many units to shift (negative values shift backwards).
        duration_unit : str
            One of ``'YEARS'``, ``'MONTHS'``, ``'WEEKS'``, ``'DAYS'``,
            ``'HOURS'``, ``'MINUTES'``, ``'SECONDS'``, ``'MILLISECONDS'``,
            ``'MICROSECONDS'``, ``'NANOSECONDS'`` (case-insensitive).

        Returns
        -------
        PyLegendDate
            The shifted date.

        Raises
        ------
        TypeError
            If *number* is not an ``int`` or ``PyLegendInteger``.
        ValueError
            If *duration_unit* is not a recognised unit.

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            frame["plus_7d"] = frame["Shipped Date"].timedelta(7, "DAYS")
            frame[["Shipped Date", "plus_7d"]].head(3).to_pandas()

        """
        self.validate_param_to_be_int_or_int_expr(number, "timedelta number parameter")
        number_op = PyLegendIntegerLiteralExpression(number) if isinstance(number, int) else number.value()
        self.validate_duration_unit_param(duration_unit)
        duration_unit_op = PyLegendStringLiteralExpression(duration_unit.upper())
        return PyLegendDate(PyLegendDateAdjustExpression([self.__value, number_op, duration_unit_op]))

    @grammar_method
    def diff(
            self,
            other: PyLegendUnion[date, datetime, "PyLegendStrictDate", "PyLegendDateTime", "PyLegendDate"],
            duration_unit: str) -> "PyLegendInteger":
        """
        Compute the difference between two dates in the given unit.

        Parameters
        ----------
        other : date, datetime, PyLegendStrictDate, PyLegendDateTime, or PyLegendDate
            The date to subtract.
        duration_unit : str
            The unit for the result (see :meth:`timedelta` for allowed
            values).

        Returns
        -------
        PyLegendInteger
            ``self − other`` expressed in *duration_unit*.

        Raises
        ------
        TypeError
            If *other* is not a date expression or literal.
        ValueError
            If *duration_unit* is not a recognised unit.

        Examples
        --------
        .. ipython:: python

            import pylegend
            from datetime import date
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            frame["lag"] = frame["Order Date"].diff(frame["Shipped Date"], "DAYS")
            frame[["Shipped Date", "lag"]].head(3).to_pandas()

        """
        self.validate_param_to_be_date(other, "Diff parameter")
        other_op = PyLegendDate.__convert_to_date_expr(other)
        self.validate_duration_unit_param(duration_unit)
        duration_unit_op = PyLegendStringLiteralExpression(duration_unit.upper())
        return PyLegendInteger(PyLegendDateDiffExpression([self.__value, other_op, duration_unit_op]))

    @grammar_method
    def adjust(
            self,
            number: PyLegendUnion[int, "PyLegendInteger"],
            duration_unit: PyLegendUnion[str, "DurationUnit"]) -> "PyLegendDate":
        """
        Alias for :meth:`timedelta` that also accepts a ``DurationUnit`` enum.

        Parameters
        ----------
        number : int or PyLegendInteger
            How many units to shift.
        duration_unit : str or DurationUnit
            The duration unit.

        Returns
        -------
        PyLegendDate

        Examples
        --------
        .. ipython:: python

            import pylegend
            from pylegend.core.language.shared.operations.date_operation_expressions import DurationUnit
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            frame["plus_1m"] = frame["Shipped Date"].adjust(1, DurationUnit.MONTHS)
            frame[["Shipped Date", "plus_1m"]].head(3).to_pandas()

        """
        duration_unit = duration_unit.name if isinstance(duration_unit, DurationUnit) else duration_unit
        return self.timedelta(number, duration_unit)

    @grammar_method
    def date_diff(
            self,
            other: PyLegendUnion[date, datetime, "PyLegendStrictDate", "PyLegendDateTime", "PyLegendDate"],
            duration_unit: str) -> "PyLegendInteger":
        """
        Alias for :meth:`diff`.

        Parameters
        ----------
        other : date, datetime, PyLegendStrictDate, PyLegendDateTime, or PyLegendDate
            The date to subtract.
        duration_unit : str
            The unit for the result.

        Returns
        -------
        PyLegendInteger
        """
        return self.diff(other, duration_unit)

    @grammar_method
    def monthNumber(self) -> "PyLegendInteger":
        """Alias for :meth:`month`."""
        return self.month()

    @grammar_method
    def quarterNumber(self) -> "PyLegendInteger":
        """Alias for :meth:`quarter`."""
        return self.quarter()

    @grammar_method
    def dayOfWeekNumber(self) -> "PyLegendInteger":
        """Alias for :meth:`day_of_week`."""
        return self.day_of_week()

    @grammar_method
    def dayOfMonth(self) -> "PyLegendInteger":
        """Alias for :meth:`day`."""
        return self.day()

    @grammar_method
    def weekOfYear(self) -> "PyLegendInteger":
        """Alias for :meth:`week_of_year`."""
        return self.week_of_year()

    @grammar_method
    def datePart(self) -> "PyLegendStrictDate":
        """Alias for :meth:`date_part`."""
        return self.date_part()

    @grammar_method
    def firstDayOfYear(self) -> "PyLegendDate":
        """Alias for :meth:`first_day_of_year`."""
        return self.first_day_of_year()

    @grammar_method
    def firstDayOfQuarter(self) -> "PyLegendDate":
        """Alias for :meth:`first_day_of_quarter`."""
        return self.first_day_of_quarter()

    @grammar_method
    def firstDayOfMonth(self) -> "PyLegendDate":
        """Alias for :meth:`first_day_of_month`."""
        return self.first_day_of_month()

    @grammar_method
    def firstDayOfWeek(self) -> "PyLegendDate":
        """Alias for :meth:`first_day_of_week`."""
        return self.first_day_of_week()

    @grammar_method
    def __lt__(
            self,
            other: PyLegendUnion[date, datetime, "PyLegendStrictDate", "PyLegendDateTime", "PyLegendDate"]
    ) -> "PyLegendBoolean":
        """
        Date less than (``<``).

        Parameters
        ----------
        other : date, datetime, PyLegendStrictDate, PyLegendDateTime, or PyLegendDate
            The right-hand operand.

        Returns
        -------
        PyLegendBoolean

        Examples
        --------
        .. ipython:: python

            import pylegend
            from datetime import date
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            frame[frame["Shipped Date"] < date(1996, 8, 1)].head(3).to_pandas()

        """
        PyLegendDate.validate_param_to_be_date(other, "Date less than (<) parameter")
        other_op = PyLegendDate.__convert_to_date_expr(other)
        return PyLegendBoolean(PyLegendDateLessThanExpression(self.__value, other_op))

    @grammar_method
    def __le__(
            self,
            other: PyLegendUnion[date, datetime, "PyLegendStrictDate", "PyLegendDateTime", "PyLegendDate"]
    ) -> "PyLegendBoolean":
        """
        Date less than or equal (``<=``).

        Parameters
        ----------
        other : date, datetime, PyLegendStrictDate, PyLegendDateTime, or PyLegendDate
            The right-hand operand.

        Returns
        -------
        PyLegendBoolean

        Examples
        --------
        .. ipython:: python

            import pylegend
            from datetime import date
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            frame[frame["Shipped Date"] <= date(1996, 8, 1)].head(3).to_pandas()

        """
        PyLegendDate.validate_param_to_be_date(other, "Date less than equal (<=) parameter")
        other_op = PyLegendDate.__convert_to_date_expr(other)
        return PyLegendBoolean(PyLegendDateLessThanEqualExpression(self.__value, other_op))

    @grammar_method
    def __gt__(
            self,
            other: PyLegendUnion[date, datetime, "PyLegendStrictDate", "PyLegendDateTime", "PyLegendDate"]
    ) -> "PyLegendBoolean":
        """
        Date greater than (``>``).

        Parameters
        ----------
        other : date, datetime, PyLegendStrictDate, PyLegendDateTime, or PyLegendDate
            The right-hand operand.

        Returns
        -------
        PyLegendBoolean

        Examples
        --------
        .. ipython:: python

            import pylegend
            from datetime import date
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            frame[frame["Shipped Date"] > date(1998, 4, 1)].head(3).to_pandas()

        """
        PyLegendDate.validate_param_to_be_date(other, "Date greater than (>) parameter")
        other_op = PyLegendDate.__convert_to_date_expr(other)
        return PyLegendBoolean(PyLegendDateGreaterThanExpression(self.__value, other_op))

    @grammar_method
    def __ge__(
            self,
            other: PyLegendUnion[date, datetime, "PyLegendStrictDate", "PyLegendDateTime", "PyLegendDate"]
    ) -> "PyLegendBoolean":
        """
        Date greater than or equal (``>=``).

        Parameters
        ----------
        other : date, datetime, PyLegendStrictDate, PyLegendDateTime, or PyLegendDate
            The right-hand operand.

        Returns
        -------
        PyLegendBoolean

        Examples
        --------
        .. ipython:: python

            import pylegend
            from datetime import date
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            frame[frame["Shipped Date"] >= date(1998, 4, 1)].head(3).to_pandas()

        """
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

    @staticmethod
    def validate_duration_unit_param(duration_unit: str) -> None:
        if duration_unit.lower() not in ('years', 'months', 'weeks', 'days', 'hours', 'minutes', 'seconds',
                                         'milliseconds', 'microseconds', 'nanoseconds'):
            raise ValueError(
                f"Unknown duration unit - {duration_unit}. Supported values are - YEARS, MONTHS, WEEKS, DAYS, HOURS,"
                " MINUTES, SECONDS, MILLISECONDS, MICROSECONDS, NANOSECONDS"
            )

    @staticmethod
    def validate_param_to_be_int_or_int_expr(
            param: PyLegendUnion[int, "PyLegendInteger"],
            desc: str
    ) -> None:
        if not isinstance(param, (int, PyLegendInteger)):
            raise TypeError(desc + " should be a int or an integer expression (PyLegendInteger)."
                                   " Got value " + str(param) + " of type: " + str(type(param)))

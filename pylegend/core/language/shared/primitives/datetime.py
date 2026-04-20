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

from datetime import datetime
from pylegend._typing import (
    PyLegendSequence,
    PyLegendDict,
    PyLegendUnion,
)
from pylegend.core.language.shared.operations.date_operation_expressions import PyLegendDateTimeBucketExpression
from pylegend.core.language.shared.primitives.integer import PyLegendInteger
from pylegend.core.language.shared.primitives.date import PyLegendDate
from pylegend.core.language.shared.expression import (
    PyLegendExpressionDateTimeReturn,
)
from pylegend.core.language.shared.literal_expressions import (
    PyLegendDateTimeLiteralExpression,
    PyLegendIntegerLiteralExpression,
    PyLegendStringLiteralExpression,
)
from pylegend.core.sql.metamodel import (
    Expression,
    QuerySpecification
)
from pylegend.core.tds.pandas_api.frames.helpers.series_helper import grammar_method
from pylegend.core.tds.tds_frame import FrameToSqlConfig


__all__: PyLegendSequence[str] = [
    "PyLegendDateTime"
]

"""
DateTime expression type in the PyLegend expression language.

``PyLegendDateTime`` represents a date-plus-time value within a PyLegend
query.  It inherits every calendar-extraction, comparison, shifting, and
differencing method from
:class:`~pylegend.core.language.shared.primitives.date.PyLegendDate` and
adds ``time_bucket`` for grouping timestamps into fixed-width buckets.

Instances are produced when a column whose Legend type is ``DateTime`` is
accessed on a TDS frame, or by calling time-related methods such as
:meth:`first_hour_of_day` on a ``PyLegendDate`` expression.
"""


class PyLegendDateTime(PyLegendDate):
    __value: PyLegendExpressionDateTimeReturn

    def __init__(
            self,
            value: PyLegendExpressionDateTimeReturn
    ) -> None:
        super().__init__(value)
        self.__value = value

    def to_sql_expression(
            self,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        return self.__value.to_sql_expression(frame_name_to_base_query_map, config)

    def value(self) -> PyLegendExpressionDateTimeReturn:
        return self.__value

    @grammar_method
    def time_bucket(
            self,
            quantity: PyLegendUnion[int, "PyLegendInteger"],
            duration_unit: str) -> "PyLegendDate":
        """
        Group the timestamp into fixed-width buckets.

        Returns the start of the bucket that each timestamp falls into.
        Useful for time-series aggregation at a coarser granularity.
        Unlike :meth:`~pylegend.core.language.shared.primitives.strictdate.PyLegendStrictDate.time_bucket`,
        this variant accepts all duration units supported by
        ``PyLegendDate`` (including hours, minutes, seconds, etc.).

        Parameters
        ----------
        quantity : int or PyLegendInteger
            The width of each bucket expressed in *duration_unit*.
        duration_unit : str
            One of ``'YEARS'``, ``'MONTHS'``, ``'WEEKS'``, ``'DAYS'``,
            ``'HOURS'``, ``'MINUTES'``, ``'SECONDS'``, ``'MILLISECONDS'``,
            ``'MICROSECONDS'``, ``'NANOSECONDS'`` (case-insensitive).

        Returns
        -------
        PyLegendDate
            The bucket-start date/time.

        Raises
        ------
        TypeError
            If *quantity* is not an ``int`` or ``PyLegendInteger``.
        ValueError
            If *duration_unit* is not a recognised unit.

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            frame["bucket"] = frame["Shipped Date"].first_hour_of_day().time_bucket(6, "HOURS")
            frame[["Shipped Date", "bucket"]].head(3).to_pandas()

        """
        self.validate_param_to_be_int_or_int_expr(quantity, "time bucket quantity parameter")
        quantity_op = PyLegendIntegerLiteralExpression(quantity) if isinstance(quantity, int) else quantity.value()
        self.validate_duration_unit_param(duration_unit)
        duration_unit_op = PyLegendStringLiteralExpression(duration_unit.upper())
        return PyLegendDate(PyLegendDateTimeBucketExpression([
            self.__value,
            quantity_op,
            duration_unit_op,
            PyLegendStringLiteralExpression("DATETIME")]))

    @staticmethod
    def __convert_to_datetime_expr(
            val: PyLegendUnion[datetime, "PyLegendDateTime"]
    ) -> PyLegendExpressionDateTimeReturn:
        if isinstance(val, datetime):
            return PyLegendDateTimeLiteralExpression(val)
        return val.__value

    @staticmethod
    def validate_param_to_be_datetime(
            param: PyLegendUnion[datetime, "PyLegendDateTime"],
            desc: str
    ) -> None:
        if not isinstance(param, (datetime, PyLegendDateTime)):
            raise TypeError(desc + " should be a datetime.datetime or a DateTime expression (PyLegendDateTime)."
                                   " Got value " + str(param) + " of type: " + str(type(param)))

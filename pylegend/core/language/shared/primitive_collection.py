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

from abc import ABCMeta
from decimal import Decimal as PythonDecimal
from datetime import date, datetime
from pylegend._typing import (
    PyLegendSequence,
    PyLegendUnion,
)
from pylegend.core.language.shared.primitives.primitive import PyLegendPrimitiveOrPythonPrimitive
from pylegend.core.language import (
    PyLegendInteger,
    PyLegendFloat,
    PyLegendNumber,
    PyLegendString,
    PyLegendBoolean,
    PyLegendDate,
    PyLegendDateTime,
    PyLegendStrictDate,
    PyLegendDecimal,
    convert_literal_to_literal_expression,
)
from pylegend.core.language.shared.operations.collection_operation_expressions import (
    PyLegendCountExpression,
    PyLegendDistinctCountExpression,
    PyLegendAverageExpression,
    PyLegendIntegerMaxExpression,
    PyLegendIntegerMinExpression,
    PyLegendIntegerSumExpression,
    PyLegendFloatMaxExpression,
    PyLegendFloatMinExpression,
    PyLegendFloatSumExpression,
    PyLegendNumberMaxExpression,
    PyLegendNumberMinExpression,
    PyLegendNumberSumExpression,
    PyLegendStdDevSampleExpression,
    PyLegendStdDevPopulationExpression,
    PyLegendVarianceSampleExpression,
    PyLegendVariancePopulationExpression,
    PyLegendDecimalMaxExpression,
    PyLegendDecimalMinExpression,
    PyLegendDecimalSumExpression,
    PyLegendDecimalUniqueValueOnlyExpression,
    PyLegendStringMaxExpression,
    PyLegendStringMinExpression,
    PyLegendJoinStringsExpression,
    PyLegendStrictDateMaxExpression,
    PyLegendStrictDateMinExpression,
    PyLegendDateMaxExpression,
    PyLegendDateMinExpression,
    PyLegendIntegerUniqueValueOnlyExpression,
    PyLegendFloatUniqueValueOnlyExpression,
    PyLegendNumberUniqueValueOnlyExpression,
    PyLegendStringUniqueValueOnlyExpression,
    PyLegendStrictDateUniqueValueOnlyExpression,
    PyLegendDateUniqueValueOnlyExpression,
    PyLegendDateTimeUniqueValueOnlyExpression,
    PyLegendBooleanUniqueValueOnlyExpression,
    PyLegendCorrExpression,
    PyLegendCovarPopulationExpression,
    PyLegendCovarSampleExpression,
    PyLegendWavgExpression,
    PyLegendMaxByExpression,
    PyLegendMinByExpression,
    PyLegendMedianExpression,
    PyLegendModeExpression,
    PyLegendPercentileContExpression,
    PyLegendPercentileDiscExpression,
)


__all__: PyLegendSequence[str] = [
    "PyLegendPrimitiveCollection",
    "PyLegendIntegerCollection",
    "PyLegendFloatCollection",
    "PyLegendNumberCollection",
    "PyLegendDecimalCollection",
    "PyLegendStringCollection",
    "PyLegendBooleanCollection",
    "PyLegendDateCollection",
    "PyLegendDateTimeCollection",
    "PyLegendStrictDateCollection",
    "PyLegendNumberPairCollection",
    "create_primitive_collection",
]


class PyLegendPrimitiveCollection(metaclass=ABCMeta):
    """
    Abstract base for all primitive collection types.

    A *collection* wraps a single primitive expression and exposes
    aggregate operations (``count``, ``distinct_count``) that are
    evaluated over the group of rows produced by a ``groupby`` or
    ``window`` operation.  Concrete sub-classes add type-specific
    aggregations such as ``sum``, ``max``, ``min``, ``join``, etc.

    Users never instantiate collection objects directly — they are
    created internally by the framework when an aggregation function is
    applied to a column inside ``groupby().agg()``.
    """
    __nested: PyLegendPrimitiveOrPythonPrimitive

    def __init__(self, nested: PyLegendPrimitiveOrPythonPrimitive) -> None:
        self.__nested = nested

    def count(self) -> "PyLegendInteger":
        """
        Count the number of rows in the group.

        Returns
        -------
        PyLegendInteger
            The row count for each group.

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            frame.groupby("Ship Name")["Order Id"].aggregate(
                lambda x: x.count()
            ).to_pandas().head(3)

        """
        if isinstance(self.__nested, (bool, int, float, str, date, datetime, PythonDecimal)):
            nested_expr = convert_literal_to_literal_expression(self.__nested)
        else:
            nested_expr = self.__nested.value()
        return PyLegendInteger(PyLegendCountExpression(nested_expr))

    def distinct_count(self) -> "PyLegendInteger":
        """
        Count the number of distinct values in the group.

        Returns
        -------
        PyLegendInteger
            The distinct value count for each group.

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            frame.groupby("Ship Name")["Order Id"].aggregate(
                lambda x: x.distinct_count()
            ).to_pandas().head(3)

        """
        if isinstance(self.__nested, (bool, int, float, str, date, datetime, PythonDecimal)):
            nested_expr = convert_literal_to_literal_expression(self.__nested)
        else:
            nested_expr = self.__nested.value()
        return PyLegendInteger(PyLegendDistinctCountExpression(nested_expr))


class PyLegendNumberCollection(PyLegendPrimitiveCollection):
    """
    Collection type for generic numeric expressions.

    ``PyLegendNumberCollection`` provides aggregate operations for
    numeric columns: ``sum``, ``min``, ``max``, ``average`` / ``mean``,
    standard deviation, variance, ``median``, ``mode``, ``percentile``,
    and ``distinct_value``.  It also supports ``row_mapper`` for
    creating paired collections used in correlation and covariance
    calculations.

    Inherits ``count`` and ``distinct_count`` from
    :class:`PyLegendPrimitiveCollection`.
    """
    __nested: PyLegendUnion[int, float, PythonDecimal, PyLegendInteger, PyLegendFloat, PyLegendNumber, PyLegendDecimal]

    def __init__(
            self,
            nested: PyLegendUnion[int, float, PythonDecimal, PyLegendInteger, PyLegendFloat, PyLegendNumber, PyLegendDecimal]
    ) -> None:
        super().__init__(nested)
        self.__nested = nested

    def average(self) -> "PyLegendFloat":
        """
        Arithmetic mean of the values in the group.

        Returns
        -------
        PyLegendFloat

        See Also
        --------
        mean : Alias for ``average``.

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            frame.groupby("Ship Name")["Order Id"].aggregate(
                lambda x: x.average()
            ).to_pandas().head(3)

        """
        nested_expr = (
            convert_literal_to_literal_expression(self.__nested) if isinstance(self.__nested, (int, float, PythonDecimal))
            else self.__nested.value()
        )
        return PyLegendFloat(PyLegendAverageExpression(nested_expr))  # type: ignore

    def mean(self) -> "PyLegendFloat":
        """Alias for :meth:`average`."""
        return self.average()  # pragma: no cover

    def max(self) -> "PyLegendNumber":
        """
        Maximum value in the group.

        Returns
        -------
        PyLegendNumber

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            frame.groupby("Ship Name")["Order Id"].aggregate(
                lambda x: x.max()
            ).to_pandas().head(3)

        """
        nested_expr = (
            convert_literal_to_literal_expression(self.__nested) if isinstance(self.__nested, (int, float, PythonDecimal))
            else self.__nested.value()
        )
        return PyLegendNumber(PyLegendNumberMaxExpression(nested_expr))  # type: ignore

    def min(self) -> "PyLegendNumber":
        """
        Minimum value in the group.

        Returns
        -------
        PyLegendNumber

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            frame.groupby("Ship Name")["Order Id"].aggregate(
                lambda x: x.min()
            ).to_pandas().head(3)

        """
        nested_expr = (
            convert_literal_to_literal_expression(self.__nested) if isinstance(self.__nested, (int, float, PythonDecimal))
            else self.__nested.value()
        )
        return PyLegendNumber(PyLegendNumberMinExpression(nested_expr))  # type: ignore

    def sum(self) -> "PyLegendNumber":
        """
        Sum of the values in the group.

        Returns
        -------
        PyLegendNumber

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            frame.groupby("Ship Name")["Order Id"].aggregate(
                lambda x: x.sum()
            ).to_pandas().head(3)

        """
        nested_expr = (
            convert_literal_to_literal_expression(self.__nested) if isinstance(self.__nested, (int, float, PythonDecimal))
            else self.__nested.value()
        )
        return PyLegendNumber(PyLegendNumberSumExpression(nested_expr))  # type: ignore

    def std_dev_sample(self) -> "PyLegendNumber":
        """
        Sample standard deviation of the values in the group.

        Returns
        -------
        PyLegendNumber

        See Also
        --------
        std_dev : Alias for ``std_dev_sample``.
        std_dev_population : Population standard deviation.

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            frame.groupby("Ship Name")["Order Id"].aggregate(
                lambda x: x.std_dev_sample()
            ).to_pandas().head(3)

        """
        nested_expr = (
            convert_literal_to_literal_expression(self.__nested) if isinstance(self.__nested, (int, float, PythonDecimal))
            else self.__nested.value()
        )
        return PyLegendNumber(PyLegendStdDevSampleExpression(nested_expr))  # type: ignore

    def std_dev(self) -> "PyLegendNumber":
        """Alias for :meth:`std_dev_sample`."""
        return self.std_dev_sample()

    def std_dev_population(self) -> "PyLegendNumber":
        """
        Population standard deviation of the values in the group.

        Returns
        -------
        PyLegendNumber

        See Also
        --------
        std_dev_sample : Sample standard deviation.

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            frame.groupby("Ship Name")["Order Id"].aggregate(
                lambda x: x.std_dev_population()
            ).to_pandas().head(3)

        """
        nested_expr = (
            convert_literal_to_literal_expression(self.__nested) if isinstance(self.__nested, (int, float, PythonDecimal))
            else self.__nested.value()
        )
        return PyLegendNumber(PyLegendStdDevPopulationExpression(nested_expr))  # type: ignore

    def variance_sample(self) -> "PyLegendNumber":
        """
        Sample variance of the values in the group.

        Returns
        -------
        PyLegendNumber

        See Also
        --------
        variance : Alias for ``variance_sample``.
        variance_population : Population variance.

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            frame.groupby("Ship Name")["Order Id"].aggregate(
                lambda x: x.variance_sample()
            ).to_pandas().head(3)

        """
        nested_expr = (
            convert_literal_to_literal_expression(self.__nested) if isinstance(self.__nested, (int, float, PythonDecimal))
            else self.__nested.value()
        )
        return PyLegendNumber(PyLegendVarianceSampleExpression(nested_expr))  # type: ignore

    def variance(self) -> "PyLegendNumber":
        """Alias for :meth:`variance_sample`."""
        return self.variance_sample()

    def variance_population(self) -> "PyLegendNumber":
        """
        Population variance of the values in the group.

        Returns
        -------
        PyLegendNumber

        See Also
        --------
        variance_sample : Sample variance.

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            frame.groupby("Ship Name")["Order Id"].aggregate(
                lambda x: x.variance_population()
            ).to_pandas().head(3)

        """
        nested_expr = (
            convert_literal_to_literal_expression(self.__nested) if isinstance(self.__nested, (int, float, PythonDecimal))
            else self.__nested.value()
        )
        return PyLegendNumber(PyLegendVariancePopulationExpression(nested_expr))  # type: ignore

    def distinct_value(self) -> "PyLegendNumber":
        """
        Return the single distinct value in the group.

        Raises an error at query time if the group contains more than
        one distinct value.

        Returns
        -------
        PyLegendNumber

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            frame.groupby("Order Id")["Order Id"].aggregate(
                lambda x: x.distinct_value()
            ).to_pandas().head(3)

        """
        nested_expr = (
            convert_literal_to_literal_expression(self.__nested) if isinstance(self.__nested, (int, float, PythonDecimal))
            else self.__nested.value()
        )
        return PyLegendNumber(PyLegendNumberUniqueValueOnlyExpression(nested_expr))  # type: ignore

    def median(self) -> "PyLegendNumber":
        """
        Median of the values in the group.

        Returns
        -------
        PyLegendNumber

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            frame.groupby("Ship Name")["Order Id"].aggregate(
                lambda x: x.median()
            ).to_pandas().head(3)

        """
        nested_expr = (
            convert_literal_to_literal_expression(self.__nested) if isinstance(self.__nested, (int, float, PythonDecimal))
            else self.__nested.value()
        )
        return PyLegendNumber(PyLegendMedianExpression(nested_expr))  # type: ignore

    def mode(self) -> "PyLegendNumber":
        """
        Mode (most frequent value) of the values in the group.

        Returns
        -------
        PyLegendNumber

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            frame.groupby("Ship Name")["Order Id"].aggregate(
                lambda x: x.mode()
            ).to_pandas().head(3)

        """
        nested_expr = (
            convert_literal_to_literal_expression(self.__nested) if isinstance(self.__nested, (int, float, PythonDecimal))
            else self.__nested.value()
        )
        return PyLegendNumber(PyLegendModeExpression(nested_expr))  # type: ignore

    def percentile(
            self,
            percentile: float,
            ascending: bool = True,
            continuous: bool = True,
    ) -> "PyLegendNumber":
        """
        Compute a percentile of the values in the group.

        Parameters
        ----------
        percentile : float
            The percentile to compute, between 0 and 1.
        ascending : bool, default True
            If ``True``, values are sorted in ascending order before
            computing the percentile.
        continuous : bool, default True
            If ``True``, use continuous (interpolated) percentile
            (``PERCENTILE_CONT``).  If ``False``, use discrete
            (``PERCENTILE_DISC``).

        Returns
        -------
        PyLegendNumber

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            frame.groupby("Ship Name")["Order Id"].aggregate(
                lambda x: x.percentile(0.5)
            ).to_pandas().head(3)

        """
        nested_expr = (
            convert_literal_to_literal_expression(self.__nested) if isinstance(self.__nested, (int, float, PythonDecimal))
            else self.__nested.value()
        )
        if continuous:
            return PyLegendNumber(
                PyLegendPercentileContExpression(nested_expr, percentile, ascending)  # type: ignore
            )
        else:
            return PyLegendNumber(
                PyLegendPercentileDiscExpression(nested_expr, percentile, ascending)  # type: ignore
            )

    def row_mapper(
        self,
        other: PyLegendUnion[
            int, float, PyLegendInteger, PyLegendFloat, PyLegendDecimal, PyLegendNumber, "PyLegendNumberCollection"
        ],
    ) -> "PyLegendNumberPairCollection":
        """
        Pair this collection with another numeric value for bivariate
        aggregations (correlation, covariance, weighted average).

        Parameters
        ----------
        other : int, float, PyLegendInteger, PyLegendFloat, PyLegendDecimal, PyLegendNumber, or PyLegendNumberCollection
            The second numeric operand.

        Returns
        -------
        PyLegendNumberPairCollection
            A paired collection supporting ``corr``, ``covar_sample``,
            ``covar_population``, and ``wavg_legend_ext``.
        """
        return PyLegendNumberPairCollection(self, other)


class PyLegendIntegerCollection(PyLegendNumberCollection):
    """
    Collection type for integer expressions.

    Overrides ``max``, ``min``, ``sum``, and ``distinct_value`` to
    return ``PyLegendInteger`` instead of ``PyLegendNumber``.  All
    other aggregation methods are inherited from
    :class:`PyLegendNumberCollection`.
    """
    __nested: PyLegendUnion[int, PyLegendInteger]

    def __init__(self, nested: PyLegendUnion[int, PyLegendInteger]) -> None:
        super().__init__(nested)
        self.__nested = nested

    def max(self) -> "PyLegendInteger":
        """
        Maximum integer value in the group.

        Returns
        -------
        PyLegendInteger

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            frame.groupby("Ship Name")["Order Id"].aggregate(
                lambda x: x.max()
            ).to_pandas().head(3)

        """
        nested_expr = (
            convert_literal_to_literal_expression(self.__nested) if isinstance(self.__nested, int)
            else self.__nested.value()
        )
        return PyLegendInteger(PyLegendIntegerMaxExpression(nested_expr))  # type: ignore

    def min(self) -> "PyLegendInteger":
        """
        Minimum integer value in the group.

        Returns
        -------
        PyLegendInteger

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            frame.groupby("Ship Name")["Order Id"].aggregate(
                lambda x: x.min()
            ).to_pandas().head(3)

        """
        nested_expr = (
            convert_literal_to_literal_expression(self.__nested) if isinstance(self.__nested, int)
            else self.__nested.value()
        )
        return PyLegendInteger(PyLegendIntegerMinExpression(nested_expr))  # type: ignore

    def sum(self) -> "PyLegendInteger":
        """
        Sum of the integer values in the group.

        Returns
        -------
        PyLegendInteger

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            frame.groupby("Ship Name")["Order Id"].aggregate(
                lambda x: x.sum()
            ).to_pandas().head(3)

        """
        nested_expr = (
            convert_literal_to_literal_expression(self.__nested) if isinstance(self.__nested, int)
            else self.__nested.value()
        )
        return PyLegendInteger(PyLegendIntegerSumExpression(nested_expr))  # type: ignore

    def distinct_value(self) -> "PyLegendInteger":
        """
        Return the single distinct integer value in the group.

        Returns
        -------
        PyLegendInteger

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            frame.groupby("Order Id")["Order Id"].aggregate(
                lambda x: x.distinct_value()
            ).to_pandas().head(3)

        """
        nested_expr = (
            convert_literal_to_literal_expression(self.__nested) if isinstance(self.__nested, int)
            else self.__nested.value()
        )
        return PyLegendInteger(PyLegendIntegerUniqueValueOnlyExpression(nested_expr))  # type: ignore[arg-type]


class PyLegendFloatCollection(PyLegendNumberCollection):
    """
    Collection type for float expressions.

    Overrides ``max``, ``min``, ``sum``, and ``distinct_value`` to
    return ``PyLegendFloat`` instead of ``PyLegendNumber``.  All other
    aggregation methods are inherited from
    :class:`PyLegendNumberCollection`.
    """
    __nested: PyLegendUnion[float, PyLegendFloat]

    def __init__(self, nested: PyLegendUnion[float, PyLegendFloat]) -> None:
        super().__init__(nested)
        self.__nested = nested

    def max(self) -> "PyLegendFloat":
        """
        Maximum float value in the group.

        Returns
        -------
        PyLegendFloat

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            frame["id_float"] = frame["Order Id"] * 1.0
            frame.groupby("Ship Name")["id_float"].aggregate(
                lambda x: x.max()
            ).to_pandas().head(3)

        """
        nested_expr = (
            convert_literal_to_literal_expression(self.__nested) if isinstance(self.__nested, float)
            else self.__nested.value()
        )
        return PyLegendFloat(PyLegendFloatMaxExpression(nested_expr))  # type: ignore

    def min(self) -> "PyLegendFloat":
        """
        Minimum float value in the group.

        Returns
        -------
        PyLegendFloat

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            frame["id_float"] = frame["Order Id"] * 1.0
            frame.groupby("Ship Name")["id_float"].aggregate(
                lambda x: x.min()
            ).to_pandas().head(3)

        """
        nested_expr = (
            convert_literal_to_literal_expression(self.__nested) if isinstance(self.__nested, float)
            else self.__nested.value()
        )
        return PyLegendFloat(PyLegendFloatMinExpression(nested_expr))  # type: ignore

    def sum(self) -> "PyLegendFloat":
        """
        Sum of the float values in the group.

        Returns
        -------
        PyLegendFloat

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            frame["id_float"] = frame["Order Id"] * 1.0
            frame.groupby("Ship Name")["id_float"].aggregate(
                lambda x: x.sum()
            ).to_pandas().head(3)

        """
        nested_expr = (
            convert_literal_to_literal_expression(self.__nested) if isinstance(self.__nested, float)
            else self.__nested.value()
        )
        return PyLegendFloat(PyLegendFloatSumExpression(nested_expr))  # type: ignore

    def distinct_value(self) -> "PyLegendFloat":
        """
        Return the single distinct float value in the group.

        Returns
        -------
        PyLegendFloat

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            frame["id_float"] = frame["Order Id"] * 1.0
            frame.groupby("Order Id")["id_float"].aggregate(
                lambda x: x.distinct_value()
            ).to_pandas().head(3)

        """
        nested_expr = (
            convert_literal_to_literal_expression(self.__nested) if isinstance(self.__nested, float)
            else self.__nested.value()
        )
        return PyLegendFloat(PyLegendFloatUniqueValueOnlyExpression(nested_expr))  # type: ignore[arg-type]


class PyLegendDecimalCollection(PyLegendNumberCollection):
    """
    Collection type for decimal expressions.

    Overrides ``max``, ``min``, ``sum``, and ``distinct_value`` to
    return ``PyLegendDecimal`` instead of ``PyLegendNumber``.  All
    other aggregation methods are inherited from
    :class:`PyLegendNumberCollection`.
    """
    __nested: PyLegendUnion[PythonDecimal, PyLegendDecimal]

    def __init__(self, nested: PyLegendUnion[PythonDecimal, PyLegendDecimal]) -> None:
        super().__init__(nested)
        self.__nested = nested

    def max(self) -> "PyLegendDecimal":
        """
        Maximum decimal value in the group.

        Returns
        -------
        PyLegendDecimal

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            frame["id_dec"] = frame["Order Id"].to_string().parse_decimal()
            frame.groupby("Ship Name")["id_dec"].aggregate(
                lambda x: x.max()
            ).to_pandas().head(3)

        """
        nested_expr = (
            convert_literal_to_literal_expression(self.__nested) if isinstance(self.__nested, PythonDecimal)
            else self.__nested.value()
        )
        return PyLegendDecimal(PyLegendDecimalMaxExpression(nested_expr))  # type: ignore

    def min(self) -> "PyLegendDecimal":
        """
        Minimum decimal value in the group.

        Returns
        -------
        PyLegendDecimal

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            frame["id_dec"] = frame["Order Id"].to_string().parse_decimal()
            frame.groupby("Ship Name")["id_dec"].aggregate(
                lambda x: x.min()
            ).to_pandas().head(3)

        """
        nested_expr = (
            convert_literal_to_literal_expression(self.__nested) if isinstance(self.__nested, PythonDecimal)
            else self.__nested.value()
        )
        return PyLegendDecimal(PyLegendDecimalMinExpression(nested_expr))  # type: ignore

    def sum(self) -> "PyLegendDecimal":
        """
        Sum of the decimal values in the group.

        Returns
        -------
        PyLegendDecimal

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            frame["id_dec"] = frame["Order Id"].to_string().parse_decimal()
            frame.groupby("Ship Name")["id_dec"].aggregate(
                lambda x: x.sum()
            ).to_pandas().head(3)

        """
        nested_expr = (
            convert_literal_to_literal_expression(self.__nested) if isinstance(self.__nested, PythonDecimal)
            else self.__nested.value()
        )
        return PyLegendDecimal(PyLegendDecimalSumExpression(nested_expr))  # type: ignore

    def distinct_value(self) -> "PyLegendDecimal":
        """
        Return the single distinct decimal value in the group.

        Returns
        -------
        PyLegendDecimal

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            frame["id_dec"] = frame["Order Id"].to_string().parse_decimal()
            frame.groupby("Order Id")["id_dec"].aggregate(
                lambda x: x.distinct_value()
            ).to_pandas().head(3)

        """
        nested_expr = (
            convert_literal_to_literal_expression(self.__nested) if isinstance(self.__nested, PythonDecimal)
            else self.__nested.value()
        )
        return PyLegendDecimal(PyLegendDecimalUniqueValueOnlyExpression(nested_expr))  # type: ignore[arg-type]


class PyLegendStringCollection(PyLegendPrimitiveCollection):
    """
    Collection type for string expressions.

    ``PyLegendStringCollection`` provides ``max``, ``min``,
    ``join`` / ``join_strings``, and ``distinct_value`` aggregations
    for string columns.  Inherits ``count`` and ``distinct_count``
    from :class:`PyLegendPrimitiveCollection`.
    """
    __nested: PyLegendUnion[str, PyLegendString]

    def __init__(self, nested: PyLegendUnion[str, PyLegendString]) -> None:
        super().__init__(nested)
        self.__nested = nested

    def max(self) -> "PyLegendString":
        """
        Lexicographic maximum string in the group.

        Returns
        -------
        PyLegendString

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            frame.groupby("Ship Name")["Ship Name"].aggregate(
                lambda x: x.max()
            ).to_pandas().head(3)

        """
        nested_expr = (
            convert_literal_to_literal_expression(self.__nested) if isinstance(self.__nested, str)
            else self.__nested.value()
        )
        return PyLegendString(PyLegendStringMaxExpression(nested_expr))  # type: ignore

    def min(self) -> "PyLegendString":
        """
        Lexicographic minimum string in the group.

        Returns
        -------
        PyLegendString

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            frame.groupby("Ship Name")["Ship Name"].aggregate(
                lambda x: x.min()
            ).to_pandas().head(3)

        """
        nested_expr = (
            convert_literal_to_literal_expression(self.__nested) if isinstance(self.__nested, str)
            else self.__nested.value()
        )
        return PyLegendString(PyLegendStringMinExpression(nested_expr))  # type: ignore

    def join(self, separator: str) -> "PyLegendString":
        """
        Concatenate all strings in the group with a separator.

        Parameters
        ----------
        separator : str
            The delimiter inserted between each string.

        Returns
        -------
        PyLegendString

        See Also
        --------
        join_strings : Alias with default separator ``";"``.

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            frame.groupby("Ship Name")["Ship Name"].aggregate(
                lambda x: x.join(", ")
            ).to_pandas().head(3)

        """
        nested_expr = (
            convert_literal_to_literal_expression(self.__nested) if isinstance(self.__nested, str)
            else self.__nested.value()
        )
        separator_expr = convert_literal_to_literal_expression(separator)
        return PyLegendString(PyLegendJoinStringsExpression(nested_expr, separator_expr))  # type: ignore

    def join_strings(self, separator: str = ";") -> "PyLegendString":
        """Alias for :meth:`join` with a default separator of ``";"``."""
        return self.join(separator=separator)

    def distinct_value(self) -> "PyLegendString":
        """
        Return the single distinct string value in the group.

        Returns
        -------
        PyLegendString

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            frame.groupby("Ship Name")["Ship Name"].aggregate(
                lambda x: x.distinct_value()
            ).to_pandas().head(3)

        """
        nested_expr = (
            convert_literal_to_literal_expression(self.__nested) if isinstance(self.__nested, str)
            else self.__nested.value()
        )
        return PyLegendString(PyLegendStringUniqueValueOnlyExpression(nested_expr))  # type: ignore[arg-type]


class PyLegendBooleanCollection(PyLegendPrimitiveCollection):
    """
    Collection type for boolean expressions.

    ``PyLegendBooleanCollection`` is used when a boolean column
    participates in an aggregation context (e.g. inside
    ``groupby().agg()``).  It inherits ``count`` and ``distinct_count``
    from :class:`PyLegendPrimitiveCollection` and adds
    ``distinct_value``.
    """
    __nested: PyLegendUnion[bool, PyLegendBoolean]

    def __init__(self, nested: PyLegendUnion[bool, PyLegendBoolean]) -> None:
        super().__init__(nested)
        self.__nested = nested

    def distinct_value(self) -> "PyLegendBoolean":
        """
        Return the single distinct value in the group.

        Raises an error at query time if the group contains more than
        one distinct value.

        Returns
        -------
        PyLegendBoolean
            The unique boolean value for each group.

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            frame["is_large"] = frame["Order Id"] > 11000
            frame.groupby("Ship Name")["is_large"].aggregate(
                lambda x: x.distinct_value()
            ).to_pandas().head(3)

        """
        nested_expr = (
            convert_literal_to_literal_expression(self.__nested) if isinstance(self.__nested, bool)
            else self.__nested.value()
        )
        return PyLegendBoolean(PyLegendBooleanUniqueValueOnlyExpression(nested_expr))  # type: ignore[arg-type]


class PyLegendDateCollection(PyLegendPrimitiveCollection):
    """
    Collection type for date expressions.

    ``PyLegendDateCollection`` provides ``max``, ``min``, and
    ``distinct_value`` aggregations for date columns.  Inherits
    ``count`` and ``distinct_count`` from
    :class:`PyLegendPrimitiveCollection`.
    """
    __nested: PyLegendUnion[date, datetime, PyLegendDate]

    def __init__(self, nested: PyLegendUnion[date, datetime, PyLegendDate]) -> None:
        super().__init__(nested)
        self.__nested = nested

    def max(self) -> "PyLegendDate":
        """
        Latest (maximum) date in the group.

        Returns
        -------
        PyLegendDate

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            frame.groupby("Ship Name")["Shipped Date"].aggregate(
                lambda x: x.max()
            ).to_pandas().head(3)

        """
        nested_expr = (
            convert_literal_to_literal_expression(self.__nested) if isinstance(self.__nested, (date, datetime))
            else self.__nested.value()
        )
        return PyLegendDate(PyLegendDateMaxExpression(nested_expr))  # type: ignore

    def min(self) -> "PyLegendDate":
        """
        Earliest (minimum) date in the group.

        Returns
        -------
        PyLegendDate

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            frame.groupby("Ship Name")["Shipped Date"].aggregate(
                lambda x: x.min()
            ).to_pandas().head(3)

        """
        nested_expr = (
            convert_literal_to_literal_expression(self.__nested) if isinstance(self.__nested, (date, datetime))
            else self.__nested.value()
        )
        return PyLegendDate(PyLegendDateMinExpression(nested_expr))  # type: ignore

    def distinct_value(self) -> "PyLegendDate":
        """
        Return the single distinct date in the group.

        Returns
        -------
        PyLegendDate

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            frame.groupby("Shipped Date")["Shipped Date"].aggregate(
                lambda x: x.distinct_value()
            ).to_pandas().head(3)

        """
        nested_expr = (
            convert_literal_to_literal_expression(self.__nested) if isinstance(self.__nested, (date, datetime))
            else self.__nested.value()
        )
        return PyLegendDate(PyLegendDateUniqueValueOnlyExpression(nested_expr))  # type: ignore[arg-type]


class PyLegendDateTimeCollection(PyLegendDateCollection):
    """
    Collection type for datetime expressions.

    Overrides ``distinct_value`` to return ``PyLegendDateTime``.
    Inherits ``max``, ``min``, ``count``, and ``distinct_count`` from
    :class:`PyLegendDateCollection`.
    """
    __nested: PyLegendUnion[datetime, PyLegendDateTime]

    def __init__(self, nested: PyLegendUnion[datetime, PyLegendDateTime]) -> None:
        super().__init__(nested)
        self.__nested = nested

    def distinct_value(self) -> "PyLegendDateTime":
        """
        Return the single distinct datetime in the group.

        Returns
        -------
        PyLegendDateTime

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            frame["dt"] = frame["Shipped Date"].first_hour_of_day()
            frame.groupby("dt")["dt"].aggregate(
                lambda x: x.distinct_value()
            ).to_pandas().head(3)

        """
        nested_expr = (
            convert_literal_to_literal_expression(self.__nested) if isinstance(self.__nested, datetime)
            else self.__nested.value()
        )
        return PyLegendDateTime(PyLegendDateTimeUniqueValueOnlyExpression(nested_expr))  # type: ignore[arg-type]


class PyLegendStrictDateCollection(PyLegendDateCollection):
    """
    Collection type for strict-date (date-only) expressions.

    Overrides ``max``, ``min``, and ``distinct_value`` to return
    ``PyLegendStrictDate``.  Inherits ``count`` and ``distinct_count``
    from :class:`PyLegendDateCollection`.
    """
    __nested: PyLegendUnion[date, PyLegendStrictDate]

    def __init__(self, nested: PyLegendUnion[date, PyLegendStrictDate]) -> None:
        super().__init__(nested)
        self.__nested = nested

    def max(self) -> "PyLegendStrictDate":
        """
        Latest (maximum) strict date in the group.

        Returns
        -------
        PyLegendStrictDate

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            frame["sd"] = frame["Shipped Date"].date_part()
            frame.groupby("Ship Name")["sd"].aggregate(
                lambda x: x.max()
            ).to_pandas().head(3)

        """
        nested_expr = (
            convert_literal_to_literal_expression(self.__nested) if isinstance(self.__nested, date)
            else self.__nested.value()
        )
        return PyLegendStrictDate(PyLegendStrictDateMaxExpression(nested_expr))  # type: ignore

    def min(self) -> "PyLegendStrictDate":
        """
        Earliest (minimum) strict date in the group.

        Returns
        -------
        PyLegendStrictDate

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            frame["sd"] = frame["Shipped Date"].date_part()
            frame.groupby("Ship Name")["sd"].aggregate(
                lambda x: x.min()
            ).to_pandas().head(3)

        """
        nested_expr = (
            convert_literal_to_literal_expression(self.__nested) if isinstance(self.__nested, date)
            else self.__nested.value()
        )
        return PyLegendStrictDate(PyLegendStrictDateMinExpression(nested_expr))  # type: ignore

    def distinct_value(self) -> "PyLegendStrictDate":
        """
        Return the single distinct strict date in the group.

        Returns
        -------
        PyLegendStrictDate

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            frame["sd"] = frame["Shipped Date"].date_part()
            frame.groupby("sd")["sd"].aggregate(
                lambda x: x.distinct_value()
            ).to_pandas().head(3)

        """
        nested_expr = (
            convert_literal_to_literal_expression(self.__nested) if isinstance(self.__nested, date)
            else self.__nested.value()
        )
        return PyLegendStrictDate(PyLegendStrictDateUniqueValueOnlyExpression(nested_expr))  # type: ignore[arg-type]


def create_primitive_collection(nested: PyLegendPrimitiveOrPythonPrimitive) -> PyLegendPrimitiveCollection:
    if isinstance(nested, (int, PyLegendInteger)):
        return PyLegendIntegerCollection(nested)

    if isinstance(nested, (float, PyLegendFloat)):
        return PyLegendFloatCollection(nested)

    if isinstance(nested, (PythonDecimal, PyLegendDecimal)):
        return PyLegendDecimalCollection(nested)

    if isinstance(nested, PyLegendNumber):
        return PyLegendNumberCollection(nested)

    if isinstance(nested, (str, PyLegendString)):
        return PyLegendStringCollection(nested)

    if isinstance(nested, (bool, PyLegendBoolean)):
        return PyLegendBooleanCollection(nested)

    if isinstance(nested, (datetime, PyLegendDateTime)):
        return PyLegendDateTimeCollection(nested)

    if isinstance(nested, (date, PyLegendStrictDate)):
        return PyLegendStrictDateCollection(nested)

    if isinstance(nested, PyLegendDate):
        return PyLegendDateCollection(nested)

    raise RuntimeError(f"Not supported type - {type(nested)}")  # pragma: no cover


class PyLegendNumberPairCollection(PyLegendPrimitiveCollection):
    """
    Collection type for a pair of numeric expressions.

    ``PyLegendNumberPairCollection`` enables bivariate aggregations such
    as correlation, covariance, weighted average, and extrema-by-key
    over two numeric columns.  Create one via
    :meth:`PyLegendNumberCollection.row_mapper`.
    """
    __nested_a: PyLegendUnion[int, float, PyLegendInteger, PyLegendFloat, PyLegendDecimal, PyLegendNumber]
    __nested_b: PyLegendUnion[int, float, PyLegendInteger, PyLegendFloat, PyLegendDecimal, PyLegendNumber]

    def __init__(
        self,
        nested_a: PyLegendUnion[
            int, float, PyLegendInteger, PyLegendFloat, PyLegendDecimal, PyLegendNumber, "PyLegendNumberCollection"
        ],
        nested_b: PyLegendUnion[
            int, float, PyLegendInteger, PyLegendFloat, PyLegendDecimal, PyLegendNumber, "PyLegendNumberCollection"
        ],
    ) -> None:
        resolved_a = self._resolve(nested_a)
        resolved_b = self._resolve(nested_b)
        super().__init__(resolved_a)
        self.__nested_a = resolved_a
        self.__nested_b = resolved_b

    @staticmethod
    def _resolve(
        val: PyLegendUnion[
            int, float, PyLegendInteger, PyLegendFloat, PyLegendDecimal, PyLegendNumber, "PyLegendNumberCollection"
        ]
    ) -> PyLegendUnion[int, float, PyLegendInteger, PyLegendFloat, PyLegendDecimal, PyLegendNumber]:
        if isinstance(val, PyLegendPrimitiveCollection):
            return val._PyLegendPrimitiveCollection__nested  # type: ignore
        return val

    def corr(self) -> "PyLegendFloat":
        """
        Pearson correlation coefficient between the two columns.

        Returns
        -------
        PyLegendFloat

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            frame.groupby("Ship Name")["Order Id"].aggregate(
                lambda x: x.row_mapper(x).corr()
            ).to_pandas().head(3)

        """
        nested_expr_a = (
            convert_literal_to_literal_expression(self.__nested_a) if isinstance(self.__nested_a, (int, float, PythonDecimal))
            else self.__nested_a.value()
        )
        nested_expr_b = (
            convert_literal_to_literal_expression(self.__nested_b) if isinstance(self.__nested_b, (int, float, PythonDecimal))
            else self.__nested_b.value()
        )
        return PyLegendFloat(PyLegendCorrExpression(nested_expr_a, nested_expr_b))  # type: ignore

    def _get_nested_exprs(self):  # type: ignore
        nested_expr_a = (
            convert_literal_to_literal_expression(self.__nested_a) if isinstance(self.__nested_a, (int, float, PythonDecimal))
            else self.__nested_a.value()
        )
        nested_expr_b = (
            convert_literal_to_literal_expression(self.__nested_b) if isinstance(self.__nested_b, (int, float, PythonDecimal))
            else self.__nested_b.value()
        )
        return nested_expr_a, nested_expr_b

    def covar_population(self) -> "PyLegendFloat":
        """
        Population covariance between the two columns.

        Returns
        -------
        PyLegendFloat

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            frame.groupby("Ship Name")["Order Id"].aggregate(
                lambda x: x.row_mapper(x).covar_population()
            ).to_pandas().head(3)

        """
        nested_expr_a, nested_expr_b = self._get_nested_exprs()  # type: ignore
        return PyLegendFloat(PyLegendCovarPopulationExpression(nested_expr_a, nested_expr_b))

    def covar_sample(self) -> "PyLegendFloat":
        """
        Sample covariance between the two columns.

        Returns
        -------
        PyLegendFloat

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            frame.groupby("Ship Name")["Order Id"].aggregate(
                lambda x: x.row_mapper(x).covar_sample()
            ).to_pandas().head(3)

        """
        nested_expr_a, nested_expr_b = self._get_nested_exprs()  # type: ignore
        return PyLegendFloat(PyLegendCovarSampleExpression(nested_expr_a, nested_expr_b))

    def wavg_legend_ext(self) -> "PyLegendFloat":
        """
        Weighted average using column *a* as values and column *b* as weights.

        .. note::
           This is a Legend extension function.

        Returns
        -------
        PyLegendFloat

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            frame.groupby("Ship Name")["Order Id"].aggregate(
                lambda x: x.row_mapper(x).wavg_legend_ext()
            ).to_pandas().head(3)

        """
        nested_expr_a, nested_expr_b = self._get_nested_exprs()  # type: ignore
        return PyLegendFloat(PyLegendWavgExpression(nested_expr_a, nested_expr_b))

    def max_by_legend_ext(self) -> "PyLegendNumber":
        """
        Value of column *a* at the row where column *b* is maximum.

        .. note::
           This is a Legend extension function.

        Returns
        -------
        PyLegendNumber

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            frame.groupby("Ship Name")["Order Id"].aggregate(
                lambda x: x.row_mapper(x).max_by_legend_ext()
            ).to_pandas().head(3)

        """
        nested_expr_a, nested_expr_b = self._get_nested_exprs()  # type: ignore
        return PyLegendNumber(PyLegendMaxByExpression(nested_expr_a, nested_expr_b))

    def min_by_legend_ext(self) -> "PyLegendNumber":
        """
        Value of column *a* at the row where column *b* is minimum.

        .. note::
           This is a Legend extension function.

        Returns
        -------
        PyLegendNumber

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            frame.groupby("Ship Name")["Order Id"].aggregate(
                lambda x: x.row_mapper(x).min_by_legend_ext()
            ).to_pandas().head(3)

        """
        nested_expr_a, nested_expr_b = self._get_nested_exprs()  # type: ignore
        return PyLegendNumber(PyLegendMinByExpression(nested_expr_a, nested_expr_b))

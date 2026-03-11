# Copyright 2025 Goldman Sachs
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

from textwrap import dedent
from typing import TYPE_CHECKING, runtime_checkable, Protocol

import pandas as pd

from pylegend._typing import (
    PyLegendDict,
)
from pylegend._typing import (
    PyLegendSequence,
    PyLegendOptional,
    PyLegendTypeVar,
    PyLegendUnion
)
from pylegend.core.database.sql_to_string import SqlToStringConfig, SqlToStringFormat
from pylegend.core.language.pandas_api.pandas_api_aggregate_specification import PyLegendAggInput
from pylegend.core.language.pandas_api.pandas_api_tds_row import PandasApiTdsRow
from pylegend.core.language.shared.column_expressions import PyLegendColumnExpression
from pylegend.core.language.shared.expression import (
    PyLegendExpressionBooleanReturn,
    PyLegendExpressionStringReturn,
    PyLegendExpressionNumberReturn,
    PyLegendExpressionIntegerReturn,
    PyLegendExpressionFloatReturn,
    PyLegendExpressionDecimalReturn,
    PyLegendExpressionDateReturn,
    PyLegendExpressionDateTimeReturn,
    PyLegendExpressionStrictDateReturn,
    PyLegendExpression,
)
from pylegend.core.language.shared.primitives.boolean import PyLegendBoolean
from pylegend.core.language.shared.primitives.date import PyLegendDate
from pylegend.core.language.shared.primitives.datetime import PyLegendDateTime
from pylegend.core.language.shared.primitives.float import PyLegendFloat
from pylegend.core.language.shared.primitives.decimal import PyLegendDecimal
from pylegend.core.language.shared.primitives.integer import PyLegendInteger
from pylegend.core.language.shared.primitives.number import PyLegendNumber
from pylegend.core.language.shared.primitives.primitive import PyLegendPrimitive, PyLegendPrimitiveOrPythonPrimitive
from pylegend.core.language.shared.primitives.strictdate import PyLegendStrictDate
from pylegend.core.language.shared.primitives.string import PyLegendString
from pylegend.core.sql.metamodel import (
    Expression, SingleColumn, QualifiedNameReference, QualifiedName,
)
from pylegend.core.sql.metamodel import QuerySpecification
from pylegend.core.tds.abstract.frames.base_tds_frame import BaseTdsFrame
from pylegend.core.tds.pandas_api.frames.functions.filter import PandasApiFilterFunction
from pylegend.core.tds.pandas_api.frames.helpers.series_helper import add_primitive_methods, assert_and_find_core_series, \
    has_window_function, get_pure_query_from_expr
from pylegend.core.tds.pandas_api.frames.pandas_api_applied_function_tds_frame import PandasApiAppliedFunctionTdsFrame
from pylegend.core.tds.pandas_api.frames.pandas_api_base_tds_frame import PandasApiBaseTdsFrame
from pylegend.core.tds.result_handler import ResultHandler, ToStringResultHandler
from pylegend.core.tds.sql_query_helpers import create_sub_query
from pylegend.core.tds.tds_column import TdsColumn
from pylegend.core.tds.tds_frame import FrameToPureConfig
from pylegend.core.tds.tds_frame import FrameToSqlConfig
from pylegend.extensions.tds.result_handler import PandasDfReadConfig, ToPandasDfResultHandler

if TYPE_CHECKING:
    from pylegend.core.tds.pandas_api.frames.pandas_api_tds_frame import PandasApiTdsFrame

__all__: PyLegendSequence[str] = [
    "Series",
    "BooleanSeries",
    "StringSeries",
    "NumberSeries",
    "IntegerSeries",
    "FloatSeries",
    "DateSeries",
    "DateTimeSeries",
    "StrictDateSeries",
    "SupportsToSqlExpression",
    "SupportsToPureExpression",
]

R = PyLegendTypeVar('R')


@runtime_checkable
class SupportsToSqlExpression(Protocol):
    def to_sql_expression(
            self,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        ...


@runtime_checkable
class SupportsToPureExpression(Protocol):
    def to_pure_expression(self, config: FrameToPureConfig) -> str:
        ...


@add_primitive_methods
class Series(PyLegendColumnExpression, PyLegendPrimitive, BaseTdsFrame):
    """
    A single-column proxy for a :class:`~pylegend.core.tds.pandas_api.frames.pandas_api_tds_frame.PandasApiTdsFrame`.

    A ``Series`` is conceptually similar to a ``pandas.Series``: it
    represents one column of a frame and supports element-wise
    arithmetic, string methods, date-part extraction, and other
    transformations.

    Obtaining a Series
    ------------------
    Use bracket notation on a ``PandasApiTdsFrame``.
    The returned subclass matches the column type.
    For example, an integer column becomes an IntegerSeries.

    Transformations
    ---------------
    A ``Series`` supports the same operator overloads as the
    underlying primitive type.  For example, an ``IntegerSeries``
    supports ``+``, ``-``, ``*``, ``/``, ``%``, comparisons, etc.
    A ``StringSeries`` supports ``.upper()``, ``.lower()``,
    ``.len()``, ``.startswith()``, ``.contains()``,
    ``.replace()``, concatenation with ``+``, etc.
    A ``DateTimeSeries`` supports ``.year()``, ``.month()``,
    ``.day()``, etc.

    Transforming a ``Series`` produces a **new** ``Series`` with
    an updated expression tree — the original column is never
    mutated.

    Assigning back to the frame
    ---------------------------
    Use bracket assignment (``__setitem__``) to write a ``Series``
    back into the frame — either overwriting an existing column or
    creating a new one.

    Constants (``int``, ``float``, ``str``, ``bool``, ``date``,
    ``datetime``) and callables (``lambda``) are also accepted on the
    right-hand side.

    .. important::

        A ``Series`` can only be assigned to the **same frame** it was
        derived from.  Assigning a ``Series`` from a different frame
        raises ``ValueError("Assignment from a different frame is not
        allowed")``.

    Window functions on a Series
    ----------------------------
    Certain window functions such as ``rank()`` can be called on a
    ``Series``.  The result is a new ``Series`` whose values are the
    window-function output for that column.

    Series with applied functions can also be combined with arithmetic in the
    same assignment, but only **one** function call is allowed
    per expression.  If you need multiple, split them into separate
    steps.

    See Also
    --------
    PandasApiTdsFrame : The parent frame class.
    PandasApiGroupbyTdsFrame : Groupby object (returns
        ``GroupbySeries`` when bracket-indexed).

    Notes
    -----
    **Differences from pandas:**

    - A ``Series`` is **not** a first-class data container. It is an
      expression builder that lazily constructs SQL / PURE. No data
      is materialised until ``execute_frame_to_string()`` or
      ``to_pandas()`` is called.
    - Cross-frame assignment is **not allowed**. In pandas you can
      freely assign a Series from one DataFrame to another (alignment
      happens on the index); here the Series must originate from the
      **same** frame instance.
    - Applying a function on a computed series expression is **not
      supported** in certain cases. For example,
      ``(frame['col'] + 5).rank()`` raises ``NotImplementedError``.
      Instead, do ``frame['col'].rank() + 5``.
    - Boolean column support is limited (Boolean columns are not
      yet fully supported in PURE).

    Examples
    --------
    .. ipython:: python

        import pylegend
        frame = pylegend.samples.pandas_api.northwind_orders_frame()

        # Retrieve a column as a Series
        series = frame["Order Id"]
        type(series).__name__

        # Arithmetic on a Series (returns a new Series)
        doubled = frame["Order Id"] * 2
        doubled.head(5).to_pandas()

        # String methods on a StringSeries
        upper_name = frame["Ship Name"].upper()
        upper_name.head(5).to_pandas()

    .. ipython:: python

        import pylegend
        frame = pylegend.samples.pandas_api.northwind_orders_frame()

        # Overwrite an existing column
        frame["Order Id"] = frame["Order Id"] + 1000
        frame.head(5).to_pandas()

        # Create a new column from a Series expression
        frame["Upper Ship"] = frame["Ship Name"].upper()
        frame.head(5).to_pandas()

    .. ipython:: python

        import pylegend
        frame = pylegend.samples.pandas_api.northwind_orders_frame()

        # Assign a constant
        frame["Flag"] = 1
        frame.head(3).to_pandas()

    .. ipython:: python

        import pylegend
        frame = pylegend.samples.pandas_api.northwind_orders_frame()

        # Append a rank column via Series
        frame["Order Rank"] = frame["Order Id"].rank()
        frame.head(5).to_pandas()

    """
    def __init__(
            self, base_frame: "PandasApiBaseTdsFrame", column: str, expr: PyLegendOptional[PyLegendExpression] = None
    ) -> None:
        row = PandasApiTdsRow.from_tds_frame("c", base_frame)
        PyLegendColumnExpression.__init__(self, row=row, column=column)

        self._base_frame = base_frame
        filtered = base_frame.filter(items=[column])
        assert isinstance(filtered, PandasApiAppliedFunctionTdsFrame)
        self._filtered_frame: PandasApiAppliedFunctionTdsFrame = filtered

        self._expr = expr
        if self._expr is not None:
            assert_and_find_core_series(self._expr)

    @property
    def expr(self) -> PyLegendOptional[PyLegendExpression]:
        return self._expr

    def value(self) -> PyLegendColumnExpression:
        return self

    def get_base_frame(self) -> "PandasApiBaseTdsFrame":
        return self._base_frame

    def get_filtered_frame(self) -> PandasApiAppliedFunctionTdsFrame:
        return self._filtered_frame

    def get_leaf_expressions(self) -> PyLegendSequence["PyLegendExpression"]:
        if self.expr is not None:
            return self.expr.get_leaf_expressions()
        return [self]

    def to_sql_expression(
            self,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        if self._expr is not None:
            return self._expr.to_sql_expression(frame_name_to_base_query_map, config)

        applied_func = self._filtered_frame.get_applied_function()
        if not isinstance(applied_func, PandasApiFilterFunction):  # pragma: no cover
            if isinstance(applied_func, SupportsToSqlExpression):
                return applied_func.to_sql_expression(frame_name_to_base_query_map, config)
            else:
                raise NotImplementedError(
                    f"The '{applied_func.name()}' function cannot provide a SQL expression"
                )

        return super().to_sql_expression(frame_name_to_base_query_map, config)

    def to_pure_expression(self, config: FrameToPureConfig) -> str:
        if self._expr is not None:
            return self._expr.to_pure_expression(config)

        applied_func = self._filtered_frame.get_applied_function()
        if not isinstance(applied_func, PandasApiFilterFunction):  # pragma: no cover
            if isinstance(applied_func, SupportsToPureExpression):
                return applied_func.to_pure_expression(config)
            else:
                raise NotImplementedError(
                    f"The '{applied_func.name()}' function cannot provide a pure expression"
                )

        return super().to_pure_expression(config)

    def columns(self) -> PyLegendSequence[TdsColumn]:
        return self._filtered_frame.columns()

    def to_sql_query(self, config: FrameToSqlConfig = FrameToSqlConfig()) -> str:
        query = self.to_sql_query_object(config)
        sql_to_string_config = SqlToStringConfig(
            format_=SqlToStringFormat(pretty=config.pretty)
        )
        return config.sql_to_string_generator().generate_sql_string(query, sql_to_string_config)

    def to_pure_query(self, config: FrameToPureConfig = FrameToPureConfig()) -> str:
        if self.expr is None:
            return self.get_filtered_frame().to_pure_query(config)

        return get_pure_query_from_expr(self, config)

    def execute_frame(
            self,
            result_handler: ResultHandler[R],
            chunk_size: PyLegendOptional[int] = None
    ) -> R:
        return BaseTdsFrame.execute_frame(self, result_handler, chunk_size)

    def execute_frame_to_string(
            self,
            chunk_size: PyLegendOptional[int] = None
    ) -> str:
        return self.execute_frame(ToStringResultHandler(), chunk_size)

    def execute_frame_to_pandas_df(
            self,
            chunk_size: PyLegendOptional[int] = None,
            pandas_df_read_config: PandasDfReadConfig = PandasDfReadConfig()
    ) -> pd.DataFrame:
        return self.execute_frame(ToPandasDfResultHandler(pandas_df_read_config), chunk_size)  # pragma: no cover

    def to_sql_query_object(self, config: FrameToSqlConfig) -> QuerySpecification:
        temp_column_name_suffix = "__pylegend_olap_column__"
        if self.expr is None:
            return self.get_filtered_frame().to_sql_query_object(config)

        expr_contains_window_func = has_window_function(self)

        db_extension = config.sql_to_string_generator().get_db_extension()
        base_query = self.get_base_frame().to_sql_query_object(config)
        col_name = self.columns()[0].get_name()

        temp_col_name = (
            db_extension.quote_identifier(col_name + temp_column_name_suffix) if expr_contains_window_func else
            db_extension.quote_identifier(col_name)
        )

        new_select_item = SingleColumn(
            temp_col_name,
            self.to_sql_expression({'c': base_query}, config)
        )
        base_query.select.selectItems = [new_select_item]

        if expr_contains_window_func:
            new_query = create_sub_query(base_query, config, "root")
            new_query.select.selectItems = [
                SingleColumn(
                    db_extension.quote_identifier(col_name),
                    QualifiedNameReference(QualifiedName([db_extension.quote_identifier("root"), temp_col_name]))
                )
            ]
            return new_query
        else:
            return base_query

    def to_pure(self, config: FrameToPureConfig) -> str:
        return self.to_pure_query(config)

    def get_all_tds_frames(self) -> PyLegendSequence["BaseTdsFrame"]:
        if self.expr is not None:
            core_series = assert_and_find_core_series(self)
            assert core_series is not None
            return core_series.get_all_tds_frames()
        return self._filtered_frame.get_all_tds_frames()

    def has_applied_function(self) -> bool:
        applied_func = self._filtered_frame.get_applied_function()
        return not isinstance(applied_func, PandasApiFilterFunction)

    def aggregate(
            self,
            func: PyLegendAggInput,
            axis: PyLegendUnion[int, str] = 0,
            *args: PyLegendPrimitiveOrPythonPrimitive,
            **kwargs: PyLegendPrimitiveOrPythonPrimitive
    ) -> "PandasApiTdsFrame":
        """
        Aggregate the Series using one or more operations.

        Reduce the single column to one or more scalar values. The
        result is returned as a single-row
        :class:`~pylegend.core.tds.pandas_api.frames.pandas_api_tds_frame.PandasApiTdsFrame`.

        Parameters
        ----------
        func : str, callable, list, or dict
            Aggregation specification:

            - **str** — a named aggregation (``'sum'``, ``'mean'``,
              ``'min'``, ``'max'``, ``'count'``, ``'std'``, ``'var'``,
              plus aliases ``'len'``, ``'size'``).
            - **callable** — a lambda receiving the Series and calling
              one of its aggregation methods
              (e.g. ``lambda x: x.sum()``).
            - **list of str** — multiple named aggregations. Result
              columns are named ``"agg(col_name)"``.
            - **dict** — ``{column_name: agg_spec}``. Keys **must**
              match the Series' column name.
        axis : {{0, 'index'}}, default 0
            Must be ``0`` or ``'index'``.

        Returns
        -------
        PandasApiTdsFrame
            A single-row frame with the aggregated value(s).

        Raises
        ------
        NotImplementedError
            If called on a computed Series expression
            (e.g. ``(frame['col'] + 5).aggregate('sum')``). Assign the
            expression to a column first, then aggregate.
        ValueError
            If a dict key does not match the Series' column name.

        See Also
        --------
        agg : Alias for ``aggregate``.
        sum : Sum of the column.
        mean : Mean of the column.

        Notes
        -----
        **Differences from pandas:**

        - In pandas, ``Series.aggregate`` can return a scalar, a
          Series, or a DataFrame depending on the input. Here the
          result is always a single-row ``PandasApiTdsFrame``.
        - Aggregation on a **computed** Series expression is **not
          supported**. Assign the expression to the frame first.
        - When ``func`` is a dict, keys must exactly match the
          Series' column name — no other column names are valid.

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            # Single named aggregation
            frame["Order Id"].aggregate("sum").to_pandas()

            # Multiple aggregations via a list
            frame["Order Id"].aggregate(["sum", "min", "max"]).to_pandas()

            # Lambda aggregation
            frame["Order Id"].aggregate(lambda x: x.count()).to_pandas()

        """
        if self._expr is not None:  # pragma: no cover
            error_msg = '''
                Applying aggregate function to a computed series expression is not supported yet.
                Please change the series itself before trying to apply aggregate function.
                For example,
                    instead of: (frame['col'] + 5).sum()
                    do: frame['new_col'] = frame['col'] + 5; frame['new_col'].sum()
            '''
            error_msg = dedent(error_msg).strip()
            raise NotImplementedError(error_msg)
        return self._filtered_frame.aggregate(func, axis, *args, **kwargs)

    def agg(
            self,
            func: PyLegendAggInput,
            axis: PyLegendUnion[int, str] = 0,
            *args: PyLegendPrimitiveOrPythonPrimitive,
            **kwargs: PyLegendPrimitiveOrPythonPrimitive
    ) -> "PandasApiTdsFrame":
        """
        Alias for :meth:`aggregate`.

        See :meth:`aggregate` for full documentation.
        """
        return self.aggregate(func, axis, *args, **kwargs)

    def sum(
            self,
            axis: PyLegendUnion[int, str] = 0,
            skipna: bool = True,
            numeric_only: bool = False,
            min_count: int = 0,
            **kwargs: PyLegendPrimitiveOrPythonPrimitive
    ) -> "PandasApiTdsFrame":
        """
        Return the sum of the Series values.

        Parameters
        ----------
        axis : {{0, 'index'}}, default 0
            Must be ``0`` or ``'index'``.
        skipna : bool, default True
            Must be ``True``. ``False`` is not supported (SQL
            aggregation ignores nulls by default).
        numeric_only : bool, default False
            Must be ``False``. ``True`` is not supported.
        min_count : int, default 0
            Must be ``0``. Non-zero values are not supported.

        Returns
        -------
        PandasApiTdsFrame
            A single-row frame with the sum.

        Notes
        -----
        Equivalent to ``series.aggregate("sum")``.

        **Differences from pandas:** returns a single-row
        ``PandasApiTdsFrame`` instead of a scalar. ``skipna=False``,
        ``numeric_only=True``, and ``min_count != 0`` are **not
        supported**.

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            frame["Order Id"].sum().to_pandas()

        """
        if axis not in [0, "index"]:
            raise NotImplementedError(f"The 'axis' parameter must be 0 or 'index' in sum function, but got: {axis}")
        if skipna is not True:
            raise NotImplementedError("skipna=False is not currently supported in sum function. "
                                      "SQL aggregation ignores nulls by default.")
        if numeric_only is not False:
            raise NotImplementedError("numeric_only=True is not currently supported in sum function.")
        if min_count != 0:
            raise NotImplementedError(f"min_count must be 0 in sum function, but got: {min_count}")
        if len(kwargs) > 0:
            raise NotImplementedError(
                f"Additional keyword arguments not supported in sum function: {list(kwargs.keys())}")
        return self.aggregate("sum", 0)

    def mean(
            self,
            axis: PyLegendUnion[int, str] = 0,
            skipna: bool = True,
            numeric_only: bool = False,
            **kwargs: PyLegendPrimitiveOrPythonPrimitive
    ) -> "PandasApiTdsFrame":
        """
        Return the mean of the Series values.

        Parameters
        ----------
        axis : {{0, 'index'}}, default 0
            Must be ``0`` or ``'index'``.
        skipna : bool, default True
            Must be ``True``. ``False`` is not supported.
        numeric_only : bool, default False
            Must be ``False``. ``True`` is not supported.

        Returns
        -------
        PandasApiTdsFrame
            A single-row frame with the mean.

        Notes
        -----
        Equivalent to ``series.aggregate("mean")``. Maps to SQL
        ``AVG()``.

        **Differences from pandas:** returns a single-row
        ``PandasApiTdsFrame`` instead of a scalar.

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            frame["Order Id"].mean().to_pandas()

        """
        if axis not in [0, "index"]:
            raise NotImplementedError(f"The 'axis' parameter must be 0 or 'index' in mean function, but got: {axis}")
        if skipna is not True:
            raise NotImplementedError("skipna=False is not currently supported in mean function.")
        if numeric_only is not False:
            raise NotImplementedError("numeric_only=True is not currently supported in mean function.")
        if len(kwargs) > 0:
            raise NotImplementedError(
                f"Additional keyword arguments not supported in mean function: {list(kwargs.keys())}")
        return self.aggregate("mean", 0)

    def min(
            self,
            axis: PyLegendUnion[int, str] = 0,
            skipna: bool = True,
            numeric_only: bool = False,
            **kwargs: PyLegendPrimitiveOrPythonPrimitive
    ) -> "PandasApiTdsFrame":
        """
        Return the minimum of the Series values.

        Parameters
        ----------
        axis : {{0, 'index'}}, default 0
            Must be ``0`` or ``'index'``.
        skipna : bool, default True
            Must be ``True``. ``False`` is not supported.
        numeric_only : bool, default False
            Must be ``False``. ``True`` is not supported.

        Returns
        -------
        PandasApiTdsFrame
            A single-row frame with the minimum value.

        Notes
        -----
        Equivalent to ``series.aggregate("min")``. Works on string
        columns as well (lexicographic minimum).

        **Differences from pandas:** returns a single-row
        ``PandasApiTdsFrame`` instead of a scalar.

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            frame["Order Id"].min().to_pandas()

        """
        if axis not in [0, "index"]:
            raise NotImplementedError(f"The 'axis' parameter must be 0 or 'index' in min function, but got: {axis}")
        if skipna is not True:
            raise NotImplementedError("skipna=False is not currently supported in min function.")
        if numeric_only is not False:
            raise NotImplementedError("numeric_only=True is not currently supported in min function.")
        if len(kwargs) > 0:
            raise NotImplementedError(
                f"Additional keyword arguments not supported in min function: {list(kwargs.keys())}")
        return self.aggregate("min", 0)

    def max(
            self,
            axis: PyLegendUnion[int, str] = 0,
            skipna: bool = True,
            numeric_only: bool = False,
            **kwargs: PyLegendPrimitiveOrPythonPrimitive
    ) -> "PandasApiTdsFrame":
        """
        Return the maximum of the Series values.

        Parameters
        ----------
        axis : {{0, 'index'}}, default 0
            Must be ``0`` or ``'index'``.
        skipna : bool, default True
            Must be ``True``. ``False`` is not supported.
        numeric_only : bool, default False
            Must be ``False``. ``True`` is not supported.

        Returns
        -------
        PandasApiTdsFrame
            A single-row frame with the maximum value.

        Notes
        -----
        Equivalent to ``series.aggregate("max")``. Works on string
        columns as well (lexicographic maximum).

        **Differences from pandas:** returns a single-row
        ``PandasApiTdsFrame`` instead of a scalar.

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            frame["Order Id"].max().to_pandas()

        """
        if axis not in [0, "index"]:
            raise NotImplementedError(f"The 'axis' parameter must be 0 or 'index' in max function, but got: {axis}")
        if skipna is not True:
            raise NotImplementedError("skipna=False is not currently supported in max function.")
        if numeric_only is not False:
            raise NotImplementedError("numeric_only=True is not currently supported in max function.")
        if len(kwargs) > 0:
            raise NotImplementedError(
                f"Additional keyword arguments not supported in max function: {list(kwargs.keys())}")
        return self.aggregate("max", 0)

    def std(
            self,
            axis: PyLegendUnion[int, str] = 0,
            skipna: bool = True,
            ddof: int = 1,
            numeric_only: bool = False,
            **kwargs: PyLegendPrimitiveOrPythonPrimitive
    ) -> "PandasApiTdsFrame":
        """
        Return the sample standard deviation of the Series values.

        Parameters
        ----------
        axis : {{0, 'index'}}, default 0
            Must be ``0`` or ``'index'``.
        skipna : bool, default True
            Must be ``True``. ``False`` is not supported.
        ddof : int, default 1
            Must be ``1`` (sample standard deviation). Other values
            are not supported.
        numeric_only : bool, default False
            Must be ``False``. ``True`` is not supported.

        Returns
        -------
        PandasApiTdsFrame
            A single-row frame with the standard deviation.

        Notes
        -----
        Equivalent to ``series.aggregate("std")``. Maps to SQL
        ``STDDEV_SAMP()``.

        **Differences from pandas:** returns a single-row
        ``PandasApiTdsFrame`` instead of a scalar. Only ``ddof=1``
        is supported.

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            frame["Order Id"].std().to_pandas()

        """
        if axis not in [0, "index"]:
            raise NotImplementedError(f"The 'axis' parameter must be 0 or 'index' in std function, but got: {axis}")
        if skipna is not True:
            raise NotImplementedError("skipna=False is not currently supported in std function.")
        if ddof != 1:
            raise NotImplementedError(
                f"Only ddof=1 (Sample Standard Deviation) is supported in std function, but got: {ddof}")
        if numeric_only is not False:
            raise NotImplementedError("numeric_only=True is not currently supported in std function.")
        if len(kwargs) > 0:
            raise NotImplementedError(
                f"Additional keyword arguments not supported in std function: {list(kwargs.keys())}")
        return self.aggregate("std", 0)

    def var(
            self,
            axis: PyLegendUnion[int, str] = 0,
            skipna: bool = True,
            ddof: int = 1,
            numeric_only: bool = False,
            **kwargs: PyLegendPrimitiveOrPythonPrimitive
    ) -> "PandasApiTdsFrame":
        """
        Return the sample variance of the Series values.

        Parameters
        ----------
        axis : {{0, 'index'}}, default 0
            Must be ``0`` or ``'index'``.
        skipna : bool, default True
            Must be ``True``. ``False`` is not supported.
        ddof : int, default 1
            Must be ``1`` (sample variance). Other values are not
            supported.
        numeric_only : bool, default False
            Must be ``False``. ``True`` is not supported.

        Returns
        -------
        PandasApiTdsFrame
            A single-row frame with the variance.

        Notes
        -----
        Equivalent to ``series.aggregate("var")``. Maps to SQL
        ``VAR_SAMP()``.

        **Differences from pandas:** returns a single-row
        ``PandasApiTdsFrame`` instead of a scalar. Only ``ddof=1``
        is supported.

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            frame["Order Id"].var().to_pandas()

        """
        if axis not in [0, "index"]:
            raise NotImplementedError(f"The 'axis' parameter must be 0 or 'index' in var function, but got: {axis}")
        if skipna is not True:
            raise NotImplementedError("skipna=False is not currently supported in var function.")
        if ddof != 1:
            raise NotImplementedError(f"Only ddof=1 (Sample Variance) is supported in var function, but got: {ddof}")
        if numeric_only is not False:
            raise NotImplementedError("numeric_only=True is not currently supported in var function.")
        if len(kwargs) > 0:
            raise NotImplementedError(
                f"Additional keyword arguments not supported in var function: {list(kwargs.keys())}")
        return self.aggregate("var", 0)

    def count(
            self,
            axis: PyLegendUnion[int, str] = 0,
            numeric_only: bool = False,
            **kwargs: PyLegendPrimitiveOrPythonPrimitive
    ) -> "PandasApiTdsFrame":
        """
        Return the count of non-null values in the Series.

        Parameters
        ----------
        axis : {{0, 'index'}}, default 0
            Must be ``0`` or ``'index'``.
        numeric_only : bool, default False
            Must be ``False``. ``True`` is not supported.

        Returns
        -------
        PandasApiTdsFrame
            A single-row frame with the count.

        Notes
        -----
        Equivalent to ``series.aggregate("count")``. Maps to SQL
        ``COUNT(column)``.

        **Differences from pandas:** returns a single-row
        ``PandasApiTdsFrame`` instead of a scalar integer.

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            frame["Order Id"].count().to_pandas()

        """
        if axis not in [0, "index"]:
            raise NotImplementedError(f"The 'axis' parameter must be 0 or 'index' in count function, but got: {axis}")
        if numeric_only is not False:
            raise NotImplementedError("numeric_only=True is not currently supported in count function.")
        if len(kwargs) > 0:
            raise NotImplementedError(
                f"Additional keyword arguments not supported in count function: {list(kwargs.keys())}")
        return self.aggregate("count", 0)

    def rank(
            self,
            axis: PyLegendUnion[int, str] = 0,
            method: str = 'min',
            numeric_only: bool = False,
            na_option: str = 'bottom',
            ascending: bool = True,
            pct: bool = False
    ) -> "Series":
        """
        Compute the rank of values in this Series.

        Return a new ``Series`` containing the rank of each value.
        The result can be assigned back to the parent frame as a new
        column, or executed directly as a standalone single-column
        query.

        Parameters
        ----------
        axis : {{0, 'index'}}, default 0
            Must be ``0`` or ``'index'``.
        method : {{'min', 'first', 'dense'}}, default 'min'
            How to rank equal values:

            - ``'min'`` : Lowest rank in the group of ties
              (SQL ``RANK()``).
            - ``'first'`` : Ranks by order of appearance
              (SQL ``ROW_NUMBER()``).
            - ``'dense'`` : Like ``'min'`` but no gaps
              (SQL ``DENSE_RANK()``).
        numeric_only : bool, default False
            If ``True``, only rank numeric columns.
        na_option : {{'bottom'}}, default 'bottom'
            Only ``'bottom'`` is supported.
        ascending : bool, default True
            Whether to rank in ascending order.
        pct : bool, default False
            If ``True``, compute percentage ranks
            (SQL ``PERCENT_RANK()``). Returns a ``FloatSeries``.
            Only supported with ``method='min'``.

        Returns
        -------
        Series
            An ``IntegerSeries`` (or ``FloatSeries`` when
            ``pct=True``) containing the ranks.

        Raises
        ------
        NotImplementedError
            If called on a computed Series expression
            (e.g. ``(frame['col'] + 5).rank()``). Call ``rank()``
            first, then apply arithmetic.
            If ``method`` is not ``'min'``, ``'first'``, or
            ``'dense'``.
            If ``na_option`` is not ``'bottom'``.
            If ``pct=True`` with a method other than ``'min'``.

        See Also
        --------
        PandasApiTdsFrame.rank : Rank all columns of a frame.
        PandasApiGroupbyTdsFrame.rank : Rank within groups.

        Notes
        -----
        **Differences from pandas:**

        - The ``'average'`` and ``'max'`` methods are **not
          supported**.
        - ``na_option`` only supports ``'bottom'``.
        - ``pct=True`` is only supported with ``method='min'``.
        - The result is a ``Series``, not a ``pandas.Series``.
          It can be assigned to the frame or executed directly.
        - Calling ``rank()`` on a **computed** Series expression is
          **not supported**. Do the rank first, then apply
          arithmetic: ``frame['col'].rank() + 5``.
        - Only **one** window-function call is allowed per
          expression. To combine multiple, use separate assignments.

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            # Execute a ranked Series directly as a single-column query
            frame["Order Id"].rank().to_pandas().head()

            # Assign a rank column to the frame
            frame["Order Rank"] = frame["Order Id"].rank()
            frame.head(5).to_pandas()

        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            # Percentage rank, descending
            frame["Order Pct"] = frame["Order Id"].rank(pct=True, ascending=False)
            frame.head(5).to_pandas()

        """
        if self._expr is not None:  # pragma: no cover
            error_msg = '''
                Applying rank function to a computed series expression is not supported yet.
                For example,
                    not supported: (frame['col'] + 5).rank()
                    supported: frame['col'].rank() + 5
            '''
            error_msg = dedent(error_msg).strip()
            raise NotImplementedError(error_msg)

        new_series: Series
        if pct:
            new_series = FloatSeries(self._filtered_frame, self.columns()[0].get_name())
        else:
            new_series = IntegerSeries(self._filtered_frame, self.columns()[0].get_name())
        new_series._base_frame = self._base_frame

        applied_function_frame = self._filtered_frame.rank(axis, method, numeric_only, na_option, ascending, pct)
        assert isinstance(applied_function_frame, PandasApiAppliedFunctionTdsFrame)

        new_series._filtered_frame = applied_function_frame
        return new_series


@add_primitive_methods
class BooleanSeries(Series, PyLegendBoolean, PyLegendExpressionBooleanReturn):  # type: ignore
    def __init__(
            self, base_frame: "PandasApiBaseTdsFrame", column: str, value: PyLegendOptional[PyLegendExpression] = None
    ) -> None:
        super().__init__(base_frame, column, value)  # pragma: no cover (Boolean column not supported in PURE)
        PyLegendBoolean.__init__(self, self)  # pragma: no cover (Boolean column not supported in PURE)


@add_primitive_methods
class StringSeries(Series, PyLegendString, PyLegendExpressionStringReturn):  # type: ignore
    def __init__(
            self, base_frame: "PandasApiBaseTdsFrame", column: str, value: PyLegendOptional[PyLegendExpression] = None
    ) -> None:
        super().__init__(base_frame, column, value)
        PyLegendString.__init__(self, self)


@add_primitive_methods
class NumberSeries(Series, PyLegendNumber, PyLegendExpressionNumberReturn):  # type: ignore
    def __init__(
            self, base_frame: "PandasApiBaseTdsFrame", column: str, value: PyLegendOptional[PyLegendExpression] = None
    ) -> None:
        super().__init__(base_frame, column, value)
        PyLegendNumber.__init__(self, self)


@add_primitive_methods
class IntegerSeries(NumberSeries, PyLegendInteger, PyLegendExpressionIntegerReturn):  # type: ignore
    def __init__(
            self, base_frame: "PandasApiBaseTdsFrame", column: str, value: PyLegendOptional[PyLegendExpression] = None
    ) -> None:
        super().__init__(base_frame, column, value)
        PyLegendInteger.__init__(self, self)


@add_primitive_methods
class FloatSeries(NumberSeries, PyLegendFloat, PyLegendExpressionFloatReturn):  # type: ignore
    def __init__(
            self, base_frame: "PandasApiBaseTdsFrame", column: str, value: PyLegendOptional[PyLegendExpression] = None
    ) -> None:
        super().__init__(base_frame, column, value)
        PyLegendFloat.__init__(self, self)


@add_primitive_methods
class DecimalSeries(NumberSeries, PyLegendDecimal, PyLegendExpressionDecimalReturn):  # type: ignore
    def __init__(
            self, base_frame: "PandasApiBaseTdsFrame", column: str, value: PyLegendOptional[PyLegendExpression] = None
    ) -> None:
        super().__init__(base_frame, column, value)
        PyLegendDecimal.__init__(self, self)


@add_primitive_methods
class DateSeries(Series, PyLegendDate, PyLegendExpressionDateReturn):  # type: ignore
    def __init__(
            self, base_frame: "PandasApiBaseTdsFrame", column: str, value: PyLegendOptional[PyLegendExpression] = None
    ) -> None:
        super().__init__(base_frame, column, value)
        PyLegendDate.__init__(self, self)


@add_primitive_methods
class DateTimeSeries(DateSeries, PyLegendDateTime, PyLegendExpressionDateTimeReturn):  # type: ignore
    def __init__(
            self, base_frame: "PandasApiBaseTdsFrame", column: str, value: PyLegendOptional[PyLegendExpression] = None
    ) -> None:
        super().__init__(base_frame, column, value)
        PyLegendDateTime.__init__(self, self)


@add_primitive_methods
class StrictDateSeries(DateSeries, PyLegendStrictDate, PyLegendExpressionStrictDateReturn):  # type: ignore
    def __init__(
            self, base_frame: "PandasApiBaseTdsFrame", column: str, value: PyLegendOptional[PyLegendExpression] = None
    ) -> None:
        super().__init__(base_frame, column, value)
        PyLegendStrictDate.__init__(self, self)

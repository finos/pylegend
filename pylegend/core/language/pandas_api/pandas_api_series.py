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
    has_window_function, get_pure_query_from_expr, get_series_from_col_type
from pylegend.core.tds.pandas_api.frames.pandas_api_applied_function_tds_frame import PandasApiAppliedFunctionTdsFrame
from pylegend.core.tds.pandas_api.frames.pandas_api_base_tds_frame import PandasApiBaseTdsFrame
from pylegend.core.tds.result_handler import ResultHandler, ToStringResultHandler
from pylegend.core.tds.sql_query_helpers import create_sub_query
from pylegend.core.tds.tds_column import TdsColumn
from pylegend.core.tds.tds_frame import FrameToPureConfig
from pylegend.core.tds.tds_frame import FrameToSqlConfig
from pylegend.extensions.tds.result_handler import PandasDfReadConfig, ToPandasDfResultHandler

if TYPE_CHECKING:
    from pylegend.core.language.pandas_api.pandas_api_frame_spec import FrameSpec
    from pylegend.core.tds.pandas_api.frames.pandas_api_tds_frame import PandasApiTdsFrame
    from pylegend.core.language.pandas_api.pandas_api_window_series import WindowSeries

__all__: PyLegendSequence[str] = [
    "Series",
    "BooleanSeries",
    "StringSeries",
    "NumberSeries",
    "IntegerSeries",
    "FloatSeries",
    "DateSeries",
    "DateTimeSeries",
    "DecimalSeries",
    "StrictDateSeries",
    "SupportsToSqlExpression",
    "SupportsToPureExpression",
]

R = PyLegendTypeVar('R')


_COL_TYPE_TO_SERIES_CLASS_NAME: PyLegendDict[str, str] = {
    "Boolean": "BooleanSeries",
    "String": "StringSeries",
    "Varchar": "StringSeries",
    "Number": "NumberSeries",
    "Integer": "IntegerSeries",
    "TinyInt": "IntegerSeries",
    "UTinyInt": "IntegerSeries",
    "SmallInt": "IntegerSeries",
    "USmallInt": "IntegerSeries",
    "Int": "IntegerSeries",
    "UInt": "IntegerSeries",
    "BigInt": "IntegerSeries",
    "UBigInt": "IntegerSeries",
    "Float": "FloatSeries",
    "Float4": "FloatSeries",
    "Double": "FloatSeries",
    "Decimal": "DecimalSeries",
    "Numeric": "DecimalSeries",
    "Date": "DateSeries",
    "DateTime": "DateTimeSeries",
    "Timestamp": "DateTimeSeries",
    "StrictDate": "StrictDateSeries",
}


def _get_new_series_for_column(
        base_frame: "PandasApiBaseTdsFrame",
        column: TdsColumn,
        applied_function_frame: PyLegendOptional[PandasApiAppliedFunctionTdsFrame] = None,
) -> "Series":
    col_type = column.get_type()
    col_name = column.get_name()

    series_cls = get_series_from_col_type(col_type)

    new_series: Series = series_cls(base_frame, col_name)
    if applied_function_frame is not None:
        new_series._filtered_frame = applied_function_frame
    return new_series


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
        raises ``ValueError``.

    Window functions on a Series
    ----------------------------
    Certain window functions such as ``rank()`` can be called on a
    ``Series``.  The result is a new ``Series`` whose values are the
    window-function output for that column.

    Series with applied functions (aggregations or window functions)
    can also be combined with arithmetic in the
    same assignment, but only **one** function call is allowed
    per expression.  If multiple function calls are needed,
    split them into separate steps.

    See Also
    --------
    PandasApiTdsFrame : The parent frame class.
    PandasApiGroupbyTdsFrame : Groupby object (returns
        ``GroupbySeries`` when bracket-indexed).

    Notes
    -----
    **Differences from pandas:**

    - A ``Series`` is **not** a first-class data container. It is an
      expression builder that lazily constructs the query. No data
      is materialised until ``execute_frame_to_string()`` or
      ``to_pandas()`` is called.
    - Cross-frame assignment is **not allowed**. In pandas you can
      freely assign a Series from one DataFrame to another (alignment
      happens on the index); here the Series must originate from the
      **same** frame instance. If you need cross-frame assignment, use join or merge.
    - Applying a function on a computed series expression is **not
      supported** in certain cases. For example,
      ``(frame['col'] + 5).rank()`` raises ``NotImplementedError``.
      Instead, do ``frame['col'].rank() + 5``.

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
        doubled.to_pandas().head()

        # String methods on a StringSeries
        upper_name = frame["Ship Name"].upper()
        upper_name.to_pandas().head()

        # Overwrite an existing column
        frame["Ship Name"] = frame["Ship Name"].upper()
        frame.head(5).to_pandas()

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

        full_sql_expr = self.to_sql_expression({'c': base_query}, config)

        if expr_contains_window_func:
            from pylegend.core.tds.pandas_api.frames.helpers.series_helper import split_window_from_arithmetic
            window_expr, make_outer = split_window_from_arithmetic(full_sql_expr)

            temp_col_name = db_extension.quote_identifier(col_name + temp_column_name_suffix)
            base_query.select.selectItems = [SingleColumn(temp_col_name, window_expr)]

            new_query = create_sub_query(base_query, config, "root")
            col_ref = QualifiedNameReference(QualifiedName([
                db_extension.quote_identifier("root"), temp_col_name
            ]))
            outer_expr = make_outer(col_ref) if make_outer is not None else col_ref
            new_query.select.selectItems = [
                SingleColumn(db_extension.quote_identifier(col_name), outer_expr)
            ]
            return new_query
        else:
            base_query.select.selectItems = [
                SingleColumn(db_extension.quote_identifier(col_name), full_sql_expr)
            ]
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
    ) -> PyLegendUnion["PandasApiTdsFrame", "Series"]:
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

        aggregated_frame = self._filtered_frame.aggregate(func, axis, *args, **kwargs)
        assert isinstance(aggregated_frame, PandasApiAppliedFunctionTdsFrame)

        potential_num_cols = len(aggregated_frame.columns())
        if potential_num_cols == 1:
            return _get_new_series_for_column(self._base_frame, aggregated_frame.columns()[0], aggregated_frame)
        else:
            return aggregated_frame

    def agg(
            self,
            func: PyLegendAggInput,
            axis: PyLegendUnion[int, str] = 0,
            *args: PyLegendPrimitiveOrPythonPrimitive,
            **kwargs: PyLegendPrimitiveOrPythonPrimitive
    ) -> PyLegendUnion["PandasApiTdsFrame", "Series"]:
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
    ) -> PyLegendUnion["PandasApiTdsFrame", "Series"]:
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
    ) -> PyLegendUnion["PandasApiTdsFrame", "Series"]:
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
    ) -> PyLegendUnion["PandasApiTdsFrame", "Series"]:
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
    ) -> PyLegendUnion["PandasApiTdsFrame", "Series"]:
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
    ) -> PyLegendUnion["PandasApiTdsFrame", "Series"]:
        """
        Return the standard deviation of the Series values.

        Parameters
        ----------
        axis : {{0, 'index'}}, default 0
            Must be ``0`` or ``'index'``.
        skipna : bool, default True
            Must be ``True``. ``False`` is not supported.
        ddof : int, default 1
            Degrees of freedom. ``1`` for sample standard deviation
            (``STDDEV_SAMP``), ``0`` for population standard deviation
            (``STDDEV_POP``).
        numeric_only : bool, default False
            Must be ``False``. ``True`` is not supported.

        Returns
        -------
        PandasApiTdsFrame
            A single-row frame with the standard deviation.

        Raises
        ------
        NotImplementedError
            If ``ddof`` is not ``0`` or ``1``, or if ``skipna``,
            ``numeric_only``, or ``**kwargs`` are set to unsupported
            values.

        Notes
        -----
        Equivalent to ``series.aggregate("std")`` (ddof=1) or
        ``series.aggregate("std_dev_population")`` (ddof=0). Maps to
        SQL ``STDDEV_SAMP()`` or ``STDDEV_POP()``.

        **Differences from pandas:** returns a single-row
        ``PandasApiTdsFrame`` instead of a scalar. Only ``ddof=0``
        and ``ddof=1`` are supported.

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
        if ddof not in (0, 1):
            raise NotImplementedError(
                f"Only ddof=0 (Population) and ddof=1 (Sample) are supported in std function, but got: {ddof}")
        if numeric_only is not False:
            raise NotImplementedError("numeric_only=True is not currently supported in std function.")
        if len(kwargs) > 0:
            raise NotImplementedError(
                f"Additional keyword arguments not supported in std function: {list(kwargs.keys())}")
        return self.aggregate("std" if ddof == 1 else "std_dev_population", 0)

    def var(
            self,
            axis: PyLegendUnion[int, str] = 0,
            skipna: bool = True,
            ddof: int = 1,
            numeric_only: bool = False,
            **kwargs: PyLegendPrimitiveOrPythonPrimitive
    ) -> PyLegendUnion["PandasApiTdsFrame", "Series"]:
        """
        Return the variance of the Series values.

        Parameters
        ----------
        axis : {{0, 'index'}}, default 0
            Must be ``0`` or ``'index'``.
        skipna : bool, default True
            Must be ``True``. ``False`` is not supported.
        ddof : int, default 1
            Degrees of freedom. ``1`` for sample variance
            (``VAR_SAMP``), ``0`` for population variance
            (``VAR_POP``).
        numeric_only : bool, default False
            Must be ``False``. ``True`` is not supported.

        Returns
        -------
        PandasApiTdsFrame
            A single-row frame with the variance.

        Raises
        ------
        NotImplementedError
            If ``ddof`` is not ``0`` or ``1``, or if ``skipna``,
            ``numeric_only``, or ``**kwargs`` are set to unsupported
            values.

        Notes
        -----
        Equivalent to ``series.aggregate("var")`` (ddof=1) or
        ``series.aggregate("variance_population")`` (ddof=0). Maps to
        SQL ``VAR_SAMP()`` or ``VAR_POP()``.

        **Differences from pandas:** returns a single-row
        ``PandasApiTdsFrame`` instead of a scalar. Only ``ddof=0``
        and ``ddof=1`` are supported.

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
        if ddof not in (0, 1):
            raise NotImplementedError(
                f"Only ddof=0 (Population) and ddof=1 (Sample) are supported in var function, but got: {ddof}")
        if numeric_only is not False:
            raise NotImplementedError("numeric_only=True is not currently supported in var function.")
        if len(kwargs) > 0:
            raise NotImplementedError(
                f"Additional keyword arguments not supported in var function: {list(kwargs.keys())}")
        return self.aggregate("var" if ddof == 1 else "variance_population", 0)

    def count(
            self,
            axis: PyLegendUnion[int, str] = 0,
            numeric_only: bool = False,
            **kwargs: PyLegendPrimitiveOrPythonPrimitive
    ) -> PyLegendUnion["PandasApiTdsFrame", "Series"]:
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

    def expanding(
            self,
            min_periods: int = 1,
            axis: PyLegendUnion[int, str] = 0,
            method: PyLegendOptional[str] = None,
            order_by: PyLegendOptional[PyLegendUnion[str, PyLegendSequence[str]]] = None,
            ascending: PyLegendUnion[bool, "PyLegendSequence[bool]"] = True,
    ) -> "WindowSeries":
        """
        Create an expanding (cumulative) window on this column.

        An expanding window includes all rows from the start of the
        frame up to the current row, enabling running totals, running
        averages, and similar cumulative calculations on a single
        column.

        Parameters
        ----------
        min_periods : int, default 1
            Minimum number of observations required to produce a value.
        axis : {{0, 'index'}}, default 0
            Only ``0`` / ``'index'`` is supported.
        method : str, optional
            Not supported. Must be ``None``.
        order_by : str or list of str, optional
            Column(s) to order by within the window.
        ascending : bool or list of bool, default True
            Sort direction(s) for ``order_by`` columns.

        Returns
        -------
        WindowSeries
            A window series on which aggregates (``sum``, ``mean``,
            etc.) can be called.

        Raises
        ------
        NotImplementedError
            If ``axis`` is not ``0``, or ``method`` is not ``None``.

        See Also
        --------
        rolling : Fixed-size sliding window on a column.
        window_frame_legend_ext : Custom window specification.

        Notes
        -----
        **Differences from pandas:**

        - ``order_by`` and ``ascending`` are pylegend extensions not
          present in pandas.
        - ``axis=1`` and ``method`` are **not supported**.

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            frame["Order Id"].expanding(
                order_by="Order Id"
            ).sum().head(5).to_pandas()

        """
        from pylegend.core.language.pandas_api.pandas_api_window_series import WindowSeries

        window_frame = self._base_frame.expanding(
            min_periods=min_periods, axis=axis, method=method, order_by=order_by, ascending=ascending
        )
        return WindowSeries(window_frame=window_frame, column_name=self.columns()[0].get_name())

    def rolling(
            self,
            window: int,
            min_periods: PyLegendOptional[int] = None,
            center: bool = False,
            win_type: PyLegendOptional[str] = None,
            on: PyLegendOptional[str] = None,
            axis: PyLegendUnion[int, str] = 0,
            closed: PyLegendOptional[str] = None,
            step: PyLegendOptional[int] = None,
            method: PyLegendOptional[str] = None,
            order_by: PyLegendOptional[PyLegendUnion[str, PyLegendSequence[str]]] = None,
            ascending: PyLegendUnion[bool, "PyLegendSequence[bool]"] = True,
    ) -> "WindowSeries":
        """
        Create a fixed-size sliding window on this column.

        A rolling window includes a fixed number of preceding rows for
        each row, enabling moving averages, moving sums, and similar
        calculations on a single column.

        Parameters
        ----------
        window : int
            Size of the moving window (number of rows).
        min_periods : int, optional
            Minimum observations required. Defaults to ``window``.
        center : bool, default False
            Not supported. Must be ``False``.
        win_type : str, optional
            Not supported. Must be ``None``.
        on : str, optional
            Not supported. Must be ``None``.
        axis : {{0, 'index'}}, default 0
            Only ``0`` / ``'index'`` is supported.
        closed : str, optional
            Not supported. Must be ``None``.
        step : int, optional
            Not supported. Must be ``None``.
        method : str, optional
            Not supported. Must be ``None``.
        order_by : str or list of str, optional
            Column(s) to order by within the window.
        ascending : bool or list of bool, default True
            Sort direction(s) for ``order_by`` columns.

        Returns
        -------
        WindowSeries
            A window series on which aggregates (``sum``, ``mean``,
            etc.) can be called.

        Raises
        ------
        NotImplementedError
            If ``center``, ``win_type``, ``on``, ``closed``, ``step``,
            or ``method`` are set to non-default values, or ``axis``
            is not ``0``.

        See Also
        --------
        expanding : Expanding (cumulative) window on a column.
        window_frame_legend_ext : Custom window specification.

        Notes
        -----
        **Differences from pandas:**

        - ``order_by`` and ``ascending`` are pylegend extensions not
          present in pandas.
        - ``center``, ``win_type``, ``on``, ``closed``, ``step``, and
          ``method`` are **not supported**.
        - ``axis=1`` is **not supported**.

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            frame["Order Id"].rolling(
                window=3, order_by="Order Id"
            ).mean().head(5).to_pandas()

        """
        from pylegend.core.language.pandas_api.pandas_api_window_series import WindowSeries

        window_frame = self._base_frame.rolling(
            window=window, min_periods=min_periods, center=center, win_type=win_type,
            on=on, axis=axis, closed=closed, step=step, method=method, order_by=order_by,
            ascending=ascending
        )
        return WindowSeries(window_frame=window_frame, column_name=self.columns()[0].get_name())

    def window_frame_legend_ext(
            self,
            frame_spec: "FrameSpec",
            order_by: PyLegendOptional[PyLegendUnion[str, PyLegendSequence[str]]] = None,
            ascending: PyLegendUnion[bool, "PyLegendSequence[bool]"] = True,
    ) -> "WindowSeries":
        """
        Create a custom window specification on this column.

        **PyLegend extension** — not present in pandas.

        The ``frame_spec`` argument controls the ``ROWS BETWEEN`` or
        ``RANGE BETWEEN`` clause.

        Parameters
        ----------
        frame_spec : RowsBetween or RangeBetween
            A window-frame specification created via
            :meth:`~PandasApiBaseTdsFrame.rows_between` or
            :meth:`~PandasApiBaseTdsFrame.range_between`.
        order_by : str or list of str, optional
            Column(s) to order by within the window.
        ascending : bool or list of bool, default True
            Sort direction(s) for ``order_by`` columns.

        Returns
        -------
        WindowSeries
            A window series on which aggregates can be called.

        Raises
        ------
        TypeError
            If ``frame_spec`` is not a ``RowsBetween`` or ``RangeBetween``.

        See Also
        --------
        expanding : Cumulative window on a column.
        rolling : Fixed-size sliding window on a column.

        Notes
        -----
        **Differences from pandas:**

        - This method has **no pandas equivalent**. It is a pylegend
          extension for fine-grained control over the SQL
          ``ROWS BETWEEN`` / ``RANGE BETWEEN`` clause.

        Examples
        --------
        .. ipython:: python

            import pylegend
            from pylegend.core.language.pandas_api.pandas_api_frame_spec import (
                RowsBetween,
            )
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            spec = RowsBetween(-2, 0)
            frame["Order Id"].window_frame_legend_ext(
                spec, order_by="Order Id"
            ).sum().head(5).to_pandas()

        """
        from pylegend.core.language.pandas_api.pandas_api_window_series import WindowSeries

        window_frame = self._base_frame.window_frame_legend_ext(
            frame_spec=frame_spec, order_by=order_by, ascending=ascending
        )
        return WindowSeries(window_frame=window_frame, column_name=self.columns()[0].get_name())

    def cume_dist_legend_ext(
            self,
            ascending: bool = True,
    ) -> "Series":
        """
        Compute the cumulative distribution of this column.

        **PyLegend extension** — not present in pandas.

        Maps to SQL ``CUME_DIST() OVER (ORDER BY col)`` and Pure
        ``cumulativeDistribution``.

        Parameters
        ----------
        ascending : bool, default True
            Whether to order in ascending direction.

        Returns
        -------
        FloatSeries
            A series containing cumulative distribution values
            (floats between 0 and 1).

        See Also
        --------
        rank : Compute ranks.
        ntile_legend_ext : Assign rows to numbered buckets.

        Notes
        -----
        **Differences from pandas:**

        - This method has **no pandas equivalent**. ``CUME_DIST`` is
          exposed as a pylegend extension.

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            frame["CumeDist"] = frame["Order Id"].cume_dist_legend_ext()
            frame.head(5).to_pandas()

        """
        new_series: Series = FloatSeries(self._filtered_frame, self.columns()[0].get_name())
        new_series._base_frame = self._base_frame

        applied_function_frame = self._filtered_frame.cume_dist_legend_ext(ascending=ascending)
        assert isinstance(applied_function_frame, PandasApiAppliedFunctionTdsFrame)

        new_series._filtered_frame = applied_function_frame
        return new_series

    def ntile_legend_ext(
            self,
            num_buckets: int,
            ascending: bool = True,
    ) -> "Series":
        """
        Assign rows to numbered buckets based on this column's ordering.

        **PyLegend extension** — not present in pandas.

        Maps to SQL ``NTILE(n) OVER (ORDER BY col)`` and Pure ``ntile``.

        Parameters
        ----------
        num_buckets : int
            Number of buckets to distribute rows into.
        ascending : bool, default True
            Whether to order in ascending direction.

        Returns
        -------
        IntegerSeries
            A series containing bucket numbers (1-based).

        See Also
        --------
        rank : Compute ranks.
        cume_dist_legend_ext : Cumulative distribution.

        Notes
        -----
        **Differences from pandas:**

        - This method has **no pandas equivalent**. ``NTILE`` is
          exposed as a pylegend extension.

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            frame["Quartile"] = frame["Order Id"].ntile_legend_ext(4)
            frame.head(5).to_pandas()

        """
        new_series: Series = IntegerSeries(self._filtered_frame, self.columns()[0].get_name())
        new_series._base_frame = self._base_frame

        applied_function_frame = self._filtered_frame.ntile_legend_ext(
            num_buckets=num_buckets, ascending=ascending,
        )
        assert isinstance(applied_function_frame, PandasApiAppliedFunctionTdsFrame)

        new_series._filtered_frame = applied_function_frame
        return new_series

    def concat_legend_ext(
            self,
            other: "Series",
    ) -> "Series":
        """
        Concatenate this series with another series vertically.

        **PyLegend extension** — not present in pandas.

        Performs a ``UNION ALL`` of this series with ``other``. Both
        series must have compatible schemas (same column name and type).

        Parameters
        ----------
        other : Series
            Another series with the same column name and type.

        Returns
        -------
        Series
            A new series containing rows from both series.

        Raises
        ------
        ValueError
            If the schemas of the two series are incompatible.

        Notes
        -----
        **Differences from pandas:**

        - In pandas, ``pd.concat`` is a top-level function. Here,
          ``concat_legend_ext`` is a method on a ``Series`` and only
          supports vertical concatenation (``UNION ALL``) of two
          single-column series with the same schema.

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            s1 = frame["Order Id"].head(3)
            s2 = frame["Order Id"].head(3)
            s1.concat_legend_ext(s2).to_pandas()

        """
        concat_frame = self._filtered_frame.concat_legend_ext(other._filtered_frame)
        assert isinstance(concat_frame, PandasApiAppliedFunctionTdsFrame)

        col = concat_frame.columns()[0]
        new_series = _get_new_series_for_column(self._base_frame, col, concat_frame)
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

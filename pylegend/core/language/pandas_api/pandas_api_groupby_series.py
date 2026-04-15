# Copyright 2026 Goldman Sachs
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
import pandas as pd
from pylegend._typing import (
    TYPE_CHECKING,
    PyLegendCallable,
    PyLegendDict,
    PyLegendOptional,
    PyLegendSequence,
    PyLegendTypeVar,
    PyLegendUnion
)
from pylegend.core.database.sql_to_string import SqlToStringConfig, SqlToStringFormat
from pylegend.core.language.pandas_api.pandas_api_aggregate_specification import PyLegendAggInput
from pylegend.core.language.pandas_api.pandas_api_frame_spec import FrameSpec, RowsBetween
from pylegend.core.language.pandas_api.pandas_api_series import (
    SupportsToPureExpression,
    SupportsToSqlExpression
)
from pylegend.core.language.pandas_api.pandas_api_tds_row import PandasApiTdsRow
from pylegend.core.language.shared.column_expressions import PyLegendColumnExpression
from pylegend.core.language.shared.expression import (
    PyLegendExpressionBooleanReturn,
    PyLegendExpressionDateReturn,
    PyLegendExpressionDateTimeReturn,
    PyLegendExpressionFloatReturn,
    PyLegendExpressionDecimalReturn,
    PyLegendExpressionIntegerReturn,
    PyLegendExpressionNumberReturn,
    PyLegendExpressionStrictDateReturn,
    PyLegendExpressionStringReturn,
    PyLegendExpression,
)
from pylegend.core.language.shared.primitives.boolean import PyLegendBoolean
from pylegend.core.language.shared.primitives.date import PyLegendDate
from pylegend.core.language.shared.primitives.datetime import PyLegendDateTime
from pylegend.core.language.shared.primitives.float import PyLegendFloat
from pylegend.core.language.shared.primitives.decimal import PyLegendDecimal
from pylegend.core.language.shared.primitives.integer import PyLegendInteger
from pylegend.core.language.shared.primitives.number import PyLegendNumber
from pylegend.core.language.shared.primitives.primitive import (
    PyLegendPrimitive,
    PyLegendPrimitiveOrPythonPrimitive
)
from pylegend.core.language.shared.primitives.strictdate import PyLegendStrictDate
from pylegend.core.language.shared.primitives.string import PyLegendString
from pylegend.core.sql.metamodel import Expression, QuerySpecification, SingleColumn, QualifiedNameReference, QualifiedName
from pylegend.core.tds.abstract.frames.base_tds_frame import BaseTdsFrame
from pylegend.core.tds.pandas_api.frames.helpers.series_helper import (
    assert_and_find_core_series,
    add_primitive_methods, has_window_function,
    needs_zero_column_for_window,
    get_pure_query_from_expr, get_groupby_series_from_col_type, query_contains_column_with_name,
)
from pylegend.core.tds.pandas_api.frames.pandas_api_applied_function_tds_frame import PandasApiAppliedFunctionTdsFrame
from pylegend.core.tds.pandas_api.frames.pandas_api_groupby_tds_frame import PandasApiGroupbyTdsFrame
from pylegend.core.tds.result_handler import ResultHandler, ToStringResultHandler
from pylegend.core.tds.sql_query_helpers import create_sub_query
from pylegend.core.tds.tds_column import TdsColumn
from pylegend.core.tds.tds_frame import FrameToPureConfig, FrameToSqlConfig
from pylegend.extensions.tds.result_handler import PandasDfReadConfig, ToPandasDfResultHandler

if TYPE_CHECKING:
    from pylegend.core.tds.pandas_api.frames.pandas_api_tds_frame import PandasApiTdsFrame
    from pylegend.core.language.pandas_api.pandas_api_window_series import WindowSeries

__all__: PyLegendSequence[str] = [
    "GroupbySeries",
    "BooleanGroupbySeries",
    "StringGroupbySeries",
    "NumberGroupbySeries",
    "IntegerGroupbySeries",
    "FloatGroupbySeries",
    "DateGroupbySeries",
    "DateTimeGroupbySeries",
    "DecimalGroupbySeries",
    "StrictDateGroupbySeries",
]

R = PyLegendTypeVar('R')


def _get_new_groupby_series_for_column(
        base_groupby_frame: PandasApiGroupbyTdsFrame,
        aggregated_frame: PandasApiAppliedFunctionTdsFrame,
        column: TdsColumn,
) -> "GroupbySeries":
    col_type = column.get_type()

    groupby_series_cls = get_groupby_series_from_col_type(col_type)
    return groupby_series_cls(base_groupby_frame, aggregated_frame)


class GroupbySeries(PyLegendColumnExpression, PyLegendPrimitive, BaseTdsFrame):
    """
    A single-column proxy within a grouped context.

    A ``GroupbySeries`` is the grouped counterpart of
    :class:`~pylegend.core.language.pandas_api.pandas_api_series.Series`.
    It represents one column of a
    :class:`~pylegend.core.tds.pandas_api.frames.pandas_api_groupby_tds_frame.PandasApiGroupbyTdsFrame`
    and is obtained by bracket-indexing a groupby object with a
    **single** column name.

    Obtaining a GroupbySeries
    -------------------------
    Use bracket notation on a ``PandasApiGroupbyTdsFrame``:

    .. code-block:: python

        grouped = frame.groupby("group_col")
        gseries = grouped["value_col"]   # -> GroupbySeries

    Passing a **list** of column names returns a narrowed
    ``PandasApiGroupbyTdsFrame`` instead (not a ``GroupbySeries``):

    .. code-block:: python

        grouped[["col_a", "col_b"]]  # -> PandasApiGroupbyTdsFrame

    The returned subclass matches the column type, following the
    same mapping as ``Series``.
    For example, an integer column becomes an IntegerGroupbySeries.

    Operations
    ----------
    A ``GroupbySeries`` **must** have an applied function (such as
    an aggregation or ``rank()``) before it can be executed or
    assigned. Attempting to call ``to_sql_query()`` on a bare
    ``GroupbySeries`` without an applied function raises
    ``RuntimeError``.

    Typical usage patterns:

    - **Grouped aggregation** — call an aggregation method directly:

      .. code-block:: python

          frame.groupby("grp")["val"].sum()
          frame.groupby("grp")["val"].aggregate(["sum", "mean"])

    - **Grouped rank** — call ``rank()`` to get a window-ranked
      ``GroupbySeries`` that can be assigned back:

      .. code-block:: python

          frame["ranked"] = frame.groupby("grp")["val"].rank()

    Assigning back to the frame
    ---------------------------
    A ``GroupbySeries`` (with an applied function like ``rank()``)
    can be assigned back to the parent
    :class:`~pylegend.core.tds.pandas_api.frames.pandas_api_tds_frame.PandasApiTdsFrame`
    using bracket assignment:

    .. code-block:: python

        frame["new_col"] = frame.groupby("grp")["val"].rank()

    The assignment **must** target the same frame that was grouped.

    See Also
    --------
    Series : The non-grouped single-column proxy.
    PandasApiGroupbyTdsFrame : The groupby object that produces this.
    PandasApiTdsFrame.groupby : Create a groupby object.

    Notes
    -----
    **Differences from pandas:**

    - A ``GroupbySeries`` is **not** iterable and does not support
      direct data access. It is an expression builder that lazily
      constructs the query.
    - Applying functions on a **computed** ``GroupbySeries`` expression is
      **not supported**. For example,
      ``(frame.groupby('grp')['col'] + 5).sum()`` raises
      ``NotImplementedError``. Instead, do
      ``frame.groupby('grp')['col'].sum() + 5``.
    - Only **one** function call is allowed per expression.
      To combine multiple, use separate assignment steps.
    - A bare ``GroupbySeries`` (without an aggregation or window
      function) **cannot be executed**. You must call an operation
      such as ``sum()``, ``rank()``, etc. first.

    Examples
    --------
    .. ipython:: python

        import pylegend
        frame = pylegend.samples.pandas_api.northwind_orders_frame()

        # Grouped aggregation via GroupbySeries
        frame.groupby("Ship Name")["Order Id"].sum().to_pandas().head()

        frame = pylegend.samples.pandas_api.northwind_orders_frame()

        # Assign a grouped rank back to the frame
        frame["Order Rank"] = frame.groupby("Ship Name")["Order Id"].rank()
        frame.head(5).to_pandas()

        frame = pylegend.samples.pandas_api.northwind_orders_frame()

        # Arithmetic with a grouped rank
        frame["Grouped Rank"] = frame.groupby(
            "Ship Name"
        )["Order Id"].rank()
        frame.head(5).to_pandas()

    """
    _base_groupby_frame: PandasApiGroupbyTdsFrame
    _applied_function_frame: PyLegendOptional[PandasApiAppliedFunctionTdsFrame]

    def __init__(
            self,
            base_groupby_frame: PandasApiGroupbyTdsFrame,
            applied_function_frame: PyLegendOptional[PandasApiAppliedFunctionTdsFrame] = None,
            expr: PyLegendOptional[PyLegendExpression] = None
    ) -> None:
        selected_columns = base_groupby_frame.get_selected_columns()
        assert selected_columns is not None and len(selected_columns) == 1, (
            "To initialize a GroupbySeries object, exactly one column must be selected, "
            f"but got selected columns: {[str(col) for col in selected_columns] if selected_columns is not None else None}"
        )

        row = PandasApiTdsRow.from_tds_frame("c", base_groupby_frame.base_frame())
        PyLegendColumnExpression.__init__(self, row=row, column=selected_columns[0].get_name())

        self._base_groupby_frame: PandasApiGroupbyTdsFrame = base_groupby_frame
        self._applied_function_frame = applied_function_frame

        self._expr = expr
        if self._expr is not None:
            assert_and_find_core_series(self._expr)

    @property
    def expr(self) -> PyLegendOptional[PyLegendExpression]:
        return self._expr

    @property
    def applied_function_frame(self) -> PyLegendOptional[PandasApiAppliedFunctionTdsFrame]:
        return self._applied_function_frame

    def raise_exception_if_no_function_applied(self) -> PandasApiAppliedFunctionTdsFrame:
        if self._applied_function_frame is None:
            raise RuntimeError(
                "The 'groupby' function requires at least one operation to be performed right after it (e.g. aggregate, rank)"
            )
        return self._applied_function_frame

    def get_base_frame(self) -> "PandasApiGroupbyTdsFrame":
        return self._base_groupby_frame

    def get_leaf_expressions(self) -> PyLegendSequence["PyLegendExpression"]:
        if self.expr is not None:
            return self.expr.get_leaf_expressions()
        return [self]

    def to_sql_expression(
            self,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        if self.expr is not None:
            return self.expr.to_sql_expression(frame_name_to_base_query_map, config)

        applied_function_frame = self.raise_exception_if_no_function_applied()
        applied_func = applied_function_frame.get_applied_function()
        if isinstance(applied_func, SupportsToSqlExpression):
            return applied_func.to_sql_expression(frame_name_to_base_query_map, config)

        raise NotImplementedError(  # pragma: no cover
            f"The '{applied_func.name()}' function cannot provide a SQL expression"
        )

    def to_pure_expression(self, config: FrameToPureConfig) -> str:
        if self._expr is not None:
            return self._expr.to_pure_expression(config)

        applied_function_frame = self.raise_exception_if_no_function_applied()
        applied_func = applied_function_frame.get_applied_function()
        if isinstance(applied_func, SupportsToPureExpression):
            return applied_func.to_pure_expression(config)

        raise NotImplementedError(  # pragma: no cover
            f"The '{applied_func.name()}' function cannot provide a pure expression"
        )

    def columns(self) -> PyLegendSequence[TdsColumn]:
        if self.has_applied_function():
            assert self.applied_function_frame is not None
            return self.applied_function_frame.columns()
        selected_columns = self.get_base_frame().get_selected_columns()
        assert selected_columns is not None and len(selected_columns) == 1
        return selected_columns

    def to_sql_query(self, config: FrameToSqlConfig = FrameToSqlConfig()) -> str:
        query = self.to_sql_query_object(config)
        sql_to_string_config = SqlToStringConfig(
            format_=SqlToStringFormat(pretty=config.pretty)
        )
        return config.sql_to_string_generator().generate_sql_string(query, sql_to_string_config)

    def to_pure_query(self, config: FrameToPureConfig = FrameToPureConfig()) -> str:
        if self.expr is None:
            return self.raise_exception_if_no_function_applied().to_pure_query(config)

        return get_pure_query_from_expr(self, config)

    def execute_frame(
            self,
            result_handler: ResultHandler[R],
            chunk_size: PyLegendOptional[int] = None
    ) -> R:  # pragma: no cover
        return BaseTdsFrame.execute_frame(self, result_handler, chunk_size)

    def execute_frame_to_string(
            self,
            chunk_size: PyLegendOptional[int] = None
    ) -> str:  # pragma: no cover
        return self.execute_frame(ToStringResultHandler(), chunk_size)

    def execute_frame_to_pandas_df(
            self,
            chunk_size: PyLegendOptional[int] = None,
            pandas_df_read_config: PandasDfReadConfig = PandasDfReadConfig()
    ) -> pd.DataFrame:  # pragma: no cover
        return self.execute_frame(ToPandasDfResultHandler(pandas_df_read_config), chunk_size)

    def to_sql_query_object(self, config: FrameToSqlConfig) -> QuerySpecification:
        temp_column_name_suffix = "__pylegend_olap_column__"
        if self.expr is None:
            return self.raise_exception_if_no_function_applied().to_sql_query_object(config)

        expr_contains_window_func = has_window_function(self)

        db_extension = config.sql_to_string_generator().get_db_extension()
        base_query = self.get_base_frame().base_frame().to_sql_query_object(config)
        col_name = self.columns()[0].get_name()

        # If the series needs the zero column, inject it into base_query
        # and wrap in a sub-query so PARTITION BY can reference it.
        from pylegend.core.tds.pandas_api.frames.pandas_api_window_tds_frame import ZERO_COLUMN_NAME
        if (
            needs_zero_column_for_window(self)
            and not query_contains_column_with_name(base_query, db_extension.quote_identifier(ZERO_COLUMN_NAME))
        ):
            from pylegend.core.sql.metamodel import IntegerLiteral
            base_query.select.selectItems.append(
                SingleColumn(
                    alias=db_extension.quote_identifier(ZERO_COLUMN_NAME),
                    expression=IntegerLiteral(0),
                )
            )
            base_query = create_sub_query(base_query, config, "root")

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
        else:  # pragma: no cover
            base_query.select.selectItems = [
                SingleColumn(db_extension.quote_identifier(col_name), full_sql_expr)
            ]
            return base_query

    def to_pure(self, config: FrameToPureConfig) -> str:
        return self.to_pure_query(config)

    def get_all_tds_frames(self) -> PyLegendSequence["BaseTdsFrame"]:
        if self.expr is not None:
            core_groupby_series = assert_and_find_core_series(self)
            assert core_groupby_series is not None
            return core_groupby_series.get_all_tds_frames()
        applied_function_frame = self.raise_exception_if_no_function_applied()
        return applied_function_frame.get_all_tds_frames()

    def has_applied_function(self) -> bool:
        return self.applied_function_frame is not None

    def aggregate(
            self,
            func: PyLegendAggInput,
            axis: PyLegendUnion[int, str] = 0,
            *args: PyLegendPrimitiveOrPythonPrimitive,
            **kwargs: PyLegendPrimitiveOrPythonPrimitive
    ) -> PyLegendUnion["PandasApiTdsFrame", "GroupbySeries"]:
        """
        Aggregate each group using one or more operations.

        Reduce the single column within each group to a scalar value.
        The result is a
        :class:`~pylegend.core.tds.pandas_api.frames.pandas_api_tds_frame.PandasApiTdsFrame`
        with one row per group, containing the grouping columns and
        the aggregated value(s).

        Parameters
        ----------
        func : str, callable, list, or dict
            Aggregation specification:

            - **str** — a named aggregation (``'sum'``, ``'mean'``,
              ``'min'``, ``'max'``, ``'count'``, ``'std'``, ``'var'``,
              plus aliases ``'len'``, ``'size'``).
            - **callable** — a lambda receiving the GroupbySeries and
              calling one of its aggregation methods
              (e.g. ``lambda x: x.sum()``).
            - **list of str** — multiple named aggregations. Result
              columns are named ``"agg(col_name)"``.
            - **dict** — ``{column_name: agg_spec}``. Keys **must**
              match the GroupbySeries' column name.
        axis : {{0, 'index'}}, default 0
            Must be ``0`` or ``'index'``.

        Returns
        -------
        PandasApiTdsFrame
            A frame with one row per group and the aggregated
            column(s), plus the grouping columns.

        Raises
        ------
        NotImplementedError
            If called on a computed GroupbySeries expression
            (e.g. ``(frame.groupby('grp')['col'] + 5).aggregate('sum')``).
        ValueError
            If a dict key does not match the GroupbySeries' column
            name.

        See Also
        --------
        agg : Alias for ``aggregate``.
        sum : Grouped sum.
        PandasApiGroupbyTdsFrame.aggregate : Aggregate on the full
            groupby frame.

        Notes
        -----
        **Differences from pandas:**

        - The result always includes the grouping columns alongside
          the aggregated values.
        - Aggregation on a **computed** GroupbySeries expression is
          **not supported**. Call the aggregation directly, then apply
          arithmetic if needed.
        - When ``func`` is a dict, keys must exactly match the
          GroupbySeries' column name.

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            # Single named aggregation
            frame.groupby("Ship Name")["Order Id"].aggregate(
                "sum"
            ).to_pandas().head(5)

            # Multiple aggregations
            frame.groupby("Ship Name")["Order Id"].aggregate(
                ["min", "max", "count"]
            ).head(5).to_pandas()

        """
        if self._expr is not None:  # pragma: no cover
            error_msg = '''
                Applying aggregate function to a computed series expression is not supported yet.
                For example,
                    not supported: (frame.groupby('grp')['col'] + 5).sum()
                    supported: frame.groupby('grp')['col'].sum() + 5
            '''
            error_msg = dedent(error_msg).strip()
            raise NotImplementedError(error_msg)

        if self.applied_function_frame is None:
            aggregated_frame = self.get_base_frame().aggregate(func, axis, *args, **kwargs)
        else:
            aggregated_frame = self.applied_function_frame.aggregate(func, axis, *args, **kwargs)  # pragma: no cover
        assert isinstance(aggregated_frame, PandasApiAppliedFunctionTdsFrame)

        num_grouping_cols = len(self._base_groupby_frame.get_grouping_columns())
        num_value_cols = len(aggregated_frame.columns()) - num_grouping_cols
        if num_value_cols == 1:
            return _get_new_groupby_series_for_column(
                self._base_groupby_frame, aggregated_frame, aggregated_frame.columns()[num_grouping_cols]
            )
        else:
            return aggregated_frame

    def agg(
            self,
            func: PyLegendAggInput,
            axis: PyLegendUnion[int, str] = 0,
            *args: PyLegendPrimitiveOrPythonPrimitive,
            **kwargs: PyLegendPrimitiveOrPythonPrimitive
    ) -> PyLegendUnion["PandasApiTdsFrame", "GroupbySeries"]:
        """
        Alias for :meth:`aggregate`.

        See :meth:`aggregate` for full documentation.
        """
        return self.aggregate(func, axis, *args, **kwargs)

    def sum(
        self,
        numeric_only: bool = False,
        min_count: int = 0,
        engine: PyLegendOptional[str] = None,
        engine_kwargs: PyLegendOptional[PyLegendDict[str, bool]] = None,
    ) -> PyLegendUnion["PandasApiTdsFrame", "GroupbySeries"]:
        """
        Compute the sum of values within each group.

        Parameters
        ----------
        numeric_only : bool, default False
            Must be ``False``. ``True`` is not supported.
        min_count : int, default 0
            Must be ``0``. Non-zero values are not supported.
        engine : str, optional
            Not supported. Must be ``None``.
        engine_kwargs : dict, optional
            Not supported. Must be ``None``.

        Returns
        -------
        PandasApiTdsFrame
            A frame with grouping columns and the summed values.

        Notes
        -----
        Equivalent to ``gseries.aggregate("sum")``.

        **Differences from pandas:** ``numeric_only``, ``engine``,
        and ``engine_kwargs`` are **not supported**. ``min_count``
        must be ``0``.

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            frame.groupby("Ship Name")["Order Id"].sum().to_pandas().head(5)

        """
        if numeric_only is not False:
            raise NotImplementedError("numeric_only=True is not currently supported in sum function.")
        if min_count != 0:
            raise NotImplementedError(f"min_count must be 0 in sum function, but got: {min_count}")
        if engine is not None:
            raise NotImplementedError("engine parameter is not supported in sum function.")
        if engine_kwargs is not None:
            raise NotImplementedError("engine_kwargs parameter is not supported in sum function.")
        return self.aggregate("sum", 0)

    def mean(
        self,
        numeric_only: bool = False,
        engine: PyLegendOptional[str] = None,
        engine_kwargs: PyLegendOptional[PyLegendDict[str, bool]] = None,
    ) -> PyLegendUnion["PandasApiTdsFrame", "GroupbySeries"]:
        """
        Compute the mean of values within each group.

        Parameters
        ----------
        numeric_only : bool, default False
            Must be ``False``. ``True`` is not supported.
        engine : str, optional
            Not supported. Must be ``None``.
        engine_kwargs : dict, optional
            Not supported. Must be ``None``.

        Returns
        -------
        PandasApiTdsFrame
            A frame with grouping columns and the mean values.

        Notes
        -----
        Equivalent to ``gseries.aggregate("mean")``. Maps to SQL
        ``AVG()``.

        **Differences from pandas:** ``numeric_only``, ``engine``,
        and ``engine_kwargs`` are **not supported**.

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            frame.groupby("Ship Name")["Order Id"].mean().to_pandas().head(5)

        """
        if numeric_only is not False:
            raise NotImplementedError("numeric_only=True is not currently supported in mean function.")
        if engine is not None:
            raise NotImplementedError("engine parameter is not supported in mean function.")
        if engine_kwargs is not None:
            raise NotImplementedError("engine_kwargs parameter is not supported in mean function.")
        return self.aggregate("mean", 0)

    def min(
        self,
        numeric_only: bool = False,
        min_count: int = -1,
        engine: PyLegendOptional[str] = None,
        engine_kwargs: PyLegendOptional[PyLegendDict[str, bool]] = None,
    ) -> PyLegendUnion["PandasApiTdsFrame", "GroupbySeries"]:
        """
        Compute the minimum of values within each group.

        Parameters
        ----------
        numeric_only : bool, default False
            Must be ``False``. ``True`` is not supported.
        min_count : int, default -1
            Must be ``-1``. Other values are not supported.
        engine : str, optional
            Not supported. Must be ``None``.
        engine_kwargs : dict, optional
            Not supported. Must be ``None``.

        Returns
        -------
        PandasApiTdsFrame
            A frame with grouping columns and the minimum values.

        Notes
        -----
        Equivalent to ``gseries.aggregate("min")``. Works on string
        columns as well (lexicographic minimum).

        **Differences from pandas:** ``numeric_only``, ``engine``,
        ``engine_kwargs``, and non-default ``min_count`` are **not
        supported**.

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            frame.groupby("Ship Name")["Order Id"].min().to_pandas().head(5)

        """
        if numeric_only is not False:
            raise NotImplementedError("numeric_only=True is not currently supported in min function.")
        if min_count != -1:
            raise NotImplementedError(f"min_count must be -1 (default) in min function, but got: {min_count}")
        if engine is not None:
            raise NotImplementedError("engine parameter is not supported in min function.")
        if engine_kwargs is not None:
            raise NotImplementedError("engine_kwargs parameter is not supported in min function.")
        return self.aggregate("min", 0)

    def max(
        self,
        numeric_only: bool = False,
        min_count: int = -1,
        engine: PyLegendOptional[str] = None,
        engine_kwargs: PyLegendOptional[PyLegendDict[str, bool]] = None,
    ) -> PyLegendUnion["PandasApiTdsFrame", "GroupbySeries"]:
        """
        Compute the maximum of values within each group.

        Parameters
        ----------
        numeric_only : bool, default False
            Must be ``False``. ``True`` is not supported.
        min_count : int, default -1
            Must be ``-1``. Other values are not supported.
        engine : str, optional
            Not supported. Must be ``None``.
        engine_kwargs : dict, optional
            Not supported. Must be ``None``.

        Returns
        -------
        PandasApiTdsFrame
            A frame with grouping columns and the maximum values.

        Notes
        -----
        Equivalent to ``gseries.aggregate("max")``. Works on string
        columns as well (lexicographic maximum).

        **Differences from pandas:** ``numeric_only``, ``engine``,
        ``engine_kwargs``, and non-default ``min_count`` are **not
        supported**.

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            frame.groupby("Ship Name")["Order Id"].max().to_pandas().head(5)

        """
        if numeric_only is not False:
            raise NotImplementedError("numeric_only=True is not currently supported in max function.")
        if min_count != -1:
            raise NotImplementedError(f"min_count must be -1 (default) in max function, but got: {min_count}")
        if engine is not None:
            raise NotImplementedError("engine parameter is not supported in max function.")
        if engine_kwargs is not None:
            raise NotImplementedError("engine_kwargs parameter is not supported in max function.")
        return self.aggregate("max", 0)

    def std(
        self,
        ddof: int = 1,
        engine: PyLegendOptional[str] = None,
        engine_kwargs: PyLegendOptional[PyLegendDict[str, bool]] = None,
        numeric_only: bool = False,
    ) -> PyLegendUnion["PandasApiTdsFrame", "GroupbySeries"]:
        """
        Compute the standard deviation within each group.

        Parameters
        ----------
        ddof : int, default 1
            Degrees of freedom. ``1`` for sample standard deviation
            (``STDDEV_SAMP``), ``0`` for population standard deviation
            (``STDDEV_POP``).
        engine : str, optional
            Not supported. Must be ``None``.
        engine_kwargs : dict, optional
            Not supported. Must be ``None``.
        numeric_only : bool, default False
            Must be ``False``. ``True`` is not supported.

        Returns
        -------
        PandasApiTdsFrame
            A frame with grouping columns and the standard deviation.

        Raises
        ------
        NotImplementedError
            If ``ddof`` is not ``0`` or ``1``, or if ``engine``,
            ``engine_kwargs``, or ``numeric_only`` are set to unsupported
            values.

        Notes
        -----
        Equivalent to ``gseries.aggregate("std")``. Maps to SQL
        ``STDDEV_SAMP()`` (ddof=1) or ``STDDEV_POP()`` (ddof=0).

        **Differences from pandas:** only ``ddof=0`` and ``ddof=1`` are
        supported. ``engine``, ``engine_kwargs``, and ``numeric_only``
        are **not supported**.

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            frame.groupby("Ship Name")["Order Id"].std().to_pandas().head(5)

        """
        if ddof not in (0, 1):
            raise NotImplementedError(
                f"Only ddof=0 (Population) and ddof=1 (Sample) are supported in std function, but got: {ddof}"
            )
        if engine is not None:
            raise NotImplementedError("engine parameter is not supported in std function.")
        if engine_kwargs is not None:
            raise NotImplementedError("engine_kwargs parameter is not supported in std function.")
        if numeric_only is not False:
            raise NotImplementedError("numeric_only=True is not currently supported in std function.")
        return self.aggregate("std_dev_sample" if ddof == 1 else "std_dev_population", 0)

    def var(
        self,
        ddof: int = 1,
        engine: PyLegendOptional[str] = None,
        engine_kwargs: PyLegendOptional[PyLegendDict[str, bool]] = None,
        numeric_only: bool = False,
    ) -> PyLegendUnion["PandasApiTdsFrame", "GroupbySeries"]:
        """
        Compute the variance within each group.

        Parameters
        ----------
        ddof : int, default 1
            Degrees of freedom. ``1`` for sample variance
            (``VAR_SAMP``), ``0`` for population variance (``VAR_POP``).
        engine : str, optional
            Not supported. Must be ``None``.
        engine_kwargs : dict, optional
            Not supported. Must be ``None``.
        numeric_only : bool, default False
            Must be ``False``. ``True`` is not supported.

        Returns
        -------
        PandasApiTdsFrame
            A frame with grouping columns and the variance.

        Raises
        ------
        NotImplementedError
            If ``ddof`` is not ``0`` or ``1``, or if ``engine``,
            ``engine_kwargs``, or ``numeric_only`` are set to unsupported
            values.

        Notes
        -----
        Equivalent to ``gseries.aggregate("var")``. Maps to SQL
        ``VAR_SAMP()`` (ddof=1) or ``VAR_POP()`` (ddof=0).

        **Differences from pandas:** only ``ddof=0`` and ``ddof=1`` are
        supported. ``engine``, ``engine_kwargs``, and ``numeric_only``
        are **not supported**.

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            frame.groupby("Ship Name")["Order Id"].var().to_pandas().head(5)

        """
        if ddof not in (0, 1):
            raise NotImplementedError(
                f"Only ddof=0 (Population) and ddof=1 (Sample) are supported in var function, but got: {ddof}"
            )
        if engine is not None:
            raise NotImplementedError("engine parameter is not supported in var function.")
        if engine_kwargs is not None:
            raise NotImplementedError("engine_kwargs parameter is not supported in var function.")
        if numeric_only is not False:
            raise NotImplementedError("numeric_only=True is not currently supported in var function.")
        return self.aggregate("variance_sample" if ddof == 1 else "variance_population", 0)

    def count(self) -> PyLegendUnion["PandasApiTdsFrame", "GroupbySeries"]:
        """
        Compute the count of non-null values within each group.

        Returns
        -------
        PandasApiTdsFrame
            A frame with grouping columns and the count per group.

        Notes
        -----
        Equivalent to ``gseries.aggregate("count")``. Maps to SQL
        ``COUNT(column)``.

        **Differences from pandas:** the signature takes no
        parameters (the pandas version accepts ``normalize`` and
        other keyword arguments which are not supported here).

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            frame.groupby("Ship Name")["Order Id"].count().to_pandas().head(5)

        """
        return self.aggregate("count", 0)

    def median(self) -> PyLegendUnion["PandasApiTdsFrame", "GroupbySeries"]:
        """
        Compute the median within each group.

        Maps to ``PERCENTILE_CONT(0.5)`` at the SQL level.

        Returns
        -------
        PandasApiTdsFrame or GroupbySeries
            Grouped median values.

        See Also
        --------
        mean : Compute group means.
        aggregate : General grouped aggregation.

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            frame.groupby("Ship Name")["Order Id"].median().to_pandas().head(5)

        """
        return self.aggregate("median", 0)

    def mode(self) -> PyLegendUnion["PandasApiTdsFrame", "GroupbySeries"]:
        """
        Compute the mode within each group.

        Returns the most frequently occurring value per group.
        Maps to ``MODE()`` at the SQL level.

        Returns
        -------
        PandasApiTdsFrame or GroupbySeries
            Grouped mode values.

        Notes
        -----
        **Differences from pandas:**

        - Returns a single value per group. Pandas may return multiple
          rows when there are ties.

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            frame.groupby("Ship Name")["Order Id"].mode().to_pandas().head(5)

        """
        return self.aggregate("mode", 0)

    def transform(  # type: ignore
            self,
            func: PyLegendUnion[str, PyLegendCallable[..., object]],
    ) -> "GroupbySeries":
        """
        Apply a partition-only window aggregate and broadcast back to every row.

        Equivalent to pandas ``groupby['col'].transform('func')``, which
        computes the aggregate per group and broadcasts the result back
        to every row.

        Generates SQL like ``FUNC(col) OVER (PARTITION BY ...)`` and
        Pure like
        ``extend(over(~[grp]), ~col:{p,w,r | $r.col}:y | $y->func())``.

        Parameters
        ----------
        func : str or callable
            The aggregation to apply within each partition. Accepts a
            named aggregation string (``'sum'``, ``'mean'``, ``'min'``,
            ``'max'``, ``'count'``, ``'std'``, ``'var'``) or a callable
            that receives a ``WindowSeries`` and returns the result.

        Returns
        -------
        GroupbySeries
            A grouped series containing the broadcasted aggregate value
            for each row within its group.

        See Also
        --------
        aggregate : Reduce groups to a single row per group.
        expanding : Expanding (cumulative) window on a grouped column.

        Notes
        -----
        **Differences from pandas:**

        - The result keeps every row (same row count as the input),
          matching pandas ``transform`` semantics.
        - Only aggregation functions are supported as ``func``.
          Arbitrary element-wise transforms (e.g. ``lambda x: x + 1``)
          are **not supported**.

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            frame["Group Sum"] = frame.groupby(
                "Ship Name"
            )["Order Id"].transform("sum")
            frame.head(5).to_pandas()

        """
        from pylegend.core.tds.pandas_api.frames.pandas_api_window_tds_frame import PandasApiWindowTdsFrame
        from pylegend.core.language.pandas_api.pandas_api_window_series import WindowSeries

        selected = self._base_groupby_frame.get_selected_columns()
        assert selected is not None and len(selected) == 1, (
            "transform() requires exactly one column selected"
        )
        col_name = selected[0].get_name()

        window_frame = PandasApiWindowTdsFrame(
            base_frame=self._base_groupby_frame,
            partition_only=True,
        )
        window_series = WindowSeries(window_frame=window_frame, column_name=col_name)
        return window_series.aggregate(func, 0)  # type: ignore

    def rank(
            self,
            method: str = 'min',
            ascending: bool = True,
            na_option: str = 'bottom',
            pct: bool = False,
            axis: PyLegendUnion[int, str] = 0
    ) -> "GroupbySeries":
        """
        Compute the rank of values within each group.

        Return a new ``GroupbySeries`` containing the rank of each
        value within its group. The grouping columns act as the
        ``PARTITION BY`` clause in the underlying SQL window function.
        The result can be assigned back to the parent frame or
        executed directly as a standalone single-column query.

        Parameters
        ----------
        method : {{'min', 'first', 'dense'}}, default 'min'
            How to rank equal values:

            - ``'min'`` : Lowest rank in the group of ties
              (SQL ``RANK()``).
            - ``'first'`` : Ranks by order of appearance within the
              group (SQL ``ROW_NUMBER()``).
            - ``'dense'`` : Like ``'min'`` but no gaps
              (SQL ``DENSE_RANK()``).
        ascending : bool, default True
            Whether to rank in ascending order.
        na_option : {{'bottom'}}, default 'bottom'
            Only ``'bottom'`` is supported.
        pct : bool, default False
            If ``True``, compute percentage ranks
            (SQL ``PERCENT_RANK()``). Returns a
            ``FloatGroupbySeries``. Only supported with
            ``method='min'``.
        axis : {{0, 'index'}}, default 0
            Must be ``0`` or ``'index'``.

        Returns
        -------
        GroupbySeries
            An ``IntegerGroupbySeries`` (or
            ``FloatGroupbySeries`` when ``pct=True``) containing
            the ranks within each group.

        Raises
        ------
        NotImplementedError
            If called on a computed GroupbySeries expression
            (e.g. ``(frame.groupby('grp')['col'] + 5).rank()``).
            Call ``rank()`` first, then apply arithmetic.
            If ``method`` is not ``'min'``, ``'first'``, or
            ``'dense'``.
            If ``na_option`` is not ``'bottom'``.
            If ``pct=True`` with a method other than ``'min'``.

        See Also
        --------
        Series.rank : Frame-level rank (no partitioning).
        PandasApiGroupbyTdsFrame.rank : Rank all non-grouping columns.

        Notes
        -----
        **Differences from pandas:**

        - The ``'average'`` and ``'max'`` methods are **not
          supported**.
        - ``na_option`` only supports ``'bottom'``.
        - ``pct=True`` is only supported with ``method='min'``.
        - Calling ``rank()`` on a **computed** GroupbySeries
          expression is **not supported**. Call ``rank()`` first,
          then apply arithmetic:
          ``frame.groupby('grp')['col'].rank() + 5``.
        - Only **one** window-function call is allowed per
          expression. To combine multiple, use separate assignments.

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            # Execute a grouped ranked series directly
            frame.groupby("Ship Name")["Order Id"].rank().to_pandas().head()

            # Assign a grouped rank to the parent frame
            frame["Order Rank"] = frame.groupby(
                "Ship Name"
            )["Order Id"].rank()
            frame.head(5).to_pandas()

            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            # Dense rank, descending
            frame["Dense Rank"] = frame.groupby(
                "Ship Name"
            )["Order Id"].rank(method="dense", ascending=False)
            frame.head(5).to_pandas()

        """
        if self._expr is not None:  # pragma: no cover
            error_msg = '''
                Applying rank function to a computed series expression is not supported yet.
                For example,
                    not supported: (frame.groupby('grp')['col'] + 5).rank()
                    supported: frame.groupby('grp')['col'].rank() + 5
            '''
            error_msg = dedent(error_msg).strip()
            raise NotImplementedError(error_msg)

        applied_function_frame = self._base_groupby_frame.rank(method, ascending, na_option, pct, axis)
        assert isinstance(applied_function_frame, PandasApiAppliedFunctionTdsFrame)

        if pct:
            return FloatGroupbySeries(self._base_groupby_frame, applied_function_frame)
        else:
            return IntegerGroupbySeries(self._base_groupby_frame, applied_function_frame)

    def expanding(
            self,
            min_periods: int = 1,
            method: PyLegendOptional[str] = None,
            order_by: PyLegendOptional[PyLegendUnion[str, PyLegendSequence[str]]] = None,
            ascending: PyLegendUnion[bool, "PyLegendSequence[bool]"] = True,
    ) -> "WindowSeries":
        """
        Create an expanding (cumulative) window on a single grouped column.

        The grouping columns are automatically used as ``PARTITION BY``.
        An expanding window includes all rows from the start of the
        partition up to the current row.

        Parameters
        ----------
        min_periods : int, default 1
            Minimum number of observations required to produce a value.
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
            If ``method`` is not ``None``.

        See Also
        --------
        rolling : Fixed-size grouped sliding window.
        window_frame_legend_ext : Custom window specification.

        Notes
        -----
        **Differences from pandas:**

        - ``order_by`` and ``ascending`` are pylegend extensions not
          present in pandas.
        - ``method`` is **not supported**.

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            frame.groupby("Ship Name")["Order Id"].expanding(
                order_by="Order Id"
            ).sum().to_pandas().head(5)

        """
        from pylegend.core.language.pandas_api.pandas_api_window_series import WindowSeries

        window_frame = self._base_groupby_frame.expanding(
            min_periods=min_periods, method=method, order_by=order_by, ascending=ascending
        )
        return WindowSeries(window_frame=window_frame, column_name=self.columns()[0].get_name())

    def rolling(
            self,
            window: int,
            min_periods: PyLegendOptional[int] = None,
            center: bool = False,
            win_type: PyLegendOptional[str] = None,
            on: PyLegendOptional[str] = None,
            closed: PyLegendOptional[str] = None,
            step: PyLegendOptional[int] = None,
            method: PyLegendOptional[str] = None,
            order_by: PyLegendOptional[PyLegendUnion[str, PyLegendSequence[str]]] = None,
            ascending: PyLegendUnion[bool, "PyLegendSequence[bool]"] = True,
    ) -> "WindowSeries":
        """
        Create a fixed-size sliding window on a single grouped column.

        The grouping columns are automatically used as ``PARTITION BY``.
        A rolling window includes a fixed number of preceding rows for
        each row within the partition.

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
            or ``method`` are set to non-default values.

        See Also
        --------
        expanding : Expanding (cumulative) grouped window.
        window_frame_legend_ext : Custom window specification.

        Notes
        -----
        **Differences from pandas:**

        - ``order_by`` and ``ascending`` are pylegend extensions not
          present in pandas.
        - ``center``, ``win_type``, ``on``, ``closed``, ``step``, and
          ``method`` are **not supported**.

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            frame.groupby("Ship Name")["Order Id"].rolling(
                window=3, order_by="Order Id"
            ).mean().to_pandas().head(5)

        """
        from pylegend.core.language.pandas_api.pandas_api_window_series import WindowSeries

        window_frame = self._base_groupby_frame.rolling(
            window=window, min_periods=min_periods, center=center, win_type=win_type,
            on=on, closed=closed, step=step, method=method, order_by=order_by,
            ascending=ascending
        )
        return WindowSeries(window_frame=window_frame, column_name=self.columns()[0].get_name())

    def window_frame_legend_ext(
            self,
            frame_spec: PyLegendOptional[FrameSpec] = RowsBetween(None, None),
            order_by: PyLegendOptional[PyLegendUnion[str, PyLegendSequence[str]]] = None,
            ascending: PyLegendUnion[bool, "PyLegendSequence[bool]"] = True,
    ) -> "WindowSeries":
        """
        Create a custom window specification on a single grouped column.

        **PyLegend extension** — not present in pandas.

        The grouping columns are automatically used as ``PARTITION BY``.
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
        expanding : Cumulative grouped window.
        rolling : Fixed-size grouped sliding window.

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
            frame.groupby("Ship Name")["Order Id"].window_frame_legend_ext(
                spec, order_by="Order Id"
            ).sum().to_pandas().head()

        """
        from pylegend.core.language.pandas_api.pandas_api_window_series import WindowSeries

        window_frame = self._base_groupby_frame.window_frame_legend_ext(
            frame_spec=frame_spec, order_by=order_by, ascending=ascending
        )
        return WindowSeries(window_frame=window_frame, column_name=self.columns()[0].get_name())

    def cume_dist_legend_ext(
            self,
            ascending: bool = True,
    ) -> "GroupbySeries":
        """
        Compute the cumulative distribution within each group.

        **PyLegend extension** — not present in pandas.

        Maps to SQL ``CUME_DIST() OVER (PARTITION BY ... ORDER BY col)``
        and Pure ``cumulativeDistribution``.

        Parameters
        ----------
        ascending : bool, default True
            Whether to order in ascending direction.

        Returns
        -------
        FloatGroupbySeries
            A grouped series containing cumulative distribution values
            (floats between 0 and 1).

        See Also
        --------
        rank : Compute grouped ranks.
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

            frame["CumeDist"] = frame.groupby(
                "Ship Name"
            )["Order Id"].cume_dist_legend_ext()
            frame.head(5).to_pandas()

        """
        applied_function_frame = self._base_groupby_frame.cume_dist_legend_ext(ascending=ascending)
        assert isinstance(applied_function_frame, PandasApiAppliedFunctionTdsFrame)
        return FloatGroupbySeries(self._base_groupby_frame, applied_function_frame)

    def ntile_legend_ext(
            self,
            num_buckets: int,
            ascending: bool = True,
    ) -> "GroupbySeries":
        """
        Assign rows to numbered buckets within each group.

        **PyLegend extension** — not present in pandas.

        Maps to SQL ``NTILE(n) OVER (PARTITION BY ... ORDER BY col)``
        and Pure ``ntile``.

        Parameters
        ----------
        num_buckets : int
            Number of buckets to distribute rows into.
        ascending : bool, default True
            Whether to order in ascending direction.

        Returns
        -------
        IntegerGroupbySeries
            A grouped series containing bucket numbers (1-based).

        See Also
        --------
        rank : Compute grouped ranks.
        cume_dist_legend_ext : Cumulative distribution within groups.

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

            frame["Quartile"] = frame.groupby(
                "Ship Name"
            )["Order Id"].ntile_legend_ext(4)
            frame.head(5).to_pandas()

        """
        applied_function_frame = self._base_groupby_frame.ntile_legend_ext(
            num_buckets=num_buckets, ascending=ascending,
        )
        assert isinstance(applied_function_frame, PandasApiAppliedFunctionTdsFrame)
        return IntegerGroupbySeries(self._base_groupby_frame, applied_function_frame)

    def max_by_legend_ext(
            self,
            by: PyLegendUnion["NumberGroupbySeries", "IntegerGroupbySeries", "FloatGroupbySeries",
                              "DecimalGroupbySeries"]
    ) -> "FloatGroupbySeries":
        """
        Return the value of this column at the row where ``by`` is maximised, per group.

        **PyLegend extension** — not present in pandas.

        Parameters
        ----------
        by : NumberGroupbySeries or IntegerGroupbySeries or FloatGroupbySeries or DecimalGroupbySeries
            A numeric grouped series whose maximum determines which
            row's value is returned.

        Returns
        -------
        FloatGroupbySeries
            The value of this column at the max of ``by`` within each group.

        See Also
        --------
        min_by : Value at the row where ``by`` is minimised.

        Notes
        -----
        **Differences from pandas:**

        - This method has **no pandas equivalent**. It is a pylegend
          extension backed by a two-column window function.

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            grp = frame.groupby("Ship Name")
            frame["Max Order By Id"] = grp["Order Id"].max_by_legend_ext(
                grp["Order Id"]
            )
            frame.head(5).to_pandas()

        """
        return self._generic_two_col_window_func(by, "max_by")

    def min_by_legend_ext(
            self,
            by: PyLegendUnion["NumberGroupbySeries", "IntegerGroupbySeries", "FloatGroupbySeries",
                              "DecimalGroupbySeries"]
    ) -> "FloatGroupbySeries":
        """
        Return the value of this column at the row where ``by`` is minimised, per group.

        **PyLegend extension** — not present in pandas.

        Parameters
        ----------
        by : NumberGroupbySeries or IntegerGroupbySeries or FloatGroupbySeries or DecimalGroupbySeries
            A numeric grouped series whose minimum determines which
            row's value is returned.

        Returns
        -------
        FloatGroupbySeries
            The value of this column at the min of ``by`` within each group.

        See Also
        --------
        max_by : Value at the row where ``by`` is maximised.

        Notes
        -----
        **Differences from pandas:**

        - This method has **no pandas equivalent**. It is a pylegend
          extension backed by a two-column window function.

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            grp = frame.groupby("Ship Name")
            frame["Min Order By Id"] = grp["Order Id"].min_by_legend_ext(
                grp["Order Id"]
            )
            frame.head(5).to_pandas()

        """
        return self._generic_two_col_window_func(by, "min_by")

    def _generic_two_col_window_func(
            self,
            other: "GroupbySeries",
            func_type: str,
    ) -> "FloatGroupbySeries":
        from pylegend.core.tds.pandas_api.frames.functions.two_column_window_function import TwoColumnWindowFunction

        selected_a = self._base_groupby_frame.get_selected_columns()
        assert selected_a is not None and len(selected_a) == 1, (
            f"{func_type}() requires exactly one column selected on self"
        )
        col_name_a = selected_a[0].get_name()

        selected_b = other._base_groupby_frame.get_selected_columns()
        assert selected_b is not None and len(selected_b) == 1, (
            f"{func_type}() requires exactly one column selected on other"
        )
        col_name_b = selected_b[0].get_name()

        applied_function_frame = PandasApiAppliedFunctionTdsFrame(TwoColumnWindowFunction(
            base_frame=self._base_groupby_frame,
            col_name_a=col_name_a,
            col_name_b=col_name_b,
            result_col_name=col_name_a,
            func_type=func_type,
        ))
        # Late-bind to avoid forward reference — FloatGroupbySeries is defined later in this module
        from pylegend.core.language.pandas_api.pandas_api_groupby_series import FloatGroupbySeries as _Float
        return _Float(self._base_groupby_frame, applied_function_frame)


@add_primitive_methods
class BooleanGroupbySeries(GroupbySeries, PyLegendBoolean, PyLegendExpressionBooleanReturn):
    def __init__(  # pragma: no cover (Boolean column not supported in PURE)
            self,
            base_groupby_frame: PandasApiGroupbyTdsFrame,
            applied_function_frame: PyLegendOptional[PandasApiAppliedFunctionTdsFrame] = None,
            expr: PyLegendOptional[PyLegendExpression] = None
    ) -> None:
        super().__init__(base_groupby_frame, applied_function_frame, expr)
        PyLegendBoolean.__init__(self, self)


@add_primitive_methods
class StringGroupbySeries(GroupbySeries, PyLegendString, PyLegendExpressionStringReturn):
    def __init__(
            self,
            base_groupby_frame: PandasApiGroupbyTdsFrame,
            applied_function_frame: PyLegendOptional[PandasApiAppliedFunctionTdsFrame] = None,
            expr: PyLegendOptional[PyLegendExpression] = None
    ) -> None:
        super().__init__(base_groupby_frame, applied_function_frame, expr)
        PyLegendString.__init__(self, self)


@add_primitive_methods
class NumberGroupbySeries(GroupbySeries, PyLegendNumber, PyLegendExpressionNumberReturn):
    def __init__(
            self,
            base_groupby_frame: PandasApiGroupbyTdsFrame,
            applied_function_frame: PyLegendOptional[PandasApiAppliedFunctionTdsFrame] = None,
            expr: PyLegendOptional[PyLegendExpression] = None
    ) -> None:
        super().__init__(base_groupby_frame, applied_function_frame, expr)
        PyLegendNumber.__init__(self, self)

    def _two_col_window_func(
            self,
            other: PyLegendUnion["NumberGroupbySeries", "IntegerGroupbySeries", "FloatGroupbySeries",
                                 "DecimalGroupbySeries"],
            func_type: str,
    ) -> "FloatGroupbySeries":
        from pylegend.core.tds.pandas_api.frames.functions.two_column_window_function import TwoColumnWindowFunction

        selected_a = self._base_groupby_frame.get_selected_columns()
        assert selected_a is not None and len(selected_a) == 1, (
            f"{func_type}() requires exactly one column selected on self"
        )
        col_name_a = selected_a[0].get_name()

        selected_b = other._base_groupby_frame.get_selected_columns()
        assert selected_b is not None and len(selected_b) == 1, (
            f"{func_type}() requires exactly one column selected on other"
        )
        col_name_b = selected_b[0].get_name()

        applied_function_frame = PandasApiAppliedFunctionTdsFrame(TwoColumnWindowFunction(
            base_frame=self._base_groupby_frame,
            col_name_a=col_name_a,
            col_name_b=col_name_b,
            result_col_name=col_name_a,
            func_type=func_type,
        ))
        return FloatGroupbySeries(self._base_groupby_frame, applied_function_frame)

    def corr(
            self,
            other: PyLegendUnion["NumberGroupbySeries", "IntegerGroupbySeries", "FloatGroupbySeries",
                                 "DecimalGroupbySeries"]
    ) -> "FloatGroupbySeries":
        """
        Compute the correlation between this column and ``other`` within each group.

        **PyLegend extension** — not present in standard pandas ``GroupBy``.

        Parameters
        ----------
        other : NumberGroupbySeries or IntegerGroupbySeries or FloatGroupbySeries or DecimalGroupbySeries
            The second grouped column to correlate with.

        Returns
        -------
        FloatGroupbySeries
            Pearson correlation coefficient per group.

        See Also
        --------
        cov : Grouped covariance.

        Notes
        -----
        **Differences from pandas:**

        - This method has **no pandas equivalent** on
          ``DataFrameGroupBy``. It is a pylegend extension backed by
          a two-column window function.

        """
        return self._two_col_window_func(other, "corr")

    def cov(
            self,
            other: PyLegendUnion["NumberGroupbySeries", "IntegerGroupbySeries", "FloatGroupbySeries",
                                 "DecimalGroupbySeries"],
            ddof: int = 1,
    ) -> "FloatGroupbySeries":
        """
        Compute the covariance between this column and ``other`` within each group.

        Parameters
        ----------
        other : NumberGroupbySeries or IntegerGroupbySeries or FloatGroupbySeries or DecimalGroupbySeries
            The second grouped column.
        ddof : {{0, 1}}, default 1
            ``1`` for sample covariance (``COVAR_SAMP``), ``0`` for
            population covariance (``COVAR_POP``).

        Returns
        -------
        FloatGroupbySeries
            Covariance per group.

        Raises
        ------
        NotImplementedError
            If ``ddof`` is not ``0`` or ``1``.

        See Also
        --------
        corr : Grouped correlation.

        Notes
        -----
        **Differences from pandas:**

        - Only ``ddof=0`` and ``ddof=1`` are supported. Other values
          raise ``NotImplementedError``.

        """
        if ddof == 1:
            return self._two_col_window_func(other, "covar_sample")
        elif ddof == 0:
            return self._two_col_window_func(other, "covar_population")
        else:
            raise NotImplementedError(
                f"Only ddof=0 (population) and ddof=1 (sample) are supported in cov function, but got: ddof={ddof}"
            )

    def wavg_legend_ext(
            self,
            weights: PyLegendUnion["NumberGroupbySeries", "IntegerGroupbySeries", "FloatGroupbySeries",
                                   "DecimalGroupbySeries"]
    ) -> "FloatGroupbySeries":
        """
        Compute the weighted average within each group.

        **PyLegend extension** — not present in pandas.

        Parameters
        ----------
        weights : NumberGroupbySeries or IntegerGroupbySeries or FloatGroupbySeries or DecimalGroupbySeries
            A numeric grouped series supplying the weight for each row.

        Returns
        -------
        FloatGroupbySeries
            Weighted average per group.

        Notes
        -----
        **Differences from pandas:**

        - This method has **no pandas equivalent**. Weighted average
          is exposed as a pylegend extension.

        See Also
        --------
        mean : Unweighted grouped mean.
        corr : Grouped correlation.

        """
        return self._two_col_window_func(weights, "wavg")

    def zscore_legend_ext(self) -> "FloatGroupbySeries":
        """
        Compute the z-score within each group.

        **PyLegend extension** — not present in pandas.

        Calculates ``(x - mean) / stddev_pop`` for each row within its
        group. Equivalent to Pure ``zScore($p, $w, $r, ~col)``.

        Returns
        -------
        FloatGroupbySeries
            Z-score values per group, suitable for assignment via
            ``frame["col"] = ...``.

        Notes
        -----
        **Differences from pandas:**

        - This method has **no pandas equivalent**. Z-score
          computation is exposed as a pylegend extension.
        - Uses population standard deviation (``STDDEV_POP``), not
          sample standard deviation.

        See Also
        --------
        std : Grouped standard deviation.
        mean : Grouped mean.

        """
        from pylegend.core.tds.pandas_api.frames.functions.zscore_window_function import ZScoreWindowFunction

        selected = self._base_groupby_frame.get_selected_columns()
        assert selected is not None and len(selected) == 1, (
            "zscore() requires exactly one column selected"
        )
        col_name = selected[0].get_name()

        applied_function_frame = PandasApiAppliedFunctionTdsFrame(ZScoreWindowFunction(
            base_frame=self._base_groupby_frame,
            col_name=col_name,
            result_col_name=col_name,
        ))
        return FloatGroupbySeries(self._base_groupby_frame, applied_function_frame)


@add_primitive_methods
class IntegerGroupbySeries(NumberGroupbySeries, PyLegendInteger, PyLegendExpressionIntegerReturn):
    def __init__(
            self,
            base_groupby_frame: PandasApiGroupbyTdsFrame,
            applied_function_frame: PyLegendOptional[PandasApiAppliedFunctionTdsFrame] = None,
            expr: PyLegendOptional[PyLegendExpression] = None
    ) -> None:
        super().__init__(base_groupby_frame, applied_function_frame, expr)
        PyLegendInteger.__init__(self, self)


@add_primitive_methods
class FloatGroupbySeries(NumberGroupbySeries, PyLegendFloat, PyLegendExpressionFloatReturn):
    def __init__(
            self,
            base_groupby_frame: PandasApiGroupbyTdsFrame,
            applied_function_frame: PyLegendOptional[PandasApiAppliedFunctionTdsFrame] = None,
            expr: PyLegendOptional[PyLegendExpression] = None
    ) -> None:
        super().__init__(base_groupby_frame, applied_function_frame, expr)
        PyLegendFloat.__init__(self, self)


@add_primitive_methods
class DecimalGroupbySeries(NumberGroupbySeries, PyLegendDecimal, PyLegendExpressionDecimalReturn):
    def __init__(
            self,
            base_groupby_frame: PandasApiGroupbyTdsFrame,
            applied_function_frame: PyLegendOptional[PandasApiAppliedFunctionTdsFrame] = None,
            expr: PyLegendOptional[PyLegendExpression] = None
    ) -> None:
        super().__init__(base_groupby_frame, applied_function_frame, expr)  # pragma: no cover
        PyLegendDecimal.__init__(self, self)  # pragma: no cover


@add_primitive_methods
class DateGroupbySeries(GroupbySeries, PyLegendDate, PyLegendExpressionDateReturn):
    def __init__(
            self,
            base_groupby_frame: PandasApiGroupbyTdsFrame,
            applied_function_frame: PyLegendOptional[PandasApiAppliedFunctionTdsFrame] = None,
            expr: PyLegendOptional[PyLegendExpression] = None
    ) -> None:
        super().__init__(base_groupby_frame, applied_function_frame, expr)
        PyLegendDate.__init__(self, self)


@add_primitive_methods
class DateTimeGroupbySeries(DateGroupbySeries, PyLegendDateTime, PyLegendExpressionDateTimeReturn):
    def __init__(
            self,
            base_groupby_frame: PandasApiGroupbyTdsFrame,
            applied_function_frame: PyLegendOptional[PandasApiAppliedFunctionTdsFrame] = None,
            expr: PyLegendOptional[PyLegendExpression] = None
    ) -> None:
        super().__init__(base_groupby_frame, applied_function_frame, expr)
        PyLegendDateTime.__init__(self, self)


@add_primitive_methods
class StrictDateGroupbySeries(DateGroupbySeries, PyLegendStrictDate, PyLegendExpressionStrictDateReturn):
    def __init__(
            self,
            base_groupby_frame: PandasApiGroupbyTdsFrame,
            applied_function_frame: PyLegendOptional[PandasApiAppliedFunctionTdsFrame] = None,
            expr: PyLegendOptional[PyLegendExpression] = None
    ) -> None:
        super().__init__(base_groupby_frame, applied_function_frame, expr)
        PyLegendStrictDate.__init__(self, self)

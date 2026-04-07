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
    get_pure_query_from_expr, get_groupby_series_from_col_type,
)
from pylegend.core.tds.pandas_api.frames.pandas_api_applied_function_tds_frame import PandasApiAppliedFunctionTdsFrame
from pylegend.core.tds.pandas_api.frames.pandas_api_groupby_tds_frame import PandasApiGroupbyTdsFrame
from pylegend.core.tds.result_handler import ResultHandler, ToStringResultHandler
from pylegend.core.tds.sql_query_helpers import create_sub_query
from pylegend.core.tds.tds_column import TdsColumn
from pylegend.core.tds.tds_frame import FrameToPureConfig, FrameToSqlConfig
from pylegend.extensions.tds.result_handler import PandasDfReadConfig, ToPandasDfResultHandler

if TYPE_CHECKING:
    from pylegend.core.language.pandas_api.pandas_api_frame_spec import FrameSpec
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
        return self.aggregate(func, axis, *args, **kwargs)

    def sum(
        self,
        numeric_only: bool = False,
        min_count: int = 0,
        engine: PyLegendOptional[str] = None,
        engine_kwargs: PyLegendOptional[PyLegendDict[str, bool]] = None,
    ) -> PyLegendUnion["PandasApiTdsFrame", "GroupbySeries"]:
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
        return self.aggregate("count", 0)

    def median(self) -> PyLegendUnion["PandasApiTdsFrame", "GroupbySeries"]:
        return self.aggregate("median", 0)

    def mode(self) -> PyLegendUnion["PandasApiTdsFrame", "GroupbySeries"]:
        return self.aggregate("mode", 0)

    def transform(  # type: ignore
            self,
            func: PyLegendUnion[str, PyLegendCallable[..., object]],
    ) -> "GroupbySeries":
        """Apply a partition-only window aggregate (no frame bounds, no order by).

        Equivalent to pandas ``groupby['col'].transform('func')``, which computes
        the aggregate per group and broadcasts the result back to every row.

        Generates SQL like ``FUNC(col) OVER (PARTITION BY ...)`` and
        Pure like ``extend(over(~[grp]), ~col:{p,w,r | $r.col}:y | $y->func())``.
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
        from pylegend.core.language.pandas_api.pandas_api_window_series import WindowSeries

        window_frame = self._base_groupby_frame.rolling(
            window=window, min_periods=min_periods, center=center, win_type=win_type,
            on=on, closed=closed, step=step, method=method, order_by=order_by,
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
        PyLegend extension (not present in pandas).

        Create a custom window specification with explicit control over the
        window frame on a single column.  When called on a groupby series
        the grouping columns are automatically used as PARTITION BY.
        """
        from pylegend.core.language.pandas_api.pandas_api_window_series import WindowSeries

        window_frame = self._base_groupby_frame.window_frame_legend_ext(
            frame_spec=frame_spec, order_by=order_by, ascending=ascending
        )
        return WindowSeries(window_frame=window_frame, column_name=self.columns()[0].get_name())


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
        return self._two_col_window_func(other, "corr")

    def cov(
            self,
            other: PyLegendUnion["NumberGroupbySeries", "IntegerGroupbySeries", "FloatGroupbySeries",
                                 "DecimalGroupbySeries"],
            ddof: int = 1,
    ) -> "FloatGroupbySeries":
        if ddof == 1:
            return self._two_col_window_func(other, "covar_sample")
        elif ddof == 0:
            return self._two_col_window_func(other, "covar_population")
        else:
            raise NotImplementedError(
                f"Only ddof=0 (population) and ddof=1 (sample) are supported in cov function, but got: ddof={ddof}"
            )

    def zscore(self) -> "FloatGroupbySeries":
        """Compute the z-score within each group: (x - mean) / stddev_pop.

        Equivalent to Pure ``zScore($p, $w, $r, ~col)`` which computes
        ``(eval(col, row) - average(partition, window, row, col)) / stdDevPopulation(partition, window, row, col)``.

        Returns a ``FloatGroupbySeries`` suitable for assignment via ``frame.assign()``.
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

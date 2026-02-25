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
    PyLegendExpressionDateReturn,
    PyLegendExpressionDateTimeReturn,
    PyLegendExpressionStrictDateReturn,
    PyLegendExpression,
)
from pylegend.core.language.shared.helpers import escape_column_name, generate_pure_lambda
from pylegend.core.language.shared.primitives.boolean import PyLegendBoolean
from pylegend.core.language.shared.primitives.date import PyLegendDate
from pylegend.core.language.shared.primitives.datetime import PyLegendDateTime
from pylegend.core.language.shared.primitives.float import PyLegendFloat
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
from pylegend.core.tds.pandas_api.frames.functions.rank_function import RankFunction
from pylegend.core.tds.pandas_api.frames.helpers.series_helper import add_primitive_methods, assert_and_find_core_series, \
    has_window_function
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

    def get_sub_expressions(self) -> PyLegendSequence["PyLegendExpression"]:
        if self.expr is not None:
            return self.expr.get_sub_expressions()
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
        temp_column_name_suffix = "__INTERNAL_PYLEGEND_COLUMN__"
        if self.expr is None:
            return self.get_filtered_frame().to_pure_query(config)

        col_name = self.columns()[0].get_name()
        full_expr = self.expr
        has_window_func = False
        window_expr = ""
        function_expr = ""
        sub_expressions = self.get_sub_expressions()
        for expr in sub_expressions:
            if isinstance(expr, Series):
                applied_func = expr.get_filtered_frame().get_applied_function()
                if isinstance(applied_func, RankFunction):
                    assert has_window_func is False
                    has_window_func = True
                    c, window = applied_func.construct_column_expression_and_window_tuples()[0]
                    window_expr = window.to_pure_expression(config)
                    function_expr = c[1].to_pure_expression(config)

        extend = ""
        if has_window_func:
            pure_expr = full_expr.to_pure_expression(config)
            temp_name = escape_column_name(col_name + temp_column_name_suffix)
            extend = f"->extend({window_expr}, ~{temp_name}:{generate_pure_lambda('p,w,r', function_expr)})"
            project = f"->project(~[{escape_column_name(col_name)}:c|{pure_expr}])"
        else:
            project = f"->project(~[{escape_column_name(col_name)}:c|{self.to_pure_expression(config)}])"

        if len(extend) > 0:
            extend = config.separator(1) + extend
        project = config.separator(1) + project

        return self.get_base_frame().to_pure_query(config) + extend + project

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
        temp_column_name_suffix = "__INTERNAL_PYLEGEND_COLUMN__"
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
        return self.aggregate(func, axis, *args, **kwargs)

    def sum(
            self,
            axis: PyLegendUnion[int, str] = 0,
            skipna: bool = True,
            numeric_only: bool = False,
            min_count: int = 0,
            **kwargs: PyLegendPrimitiveOrPythonPrimitive
    ) -> "PandasApiTdsFrame":
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

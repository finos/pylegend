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
)
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
    Expression,
)
from pylegend.core.sql.metamodel import QuerySpecification
from pylegend.core.tds.abstract.frames.base_tds_frame import BaseTdsFrame
from pylegend.core.tds.pandas_api.frames.functions.filter import PandasApiFilterFunction
from pylegend.core.tds.pandas_api.frames.pandas_api_applied_function_tds_frame import PandasApiAppliedFunctionTdsFrame
from pylegend.core.tds.result_handler import ResultHandler
from pylegend.core.tds.tds_column import TdsColumn
from pylegend.core.tds.tds_frame import FrameToPureConfig
from pylegend.core.tds.tds_frame import FrameToSqlConfig
from pylegend.extensions.tds.result_handler import PandasDfReadConfig

if TYPE_CHECKING:
    from pylegend.core.tds.pandas_api.frames.pandas_api_tds_frame import PandasApiTdsFrame

__all__: PyLegendSequence[str] = [
    "Series",
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


class Series(PyLegendColumnExpression, PyLegendPrimitive, BaseTdsFrame):
    def __init__(self, base_frame: "PandasApiTdsFrame", column: str):
        row = PandasApiTdsRow.from_tds_frame("c", base_frame)
        PyLegendColumnExpression.__init__(self, row=row, column=column)

        self.__base_frame = base_frame
        filtered = base_frame.filter(items=[column])
        assert isinstance(filtered, PandasApiAppliedFunctionTdsFrame)
        self._filtered_frame: PandasApiAppliedFunctionTdsFrame = filtered

    def value(self) -> PyLegendColumnExpression:
        return self

    def get_base_frame(self) -> "PandasApiTdsFrame":
        return self.__base_frame

    def to_sql_expression(
            self,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
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
        return self._filtered_frame.to_sql_query(config)

    def to_pure_query(self, config: FrameToPureConfig = FrameToPureConfig()) -> str:
        return self._filtered_frame.to_pure_query(config)

    def execute_frame(
            self,
            result_handler: ResultHandler[R],
            chunk_size: PyLegendOptional[int] = None
    ) -> R:
        return self._filtered_frame.execute_frame(result_handler, chunk_size)  # pragma: no cover

    def execute_frame_to_string(
            self,
            chunk_size: PyLegendOptional[int] = None
    ) -> str:
        return self._filtered_frame.execute_frame_to_string(chunk_size)

    def execute_frame_to_pandas_df(
            self,
            chunk_size: PyLegendOptional[int] = None,
            pandas_df_read_config: PandasDfReadConfig = PandasDfReadConfig()
    ) -> pd.DataFrame:
        return self._filtered_frame.execute_frame_to_pandas_df(chunk_size, pandas_df_read_config)  # pragma: no cover

    def to_sql_query_object(self, config: FrameToSqlConfig) -> QuerySpecification:
        return self._filtered_frame.to_sql_query_object(config)

    def to_pure(self, config: FrameToPureConfig) -> str:
        return self._filtered_frame.to_pure(config)

    def get_all_tds_frames(self) -> PyLegendSequence["BaseTdsFrame"]:
        return self._filtered_frame.get_all_tds_frames()

    def aggregate(
            self,
            func: PyLegendAggInput,
            axis: PyLegendUnion[int, str] = 0,
            *args: PyLegendPrimitiveOrPythonPrimitive,
            **kwargs: PyLegendPrimitiveOrPythonPrimitive
    ) -> "PandasApiTdsFrame":
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
        new_series: Series
        if pct:
            new_series = FloatSeries(self._filtered_frame, self.columns()[0].get_name())
        else:
            new_series = IntegerSeries(self._filtered_frame, self.columns()[0].get_name())
        new_series.__base_frame = self.__base_frame

        applied_function_frame = self._filtered_frame.rank(axis, method, numeric_only, na_option, ascending, pct)
        assert isinstance(applied_function_frame, PandasApiAppliedFunctionTdsFrame)

        new_series._filtered_frame = applied_function_frame
        return new_series


class BooleanSeries(Series, PyLegendBoolean, PyLegendExpressionBooleanReturn):  # type: ignore
    def __init__(self, base_frame: "PandasApiTdsFrame", column: str):
        super().__init__(base_frame, column)  # pragma: no cover (Boolean column not supported in PURE)
        PyLegendBoolean.__init__(self, self)  # pragma: no cover (Boolean column not supported in PURE)


class StringSeries(Series, PyLegendString, PyLegendExpressionStringReturn):  # type: ignore
    def __init__(self, base_frame: "PandasApiTdsFrame", column: str):
        super().__init__(base_frame, column)
        PyLegendString.__init__(self, self)


class NumberSeries(Series, PyLegendNumber, PyLegendExpressionNumberReturn):  # type: ignore
    def __init__(self, base_frame: "PandasApiTdsFrame", column: str):
        super().__init__(base_frame, column)
        PyLegendNumber.__init__(self, self)


class IntegerSeries(NumberSeries, PyLegendInteger, PyLegendExpressionIntegerReturn):  # type: ignore
    def __init__(self, base_frame: "PandasApiTdsFrame", column: str):
        super().__init__(base_frame, column)
        PyLegendInteger.__init__(self, self)


class FloatSeries(NumberSeries, PyLegendFloat, PyLegendExpressionFloatReturn):  # type: ignore
    def __init__(self, base_frame: "PandasApiTdsFrame", column: str):
        super().__init__(base_frame, column)
        PyLegendFloat.__init__(self, self)


class DateSeries(Series, PyLegendDate, PyLegendExpressionDateReturn):  # type: ignore
    def __init__(self, base_frame: "PandasApiTdsFrame", column: str):
        super().__init__(base_frame, column)
        PyLegendDate.__init__(self, self)


class DateTimeSeries(DateSeries, PyLegendDateTime, PyLegendExpressionDateTimeReturn):  # type: ignore
    def __init__(self, base_frame: "PandasApiTdsFrame", column: str):
        super().__init__(base_frame, column)
        PyLegendDateTime.__init__(self, self)


class StrictDateSeries(DateSeries, PyLegendStrictDate, PyLegendExpressionStrictDateReturn):  # type: ignore
    def __init__(self, base_frame: "PandasApiTdsFrame", column: str):
        super().__init__(base_frame, column)
        PyLegendStrictDate.__init__(self, self)

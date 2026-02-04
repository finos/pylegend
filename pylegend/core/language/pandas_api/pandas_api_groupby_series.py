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

import copy
import pandas as pd
from pylegend._typing import (
    TYPE_CHECKING,
    PyLegendDict,
    PyLegendOptional,
    PyLegendSequence,
    PyLegendTypeVar,
    PyLegendUnion
)
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
    PyLegendExpressionIntegerReturn,
    PyLegendExpressionNumberReturn,
    PyLegendExpressionStrictDateReturn,
    PyLegendExpressionStringReturn
)
from pylegend.core.language.shared.primitives.boolean import PyLegendBoolean
from pylegend.core.language.shared.primitives.date import PyLegendDate
from pylegend.core.language.shared.primitives.datetime import PyLegendDateTime
from pylegend.core.language.shared.primitives.float import PyLegendFloat
from pylegend.core.language.shared.primitives.integer import PyLegendInteger
from pylegend.core.language.shared.primitives.number import PyLegendNumber
from pylegend.core.language.shared.primitives.primitive import (
    PyLegendPrimitive,
    PyLegendPrimitiveOrPythonPrimitive
)
from pylegend.core.language.shared.primitives.strictdate import PyLegendStrictDate
from pylegend.core.language.shared.primitives.string import PyLegendString
from pylegend.core.sql.metamodel import Expression, QuerySpecification
from pylegend.core.tds.abstract.frames.base_tds_frame import BaseTdsFrame
from pylegend.core.tds.pandas_api.frames.pandas_api_applied_function_tds_frame import PandasApiAppliedFunctionTdsFrame
from pylegend.core.tds.pandas_api.frames.pandas_api_groupby_tds_frame import PandasApiGroupbyTdsFrame
from pylegend.core.tds.result_handler import ResultHandler
from pylegend.core.tds.tds_column import TdsColumn
from pylegend.core.tds.tds_frame import FrameToPureConfig, FrameToSqlConfig
from pylegend.extensions.tds.result_handler import PandasDfReadConfig

if TYPE_CHECKING:
    from pylegend.core.tds.pandas_api.frames.pandas_api_tds_frame import PandasApiTdsFrame

__all__: PyLegendSequence[str] = [
    "GroupbySeries",
    "BooleanGroupbySeries",
    "StringGroupbySeries",
    "NumberGroupbySeries",
    "IntegerGroupbySeries",
    "FloatGroupbySeries",
    "DateGroupbySeries",
    "DateTimeGroupbySeries",
    "StrictDateGroupbySeries",
]

R = PyLegendTypeVar('R')


class GroupbySeries(PyLegendColumnExpression, PyLegendPrimitive, BaseTdsFrame):
    _base_groupby_frame: PandasApiGroupbyTdsFrame
    _applied_function_frame: PyLegendOptional[PandasApiAppliedFunctionTdsFrame]

    def __init__(self, base_groupby_frame: PandasApiGroupbyTdsFrame):
        selected_columns = base_groupby_frame.get_selected_columns()
        assert selected_columns is not None and len(selected_columns) == 1, (
            "To initialize a GroupbySeries object, exactly one column must be selected, "
            f"but got selected columns: {[str(col) for col in selected_columns] if selected_columns is not None else None}"
        )

        row = PandasApiTdsRow.from_tds_frame("c", base_groupby_frame.base_frame())
        PyLegendColumnExpression.__init__(self, row=row, column=selected_columns[0].get_name())

        self._base_groupby_frame: PandasApiGroupbyTdsFrame = base_groupby_frame
        self._applied_function_frame = None

    @property
    def applied_function_frame(self) -> PyLegendOptional[PandasApiAppliedFunctionTdsFrame]:
        return self._applied_function_frame

    @applied_function_frame.setter
    def applied_function_frame(self, value: PandasApiAppliedFunctionTdsFrame) -> None:
        self._applied_function_frame = value

    def _raise_exception_if_no_function_applied(self) -> PandasApiAppliedFunctionTdsFrame:
        if self._applied_function_frame is None:
            raise RuntimeError(
                "The 'groupby' function requires at least one operation to be performed right after it (e.g. aggregate, rank)"
            )
        return self._applied_function_frame

    def get_base_frame(self) -> "PandasApiGroupbyTdsFrame":
        return self._base_groupby_frame

    def to_sql_expression(
            self,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        applied_function_frame = self._raise_exception_if_no_function_applied()
        applied_func = applied_function_frame.get_applied_function()
        if isinstance(applied_func, SupportsToSqlExpression):
            return applied_func.to_sql_expression(frame_name_to_base_query_map, config)

        raise NotImplementedError(  # pragma: no cover
            f"The '{applied_func.name()}' function cannot provide a SQL expression"
        )

    def to_pure_expression(self, config: FrameToPureConfig) -> str:
        applied_function_frame = self._raise_exception_if_no_function_applied()
        applied_func = applied_function_frame.get_applied_function()
        if isinstance(applied_func, SupportsToPureExpression):
            return applied_func.to_pure_expression(config)

        raise NotImplementedError(  # pragma: no cover
            f"The '{applied_func.name()}' function cannot provide a pure expression"
        )

    def columns(self) -> PyLegendSequence[TdsColumn]:
        applied_function_frame = self._raise_exception_if_no_function_applied()
        return applied_function_frame.columns()

    def to_sql_query(self, config: FrameToSqlConfig = FrameToSqlConfig()) -> str:
        applied_function_frame = self._raise_exception_if_no_function_applied()
        return applied_function_frame.to_sql_query(config)

    def to_pure_query(self, config: FrameToPureConfig = FrameToPureConfig()) -> str:
        applied_function_frame = self._raise_exception_if_no_function_applied()
        return applied_function_frame.to_pure_query(config)

    def execute_frame(
            self,
            result_handler: ResultHandler[R],
            chunk_size: PyLegendOptional[int] = None
    ) -> R:  # pragma: no cover
        applied_function_frame = self._raise_exception_if_no_function_applied()
        return applied_function_frame.execute_frame(result_handler, chunk_size)

    def execute_frame_to_string(
            self,
            chunk_size: PyLegendOptional[int] = None
    ) -> str:  # pragma: no cover
        applied_function_frame = self._raise_exception_if_no_function_applied()
        return applied_function_frame.execute_frame_to_string(chunk_size)

    def execute_frame_to_pandas_df(
            self,
            chunk_size: PyLegendOptional[int] = None,
            pandas_df_read_config: PandasDfReadConfig = PandasDfReadConfig()
    ) -> pd.DataFrame:  # pragma: no cover
        applied_function_frame = self._raise_exception_if_no_function_applied()
        return applied_function_frame.execute_frame_to_pandas_df(chunk_size, pandas_df_read_config)

    def to_sql_query_object(self, config: FrameToSqlConfig) -> QuerySpecification:
        applied_function_frame = self._raise_exception_if_no_function_applied()
        return applied_function_frame.to_sql_query_object(config)

    def to_pure(self, config: FrameToPureConfig) -> str:
        applied_function_frame = self._raise_exception_if_no_function_applied()
        return applied_function_frame.to_pure(config)

    def get_all_tds_frames(self) -> PyLegendSequence["BaseTdsFrame"]:
        applied_function_frame = self._raise_exception_if_no_function_applied()
        return applied_function_frame.get_all_tds_frames()

    def aggregate(
            self,
            func: PyLegendAggInput,
            axis: PyLegendUnion[int, str] = 0,
            *args: PyLegendPrimitiveOrPythonPrimitive,
            **kwargs: PyLegendPrimitiveOrPythonPrimitive
    ) -> "PandasApiTdsFrame":
        new_series = copy.copy(self)
        if new_series.applied_function_frame is None:
            return new_series.get_base_frame().aggregate(func, axis, *args, **kwargs)
        else:
            return new_series.applied_function_frame.aggregate(func, axis, *args, **kwargs)

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
        numeric_only: bool = False,
        min_count: int = 0,
        engine: PyLegendOptional[str] = None,
        engine_kwargs: PyLegendOptional[PyLegendDict[str, bool]] = None,
    ) -> "PandasApiTdsFrame":
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
    ) -> "PandasApiTdsFrame":
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
    ) -> "PandasApiTdsFrame":
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
    ) -> "PandasApiTdsFrame":
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
    ) -> "PandasApiTdsFrame":
        if ddof != 1:
            raise NotImplementedError(f"Only ddof=1 (Sample Standard Deviation) is supported in std function, but got: {ddof}")
        if engine is not None:
            raise NotImplementedError("engine parameter is not supported in std function.")
        if engine_kwargs is not None:
            raise NotImplementedError("engine_kwargs parameter is not supported in std function.")
        if numeric_only is not False:
            raise NotImplementedError("numeric_only=True is not currently supported in std function.")
        return self.aggregate("std", 0)

    def var(
        self,
        ddof: int = 1,
        engine: PyLegendOptional[str] = None,
        engine_kwargs: PyLegendOptional[PyLegendDict[str, bool]] = None,
        numeric_only: bool = False,
    ) -> "PandasApiTdsFrame":
        if ddof != 1:
            raise NotImplementedError(f"Only ddof=1 (Sample Variance) is supported in var function, but got: {ddof}")
        if engine is not None:
            raise NotImplementedError("engine parameter is not supported in var function.")
        if engine_kwargs is not None:
            raise NotImplementedError("engine_kwargs parameter is not supported in var function.")
        if numeric_only is not False:
            raise NotImplementedError("numeric_only=True is not currently supported in var function.")
        return self.aggregate("var", 0)

    def count(self) -> "PandasApiTdsFrame":
        return self.aggregate("count", 0)

    def rank(
            self,
            method: str = 'min',
            ascending: bool = True,
            na_option: str = 'bottom',
            pct: bool = False,
            axis: PyLegendUnion[int, str] = 0
    ) -> "GroupbySeries":
        new_series: GroupbySeries
        if pct:
            new_series = FloatGroupbySeries(self._base_groupby_frame)
        else:
            new_series = IntegerGroupbySeries(self._base_groupby_frame)

        applied_function_frame = self._base_groupby_frame.rank(method, ascending, na_option, pct, axis)
        assert isinstance(applied_function_frame, PandasApiAppliedFunctionTdsFrame)

        new_series.applied_function_frame = applied_function_frame
        return new_series


class BooleanGroupbySeries(GroupbySeries, PyLegendBoolean, PyLegendExpressionBooleanReturn):
    def __init__(self, base_frame: "PandasApiGroupbyTdsFrame"):
        super().__init__(base_frame)  # pragma: no cover (Boolean column not supported in PURE)
        PyLegendBoolean.__init__(self, self)  # pragma: no cover (Boolean column not supported in PURE)


class StringGroupbySeries(GroupbySeries, PyLegendString, PyLegendExpressionStringReturn):
    def __init__(self, base_frame: "PandasApiGroupbyTdsFrame"):
        super().__init__(base_frame)
        PyLegendString.__init__(self, self)


class NumberGroupbySeries(GroupbySeries, PyLegendNumber, PyLegendExpressionNumberReturn):
    def __init__(self, base_frame: "PandasApiGroupbyTdsFrame"):
        super().__init__(base_frame)
        PyLegendNumber.__init__(self, self)


class IntegerGroupbySeries(NumberGroupbySeries, PyLegendInteger, PyLegendExpressionIntegerReturn):
    def __init__(self, base_frame: "PandasApiGroupbyTdsFrame"):
        super().__init__(base_frame)
        PyLegendInteger.__init__(self, self)


class FloatGroupbySeries(NumberGroupbySeries, PyLegendFloat, PyLegendExpressionFloatReturn):
    def __init__(self, base_frame: "PandasApiGroupbyTdsFrame"):
        super().__init__(base_frame)
        PyLegendFloat.__init__(self, self)


class DateGroupbySeries(GroupbySeries, PyLegendDate, PyLegendExpressionDateReturn):
    def __init__(self, base_frame: "PandasApiGroupbyTdsFrame"):
        super().__init__(base_frame)
        PyLegendDate.__init__(self, self)


class DateTimeGroupbySeries(DateGroupbySeries, PyLegendDateTime, PyLegendExpressionDateTimeReturn):
    def __init__(self, base_frame: "PandasApiGroupbyTdsFrame"):
        super().__init__(base_frame)
        PyLegendDateTime.__init__(self, self)


class StrictDateGroupbySeries(DateGroupbySeries, PyLegendStrictDate, PyLegendExpressionStrictDateReturn):
    def __init__(self, base_frame: "PandasApiGroupbyTdsFrame"):
        super().__init__(base_frame)
        PyLegendStrictDate.__init__(self, self)

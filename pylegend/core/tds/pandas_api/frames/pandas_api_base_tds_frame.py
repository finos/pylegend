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

import copy
from abc import ABCMeta, abstractmethod
from datetime import date, datetime
from typing import TYPE_CHECKING, overload

from typing_extensions import Concatenate

try:
    from typing import ParamSpec
except Exception:
    from typing_extensions import ParamSpec  # type: ignore

import pandas as pd

from pylegend._typing import (
    PyLegendSequence,
    PyLegendTypeVar,
    PyLegendType,
    PyLegendList,
    PyLegendTuple,
    PyLegendSet,
    PyLegendOptional,
    PyLegendCallable,
    PyLegendUnion,
    PyLegendDict
)
from pylegend.core.database.sql_to_string import (
    SqlToStringConfig,
    SqlToStringFormat
)
from pylegend.core.language import (
    PyLegendPrimitive,
    PyLegendInteger,
    PyLegendBoolean,
)
from pylegend.core.language.pandas_api.pandas_api_aggregate_specification import PyLegendAggInput
from pylegend.core.language.pandas_api.pandas_api_tds_row import PandasApiTdsRow
from pylegend.core.language.shared.primitives.primitive import PyLegendPrimitiveOrPythonPrimitive
from pylegend.core.language.shared.tds_row import AbstractTdsRow
from pylegend.core.sql.metamodel import QuerySpecification
from pylegend.core.tds.abstract.frames.base_tds_frame import BaseTdsFrame
from pylegend.core.tds.pandas_api.frames.pandas_api_tds_frame import PandasApiTdsFrame
from pylegend.core.tds.result_handler import (
    ResultHandler,
    ToStringResultHandler,
)
from pylegend.core.tds.tds_column import TdsColumn
from pylegend.core.tds.tds_frame import FrameToPureConfig
from pylegend.core.tds.tds_frame import FrameToSqlConfig, PyLegendTdsFrame
from pylegend.extensions.tds.result_handler import (
    ToPandasDfResultHandler,
    PandasDfReadConfig,
)

if TYPE_CHECKING:
    from pylegend.core.language.pandas_api.pandas_api_series import Series
    from pylegend.core.tds.pandas_api.frames.pandas_api_groupby_tds_frame import PandasApiGroupbyTdsFrame
    from pylegend.core.tds.pandas_api.frames.functions.iloc import PandasApiIlocIndexer
    from pylegend.core.tds.pandas_api.frames.functions.loc import PandasApiLocIndexer

__all__: PyLegendSequence[str] = [
    "PandasApiBaseTdsFrame"
]

R = PyLegendTypeVar('R')
P = ParamSpec("P")


class PandasApiBaseTdsFrame(PandasApiTdsFrame, BaseTdsFrame, metaclass=ABCMeta):
    __columns: PyLegendSequence[TdsColumn]

    def __init__(self, columns: PyLegendSequence[TdsColumn]) -> None:
        col_names = [c.get_name() for c in columns]
        if len(col_names) != len(set(col_names)):
            cols = "[" + ", ".join([str(c) for c in columns]) + "]"
            raise ValueError(f"TdsFrame cannot have duplicated column names. Passed columns: {cols}")
        self.__columns = [c.copy() for c in columns]
        self._transformed_frame = None

    def columns(self) -> PyLegendSequence[TdsColumn]:
        if self._transformed_frame is None:
            return [c.copy() for c in self.__columns]
        return self._transformed_frame.columns()

    @overload  # type: ignore[override]
    def __getitem__(self, key: str) -> "Series":
        ...

    @overload
    def __getitem__(self, key: PyLegendList[str]) -> "PandasApiTdsFrame":
        ...

    def __getitem__(
            self,
            key: PyLegendUnion[str, PyLegendList[str], PyLegendBoolean]
    ) -> PyLegendUnion["PandasApiTdsFrame", "Series"]:
        from pylegend.core.tds.pandas_api.frames.pandas_api_applied_function_tds_frame import \
            PandasApiAppliedFunctionTdsFrame
        from pylegend.core.tds.pandas_api.frames.functions.filtering import \
            PandasApiFilteringFunction
        from pylegend.core.language.shared.primitives.boolean import PyLegendBoolean

        if isinstance(key, PyLegendBoolean):
            return PandasApiAppliedFunctionTdsFrame(
                PandasApiFilteringFunction(self, filter_expr=key)
            )

        elif isinstance(key, str):
            for col in self.__columns:
                if col.get_name() == key:
                    col_type = col.get_type()
                    if col_type == "Boolean":
                        from pylegend.core.language.pandas_api.pandas_api_series import \
                            BooleanSeries  # pragma: no cover
                        return BooleanSeries(self, key)  # pragma: no cover (Boolean column not supported in PURE)
                    elif col_type == "String":
                        from pylegend.core.language.pandas_api.pandas_api_series import StringSeries
                        return StringSeries(self, key)
                    elif col_type == "Integer":
                        from pylegend.core.language.pandas_api.pandas_api_series import IntegerSeries
                        return IntegerSeries(self, key)
                    elif col_type == "Float":
                        from pylegend.core.language.pandas_api.pandas_api_series import FloatSeries
                        return FloatSeries(self, key)
                    elif col_type == "Date":
                        from pylegend.core.language.pandas_api.pandas_api_series import DateSeries
                        return DateSeries(self, key)
                    elif col_type == "DateTime":
                        from pylegend.core.language.pandas_api.pandas_api_series import DateTimeSeries
                        return DateTimeSeries(self, key)
                    elif col_type == "StrictDate":
                        from pylegend.core.language.pandas_api.pandas_api_series import StrictDateSeries
                        return StrictDateSeries(self, key)
                    else:
                        raise ValueError(f"Unsupported column type '{col_type}' for column '{key}'")  # pragma: no cover
            raise KeyError(f"['{key}'] not in index")

        elif isinstance(key, list):
            valid_col_names = {col.get_name() for col in self.__columns}
            invalid_cols = [k for k in key if k not in valid_col_names]
            if invalid_cols:
                raise KeyError(f"{invalid_cols} not in index")
            return self.filter(items=key)
        else:
            raise TypeError(f"Invalid key type: {type(key)}. Expected str, list, or boolean expression")

    def __setitem__(self, key: str, value: PyLegendUnion["Series", PyLegendPrimitiveOrPythonPrimitive]) -> None:
        """
        Pandas-like column assignment with replace semantics:
        - If column exists, drop it first.
        - Then assign the new value (Series or constant).
        """
        from pylegend.core.tds.pandas_api.frames.pandas_api_applied_function_tds_frame import (
            PandasApiAppliedFunctionTdsFrame
        )
        from pylegend.core.tds.pandas_api.frames.functions.assign_function import AssignFunction
        from pylegend.core.language.pandas_api.pandas_api_series import Series

        # Type Check
        if not isinstance(key, str):
            raise TypeError(f"Column name must be a string, got: {type(key)}")

        # Reject cross-frame assignment
        if isinstance(value, Series):
            origin = value.get_base_frame()
            if origin is not None and origin is not self:
                raise ValueError("Assignment from a different frame is not allowed")

        # Normalize the assignment value
        col_def = {}
        if callable(value):
            col_def[key] = value
        else:
            col_def[key] = lambda row: value

        working_frame = copy.deepcopy(self)
        assign_applied = PandasApiAppliedFunctionTdsFrame(AssignFunction(working_frame, col_definitions=col_def))

        self._transformed_frame = assign_applied  # type: ignore
        self.__columns = assign_applied.columns()

    def assign(
            self,
            **kwargs: PyLegendCallable[
                [PandasApiTdsRow],
                PyLegendUnion[int, float, bool, str, date, datetime, PyLegendPrimitive]
            ],
    ) -> "PandasApiTdsFrame":
        from pylegend.core.tds.pandas_api.frames.pandas_api_applied_function_tds_frame import (
            PandasApiAppliedFunctionTdsFrame
        )
        from pylegend.core.tds.pandas_api.frames.functions.assign_function import AssignFunction
        return PandasApiAppliedFunctionTdsFrame(AssignFunction(self, col_definitions=kwargs))

    def filter(
            self,
            items: PyLegendOptional[PyLegendList[str]] = None,
            like: PyLegendOptional[str] = None,
            regex: PyLegendOptional[str] = None,
            axis: PyLegendOptional[PyLegendUnion[str, int, PyLegendInteger]] = None
    ) -> "PandasApiTdsFrame":
        from pylegend.core.tds.pandas_api.frames.pandas_api_applied_function_tds_frame import (
            PandasApiAppliedFunctionTdsFrame
        )
        from pylegend.core.tds.pandas_api.frames.functions.filter import PandasApiFilterFunction
        return PandasApiAppliedFunctionTdsFrame(
            PandasApiFilterFunction(
                self,
                items=items,
                like=like,
                regex=regex,
                axis=axis
            )
        )

    def sort_values(
            self,
            by: PyLegendUnion[str, PyLegendList[str]],
            axis: PyLegendUnion[str, int] = 0,
            ascending: PyLegendUnion[bool, PyLegendList[bool]] = True,
            inplace: bool = False,
            kind: PyLegendOptional[str] = None,
            na_position: str = 'last',
            ignore_index: bool = True,
            key: PyLegendOptional[PyLegendCallable[[AbstractTdsRow], AbstractTdsRow]] = None
    ) -> "PandasApiTdsFrame":
        from pylegend.core.tds.pandas_api.frames.pandas_api_applied_function_tds_frame import (
            PandasApiAppliedFunctionTdsFrame
        )
        from pylegend.core.tds.pandas_api.frames.functions.sort_values_function import SortValuesFunction
        return PandasApiAppliedFunctionTdsFrame(SortValuesFunction(
            base_frame=self,
            by=by,
            axis=axis,
            ascending=ascending,
            inplace=inplace,
            kind=kind,
            na_position=na_position,
            ignore_index=ignore_index,
            key=key
        ))

    def truncate(
            self,
            before: PyLegendUnion[date, str, int, None] = None,
            after: PyLegendUnion[date, str, int, None] = None,
            axis: PyLegendUnion[str, int] = 0,
            copy: bool = True
    ) -> "PandasApiTdsFrame":
        from pylegend.core.tds.pandas_api.frames.pandas_api_applied_function_tds_frame import (
            PandasApiAppliedFunctionTdsFrame
        )
        from pylegend.core.tds.pandas_api.frames.functions.truncate_function import TruncateFunction
        return PandasApiAppliedFunctionTdsFrame(TruncateFunction(
            base_frame=self,
            before=before,
            after=after,
            axis=axis,
            copy=copy
        ))

    def drop(
            self,
            labels: PyLegendOptional[PyLegendUnion[str, PyLegendSequence[str], PyLegendSet[str]]] = None,
            axis: PyLegendUnion[str, int, PyLegendInteger] = 1,
            index: PyLegendOptional[PyLegendUnion[str, PyLegendSequence[str], PyLegendSet[str]]] = None,
            columns: PyLegendOptional[PyLegendUnion[str, PyLegendSequence[str], PyLegendSet[str]]] = None,
            level: PyLegendOptional[PyLegendUnion[int, PyLegendInteger, str]] = None,
            inplace: PyLegendUnion[bool, PyLegendBoolean] = False,
            errors: str = "raise",
    ) -> "PandasApiTdsFrame":
        from pylegend.core.tds.pandas_api.frames.pandas_api_applied_function_tds_frame import \
            PandasApiAppliedFunctionTdsFrame
        from pylegend.core.tds.pandas_api.frames.functions.drop import PandasApiDropFunction

        return PandasApiAppliedFunctionTdsFrame(
            PandasApiDropFunction(
                base_frame=self,
                labels=labels,
                axis=axis,
                index=index,
                columns=columns,
                level=level,
                inplace=inplace,
                errors=errors
            )
        )

    def aggregate(
            self,
            func: PyLegendAggInput,
            axis: PyLegendUnion[int, str] = 0,
            *args: PyLegendPrimitiveOrPythonPrimitive,
            **kwargs: PyLegendPrimitiveOrPythonPrimitive
    ) -> "PandasApiTdsFrame":
        from pylegend.core.tds.pandas_api.frames.pandas_api_applied_function_tds_frame import (
            PandasApiAppliedFunctionTdsFrame
        )
        from pylegend.core.tds.pandas_api.frames.functions.aggregate_function import AggregateFunction
        return PandasApiAppliedFunctionTdsFrame(AggregateFunction(
            self,
            func,
            axis,
            *args,
            **kwargs
        ))

    def agg(
            self,
            func: PyLegendAggInput,
            axis: PyLegendUnion[int, str] = 0,
            *args: PyLegendPrimitiveOrPythonPrimitive,
            **kwargs: PyLegendPrimitiveOrPythonPrimitive
    ) -> "PandasApiTdsFrame":
        from pylegend.core.tds.pandas_api.frames.pandas_api_applied_function_tds_frame import (
            PandasApiAppliedFunctionTdsFrame
        )
        from pylegend.core.tds.pandas_api.frames.functions.aggregate_function import AggregateFunction
        return PandasApiAppliedFunctionTdsFrame(AggregateFunction(
            self,
            func,
            axis,
            *args,
            **kwargs
        ))

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

    def groupby(
            self,
            by: PyLegendUnion[str, PyLegendList[str]],
            level: PyLegendOptional[PyLegendUnion[str, int, PyLegendList[str]]] = None,
            as_index: bool = False,
            sort: bool = True,
            group_keys: bool = False,
            observed: bool = False,
            dropna: bool = False,
    ) -> "PandasApiGroupbyTdsFrame":
        from pylegend.core.tds.pandas_api.frames.pandas_api_groupby_tds_frame import (
            PandasApiGroupbyTdsFrame
        )
        return PandasApiGroupbyTdsFrame(
            base_frame=self,
            by=by,
            level=level,
            as_index=as_index,
            sort=sort,
            group_keys=group_keys,
            observed=observed,
            dropna=dropna
        )

    def merge(
            self,
            other: "PandasApiTdsFrame",
            how: PyLegendOptional[str] = "inner",
            on: PyLegendOptional[PyLegendUnion[str, PyLegendSequence[str]]] = None,
            left_on: PyLegendOptional[PyLegendUnion[str, PyLegendSequence[str]]] = None,
            right_on: PyLegendOptional[PyLegendUnion[str, PyLegendSequence[str]]] = None,
            left_index: PyLegendOptional[bool] = False,
            right_index: PyLegendOptional[bool] = False,
            sort: PyLegendOptional[bool] = False,
            suffixes: PyLegendOptional[
                PyLegendUnion[
                    PyLegendTuple[PyLegendUnion[str, None], PyLegendUnion[str, None]],
                    PyLegendList[PyLegendUnion[str, None]],
                ]
            ] = ("_x", "_y"),
            indicator: PyLegendOptional[PyLegendUnion[bool, str]] = False,
            validate: PyLegendOptional[str] = None
    ) -> "PandasApiTdsFrame":
        """
        Pandas-like merge:
        - Mutually exclusive: `on` vs (`left_on`, `right_on`)
        - If no keys provided, infer intersection of column names
        - `how`: inner | left | right | outer (outer mapped to full)
        - `suffixes`: applied to overlapping non-key columns
        """
        from pylegend.core.tds.pandas_api.frames.pandas_api_applied_function_tds_frame import (
            PandasApiAppliedFunctionTdsFrame
        )
        from pylegend.core.tds.pandas_api.frames.functions.merge import (
            PandasApiMergeFunction
        )
        merge_fn = PandasApiMergeFunction(
            self,
            other,  # type: ignore
            how=how,
            on=on,
            left_on=left_on,
            right_on=right_on,
            left_index=left_index,
            right_index=right_index,
            sort=sort,
            suffixes=suffixes,
            indicator=indicator,
            validate=validate
        )
        merged = PandasApiAppliedFunctionTdsFrame(merge_fn)

        if sort:
            return merged.sort_values(
                by=merge_fn.get_sort_keys(),
                axis=0,
                ascending=True,
                inplace=False,
                kind=None,
                na_position="last",
                ignore_index=True,
                key=None
            )
        else:
            return merged

    def join(
            self,
            other: "PandasApiTdsFrame",
            on: PyLegendOptional[PyLegendUnion[str, PyLegendSequence[str]]] = None,
            how: PyLegendOptional[str] = "left",
            lsuffix: str = "",
            rsuffix: str = "",
            sort: PyLegendOptional[bool] = False,
            validate: PyLegendOptional[str] = None
    ) -> "PandasApiTdsFrame":
        """
        Pandas-like join delegating to merge. No index support, only column-on-column via `on`.
        """
        return self.merge(
            other=other,
            how=how,
            on=on,
            sort=sort,
            suffixes=[lsuffix, rsuffix],
            validate=validate
        )

    def rename(
            self,
            mapper: PyLegendOptional[PyLegendUnion[PyLegendDict[str, str], PyLegendCallable[[str], str]]] = None,
            index: PyLegendOptional[PyLegendUnion[PyLegendDict[str, str], PyLegendCallable[[str], str]]] = None,
            columns: PyLegendOptional[PyLegendUnion[PyLegendDict[str, str], PyLegendCallable[[str], str]]] = None,
            axis: PyLegendUnion[str, int] = 1,
            inplace: PyLegendUnion[bool] = False,
            copy: PyLegendUnion[bool] = True,
            level: PyLegendOptional[PyLegendUnion[int, str]] = None,
            errors: str = "ignore",
    ) -> "PandasApiTdsFrame":
        """
        Pandas-like rename:
        - Supports mapping via `mapper` or explicit `index`/`columns`
        - Only column renames are applied when `axis` is 1
        - `errors`: ignore | raise
        """

        from pylegend.core.tds.pandas_api.frames.pandas_api_applied_function_tds_frame import (
            PandasApiAppliedFunctionTdsFrame
        )
        from pylegend.core.tds.pandas_api.frames.functions.rename import (
            PandasApiRenameFunction
        )
        return PandasApiAppliedFunctionTdsFrame(
            PandasApiRenameFunction(
                base_frame=self,
                mapper=mapper,
                axis=axis,
                index=index,
                columns=columns,
                copy=copy,
                inplace=inplace,
                level=level,
                errors=errors
            )
        )

    def apply(
            self,
            func: PyLegendUnion[
                PyLegendCallable[Concatenate["Series", P], PyLegendPrimitiveOrPythonPrimitive],
                str
            ],
            axis: PyLegendUnion[int, str] = 0,
            raw: bool = False,
            result_type: PyLegendOptional[str] = None,
            args: PyLegendTuple[PyLegendPrimitiveOrPythonPrimitive, ...] = (),
            by_row: PyLegendUnion[bool, str] = "compat",
            engine: str = "python",
            engine_kwargs: PyLegendOptional[PyLegendDict[str, PyLegendPrimitiveOrPythonPrimitive]] = None,
            **kwargs: PyLegendPrimitiveOrPythonPrimitive
    ) -> "PandasApiTdsFrame":
        """
        Pandas-like apply (columns-only):
        - Supports callable func applied to each column (axis=0 or 'index')
        - Internally delegates to assign by constructing lambdas per column
        - Unsupported params raise NotImplementedError
        """

        from pylegend.core.tds.pandas_api.frames.pandas_api_applied_function_tds_frame import (
            PandasApiAppliedFunctionTdsFrame
        )
        from pylegend.core.tds.pandas_api.frames.functions.assign_function import AssignFunction
        from pylegend.core.language.pandas_api.pandas_api_series import Series

        # Validation
        if axis not in (0, "index"):
            raise ValueError("Only column-wise apply is supported. Use axis=0 or 'index'")
        if raw:
            raise NotImplementedError("raw=True is not supported. Use raw=False")
        if result_type is not None:
            raise NotImplementedError("result_type is not supported")
        if by_row not in (False, "compat"):
            raise NotImplementedError("by_row must be False or 'compat'")
        if engine != "python":
            raise NotImplementedError("Only engine='python' is supported")
        if engine_kwargs is not None:
            raise NotImplementedError("engine_kwargs are not supported")
        if isinstance(func, str):
            raise NotImplementedError("String-based apply is not supported")
        if not callable(func):
            raise TypeError("Function must be a callable")

        # Build assign column definitions: apply func to each column Series
        col_definitions = {}
        for c in self.columns():
            col_name = c.get_name()
            series = self[col_name]

            # Compute row callable via func on the Series
            def _row_callable(
                    _row: PandasApiTdsRow,
                    _s: Series = series,
                    _a: PyLegendTuple[PyLegendPrimitiveOrPythonPrimitive, ...] = args,
                    _k: PyLegendPrimitiveOrPythonPrimitive = kwargs  # type: ignore
            ) -> PyLegendPrimitiveOrPythonPrimitive:
                return func(_s, *_a, **_k)  # type: ignore

            col_definitions[col_name] = _row_callable

        return PandasApiAppliedFunctionTdsFrame(
            AssignFunction(self, col_definitions=col_definitions)  # type: ignore
        )

    @property
    def iloc(self) -> "PandasApiIlocIndexer":
        """
        Purely integer-location based indexing for selection by position.
        .iloc[] is primarily integer position based (from 0 to length-1 of the axis).

        Allowed inputs are:
        - An integer, e.g. 5.
        - A slice object with ints, e.g. 1:7.
        - A tuple of row and column indexes, e.g., (slice(1, 5), slice(0, 2))

        Other pandas iloc features such as list of integers, boolean arrays, and callables
        are not supported and will raise a NotImplementedError.
        """
        from pylegend.core.tds.pandas_api.frames.functions.iloc import PandasApiIlocIndexer
        return PandasApiIlocIndexer(self)

    @property
    def loc(self) -> "PandasApiLocIndexer":
        """
        Access a group of rows and columns by label(s) or a boolean array.
        .loc[] is primarily label based, but may also be used with a boolean array.

        Allowed inputs are:
        - A single label, e.g. 5 or 'a', (note that 5 is interpreted as a
          label of the index, not as an integer position along the index).
        - A list or array of labels, e.g. ['a', 'b', 'c'].
        - A slice object with labels, e.g. 'a':'f'.
        - A boolean array of the same length as the axis being sliced.
        - A callable function with one argument (the calling Series or
          DataFrame) and that returns valid output for indexing (one of the above).

        Currently, for row selection, only callable function or complete slice are supported.
        For column selection, string labels, lists of string labels, and slices of string labels are supported.
        """
        from pylegend.core.tds.pandas_api.frames.functions.loc import PandasApiLocIndexer
        return PandasApiLocIndexer(self)

    def head(self, n: int = 5) -> "PandasApiTdsFrame":
        """
        Return the first `n` rows by calling truncate on rows.
        Negative `n` is not supported.
        """
        if not isinstance(n, int):
            raise TypeError(f"n must be an int, got {type(n)}")
        if n < 0:
            raise NotImplementedError("Negative n is not supported yet in Pandas API head")

        return self.truncate(before=None, after=max(n - 1, -1), axis=0, copy=True)

    @property
    def shape(self) -> PyLegendTuple[int, int]:
        """
        Return a tuple representing the dimensionality of the TdsFrame
        as (number of rows, number of columns).
        """

        col_name = self.columns()[0].get_name()
        newframe = self.aggregate(func={col_name: "count"}, axis=0)

        df = newframe.execute_frame_to_pandas_df()

        total_rows = df.iloc[0, 0]
        total_cols = len(self.columns())

        return (total_rows, total_cols)  # type: ignore

    def dropna(
            self,
            axis: PyLegendUnion[int, str] = 0,
            how: str = "any",
            thresh: PyLegendOptional[int] = None,
            subset: PyLegendOptional[PyLegendUnion[str, PyLegendSequence[str]]] = None,
            inplace: bool = False,
            ignore_index: bool = False
    ) -> "PandasApiTdsFrame":
        """
        Remove missing values.

        Parameters
        ----------
        axis : {0 or 'index'}, default 0
            Determine if rows or columns which contain missing values are removed.
            * 0, or 'index' : Drop rows which contain missing values.
            Currently, only `axis=0` is supported.
        how : {'any', 'all'}, default 'any'
            Determine if row is removed from TdsFrame, when we have at least one NA or all NA.
            * 'any' : If any NA values are present, drop that row.
            * 'all' : If all values are NA, drop that row.
        thresh : int, optional
            Not implemented yet.
        subset : list-like, optional
            Labels along other axis to consider, e.g. if you are dropping rows
            these would be a list of columns to include.
        inplace : bool, default False
            Not implemented yet.
        ignore_index : bool, default False
            Not implemented yet.

        Returns
        -------
        PandasApiTdsFrame
            TdsFrame with NA entries dropped.
        """
        from pylegend.core.tds.pandas_api.frames.pandas_api_applied_function_tds_frame import (
            PandasApiAppliedFunctionTdsFrame
        )
        from pylegend.core.tds.pandas_api.frames.functions.dropna import PandasApiDropnaFunction
        return PandasApiAppliedFunctionTdsFrame(
            PandasApiDropnaFunction(
                base_frame=self,
                axis=axis,
                how=how,
                thresh=thresh,
                subset=subset,
                inplace=inplace,
                ignore_index=ignore_index
            )
        )

    def fillna(
            self,
            value: PyLegendUnion[
                int, float, str, bool, date, datetime,
                PyLegendDict[str, PyLegendUnion[int, float, str, bool, date, datetime]]
            ] = None,  # type: ignore
            axis: PyLegendOptional[PyLegendUnion[int, str]] = 0,
            inplace: bool = False,
            limit: PyLegendOptional[int] = None
    ) -> "PandasApiTdsFrame":
        """
        Fill missing values.

        Parameters
        ----------
        base_frame : PandasApiBaseTdsFrame
            The base frame to apply fillna on.
        value : scalar, dict, default None
            Value to use to fill holes (e.g. 0), alternately a dict of values specifying
            which value to use for each column of TdsFrame.
        axis : {0 or 'index'}, default 0
            Axis along which to fill missing values.
            * 0, or 'index' : Fill missing values for each column.
            Currently, only `axis=0` is supported.
        inplace : bool, default False
            Not implemented yet.
        limit : int, optional
            Not implemented yet.

        Returns
        -------
        PandasApiTdsFrame
            TdsFrame with NA entries filled.
        """
        from pylegend.core.tds.pandas_api.frames.pandas_api_applied_function_tds_frame import (
            PandasApiAppliedFunctionTdsFrame
        )
        from pylegend.core.tds.pandas_api.frames.functions.fillna import PandasApiFillnaFunction
        return PandasApiAppliedFunctionTdsFrame(
            PandasApiFillnaFunction(
                base_frame=self,
                value=value,
                axis=axis,
                inplace=inplace,
                limit=limit
            )
        )

    def rank(
            self,
            axis: PyLegendUnion[int, str] = 0,
            method: str = 'min',
            numeric_only: bool = False,
            na_option: str = 'bottom',
            ascending: bool = True,
            pct: bool = False
    ) -> "PandasApiTdsFrame":
        from pylegend.core.tds.pandas_api.frames.pandas_api_applied_function_tds_frame import (
            PandasApiAppliedFunctionTdsFrame
        )
        from pylegend.core.tds.pandas_api.frames.functions.rank_function import RankFunction
        return PandasApiAppliedFunctionTdsFrame(RankFunction(
            base_frame=self,
            axis=axis,
            method=method,
            numeric_only=numeric_only,
            na_option=na_option,
            ascending=ascending,
            pct=pct
        ))

    @abstractmethod
    def get_super_type(self) -> PyLegendType[PyLegendTdsFrame]:
        pass  # pragma: no cover

    def to_sql_query_object(self, config: FrameToSqlConfig) -> QuerySpecification:
        if self._transformed_frame is None:
            return self.get_super_type().to_sql_query_object(self, config)  # type: ignore
        else:
            return self._transformed_frame.to_sql_query_object(config)

    def to_pure(self, config: FrameToPureConfig) -> str:
        if self._transformed_frame is None:
            return self.get_super_type().to_pure(self, config)  # type: ignore
        else:
            return self._transformed_frame.to_pure(config)

    def to_pure_query(self, config: FrameToPureConfig = FrameToPureConfig()) -> str:
        return self.to_pure(config)

    @abstractmethod
    def get_all_tds_frames(self) -> PyLegendList["PandasApiBaseTdsFrame"]:
        pass  # pragma: no cover

    def to_sql_query(self, config: FrameToSqlConfig = FrameToSqlConfig()) -> str:
        query = self.to_sql_query_object(config)
        sql_to_string_config = SqlToStringConfig(
            format_=SqlToStringFormat(
                pretty=config.pretty
            )
        )
        return config.sql_to_string_generator().generate_sql_string(query, sql_to_string_config)

    def execute_frame(
            self,
            result_handler: ResultHandler[R],
            chunk_size: PyLegendOptional[int] = None
    ) -> R:
        from pylegend.core.tds.pandas_api.frames.pandas_api_input_tds_frame import (
            PandasApiInputTdsFrame,
            PandasApiExecutableInputTdsFrame
        )
        tds_frames = self.get_all_tds_frames()
        input_frames = [x for x in tds_frames if isinstance(x, PandasApiInputTdsFrame)]

        non_exec_frames = [x for x in input_frames if not isinstance(x, PandasApiExecutableInputTdsFrame)]
        if non_exec_frames:
            raise ValueError(
                "Cannot execute frame as its built on top of non-executable input frames: [" +
                (", ".join([str(f) for f in non_exec_frames]) + "]")
            )

        exec_frames = [x for x in input_frames if isinstance(x, PandasApiExecutableInputTdsFrame)]

        all_legend_clients = []
        for e in exec_frames:
            c = e.get_legend_client()
            if c not in all_legend_clients:
                all_legend_clients.append(c)
        if len(all_legend_clients) > 1:
            raise ValueError(
                "Found tds frames with multiple legend_clients (which is not supported): [" +
                (", ".join([str(f) for f in all_legend_clients]) + "]")
            )
        legend_client = all_legend_clients[0]
        result = legend_client.execute_sql_string(self.to_sql_query(), chunk_size=chunk_size)
        return result_handler.handle_result(self, result)

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
        return self.execute_frame(
            ToPandasDfResultHandler(pandas_df_read_config),
            chunk_size
        )

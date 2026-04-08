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
from typing import overload

from pylegend._typing import (
    PyLegendOptional,
    PyLegendUnion,
    PyLegendList,
    PyLegendDict,
    PyLegendSet,
    PyLegendHashable,
    PyLegendSequence,
    TYPE_CHECKING,
)
from pylegend.core.language.pandas_api.pandas_api_aggregate_specification import PyLegendAggInput
from pylegend.core.language.shared.primitives.primitive import PyLegendPrimitiveOrPythonPrimitive
from pylegend.core.tds.pandas_api.frames.helpers.series_helper import get_groupby_series_from_col_type
from pylegend.core.tds.pandas_api.frames.pandas_api_base_tds_frame import PandasApiBaseTdsFrame
from pylegend.core.tds.tds_column import TdsColumn

if TYPE_CHECKING:
    from pylegend.core.language.pandas_api.pandas_api_frame_spec import FrameSpec
    from pylegend.core.tds.pandas_api.frames.pandas_api_tds_frame import PandasApiTdsFrame
    from pylegend.core.language.pandas_api.pandas_api_groupby_series import GroupbySeries
    from pylegend.core.tds.pandas_api.frames.pandas_api_window_tds_frame import PandasApiWindowTdsFrame


class PandasApiGroupbyTdsFrame:
    __base_frame: PandasApiBaseTdsFrame
    __by: PyLegendUnion[str, PyLegendList[str]]
    __level: PyLegendOptional[PyLegendUnion[str, int, PyLegendList[str]]]
    __as_index: bool
    __sort: bool
    __group_keys: bool
    __observed: bool
    __dropna: bool

    __grouping_columns: PyLegendList[TdsColumn]
    __selected_columns: PyLegendOptional[PyLegendList[TdsColumn]]

    @classmethod
    def name(cls) -> str:
        return "groupby"  # pragma: no cover

    def __init__(
        self,
        base_frame: PandasApiBaseTdsFrame,
        by: PyLegendUnion[str, PyLegendList[str]],
        level: PyLegendOptional[PyLegendUnion[str, int, PyLegendList[str]]] = None,
        as_index: bool = False,
        sort: bool = True,
        group_keys: bool = False,
        observed: bool = False,
        dropna: bool = False,
    ) -> None:
        self.__base_frame = base_frame
        self.__by = by
        self.__level = level
        self.__as_index = as_index
        self.__sort = sort
        self.__group_keys = group_keys
        self.__observed = observed
        self.__dropna = dropna

        self.__selected_columns = None

        self.__validate()

    def __validate(self) -> None:

        if self.__level is not None:
            raise NotImplementedError(
                "The 'level' parameter of the groupby function is not supported yet. "
                "Please specify groupby column names using the 'by' parameter."
            )

        if self.__as_index is not False:
            raise NotImplementedError(
                f"The 'as_index' parameter of the groupby function must be False, "
                f"but got: {self.__as_index} (type: {type(self.__as_index).__name__})"
            )

        if self.__group_keys is not False:
            raise NotImplementedError(
                f"The 'group_keys' parameter of the groupby function must be False, "
                f"but got: {self.__group_keys} (type: {type(self.__group_keys).__name__})"
            )

        if self.__observed is not False:
            raise NotImplementedError(
                f"The 'observed' parameter of the groupby function must be False, "
                f"but got: {self.__observed} (type: {type(self.__observed).__name__})"
            )

        if self.__dropna is not False:
            raise NotImplementedError(
                f"The 'dropna' parameter of the groupby function must be False, "
                f"but got: {self.__dropna} (type: {type(self.__dropna).__name__})"
            )

        input_cols: PyLegendSet[str]
        if isinstance(self.__by, str):
            input_cols = set([self.__by])
        elif isinstance(self.__by, list):
            input_cols = set(self.__by)
        else:
            raise TypeError(
                f"The 'by' parameter in groupby function must be a string or a list of strings."
                f"but got: {self.__by} (type: {type(self.__by).__name__})"
            )  # pragma: no cover
        group_by_names: PyLegendList[str]
        if isinstance(self.__by, str):
            group_by_names = [self.__by]
        elif isinstance(self.__by, list):
            group_by_names = self.__by
        else:
            raise TypeError(
                f"The 'by' parameter in groupby function must be a string or a list of strings."
                f"but got: {self.__by} (type: {type(self.__by).__name__})"
            )  # pragma: no cover

        if len(group_by_names) == 0:
            raise ValueError("The 'by' parameter in groupby function must contain at least one column name.")

        base_col_map = {col.get_name(): col for col in self.__base_frame.columns()}

        self.__grouping_columns = [
            base_col_map[name]
            for name in group_by_names
            if name in base_col_map
        ]

        if len(self.__grouping_columns) < len(input_cols):
            available_columns = {c.get_name() for c in self.__base_frame.columns()}
            missing_cols = [col for col in input_cols if col not in available_columns]
            raise KeyError(
                f"Column(s) {missing_cols} in groupby function's provided columns list "
                f"do not exist in the current frame. "
                f"Current frame columns: {sorted(available_columns)}"
            )

    @overload
    def __getitem__(self, key: str) -> "GroupbySeries":
        ...

    @overload
    def __getitem__(self, key: PyLegendList[str]) -> "PandasApiGroupbyTdsFrame":
        ...

    def __getitem__(
            self,
            item: PyLegendUnion[str, PyLegendList[str]]
    ) -> PyLegendUnion["PandasApiGroupbyTdsFrame", "GroupbySeries"]:
        columns_to_select: PyLegendSet[str]

        if isinstance(item, str):
            columns_to_select = set([item])
        elif isinstance(item, list):
            columns_to_select = set(item)
        else:
            raise TypeError(
                f"Column selection after groupby function must be a string or a list of strings, "
                f"but got: {item} (type: {type(item).__name__})"
            )

        if len(columns_to_select) == 0:
            raise ValueError("When performing column selection after groupby, at least one column must be selected.")

        selected_columns: PyLegendList[TdsColumn] = [
            col for col in self.__base_frame.columns() if col.get_name() in columns_to_select]

        if len(selected_columns) < len(columns_to_select):
            available_columns = {c.get_name() for c in self.__base_frame.columns()}
            missing_cols = [col for col in columns_to_select if col not in available_columns]
            raise KeyError(
                f"Column(s) {missing_cols} selected after groupby do not exist in the current frame. "
                f"Current frame columns: {sorted(available_columns)}"
            )

        new_frame = PandasApiGroupbyTdsFrame(
            base_frame=self.__base_frame,
            by=self.__by,
            level=self.__level,
            as_index=self.__as_index,
            sort=self.__sort,
            group_keys=self.__group_keys,
            observed=self.__observed,
            dropna=self.__dropna,
        )

        new_frame.__selected_columns = selected_columns

        if selected_columns is not None and isinstance(item, str):
            column: TdsColumn = selected_columns[0]
            col_type = column.get_type()
            groupby_series_cls = get_groupby_series_from_col_type(col_type)
            return groupby_series_cls(new_frame)

        return new_frame

    def base_frame(self) -> PandasApiBaseTdsFrame:
        return self.__base_frame

    def get_grouping_columns(self) -> PyLegendList[TdsColumn]:
        return self.__grouping_columns.copy()

    def get_selected_columns(self) -> PyLegendOptional[PyLegendList[TdsColumn]]:
        if self.__selected_columns is None:
            return None
        return self.__selected_columns.copy()

    def aggregate(
        self,
        func: PyLegendAggInput,
        axis: PyLegendUnion[int, str] = 0,
        *args: PyLegendPrimitiveOrPythonPrimitive,
        **kwargs: PyLegendPrimitiveOrPythonPrimitive,
    ) -> "PandasApiTdsFrame":
        from pylegend.core.tds.pandas_api.frames.pandas_api_applied_function_tds_frame import PandasApiAppliedFunctionTdsFrame
        from pylegend.core.tds.pandas_api.frames.functions.aggregate_function import AggregateFunction

        aggregated_result: PandasApiAppliedFunctionTdsFrame = PandasApiAppliedFunctionTdsFrame(
            AggregateFunction(self, func, axis, *args, **kwargs)
        )

        if self.__sort:
            from pylegend.core.tds.pandas_api.frames.functions.sort_values_function import SortValuesFunction

            aggregated_result = PandasApiAppliedFunctionTdsFrame(
                SortValuesFunction(
                    base_frame=aggregated_result,
                    by=[col.get_name() for col in self.get_grouping_columns()],
                    axis=0,
                    ascending=True,
                    inplace=False,
                    kind=None,
                    na_position="last",
                    ignore_index=True,
                    key=None,
                )
            )

        return aggregated_result

    def agg(
        self,
        func: PyLegendAggInput,
        axis: PyLegendUnion[int, str] = 0,
        *args: PyLegendPrimitiveOrPythonPrimitive,
        **kwargs: PyLegendPrimitiveOrPythonPrimitive,
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
        return self.aggregate(self._numeric_only_func_map("sum"), 0)

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
        return self.aggregate(self._numeric_only_func_map("mean"), 0)

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
    ) -> "PandasApiTdsFrame":
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

    def count(self) -> "PandasApiTdsFrame":
        return self.aggregate("count", 0)

    def median(self) -> "PandasApiTdsFrame":
        numeric_func_map = self._numeric_only_func_map("median")
        return self.aggregate(numeric_func_map, 0)

    def mode(self) -> "PandasApiTdsFrame":
        numeric_func_map = self._numeric_only_func_map("mode")
        return self.aggregate(numeric_func_map, 0)

    def _numeric_only_func_map(self, func_name: str) -> PyLegendAggInput:
        """Build a {col: func_name} dict for numeric non-groupby columns only."""
        from pylegend.core.tds.tds_column import PrimitiveTdsColumn
        grouping_names = {c.get_name() for c in self.get_grouping_columns()}
        numeric_types = {
            "Integer", "Float", "Number", "Decimal",
            "TinyInt", "UTinyInt", "SmallInt", "USmallInt",
            "Int", "UInt", "BigInt", "UBigInt",
        }
        result: PyLegendDict[PyLegendHashable, str] = {}
        selected = self.get_selected_columns()
        columns = selected if selected is not None else self.base_frame().columns()
        for col in columns:
            # Skip groupby columns only when no explicit column selection was made
            # If user explicitly selected a groupby column, allow aggregation on it
            if selected is None and col.get_name() in grouping_names:
                continue
            if isinstance(col, PrimitiveTdsColumn) and col.get_type() in numeric_types:
                result[col.get_name()] = func_name
        return result

    def rank(
            self,
            method: str = 'min',
            ascending: bool = True,
            na_option: str = 'bottom',
            pct: bool = False,
            axis: PyLegendUnion[int, str] = 0
    ) -> "PandasApiTdsFrame":
        from pylegend.core.tds.pandas_api.frames.pandas_api_applied_function_tds_frame import (
            PandasApiAppliedFunctionTdsFrame
        )
        from pylegend.core.tds.pandas_api.frames.functions.rank_function import RankFunction
        return PandasApiAppliedFunctionTdsFrame(RankFunction(
            base_frame=self,
            axis=axis,
            method=method,
            numeric_only=False,
            na_option=na_option,
            ascending=ascending,
            pct=pct
        ))

    def shift(
            self,
            order_by: PyLegendUnion[str, PyLegendSequence[str]],
            periods: PyLegendUnion[int, PyLegendSequence[int]] = 1,
            freq: PyLegendOptional[PyLegendUnion[str, int]] = None,
            axis: PyLegendUnion[int, str] = 0,
            fill_value: PyLegendOptional[PyLegendHashable] = None,
            suffix: PyLegendOptional[str] = None
    ) -> "PandasApiTdsFrame":
        from pylegend.core.tds.pandas_api.frames.pandas_api_applied_function_tds_frame import (
            PandasApiAppliedFunctionTdsFrame
        )
        from pylegend.core.tds.pandas_api.frames.functions.shift_function import ShiftExtendFunction, ShiftFunction
        shift_extended_frame = PandasApiAppliedFunctionTdsFrame(ShiftExtendFunction(
            order_by=order_by,
            base_frame=self,
            periods=periods,
            freq=freq,
            axis=axis,
            fill_value=fill_value,
            suffix=suffix
        ))
        return PandasApiAppliedFunctionTdsFrame(ShiftFunction(shift_extended_frame))

    def diff(
            self,
            order_by: PyLegendUnion[str, PyLegendSequence[str]],
            periods: PyLegendUnion[int, PyLegendSequence[int]] = 1
    ) -> "PandasApiTdsFrame":
        from pylegend.core.tds.pandas_api.frames.pandas_api_applied_function_tds_frame import (
            PandasApiAppliedFunctionTdsFrame
        )
        from pylegend.core.tds.pandas_api.frames.functions.shift_function import ShiftExtendFunction, DiffFunction
        shift_extended_frame = PandasApiAppliedFunctionTdsFrame(ShiftExtendFunction(
            order_by=order_by,
            base_frame=self,
            periods=periods,
        ))
        return PandasApiAppliedFunctionTdsFrame(DiffFunction(shift_extended_frame))

    def pct_change(
            self,
            order_by: PyLegendUnion[str, PyLegendSequence[str]],
            periods: PyLegendUnion[int, PyLegendSequence[int]] = 1,
            freq: PyLegendOptional[PyLegendUnion[str, int]] = None
    ) -> "PandasApiTdsFrame":
        from pylegend.core.tds.pandas_api.frames.pandas_api_applied_function_tds_frame import (
            PandasApiAppliedFunctionTdsFrame
        )
        from pylegend.core.tds.pandas_api.frames.functions.shift_function import ShiftExtendFunction, PctChangeFunction
        shift_extended_frame = PandasApiAppliedFunctionTdsFrame(ShiftExtendFunction(
            order_by=order_by,
            base_frame=self,
            periods=periods,
            freq=freq
        ))
        return PandasApiAppliedFunctionTdsFrame(PctChangeFunction(shift_extended_frame))

    def expanding(
            self,
            min_periods: int = 1,
            method: PyLegendOptional[str] = None,
            order_by: PyLegendOptional[PyLegendUnion[str, PyLegendSequence[str]]] = None,
            ascending: PyLegendUnion[bool, PyLegendSequence[bool]] = True,
    ) -> "PandasApiWindowTdsFrame":
        from pylegend.core.tds.pandas_api.frames.pandas_api_window_tds_frame import PandasApiWindowTdsFrame
        from pylegend.core.language.pandas_api.pandas_api_frame_spec import RowsBetween

        if min_periods != 1:
            raise NotImplementedError(
                f"The expanding function is only supported for min_periods=1, but got: min_periods={min_periods!r}"
            )
        if method is not None:
            raise NotImplementedError(
                f"The expanding function does not support the 'method' parameter, but got: method={method!r}"
            )

        return PandasApiWindowTdsFrame(
            base_frame=self,
            order_by=order_by,
            frame_spec=RowsBetween(None, 0),
            ascending=ascending,
        )

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
            ascending: PyLegendUnion[bool, PyLegendSequence[bool]] = True,
    ) -> "PandasApiWindowTdsFrame":
        from pylegend.core.tds.pandas_api.frames.pandas_api_window_tds_frame import PandasApiWindowTdsFrame
        from pylegend.core.language.pandas_api.pandas_api_frame_spec import RowsBetween

        if min_periods is not None and min_periods != 1:
            raise NotImplementedError(
                f"The rolling function is only supported for min_periods=1 or None, but got: min_periods={min_periods!r}"
            )
        if center is not False:
            raise NotImplementedError(
                f"The rolling function does not support center=True, but got: center={center!r}"
            )
        if win_type is not None:
            raise NotImplementedError(
                f"The rolling function does not support the 'win_type' parameter, but got: win_type={win_type!r}"
            )
        if on is not None:
            raise NotImplementedError(
                f"The rolling function does not support the 'on' parameter, but got: on={on!r}"
            )
        if closed is not None:
            raise NotImplementedError(
                f"The rolling function does not support the 'closed' parameter, but got: closed={closed!r}"
            )
        if step is not None:
            raise NotImplementedError(
                f"The rolling function does not support the 'step' parameter, but got: step={step!r}"
            )
        if method is not None:
            raise NotImplementedError(
                f"The rolling function does not support the 'method' parameter, but got: method={method!r}"
            )

        return PandasApiWindowTdsFrame(
            base_frame=self,
            order_by=order_by,
            frame_spec=RowsBetween(-(window - 1), 0),
            ascending=ascending,
        )

    def window_frame_legend_ext(
            self,
            frame_spec: "FrameSpec",
            order_by: PyLegendOptional[PyLegendUnion[str, PyLegendSequence[str]]] = None,
            ascending: PyLegendUnion[bool, "PyLegendSequence[bool]"] = True,
    ) -> "PandasApiWindowTdsFrame":
        """
        PyLegend extension (not present in pandas).

        Create a custom window specification with explicit control over the
        window frame.  When called on a groupby frame the grouping columns
        are automatically used as PARTITION BY columns.

        Parameters
        ----------
        frame_spec:
            A ``RowsBetween`` or ``RangeBetween`` specification object.
        order_by:
            Column name(s) to use for ORDER BY within the window.
        ascending:
            Sort direction(s) for the ORDER BY columns.
        """
        from pylegend.core.tds.pandas_api.frames.pandas_api_window_tds_frame import PandasApiWindowTdsFrame
        from pylegend.core.language.pandas_api.pandas_api_frame_spec import FrameSpec as FrameSpecCls

        if not isinstance(frame_spec, FrameSpecCls):
            raise TypeError(
                f"frame_spec must be a RowsBetween or RangeBetween, got {type(frame_spec).__name__}"
            )

        return PandasApiWindowTdsFrame(
            base_frame=self,
            order_by=order_by,
            frame_spec=frame_spec,
            ascending=ascending,
        )

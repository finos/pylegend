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


from pylegend._typing import (
    PyLegendUnion,
    TYPE_CHECKING,
)
from pylegend.core.language.pandas_api.pandas_api_aggregate_specification import PyLegendAggInput
from pylegend.core.language.shared.primitives.primitive import PyLegendPrimitiveOrPythonPrimitive
from pylegend.core.tds.pandas_api.frames.helpers.series_helper import get_series_from_col_type, \
    get_groupby_series_from_col_type
from pylegend.core.tds.pandas_api.frames.pandas_api_window_tds_frame import PandasApiWindowTdsFrame

if TYPE_CHECKING:
    from pylegend.core.language.pandas_api.pandas_api_series import Series
    from pylegend.core.language.pandas_api.pandas_api_groupby_series import GroupbySeries


class WindowSeries:
    """
    Represents a single column selected on a window frame.

    Created by ``__getitem__`` on a ``PandasApiWindowTdsFrame``, e.g.::

        frame.expanding()["col"]
        frame.groupby("grp").expanding()["col"]

    Calling an aggregate (e.g. ``.sum()``, ``.mean()``) on this object produces
    a ``Series`` (or ``GroupbySeries``) that can be assigned back to a frame column.
    """

    _window_frame: PandasApiWindowTdsFrame
    _column_name: str

    def __init__(
        self,
        window_frame: PandasApiWindowTdsFrame,
        column_name: str,
    ) -> None:
        self._window_frame = window_frame
        self._column_name = column_name

    @property
    def window_frame(self) -> PandasApiWindowTdsFrame:
        return self._window_frame

    @property
    def column_name(self) -> str:
        return self._column_name

    def aggregate(
        self,
        func: PyLegendAggInput,
        axis: PyLegendUnion[int, str] = 0,
        *args: PyLegendPrimitiveOrPythonPrimitive,
        **kwargs: PyLegendPrimitiveOrPythonPrimitive,
    ) -> PyLegendUnion["Series", "GroupbySeries"]:
        from pylegend.core.tds.pandas_api.frames.pandas_api_applied_function_tds_frame import (
            PandasApiAppliedFunctionTdsFrame,
        )
        from pylegend.core.tds.pandas_api.frames.pandas_api_groupby_tds_frame import PandasApiGroupbyTdsFrame
        from pylegend.core.tds.pandas_api.frames.functions.window_aggregate_function import (
            WindowAggregateFunction,
        )

        base = self._window_frame._base_frame
        base_frame_unwrapped = self._window_frame.base_frame()
        column = self._column_name

        # Wrap scalar func to target only the selected column
        single_col_func: PyLegendAggInput = {column: func} if isinstance(func, (str, list)) or callable(func) else func

        applied_function_frame = PandasApiAppliedFunctionTdsFrame(
            WindowAggregateFunction(self._window_frame, single_col_func, axis, *args, **kwargs)
        )

        result_columns = applied_function_frame.columns()
        assert len(result_columns) == 1, (
            "WindowSeries.aggregate() should produce exactly one result column"
        )
        col_type = result_columns[0].get_type()

        if isinstance(base, PandasApiGroupbyTdsFrame):
            gb_series_cls = get_groupby_series_from_col_type(col_type)
            # Use __getitem__ to get a groupby frame with the column selected
            new_gb_frame_or_series = base[column]
            if isinstance(new_gb_frame_or_series, PandasApiGroupbyTdsFrame):
                new_gb_frame = new_gb_frame_or_series  # pragma: no cover
            else:
                # __getitem__ with a string returns a GroupbySeries; extract its frame
                new_gb_frame = new_gb_frame_or_series._base_groupby_frame
            return gb_series_cls(new_gb_frame, applied_function_frame)
        else:
            series_cls = get_series_from_col_type(col_type)
            new_series = series_cls(base_frame_unwrapped, column)
            new_series._filtered_frame = applied_function_frame
            return new_series

    agg = aggregate

    def sum(
        self,
        numeric_only: bool = False,
        min_count: int = 0,
    ) -> PyLegendUnion["Series", "GroupbySeries"]:
        if numeric_only is not False:
            raise NotImplementedError("numeric_only=True is not currently supported in sum function.")
        if min_count != 0:
            raise NotImplementedError(f"min_count must be 0 in sum function, but got: {min_count}")
        return self.aggregate("sum", 0)

    def mean(
        self,
        numeric_only: bool = False,
    ) -> PyLegendUnion["Series", "GroupbySeries"]:
        if numeric_only is not False:
            raise NotImplementedError("numeric_only=True is not currently supported in mean function.")
        return self.aggregate("mean", 0)

    def min(
        self,
        numeric_only: bool = False,
    ) -> PyLegendUnion["Series", "GroupbySeries"]:
        if numeric_only is not False:
            raise NotImplementedError("numeric_only=True is not currently supported in min function.")
        return self.aggregate("min", 0)

    def max(
        self,
        numeric_only: bool = False,
    ) -> PyLegendUnion["Series", "GroupbySeries"]:
        if numeric_only is not False:
            raise NotImplementedError("numeric_only=True is not currently supported in max function.")
        return self.aggregate("max", 0)

    def count(self) -> PyLegendUnion["Series", "GroupbySeries"]:
        return self.aggregate("count", 0)

    def std(
        self,
        ddof: int = 1,
        numeric_only: bool = False,
    ) -> PyLegendUnion["Series", "GroupbySeries"]:
        if numeric_only is not False:
            raise NotImplementedError("numeric_only=True is not currently supported in std function.")
        if ddof == 1:
            return self.aggregate("std_dev_sample", 0)
        elif ddof == 0:
            return self.aggregate("std_dev_population", 0)
        else:
            raise NotImplementedError(
                f"Only ddof=0 (Population) and ddof=1 (Sample) are supported in std function, but got: {ddof}"
            )

    def var(
        self,
        ddof: int = 1,
        numeric_only: bool = False,
    ) -> PyLegendUnion["Series", "GroupbySeries"]:
        if numeric_only is not False:
            raise NotImplementedError("numeric_only=True is not currently supported in var function.")
        if ddof == 1:
            return self.aggregate("variance_sample", 0)
        elif ddof == 0:
            return self.aggregate("variance_population", 0)
        else:
            raise NotImplementedError(
                f"Only ddof=0 (Population) and ddof=1 (Sample) are supported in var function, but got: {ddof}"
            )

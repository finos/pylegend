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

from pylegend._typing import (
    PyLegendList,
    PyLegendOptional,
    PyLegendSequence,
    PyLegendUnion,
    TYPE_CHECKING,
)
from pylegend.core.language.pandas_api.pandas_api_aggregate_specification import PyLegendAggInput
from pylegend.core.language.pandas_api.pandas_api_custom_expressions import (
    PandasApiFrameBound,
    PandasApiFrameBoundType,
    PandasApiSortDirection,
    PandasApiSortInfo,
    PandasApiWindow,
    PandasApiWindowFrame,
    PandasApiWindowFrameMode,
)
from pylegend.core.language.shared.primitives.primitive import PyLegendPrimitiveOrPythonPrimitive
from pylegend.core.tds.pandas_api.frames.pandas_api_base_tds_frame import PandasApiBaseTdsFrame
from pylegend.core.tds.pandas_api.frames.pandas_api_groupby_tds_frame import PandasApiGroupbyTdsFrame

if TYPE_CHECKING:
    from pylegend.core.language.pandas_api.pandas_api_window_series import WindowSeries

ZERO_COLUMN_NAME = "__internal_pylegend_column__"


class PandasApiWindowTdsFrame:
    """
    Represents a window specification over a base frame.

    Created by ``expanding()`` or ``rolling()`` on a frame or groupby frame.
    Calling an aggregate (e.g. ``.sum()``, ``.mean()``) on this object will
    produce a windowed aggregate that can be assigned back to a frame column.

    Parameters
    ----------
    base_frame:
        The underlying frame or groupby frame.
    order_by:
        Column name(s) to use for ORDER BY within the window.
        ``None`` means no explicit ordering (caller must provide a fallback).
    preceding_rows:
        Number of rows preceding the current row to include.
        ``None`` means unbounded preceding.
    following_rows:
        Number of rows following the current row to include.
        ``None`` means unbounded following.
        ``0`` means current row (the default for expanding/rolling).
    """

    _base_frame: PyLegendUnion[PandasApiBaseTdsFrame, PandasApiGroupbyTdsFrame]
    _order_by: PyLegendOptional[PyLegendUnion[str, PyLegendSequence[str]]]
    _preceding_rows: PyLegendOptional[int]
    _following_rows: PyLegendOptional[int]
    _partition_only: bool

    def __init__(
            self,
            base_frame: PyLegendUnion[PandasApiBaseTdsFrame, PandasApiGroupbyTdsFrame],
            order_by: PyLegendOptional[PyLegendUnion[str, PyLegendSequence[str]]] = None,
            preceding_rows: PyLegendOptional[int] = None,
            following_rows: PyLegendOptional[int] = 0,
            partition_only: bool = False,
    ) -> None:
        self._base_frame = base_frame
        self._order_by = order_by
        self._preceding_rows = preceding_rows
        self._following_rows = following_rows
        self._partition_only = partition_only

    def base_frame(self) -> PandasApiBaseTdsFrame:
        """Return the unwrapped base frame (unwrapping groupby if needed)."""
        if isinstance(self._base_frame, PandasApiGroupbyTdsFrame):
            return self._base_frame.base_frame()
        return self._base_frame

    def get_partition_columns(self) -> PyLegendList[str]:
        """Return grouping column names if the base is a groupby frame, else empty."""
        if isinstance(self._base_frame, PandasApiGroupbyTdsFrame):
            return [col.get_name() for col in self._base_frame.get_grouping_columns()]
        return []

    def with_order_by(
        self,
        order_by: PyLegendUnion[str, PyLegendSequence[str]],
    ) -> "PandasApiWindowTdsFrame":
        """Return a shallow copy of this window frame with a different order_by."""
        new = copy.copy(self)
        new._order_by = order_by
        return new

    def construct_window(self, include_zero_column: bool = True) -> PandasApiWindow:
        """
        Build a ``PandasApiWindow`` from this window specification.
        Uses the ``order_by`` parameter provided at construction time.
        Always includes the zero column in PARTITION BY unless ``include_zero_column`` is False.

        When ``partition_only`` is True, produces a window with only PARTITION BY
        (no ORDER BY, no frame bounds, no zero column) — equivalent to pandas ``transform()``.
        """
        if self._partition_only:
            partition_cols = self.get_partition_columns()
            return PandasApiWindow(
                partition_by=partition_cols or None,
                order_by=None,
                frame=None,
            )

        partition_cols = self.get_partition_columns()
        if include_zero_column:
            partition_cols = partition_cols + [ZERO_COLUMN_NAME]
        partition_by = partition_cols or None

        order_by: PyLegendOptional[PyLegendList[PandasApiSortInfo]] = None
        if self._order_by is not None:
            order_by_list = (
                [self._order_by] if isinstance(self._order_by, str)
                else list(self._order_by)
            )
            order_by = [
                PandasApiSortInfo(col, PandasApiSortDirection.ASC)
                for col in order_by_list
            ]

        # Build start bound
        if self._preceding_rows is None:
            start = PandasApiFrameBound(PandasApiFrameBoundType.UNBOUNDED_PRECEDING)
        elif self._preceding_rows == 0:
            start = PandasApiFrameBound(PandasApiFrameBoundType.CURRENT_ROW)
        else:
            start = PandasApiFrameBound(PandasApiFrameBoundType.PRECEDING, self._preceding_rows)

        # Build end bound
        if self._following_rows is None:
            end = PandasApiFrameBound(PandasApiFrameBoundType.UNBOUNDED_FOLLOWING)
        elif self._following_rows == 0:
            end = PandasApiFrameBound(PandasApiFrameBoundType.CURRENT_ROW)
        else:
            end = PandasApiFrameBound(PandasApiFrameBoundType.FOLLOWING, self._following_rows)

        window_frame = PandasApiWindowFrame(PandasApiWindowFrameMode.ROWS, start, end)

        return PandasApiWindow(
            partition_by=partition_by,
            order_by=order_by,
            frame=window_frame,
        )

    def __getitem__(self, column_name: str) -> "WindowSeries":
        from pylegend.core.language.pandas_api.pandas_api_window_series import WindowSeries
        return WindowSeries(window_frame=self, column_name=column_name)

    def aggregate(
            self,
            func: PyLegendAggInput,
            axis: PyLegendUnion[int, str] = 0,
            *args: PyLegendPrimitiveOrPythonPrimitive,
            **kwargs: PyLegendPrimitiveOrPythonPrimitive,
    ) -> PandasApiBaseTdsFrame:
        from pylegend.core.tds.pandas_api.frames.pandas_api_applied_function_tds_frame import (
            PandasApiAppliedFunctionTdsFrame,
        )
        from pylegend.core.tds.pandas_api.frames.functions.window_aggregate_function import (
            WindowAggregateFunction,
        )
        return PandasApiAppliedFunctionTdsFrame(
            WindowAggregateFunction(self, func, axis, *args, **kwargs)
        )

    agg = aggregate

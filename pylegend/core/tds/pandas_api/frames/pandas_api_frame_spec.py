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
    PyLegendOptional,
)
from pylegend.core.language.pandas_api.pandas_api_custom_expressions import (
    PandasApiFrameBound,
    PandasApiFrameBoundType,
    PandasApiWindowFrameMode,
)


class FrameSpec:
    """
    Base class for window frame specifications.

    Sign convention (same as legendQL):
      * ``None``  → UNBOUNDED (PRECEDING for start, FOLLOWING for end)
      * Negative  → PRECEDING (e.g. ``-3`` → ``3 PRECEDING``)
      * ``0``     → CURRENT ROW
      * Positive  → FOLLOWING (e.g. ``2`` → ``2 FOLLOWING``)
    """

    _start: PyLegendOptional[int]
    _end: PyLegendOptional[int]
    _frame_mode: PandasApiWindowFrameMode

    def __init__(
            self,
            start: PyLegendOptional[int],
            end: PyLegendOptional[int],
            frame_mode: PandasApiWindowFrameMode,
    ) -> None:
        if start is not None and end is not None and start > end:
            raise ValueError(f"start ({start}) must be <= end ({end})")
        self._start = start
        self._end = end
        self._frame_mode = frame_mode

    @property
    def start(self) -> PyLegendOptional[int]:
        return self._start

    @property
    def end(self) -> PyLegendOptional[int]:
        return self._end

    @property
    def frame_mode(self) -> PandasApiWindowFrameMode:
        return self._frame_mode

    def build_start_bound(self) -> PandasApiFrameBound:
        return self._build_frame_bound(self._start, is_start=True)

    def build_end_bound(self) -> PandasApiFrameBound:
        return self._build_frame_bound(self._end, is_start=False)

    @staticmethod
    def _build_frame_bound(value: PyLegendOptional[int], is_start: bool) -> PandasApiFrameBound:
        """
        Convert a signed integer (or None) into a ``PandasApiFrameBound``.

        Sign convention:
          * ``None``  → UNBOUNDED PRECEDING (if is_start) / UNBOUNDED FOLLOWING (if not is_start)
          * ``0``     → CURRENT ROW
          * negative  → PRECEDING with ``abs(value)``
          * positive  → FOLLOWING with ``value``
        """
        if value is None:
            if is_start:
                return PandasApiFrameBound(PandasApiFrameBoundType.UNBOUNDED_PRECEDING)
            else:
                return PandasApiFrameBound(PandasApiFrameBoundType.UNBOUNDED_FOLLOWING)
        elif value == 0:
            return PandasApiFrameBound(PandasApiFrameBoundType.CURRENT_ROW)
        elif value < 0:
            return PandasApiFrameBound(PandasApiFrameBoundType.PRECEDING, abs(value))
        else:
            return PandasApiFrameBound(PandasApiFrameBoundType.FOLLOWING, value)


class RowsBetween(FrameSpec):
    """Specification for a ROWS BETWEEN window frame."""

    def __init__(self, start: PyLegendOptional[int] = None, end: PyLegendOptional[int] = None) -> None:
        super().__init__(start, end, PandasApiWindowFrameMode.ROWS)


class RangeBetween(FrameSpec):
    """Specification for a RANGE BETWEEN window frame."""

    def __init__(self, start: PyLegendOptional[int] = None, end: PyLegendOptional[int] = None) -> None:
        super().__init__(start, end, PandasApiWindowFrameMode.RANGE)


def rows_between(start: PyLegendOptional[int] = None, end: PyLegendOptional[int] = None) -> RowsBetween:
    """Create a ROWS BETWEEN frame specification."""
    return RowsBetween(start, end)


def range_between(start: PyLegendOptional[int] = None, end: PyLegendOptional[int] = None) -> RangeBetween:
    """Create a RANGE BETWEEN frame specification."""
    return RangeBetween(start, end)


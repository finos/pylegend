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

from decimal import Decimal as PythonDecimal

from pylegend._typing import (
    PyLegendOptional,
    PyLegendUnion,
)
from pylegend.core.language.pandas_api.pandas_api_custom_expressions import (
    PandasApiDurationUnit,
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

    _frame_mode: PandasApiWindowFrameMode

    def __init__(self, frame_mode: PandasApiWindowFrameMode) -> None:
        self._frame_mode = frame_mode

    @property
    def frame_mode(self) -> PandasApiWindowFrameMode:
        return self._frame_mode

    def build_start_bound(self) -> PandasApiFrameBound:
        raise NotImplementedError  # pragma: no cover

    def build_end_bound(self) -> PandasApiFrameBound:
        raise NotImplementedError  # pragma: no cover

    @staticmethod
    def _build_frame_bound(
            value: PyLegendOptional[PyLegendUnion[int, float, PythonDecimal]],
            is_start: bool,
            duration_unit: PyLegendOptional[PandasApiDurationUnit] = None,
    ) -> PandasApiFrameBound:
        """
        Convert a signed number (or None) into a ``PandasApiFrameBound``.

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
            return PandasApiFrameBound(PandasApiFrameBoundType.CURRENT_ROW, duration_unit=duration_unit)
        elif value < 0:
            abs_val: PyLegendUnion[int, float, PythonDecimal] = abs(value)  # type: ignore
            return PandasApiFrameBound(PandasApiFrameBoundType.PRECEDING, abs_val, duration_unit=duration_unit)
        else:
            return PandasApiFrameBound(PandasApiFrameBoundType.FOLLOWING, value, duration_unit=duration_unit)


class RowsBetween(FrameSpec):
    """Specification for a ROWS BETWEEN window frame."""

    _start: PyLegendOptional[int]
    _end: PyLegendOptional[int]

    def __init__(self, start: PyLegendOptional[int] = None, end: PyLegendOptional[int] = None) -> None:
        super().__init__(PandasApiWindowFrameMode.ROWS)
        if start is not None and end is not None and start > end:
            raise ValueError(
                "Invalid window frame boundary - lower bound of window"
                " frame cannot be greater than the upper bound!"
            )
        self._start = start
        self._end = end

    def build_start_bound(self) -> PandasApiFrameBound:
        return self._build_frame_bound(self._start, is_start=True)

    def build_end_bound(self) -> PandasApiFrameBound:
        return self._build_frame_bound(self._end, is_start=False)


class RangeBetween(FrameSpec):
    """
    Specification for a RANGE BETWEEN window frame.

    Supports two calling styles:

    **Simple numeric bounds** (same sign convention as ``RowsBetween``)::

        range_between(start=-100, end=0)
        # → RANGE BETWEEN 100 PRECEDING AND CURRENT ROW

    **Duration-based bounds** (for date/time ORDER BY columns)::

        range_between(
            duration_start=-1, duration_start_unit="DAYS",
            duration_end=1, duration_end_unit="MONTHS",
        )
        # → RANGE BETWEEN INTERVAL '1 DAY' PRECEDING AND INTERVAL '1 MONTH' FOLLOWING

    Either side may be ``None`` (unbounded) or ``"unbounded"`` (string alias
    accepted only when using the duration kwargs).
    """

    _start: PyLegendOptional[PyLegendUnion[int, float, PythonDecimal]]
    _end: PyLegendOptional[PyLegendUnion[int, float, PythonDecimal]]
    _start_duration_unit: PyLegendOptional[PandasApiDurationUnit]
    _end_duration_unit: PyLegendOptional[PandasApiDurationUnit]

    def __init__(
            self,
            start: PyLegendOptional[PyLegendUnion[int, float, PythonDecimal]] = None,
            end: PyLegendOptional[PyLegendUnion[int, float, PythonDecimal]] = None,
            *,
            duration_start: PyLegendOptional[PyLegendUnion[int, float, PythonDecimal, str]] = None,
            duration_start_unit: PyLegendOptional[str] = None,
            duration_end: PyLegendOptional[PyLegendUnion[int, float, PythonDecimal, str]] = None,
            duration_end_unit: PyLegendOptional[str] = None,
    ) -> None:
        super().__init__(PandasApiWindowFrameMode.RANGE)

        has_simple = start is not None or end is not None
        has_duration = (duration_start is not None or duration_start_unit is not None
                        or duration_end is not None or duration_end_unit is not None)

        if has_simple and has_duration:
            raise ValueError(
                "Cannot mix positional start/end with duration_start/duration_end keyword arguments"
            )

        if has_duration:
            self._start, self._start_duration_unit = self._parse_duration_bound(
                duration_start, duration_start_unit, "duration_start"
            )
            self._end, self._end_duration_unit = self._parse_duration_bound(
                duration_end, duration_end_unit, "duration_end"
            )
        else:
            if start is not None and end is not None and start > end:
                raise ValueError(
                    "Invalid window frame boundary - lower bound of window"
                    " frame cannot be greater than the upper bound!"
                )
            self._start = start
            self._end = end
            self._start_duration_unit = None
            self._end_duration_unit = None

    @staticmethod
    def _parse_duration_bound(
            value: PyLegendOptional[PyLegendUnion[int, float, PythonDecimal, str]],
            unit: PyLegendOptional[str],
            param_name: str,
    ) -> "tuple[PyLegendOptional[PyLegendUnion[int, float, PythonDecimal]], PyLegendOptional[PandasApiDurationUnit]]":
        if value is None:
            return None, None
        if isinstance(value, str):
            if value.lower() == "unbounded":
                return None, None
            raise ValueError(
                f"{param_name} string value must be 'unbounded', got '{value}'"
            )
        duration_unit = PandasApiDurationUnit.from_string(unit) if unit is not None else None
        return value, duration_unit

    def build_start_bound(self) -> PandasApiFrameBound:
        return self._build_frame_bound(self._start, is_start=True, duration_unit=self._start_duration_unit)

    def build_end_bound(self) -> PandasApiFrameBound:
        return self._build_frame_bound(self._end, is_start=False, duration_unit=self._end_duration_unit)

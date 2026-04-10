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
    PandasApiSortDirection,
    PandasApiSortInfo,
    PandasApiWindow,
    PandasApiWindowFrame,
)
from pylegend.core.language.shared.primitives.primitive import PyLegendPrimitiveOrPythonPrimitive
from pylegend.core.tds.pandas_api.frames.pandas_api_base_tds_frame import PandasApiBaseTdsFrame
from pylegend.core.language.pandas_api.pandas_api_frame_spec import FrameSpec, RowsBetween
from pylegend.core.tds.pandas_api.frames.pandas_api_groupby_tds_frame import PandasApiGroupbyTdsFrame

if TYPE_CHECKING:
    from pylegend.core.language.pandas_api.pandas_api_window_series import WindowSeries

ZERO_COLUMN_NAME = "__pylegend_zero_column__"


class PandasApiWindowTdsFrame:
    """
    Represents a window specification over a base frame.

    Created by ``expanding()``, ``rolling()``, or ``window_frame_legend_ext()``
    on a :class:`~pylegend.core.tds.pandas_api.frames.pandas_api_tds_frame.PandasApiTdsFrame`
    or a
    :class:`~pylegend.core.tds.pandas_api.frames.pandas_api_groupby_tds_frame.PandasApiGroupbyTdsFrame`.
    Calling an aggregate (e.g. ``.aggregate('sum')``) on this object produces
    a new TDS frame whose columns contain the windowed values.  Alternatively,
    use bracket notation to select a single column first and obtain a
    :class:`~pylegend.core.language.pandas_api.pandas_api_window_series.WindowSeries`.

    Obtaining a PandasApiWindowTdsFrame
    ------------------------------------
    .. code-block:: python

        # Expanding (cumulative) window — all rows up to current row
        window = frame.expanding(order_by="col")

        # Fixed-size sliding window — 5 preceding rows to current row
        window = frame.rolling(5, order_by="col")

        # Custom window bounds (pylegend extension)
        window = frame.window_frame_legend_ext(
            frame_spec=frame.rows_between(-3, 3),
            order_by="col",
        )

    When created from a groupby, the grouping columns are automatically
    used as ``PARTITION BY`` in the generated SQL:

    .. code-block:: python

        window = frame.groupby("grp").expanding(order_by="val")
        # SQL: ... PARTITION BY "grp", ... ORDER BY "val" ...

    Selecting a single column
    -------------------------
    Use bracket notation to narrow to one column before aggregating.
    This returns a
    :class:`~pylegend.core.language.pandas_api.pandas_api_window_series.WindowSeries`
    whose aggregate result can be assigned back to the parent frame:

    .. code-block:: python

        frame["cumsum"] = frame.expanding(order_by="col")["col"].sum()

    Order-by resolution
    -------------------
    If no ``order_by`` is supplied when creating the window, the first
    column of the base frame is used as a fallback.  Providing an
    explicit ``order_by`` is recommended for deterministic results.

    Parameters
    ----------
    base_frame :
        The underlying frame or groupby frame.
    order_by : str, list of str, or None
        Column name(s) to use for ``ORDER BY`` within the window.
        ``None`` means no explicit ordering (the first column of the
        base frame is used as a fallback).
    frame_spec : RowsBetween or RangeBetween, optional
        A ``FrameSpec`` describing the window frame bounds.  Defaults
        to ``ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING``
        when ``None``.
    ascending : bool or list of bool, default True
        Sort direction(s) for the ``ORDER BY`` columns.  ``True``
        means ascending.  Can be a single ``bool`` (applied to all
        columns) or a ``list[bool]`` whose length must match the
        number of ``order_by`` columns.

    See Also
    --------
    PandasApiTdsFrame.expanding : Create an expanding window.
    PandasApiTdsFrame.rolling : Create a rolling window.
    PandasApiTdsFrame.window_frame_legend_ext : Create a custom window.
    WindowSeries : Single-column proxy on a window frame.

    Notes
    -----
    **Differences from pandas:**

    - In pandas, ``Expanding`` and ``Rolling`` objects have built-in
      convenience methods (``sum()``, ``mean()``, etc.) that return a
      ``DataFrame``.  Here, the window frame object exposes only
      ``aggregate()`` / ``agg()`` for multi-column use.  For
      single-column convenience methods (``sum()``, ``mean()``, etc.),
      use bracket notation to get a ``WindowSeries`` first.
    - When created from a groupby, the grouping columns are
      **excluded** from the result columns (only the aggregated
      value columns appear).
    - The ``order_by`` parameter is a pylegend extension.  In pandas,
      window ordering relies on the implicit order of the DataFrame
      index.
    - Extra ``*args`` / ``**kwargs`` on ``aggregate()`` are **not
      supported**.

    Examples
    --------
    .. ipython:: python

        import pylegend
        frame = pylegend.samples.pandas_api.northwind_orders_frame()

        # Multi-column expanding sum (returns a new frame)
        frame.filter(
            items=["Order Id"]
        ).expanding(order_by="Order Id").aggregate("sum").head(5).to_pandas()

        # Single-column rolling mean via WindowSeries, assigned back
        frame["Rolling Mean"] = frame.rolling(
            3, order_by="Order Id"
        )["Order Id"].mean()
        frame.head(5).to_pandas()

    """

    _base_frame: PyLegendUnion[PandasApiBaseTdsFrame, PandasApiGroupbyTdsFrame]
    _order_by: PyLegendOptional[PyLegendList[str]]
    _ascending: PyLegendList[bool]
    _frame_spec: FrameSpec
    _partition_only: bool

    def __init__(
            self,
            base_frame: PyLegendUnion[PandasApiBaseTdsFrame, PandasApiGroupbyTdsFrame],
            order_by: PyLegendOptional[PyLegendUnion[str, PyLegendSequence[str]]] = None,
            frame_spec: PyLegendOptional[FrameSpec] = None,
            ascending: PyLegendUnion[bool, PyLegendSequence[bool]] = True,
            partition_only: bool = False,
    ) -> None:
        self._base_frame = base_frame
        self._frame_spec = frame_spec if frame_spec is not None else RowsBetween(None, None)
        self._partition_only = partition_only

        # Normalize order_by to Optional[List[str]]
        if order_by is None:
            order_by_list: PyLegendOptional[PyLegendList[str]] = None
        elif isinstance(order_by, str):
            order_by_list = [order_by]
        else:
            order_by_list = list(order_by)
        self._order_by = order_by_list

        # Normalize ascending to List[bool] matching order_by length
        if isinstance(ascending, bool):
            num_cols = len(order_by_list) if order_by_list is not None else 0
            self._ascending = [ascending] * num_cols
        else:
            ascending_list = list(ascending)
            if order_by_list is not None and len(ascending_list) != len(order_by_list):
                raise ValueError(
                    f"Length of ascending ({len(ascending_list)}) must match "
                    f"length of order_by ({len(order_by_list)})"
                )
            self._ascending = ascending_list

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
        ascending: PyLegendUnion[bool, PyLegendSequence[bool]] = True,
    ) -> "PandasApiWindowTdsFrame":
        """Return a shallow copy of this window frame with a different order_by."""
        new = copy.copy(self)
        if isinstance(order_by, str):
            new._order_by = [order_by]  # pragma: no cover
        else:
            new._order_by = list(order_by)

        if isinstance(ascending, bool):
            new._ascending = [ascending] * len(new._order_by)  # pragma: no cover
        else:
            asc_list = list(ascending)
            if len(asc_list) != len(new._order_by):
                raise ValueError(  # pragma: no cover
                    f"Length of ascending ({len(asc_list)}) must match "
                    f"length of order_by ({len(new._order_by)})"
                )
            new._ascending = asc_list
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
            order_by = [
                PandasApiSortInfo(
                    col,
                    PandasApiSortDirection.ASC if asc else PandasApiSortDirection.DESC,
                )
                for col, asc in zip(self._order_by, self._ascending)
            ]

        start = self._frame_spec.build_start_bound()
        end = self._frame_spec.build_end_bound()

        window_frame = PandasApiWindowFrame(self._frame_spec.frame_mode, start, end)

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
        """
        Apply a window aggregate function across all non-grouping columns.

        Compute the window aggregate specified by ``func`` over the
        window defined by this ``PandasApiWindowTdsFrame``.  The result
        is a new TDS frame whose columns contain the windowed values.

        Parameters
        ----------
        func : str, callable, list, or dict
            Aggregation specification.  Accepted forms:

            - ``str`` — a named aggregation (e.g. ``'sum'``, ``'mean'``,
              ``'min'``, ``'max'``, ``'count'``, ``'std'``, ``'var'``).
            - ``callable`` — a function receiving a column proxy and
              returning an aggregated value.
            - ``list`` — a list of the above.
            - ``dict`` — a mapping of column name → aggregation(s).
        axis : {{0, 'index'}}, default 0
            Only ``0`` / ``'index'`` is supported.
        *args
            Not supported.
        **kwargs
            Not supported.

        Returns
        -------
        PandasApiBaseTdsFrame
            A new TDS frame with the windowed aggregate values.

        See Also
        --------
        agg : Alias for ``aggregate``.
        PandasApiTdsFrame.aggregate : Frame-level aggregation (no window).
        PandasApiGroupbyTdsFrame.aggregate : Grouped aggregation.

        Notes
        -----
        **Differences from pandas:**

        - In pandas, ``Expanding.aggregate()`` and ``Rolling.aggregate()``
          accept ``*args`` and ``**kwargs`` forwarded to the aggregation
          function.  Here, extra positional and keyword arguments are
          **not supported**.
        - The result is a full TDS frame (all non-grouping columns are
          aggregated), not a single column.  Use bracket notation on the
          window frame to select a single column before aggregating
          (returns a ``WindowSeries``).

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            # Expanding sum over all numeric columns
            frame.filter(
                items=["Order Id"]
            ).expanding(order_by="Order Id").aggregate("sum").head(5).to_pandas()

            # Rolling mean with a window of 3
            frame.filter(
                items=["Order Id"]
            ).rolling(3, order_by="Order Id").aggregate("mean").head(5).to_pandas()

        """
        from pylegend.core.tds.pandas_api.frames.pandas_api_applied_function_tds_frame import (
            PandasApiAppliedFunctionTdsFrame,
        )
        from pylegend.core.tds.pandas_api.frames.functions.window_aggregate_function import (
            WindowAggregateFunction,
        )
        return PandasApiAppliedFunctionTdsFrame(
            WindowAggregateFunction(self, func, axis, *args, **kwargs)
        )

    def agg(
            self,
            func: PyLegendAggInput,
            axis: PyLegendUnion[int, str] = 0,
            *args: PyLegendPrimitiveOrPythonPrimitive,
            **kwargs: PyLegendPrimitiveOrPythonPrimitive,
    ) -> PandasApiBaseTdsFrame:
        """
        Apply a window aggregate function across all non-grouping columns.

        Alias for :meth:`aggregate`.  See ``aggregate`` for full
        documentation.

        See Also
        --------
        aggregate : Equivalent method (canonical name).
        """
        return self.aggregate(func, axis, *args, **kwargs)

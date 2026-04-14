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
    PyLegendUnion,
    TYPE_CHECKING,
)
from pylegend.core.language.pandas_api.pandas_api_aggregate_specification import PyLegendAggInput
from pylegend.core.language.shared.primitives.primitive import PyLegendPrimitiveOrPythonPrimitive
from pylegend.core.tds.pandas_api.frames.functions.single_column_window_function import ValueFunc, AggFunc
from pylegend.core.tds.pandas_api.frames.helpers.series_helper import get_series_from_col_type, \
    get_groupby_series_from_col_type
from pylegend.core.tds.pandas_api.frames.pandas_api_window_tds_frame import PandasApiWindowTdsFrame

if TYPE_CHECKING:
    from pylegend.core.language.pandas_api.pandas_api_series import Series
    from pylegend.core.language.pandas_api.pandas_api_groupby_series import GroupbySeries


class WindowSeries:
    """
    A single-column proxy on a window frame.

    A ``WindowSeries`` is obtained by bracket-indexing a
    :class:`~pylegend.core.tds.pandas_api.frames.pandas_api_window_tds_frame.PandasApiWindowTdsFrame`
    with a column name.  It can also be obtained by calling
    ``expanding()``, ``rolling()``, or ``window_frame_legend_ext()``
    directly on a
    :class:`~pylegend.core.language.pandas_api.pandas_api_series.Series`
    or
    :class:`~pylegend.core.language.pandas_api.pandas_api_groupby_series.GroupbySeries`.

    Calling an aggregation method (``sum()``, ``mean()``, etc.) on a
    ``WindowSeries`` returns a
    :class:`~pylegend.core.language.pandas_api.pandas_api_series.Series`
    (or a
    :class:`~pylegend.core.language.pandas_api.pandas_api_groupby_series.GroupbySeries`
    when the underlying window was created from a groupby).  Positional
    window functions (``first()``, ``last()``, ``shift()``) and the
    general-purpose ``window_extend_legend_ext()`` are also available.
    The result can then be assigned back to the parent frame.

    Obtaining a WindowSeries
    -------------------------
    .. code-block:: python

        # Via bracket notation on a window frame
        ws = frame.expanding(order_by="col")["col"]

        # Via Series shortcut
        ws = frame["col"].expanding(order_by="col")

        # Grouped variant (returns GroupbySeries after aggregation)
        ws = frame.groupby("grp")["val"].expanding(order_by="val")

    Result type preservation
    -------------------------
    The type of the returned ``Series`` (or ``GroupbySeries``) matches
    the column type.  For example, an integer column produces an
    ``IntegerSeries`` after ``.sum()``, while ``count()`` always
    returns an ``IntegerSeries`` regardless of the source column type.

    Composing with arithmetic
    -------------------------
    The ``Series`` returned by a ``WindowSeries`` aggregation supports
    arithmetic, so expressions like the following work:

    .. code-block:: python

        frame["shifted"] = frame["col"].expanding().sum() - 100
        frame["ratio"]   = frame["a"].expanding().sum() / frame["b"].rolling(3).mean()

    Multiple window assignments can be applied sequentially to the
    same frame:

    .. code-block:: python

        frame["cumsum"]    = frame["col"].expanding().sum()
        frame["roll_mean"] = frame["col2"].rolling(5, order_by="col2").mean()

    See Also
    --------
    PandasApiWindowTdsFrame : The window frame that produces this.
    Series : Non-grouped single-column proxy.
    GroupbySeries : Grouped single-column proxy.
    PandasApiTdsFrame.expanding : Create an expanding window on a frame.
    PandasApiTdsFrame.rolling : Create a rolling window on a frame.

    Notes
    -----
    **Differences from pandas:**

    - A ``WindowSeries`` is **not** a data container.  It is an
      expression builder that lazily constructs the SQL / Pure query.
      No data is materialised until the result is executed.
    - In pandas, ``Expanding['col']`` and ``Rolling['col']`` have
      built-in convenience methods that return a ``Series``.  Here,
      the same convenience methods are available (``sum()``,
      ``mean()``, ``min()``, ``max()``, ``count()``, ``std()``,
      ``var()``), plus positional window methods (``first()``,
      ``last()``, ``shift()``), and a general ``aggregate()`` /
      ``agg()`` method.  ``window_extend_legend_ext()`` is available
      for fully custom window expressions.
    - Extra ``*args`` / ``**kwargs`` on ``aggregate()`` are **not
      supported**.
    - The ``numeric_only`` parameter on convenience methods is **not
      supported** and must be ``False``.

    Examples
    --------
    .. ipython:: python

        import pylegend
        frame = pylegend.samples.pandas_api.northwind_orders_frame()

        # Assign an expanding sum via WindowSeries
        frame["Cumulative Sum"] = frame.expanding(
            order_by="Order Id"
        )["Order Id"].sum()
        frame.head(5).to_pandas()

        frame = pylegend.samples.pandas_api.northwind_orders_frame()

        # Grouped expanding sum assigned back
        frame["Group Cumsum"] = frame.groupby(
            "Ship Name"
        )["Order Id"].expanding(order_by="Order Id").sum()
        frame.head(5).to_pandas()

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
        """
        Apply a window aggregate to this single column.

        Compute the window aggregate specified by ``func`` over the
        window defined on this ``WindowSeries``.  The result is a
        :class:`~pylegend.core.language.pandas_api.pandas_api_series.Series`
        (or
        :class:`~pylegend.core.language.pandas_api.pandas_api_groupby_series.GroupbySeries`
        when the underlying window was created from a groupby) that
        can be assigned back to a frame column.

        Parameters
        ----------
        func : str, callable, list, or dict
            Aggregation specification:

            - ``str`` — a named aggregation (``'sum'``, ``'mean'``,
              ``'min'``, ``'max'``, ``'count'``, ``'std'``, ``'var'``).
            - ``callable`` — a function receiving a column proxy and
              returning an aggregated value.
            - ``list`` — a list of the above.
            - ``dict`` — ``{column_name: agg_spec}``.
        axis : {{0, 'index'}}, default 0
            Only ``0`` / ``'index'`` is supported.
        *args
            Not supported.
        **kwargs
            Not supported.

        Returns
        -------
        Series or GroupbySeries
            A single-column proxy containing the windowed aggregate
            values.

        See Also
        --------
        agg : Alias for ``aggregate``.
        sum : Windowed sum convenience method.
        mean : Windowed mean convenience method.
        PandasApiWindowTdsFrame.aggregate : Window aggregate on
            all columns.

        Notes
        -----
        **Differences from pandas:**

        - In pandas, ``Expanding['col'].aggregate()`` and
          ``Rolling['col'].aggregate()`` accept ``*args`` and
          ``**kwargs`` forwarded to the aggregation function.  Here,
          extra positional and keyword arguments are **not supported**.
        - The result is always a single-column proxy (``Series`` or
          ``GroupbySeries``), never a DataFrame.

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            # Expanding sum on a single column
            frame["Expanding Sum"] = frame.expanding(
                order_by="Order Id"
            )["Order Id"].aggregate("sum")
            frame.head(5).to_pandas()

        """
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

    def agg(
        self,
        func: PyLegendAggInput,
        axis: PyLegendUnion[int, str] = 0,
        *args: PyLegendPrimitiveOrPythonPrimitive,
        **kwargs: PyLegendPrimitiveOrPythonPrimitive,
    ) -> PyLegendUnion["Series", "GroupbySeries"]:
        """
        Apply a window aggregate to this single column.

        Alias for :meth:`aggregate`.  See ``aggregate`` for full
        documentation.

        See Also
        --------
        aggregate : Equivalent method (canonical name).
        """
        return self.aggregate(func, axis, *args, **kwargs)

    def sum(
        self,
        numeric_only: bool = False,
        min_count: int = 0,
    ) -> PyLegendUnion["Series", "GroupbySeries"]:
        """
        Compute the windowed sum of this column.

        Convenience method equivalent to ``aggregate('sum')`` on this
        window series.

        Parameters
        ----------
        numeric_only : bool, default False
            Must be ``False``. ``True`` is not supported.
        min_count : int, default 0
            Must be ``0``. Non-zero values are not supported.

        Returns
        -------
        Series or GroupbySeries
            A single-column proxy containing the windowed sum values.

        Raises
        ------
        NotImplementedError
            If any parameter is set to an unsupported value.

        See Also
        --------
        aggregate : General windowed aggregation.
        mean : Windowed mean.
        PandasApiTdsFrame.sum : Frame-level sum (no window).

        Notes
        -----
        **Differences from pandas:**

        - ``numeric_only`` and ``min_count`` are **not supported**
          and must remain at their default values.

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            # Expanding sum on a single column
            frame["Expanding Sum"] = frame.expanding(
                order_by="Order Id"
            )["Order Id"].sum()
            frame.head(5).to_pandas()

        """
        if numeric_only is not False:
            raise NotImplementedError("numeric_only=True is not currently supported in sum function.")
        if min_count != 0:
            raise NotImplementedError(f"min_count must be 0 in sum function, but got: {min_count}")
        return self.aggregate("sum", 0)

    def mean(
        self,
        numeric_only: bool = False,
    ) -> PyLegendUnion["Series", "GroupbySeries"]:
        """
        Compute the windowed mean of this column.

        Convenience method equivalent to ``aggregate('mean')`` on this
        window series.

        Parameters
        ----------
        numeric_only : bool, default False
            Must be ``False``. ``True`` is not supported.

        Returns
        -------
        Series or GroupbySeries
            A single-column proxy containing the windowed mean values.

        Raises
        ------
        NotImplementedError
            If ``numeric_only`` is ``True``.

        See Also
        --------
        aggregate : General windowed aggregation.
        sum : Windowed sum.
        PandasApiTdsFrame.mean : Frame-level mean (no window).

        Notes
        -----
        **Differences from pandas:**

        - ``numeric_only`` is **not supported** and must be ``False``.

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            # Rolling mean with a window of 3
            frame["Rolling Mean"] = frame.rolling(
                3, order_by="Order Id"
            )["Order Id"].mean()
            frame.head(5).to_pandas()

        """
        if numeric_only is not False:
            raise NotImplementedError("numeric_only=True is not currently supported in mean function.")
        return self.aggregate("mean", 0)

    def min(
        self,
        numeric_only: bool = False,
    ) -> PyLegendUnion["Series", "GroupbySeries"]:
        """
        Compute the windowed minimum of this column.

        Convenience method equivalent to ``aggregate('min')`` on this
        window series.

        Parameters
        ----------
        numeric_only : bool, default False
            Must be ``False``. ``True`` is not supported.

        Returns
        -------
        Series or GroupbySeries
            A single-column proxy containing the windowed minimum
            values.

        Raises
        ------
        NotImplementedError
            If ``numeric_only`` is ``True``.

        See Also
        --------
        aggregate : General windowed aggregation.
        max : Windowed maximum.
        PandasApiTdsFrame.min : Frame-level min (no window).

        Notes
        -----
        **Differences from pandas:**

        - ``numeric_only`` is **not supported** and must be ``False``.

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            # Expanding min on a single column
            frame["Expanding Min"] = frame.expanding(
                order_by="Order Id"
            )["Order Id"].min()
            frame.head(5).to_pandas()

        """
        if numeric_only is not False:
            raise NotImplementedError("numeric_only=True is not currently supported in min function.")
        return self.aggregate("min", 0)

    def max(
        self,
        numeric_only: bool = False,
    ) -> PyLegendUnion["Series", "GroupbySeries"]:
        """
        Compute the windowed maximum of this column.

        Convenience method equivalent to ``aggregate('max')`` on this
        window series.

        Parameters
        ----------
        numeric_only : bool, default False
            Must be ``False``. ``True`` is not supported.

        Returns
        -------
        Series or GroupbySeries
            A single-column proxy containing the windowed maximum
            values.

        Raises
        ------
        NotImplementedError
            If ``numeric_only`` is ``True``.

        See Also
        --------
        aggregate : General windowed aggregation.
        min : Windowed minimum.
        PandasApiTdsFrame.max : Frame-level max (no window).

        Notes
        -----
        **Differences from pandas:**

        - ``numeric_only`` is **not supported** and must be ``False``.

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            # Expanding max on a single column
            frame["Expanding Max"] = frame.expanding(
                order_by="Order Id"
            )["Order Id"].max()
            frame.head(5).to_pandas()

        """
        if numeric_only is not False:
            raise NotImplementedError("numeric_only=True is not currently supported in max function.")
        return self.aggregate("max", 0)

    def count(self) -> PyLegendUnion["Series", "GroupbySeries"]:
        """
        Compute the windowed count of non-null values for this column.

        Convenience method equivalent to ``aggregate('count')`` on this
        window series.

        Returns
        -------
        Series or GroupbySeries
            A single-column proxy containing the windowed count values.
            The return type is always ``IntegerSeries`` (or its
            ``GroupbySeries`` equivalent), regardless of the source
            column's type.

        See Also
        --------
        aggregate : General windowed aggregation.
        sum : Windowed sum.
        PandasApiTdsFrame.count : Frame-level count (no window).

        Notes
        -----
        **Differences from pandas:**

        - The signature takes no parameters.  The pandas
          ``Expanding.count()`` / ``Rolling.count()`` accept
          ``numeric_only`` which is not supported here.

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            # Expanding count on a single column
            frame["Expanding Count"] = frame.expanding(
                order_by="Order Id"
            )["Order Id"].count()
            frame.head(5).to_pandas()

        """
        return self.aggregate("count", 0)

    def std(
        self,
        ddof: int = 1,
        numeric_only: bool = False,
    ) -> PyLegendUnion["Series", "GroupbySeries"]:
        """
        Compute the windowed standard deviation of this column.

        Convenience method equivalent to ``aggregate('std')`` on this
        window series.

        Parameters
        ----------
        ddof : int, default 1
            Degrees of freedom.  ``1`` for sample standard deviation
            (``STDDEV_SAMP``), ``0`` for population standard deviation
            (``STDDEV_POP``).
        numeric_only : bool, default False
            Must be ``False``. ``True`` is not supported.

        Returns
        -------
        Series or GroupbySeries
            A single-column proxy containing the windowed standard
            deviation values.

        Raises
        ------
        NotImplementedError
            If ``ddof`` is not ``0`` or ``1``, or if ``numeric_only``
            is ``True``.

        See Also
        --------
        aggregate : General windowed aggregation.
        var : Windowed variance.
        PandasApiTdsFrame.std : Frame-level std (no window).

        Notes
        -----
        **Differences from pandas:**

        - Only ``ddof=0`` (population) and ``ddof=1`` (sample) are
          supported.  Other values raise ``NotImplementedError``.
        - ``numeric_only`` is **not supported** and must be ``False``.

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            # Rolling standard deviation with a window of 3
            frame["Rolling Std"] = frame.rolling(
                3, order_by="Order Id"
            )["Order Id"].std()
            frame.head(5).to_pandas()

        """
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
        """
        Compute the windowed variance of this column.

        Convenience method equivalent to ``aggregate('var')`` on this
        window series.

        Parameters
        ----------
        ddof : int, default 1
            Degrees of freedom.  ``1`` for sample variance
            (``VAR_SAMP``), ``0`` for population variance
            (``VAR_POP``).
        numeric_only : bool, default False
            Must be ``False``. ``True`` is not supported.

        Returns
        -------
        Series or GroupbySeries
            A single-column proxy containing the windowed variance
            values.

        Raises
        ------
        NotImplementedError
            If ``ddof`` is not ``0`` or ``1``, or if ``numeric_only``
            is ``True``.

        See Also
        --------
        aggregate : General windowed aggregation.
        std : Windowed standard deviation.
        PandasApiTdsFrame.var : Frame-level var (no window).

        Notes
        -----
        **Differences from pandas:**

        - Only ``ddof=0`` (population) and ``ddof=1`` (sample) are
          supported.  Other values raise ``NotImplementedError``.
        - ``numeric_only`` is **not supported** and must be ``False``.

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            # Rolling variance with a window of 3
            frame["Rolling Var"] = frame.rolling(
                3, order_by="Order Id"
            )["Order Id"].var()
            frame.head(5).to_pandas()

        """
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

    def window_extend_legend_ext(
            self,
            value_func: "ValueFunc",
            agg_func: "PyLegendOptional[AggFunc]" = None,
    ) -> PyLegendUnion["Series", "GroupbySeries"]:
        """
        Apply a custom window function to this single column.

        **PyLegend extension** — not present in pandas.

        Compute a user-defined window expression for the selected column.
        The ``value_func`` receives three arguments —
        a :class:`PandasApiPartialFrame` (``p``), a
        :class:`PandasApiWindowReference` (``w``), and a
        :class:`PandasApiTdsRow` (``r``) — and must return a single
        primitive.  The result is a ``Series`` (or ``GroupbySeries``)
        that can be assigned back to the parent frame.

        Parameters
        ----------
        value_func : callable
            ``(p, w, r) -> primitive``.

            Common patterns:

            - ``lambda p, w, r: p.first(w, r)["col"]``  — first value.
            - ``lambda p, w, r: p.last(w, r)["col"]``   — last value.
            - ``lambda p, w, r: p.nth(w, r, 3)["col"]`` — nth value.
            - ``lambda p, w, r: p.lag(r, 1)["col"]``    — lag.
            - ``lambda p, w, r: p.lead(r, 2)["col"]``   — lead.
            - ``lambda p, w, r: r["col"]``               — raw column
              ref (combined with ``agg_func``).
        agg_func : callable or None, default None
            ``(collection) -> primitive``.  If provided, an additional
            aggregation step (e.g. ``lambda c: c.sum()``) is applied
            on top of the ``value_func`` result.

        Returns
        -------
        Series or GroupbySeries
            A single-column proxy containing the window function result.

        See Also
        --------
        PandasApiWindowTdsFrame.window_extend_legend_ext :
            Same operation applied to all columns.
        first : Convenience wrapper using ``p.first(w, r)["col"]``.
        last : Convenience wrapper using ``p.last(w, r)["col"]``.
        shift : Convenience wrapper for lag/lead.

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            # nth-value of a single column
            frame["Nth Order"] = frame.window_frame_legend_ext(
                frame_spec=frame.rows_between(),
                order_by="Order Id",
            )["Order Id"].window_extend_legend_ext(
                value_func=lambda p, w, r: p.nth(w, r, 3)["Order Id"],
            )
            frame.head(5).to_pandas()

        """
        from pylegend.core.tds.pandas_api.frames.pandas_api_applied_function_tds_frame import (
            PandasApiAppliedFunctionTdsFrame,
        )
        from pylegend.core.tds.pandas_api.frames.pandas_api_groupby_tds_frame import PandasApiGroupbyTdsFrame
        from pylegend.core.tds.pandas_api.frames.functions.single_column_window_function import (
            SingleColumnWindowFunction,
        )

        column = self._column_name
        base = self._window_frame._base_frame
        base_frame_unwrapped = self._window_frame.base_frame()

        applied_function_frame = PandasApiAppliedFunctionTdsFrame(
            SingleColumnWindowFunction(
                base_window_frame=self._window_frame,
                value_func=value_func,
                agg_func=agg_func,
            )
        )

        result_columns = applied_function_frame.columns()
        assert len(result_columns) == 1, (
            "WindowSeries.window_extend_legend_ext() should produce exactly one result column"
        )
        col_type = result_columns[0].get_type()

        if isinstance(base, PandasApiGroupbyTdsFrame):
            gb_series_cls = get_groupby_series_from_col_type(col_type)
            new_gb_frame_or_series = base[column]
            if isinstance(new_gb_frame_or_series, PandasApiGroupbyTdsFrame):  # pragma: no cover
                new_gb_frame = new_gb_frame_or_series
            else:
                new_gb_frame = new_gb_frame_or_series._base_groupby_frame
            return gb_series_cls(new_gb_frame, applied_function_frame)
        else:
            series_cls = get_series_from_col_type(col_type)
            new_series = series_cls(base_frame_unwrapped, column)
            new_series._filtered_frame = applied_function_frame
            return new_series

    def first(self) -> PyLegendUnion["Series", "GroupbySeries"]:
        """
        Return the first value in the window for this column.

        Generates ``first_value(col) OVER (...)`` in SQL.

        Returns
        -------
        Series or GroupbySeries
            A single-column proxy containing the first value within
            the window for every row.

        See Also
        --------
        last : Last value in the window.
        PandasApiWindowTdsFrame.first : All-column version.
        shift : Lag/lead by N rows.

        Notes
        -----
        **Differences from pandas:**

        - ``first()`` is a **pylegend extension**.  There is no
          ``Expanding['col'].first()`` or ``Rolling['col'].first()``
          in pandas.
        - Internally delegates to ``window_extend_legend_ext`` with
          ``value_func = lambda p, w, r: p.first(w, r)["col"]``.

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            frame["First Order"] = frame.window_frame_legend_ext(
                frame_spec=frame.rows_between(),
                order_by="Order Id",
            )["Order Id"].first()
            frame.head(5).to_pandas()

        """
        from pylegend.core.language.pandas_api.pandas_api_custom_expressions import (
            PandasApiPartialFrame,
            PandasApiWindowReference,
        )
        from pylegend.core.language.pandas_api.pandas_api_tds_row import PandasApiTdsRow

        column = self._column_name

        def value_func(
                p: PandasApiPartialFrame,
                w: PandasApiWindowReference,
                r: PandasApiTdsRow,
                _col: str = column,
        ) -> "PyLegendPrimitiveOrPythonPrimitive":
            return p.first(w, r)[_col]

        return self.window_extend_legend_ext(value_func=value_func)

    def last(self) -> PyLegendUnion["Series", "GroupbySeries"]:
        """
        Return the last value in the window for this column.

        Generates ``last_value(col) OVER (...)`` in SQL.

        Returns
        -------
        Series or GroupbySeries
            A single-column proxy containing the last value within
            the window for every row.

        See Also
        --------
        first : First value in the window.
        PandasApiWindowTdsFrame.last : All-column version.
        shift : Lag/lead by N rows.

        Notes
        -----
        **Differences from pandas:**

        - ``last()`` is a **pylegend extension**.  There is no
          ``Expanding['col'].last()`` or ``Rolling['col'].last()``
          in pandas.

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            frame["Last Order"] = frame.window_frame_legend_ext(
                frame_spec=frame.rows_between(),
                order_by="Order Id",
            )["Order Id"].last()
            frame.head(5).to_pandas()

        """
        from pylegend.core.language.pandas_api.pandas_api_custom_expressions import (
            PandasApiPartialFrame,
            PandasApiWindowReference,
        )
        from pylegend.core.language.pandas_api.pandas_api_tds_row import PandasApiTdsRow

        column = self._column_name

        def value_func(
                p: PandasApiPartialFrame,
                w: PandasApiWindowReference,
                r: PandasApiTdsRow,
                _col: str = column,
        ) -> "PyLegendPrimitiveOrPythonPrimitive":
            return p.last(w, r)[_col]

        return self.window_extend_legend_ext(value_func=value_func)

    def shift(
            self,
            periods: int = 1,
            freq: PyLegendOptional[str] = None,
            axis: int = 0,
            fill_value: PyLegendOptional[object] = None,
            suffix: PyLegendOptional[str] = None,
    ) -> PyLegendUnion["Series", "GroupbySeries"]:
        """
        Shift (lag or lead) this column by N rows within the window.

        Generates ``lag(col, N)`` for positive ``periods`` and
        ``lead(col, N)`` for non-positive ``periods`` in SQL.

        Because lag/lead SQL functions do not accept a frame clause,
        ``shift()`` automatically strips the ``frame_spec`` when it is
        the default ``RowsBetween(None, None)`` or ``None``.  If a
        non-default frame spec (e.g. ``rows_between(-2, 2)``) is set,
        a ``ValueError`` is raised.

        Parameters
        ----------
        periods : int, default 1
            Number of rows to shift.

            - ``periods = 1`` - ``lag`` (look backward).
            - ``periods = -1`` - ``lead`` (look forward), with
              offset ``abs(periods)``.
            - ``periods = 0`` → ``lead(col, 0)`` (current row).
        freq : str or None, default None
            **Not supported.**  Raises ``NotImplementedError``.
        axis : {{0, 'index'}}, default 0
            Only ``0`` / ``'index'`` is supported.
        fill_value : object or None, default None
            **Not supported.**  Raises ``NotImplementedError``.
        suffix : str or None, default None
            **Not supported.**  Raises ``NotImplementedError``.

        Returns
        -------
        Series or GroupbySeries
            A single-column proxy containing the shifted values.

        Raises
        ------
        NotImplementedError
            If ``freq``, ``fill_value``, ``suffix`` is not ``None``,
            ``axis`` is not ``0``, or ``periods`` is not an ``int``.
        ValueError
            If the window has a non-default ``frame_spec`` (only
            ``RowsBetween(None, None)`` or ``None`` are permitted).

        See Also
        --------
        first : First value in the window.
        last : Last value in the window.

        Notes
        -----
        **Differences from pandas:**

        - In pandas, ``Series.shift()`` accepts ``freq``,
          ``fill_value``, and ``suffix``, none of which are supported
          here.
        - ``shift()`` does **not** mutate the original window frame.
          Internally it creates a shallow copy with
          ``frame_spec=None`` so that the generated SQL omits the
          ``ROWS BETWEEN`` / ``RANGE BETWEEN`` clause.

        **Edge cases:**

        - ``shift(periods=0)`` generates ``lead(col, 0)``, which
          returns the current row's value (identity operation).

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            # Previous row's Order Id (lag by 1)
            frame["Prev Order"] = frame.window_frame_legend_ext(
                order_by="Order Id",
            )["Order Id"].shift(periods=1)
            frame.head(5).to_pandas()

            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            # Next row's Order Id (lead by 1)
            frame["Next Order"] = frame.window_frame_legend_ext(
                order_by="Order Id",
            )["Order Id"].shift(periods=-1)
            frame.head(5).to_pandas()

        """
        if freq is not None:
            raise NotImplementedError(
                f"The 'freq' argument of the shift function is not supported, but got: freq={freq!r}"
            )
        if axis not in [0, "index"]:
            raise NotImplementedError(
                f"The 'axis' argument of the shift function must be 0 or 'index', but got: axis={axis!r}"
            )
        if fill_value is not None:
            raise NotImplementedError(
                f"The 'fill_value' argument of the shift function is not supported, but got: fill_value={fill_value!r}"
            )
        if suffix is not None:
            raise NotImplementedError(
                f"The 'suffix' argument of the shift function is not supported for WindowSeries, but got: suffix={suffix!r}"
            )
        if not (isinstance(periods, int) and periods not in [-1, 1]):
            raise NotImplementedError(
                "The 'periods' argument of the shift function must be an int for WindowSeries."
            )

        import copy
        from pylegend.core.language.pandas_api.pandas_api_frame_spec import RowsBetween
        from pylegend.core.language.pandas_api.pandas_api_custom_expressions import (
            PandasApiPartialFrame,
            PandasApiWindowReference,
        )
        from pylegend.core.language.pandas_api.pandas_api_tds_row import PandasApiTdsRow

        # lag/lead window functions do not support a frame clause.
        # Ensure frame_spec is either None or RowsBetween(None, None) (the default),
        # then use a copy of the window frame with frame_spec=None
        frame_spec = self._window_frame._frame_spec
        if frame_spec is None:
            shift_window_series = self
        elif (isinstance(frame_spec, RowsBetween)
              and frame_spec._start is None
              and frame_spec._end is None):
            # Default RowsBetween(None, None) or manually put - make a shallow copy with frame_spec=None
            copied_window_frame = copy.copy(self._window_frame)
            copied_window_frame._frame_spec = None
            shift_window_series = WindowSeries(
                window_frame=copied_window_frame,
                column_name=self._column_name,
            )
        else:
            raise ValueError(
                "The shift function (lag/lead) does not support a window frame clause. "
                "frame_spec must be None or RowsBetween(None, None), "
                f"but got: {frame_spec!r}"
            )

        column = self._column_name

        if periods > 0:
            def value_func(
                    p: PandasApiPartialFrame,
                    w: PandasApiWindowReference,
                    r: PandasApiTdsRow,
                    _col: str = column,
                    _periods: int = periods,
            ) -> "PyLegendPrimitiveOrPythonPrimitive":
                return p.lag(r, _periods)[_col]
        else:
            def value_func(
                    p: PandasApiPartialFrame,
                    w: PandasApiWindowReference,
                    r: PandasApiTdsRow,
                    _col: str = column,
                    _periods: int = -periods,
            ) -> "PyLegendPrimitiveOrPythonPrimitive":
                return p.lead(r, _periods)[_col]

        return shift_window_series.window_extend_legend_ext(value_func=value_func)

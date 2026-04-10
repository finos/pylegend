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

from abc import abstractmethod
from datetime import date, datetime
from decimal import Decimal as PythonDecimal
from io import StringIO
from typing import IO, TYPE_CHECKING

from typing_extensions import Concatenate

try:
    from typing import ParamSpec
except Exception:
    from typing_extensions import ParamSpec  # type: ignore

from pylegend._typing import (
    PyLegendCallable,
    PyLegendSequence,
    PyLegendUnion,
    PyLegendOptional,
    PyLegendList,
    PyLegendSet,
    PyLegendTuple,
    PyLegendDict,
    PyLegendHashable,
)
from pylegend.core.language import (
    PyLegendPrimitive,
)
from pylegend.core.language.pandas_api.pandas_api_aggregate_specification import PyLegendAggInput
from pylegend.core.language.pandas_api.pandas_api_tds_row import PandasApiTdsRow
from pylegend.core.language.shared.primitives.boolean import PyLegendBoolean
from pylegend.core.language.shared.primitives.integer import PyLegendInteger
from pylegend.core.language.shared.primitives.primitive import PyLegendPrimitiveOrPythonPrimitive
from pylegend.core.language.shared.tds_row import AbstractTdsRow
from pylegend.core.tds.tds_frame import PyLegendTdsFrame

if TYPE_CHECKING:
    from pylegend.core.language.pandas_api.pandas_api_frame_spec import FrameSpec, RowsBetween, RangeBetween
    from pylegend.core.language.pandas_api.pandas_api_series import Series
    from pylegend.core.tds.pandas_api.frames.pandas_api_groupby_tds_frame import PandasApiGroupbyTdsFrame
    from pylegend.core.tds.pandas_api.frames.pandas_api_window_tds_frame import PandasApiWindowTdsFrame
    from pylegend.core.tds.pandas_api.frames.functions.iloc import PandasApiIlocIndexer
    from pylegend.core.tds.pandas_api.frames.functions.loc import PandasApiLocIndexer

__all__: PyLegendSequence[str] = [
    "PandasApiTdsFrame"
]

P = ParamSpec("P")


class PandasApiTdsFrame(PyLegendTdsFrame):

    @abstractmethod
    def __getitem__(
            self,
            key: PyLegendUnion[str, PyLegendList[str], PyLegendBoolean]
    ) -> PyLegendUnion["PandasApiTdsFrame", "Series"]:
        pass  # pragma: no cover

    @abstractmethod
    def __setitem__(
            self,
            key: str,
            value: PyLegendUnion["Series", PyLegendPrimitiveOrPythonPrimitive]
    ) -> None:
        pass  # pragma: no cover

    @abstractmethod
    def assign(
            self,
            **kwargs: PyLegendCallable[
                [PandasApiTdsRow],
                PyLegendUnion[int, float, bool, str, date, datetime, PyLegendPrimitive]
            ],
    ) -> "PandasApiTdsFrame":
        """
        Add or overwrite columns using keyword arguments.

        Return a new TDS frame with new columns added (or existing columns
        overwritten). Each keyword argument defines a column name and a
        callable that computes the column's value from each row.

        Parameters
        ----------
        **kwargs : callable
            Each keyword argument is a column name mapped to a function
            that takes a ``PandasApiTdsRow`` and returns a scalar value.
            Supported return types are ``int``, ``float``, ``bool``, ``str``,
            ``date``, ``datetime``, and ``PyLegendPrimitive``.

        Returns
        -------
        PandasApiTdsFrame
            A new TDS frame with the additional (or overwritten) columns.

        Raises
        ------
        RuntimeError
            If the callable returns an unsupported type (e.g. a list).

        See Also
        --------
        filter : Select columns by name, substring, or regex.
        drop : Remove columns by label.
        rename : Rename existing columns.

        Notes
        -----
        **Differences from pandas:**

        - In pandas ``assign``, each keyword argument can be a callable
          **or** a static value (e.g. ``frame.assign(col=5)``). Here,
          every value **must** be a callable that takes a row, even for
          constants (e.g. ``frame.assign(col=lambda x: 5)``).
        - Column values are accessed via typed accessor methods such as
          ``x.get_integer("col")`` and ``x.get_string("col")``, or via
          bracket notation ``x["col"]``.
        - Returning a non-scalar type (e.g. a list) from the callable
          raises a ``RuntimeError``, unlike pandas which would broadcast
          or create nested data.

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            # Add a constant column
            frame.assign(constant=lambda x: 100).head(3).to_pandas()

            # Add a computed column derived from existing columns
            frame.assign(
                ship_upper=lambda x: x.get_string("Ship Name").upper()
            ).head(3).to_pandas()

            # Overwrite an existing column
            frame.assign(
                **{"Ship Name": lambda x: x.get_string("Ship Name").upper()}
            ).head(3).to_pandas()

        """
        pass  # pragma: no cover

    @abstractmethod
    def filter(
            self,
            items: PyLegendOptional[PyLegendList[str]] = None,
            like: PyLegendOptional[str] = None,
            regex: PyLegendOptional[str] = None,
            axis: PyLegendOptional[PyLegendUnion[str, int, PyLegendInteger]] = None
    ) -> "PandasApiTdsFrame":
        """
        Select columns by label, substring match, or regular expression.

        This method selects **columns** from the TDS frame based on their
        names. Exactly one of ``items``, ``like``, or ``regex`` must be
        provided; they are mutually exclusive.

        Parameters
        ----------
        items : list of str, optional
            Exact column names to keep. All names must exist in the frame.
        like : str, optional
            Keep columns whose names contain this substring.
        regex : str, optional
            Keep columns whose names match this regular expression
            (uses ``re.search``).
        axis : {{1, 'columns'}}, optional
            The axis to filter on. Only column-axis filtering is supported.
            Defaults to ``1`` (columns) when omitted.

        Returns
        -------
        PandasApiTdsFrame
            A new TDS frame containing only the selected columns.

        Raises
        ------
        TypeError
            If more than one of ``items``, ``like``, or ``regex`` is
            provided, or if none of them is provided.
            If ``items`` is a string instead of a list, or
            if ``like`` / ``regex`` is not a string.
        ValueError
            If ``axis`` is not ``1`` or ``'columns'``.
            If any name in ``items`` does not exist in the frame.
            If no columns match the ``like`` substring or ``regex`` pattern.
            If the ``regex`` pattern is invalid.

        See Also
        --------
        assign : Add or overwrite columns.
        drop : Remove columns by label.
        rename : Rename columns.

        Notes
        -----
        **Differences from pandas:**

        - In pandas, ``filter`` supports both row-axis (``axis=0``) and
          column-axis (``axis=1``) filtering. Here, **only column-axis
          filtering is supported** (``axis=1`` or ``axis='columns'``). Passing
          ``axis=0`` or ``'index'`` raises ``ValueError``.
        - In pandas, ``items`` silently ignores names that do not exist in
          the frame. Here, **all names must exist**; unknown names raise a
          ``ValueError`` listing the missing and available columns.
        - In pandas, ``like`` and ``regex`` return an empty DataFrame when
          no columns match. Here, they **raise** ``ValueError`` when no
          columns match.

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            # Select specific columns by name
            frame.filter(items=["Order Id", "Ship Name"]).head(3).to_pandas()

            # Select columns whose names contain a substring
            frame.filter(like="Ship").head(3).to_pandas()

            # Select columns matching a regex pattern
            frame.filter(regex="^Ship").head(3).to_pandas()

            # Chain filters to progressively narrow columns
            frame.filter(like="Ship").filter(regex="Name$").head(3).to_pandas()

        """
        pass  # pragma: no cover

    @abstractmethod
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
        """
        Sort the TDS frame by one or more columns.

        Return a new TDS frame sorted by the values in the specified
        column(s). Supports ascending and descending sort order per column.

        Parameters
        ----------
        by : str or list of str
            Column name or list of column names to sort by. All names
            must exist in the current frame.
        axis : {{0, 'index'}}, default 0
            Axis along which to sort. Only ``0`` / ``'index'`` (row-wise
            sorting) is supported.
        ascending : bool or list of bool, default True
            Sort order. If a list, must have the same length as ``by``.
        inplace : bool, default False
            Must be ``False``. In-place mutation is not supported.
        kind : None
            Not supported. Must be ``None``; passing any value raises
            ``NotImplementedError``.
        na_position : str, default 'last'
            Position of null values. Accepted but handled at the SQL
            engine level.
        ignore_index : bool, default True
            Must be ``True``. Setting to ``False`` raises ``ValueError``.
        key : None
            Not supported. Must be ``None``; passing a callable raises
            ``NotImplementedError``.

        Returns
        -------
        PandasApiTdsFrame
            A new TDS frame sorted by the specified columns.

        Raises
        ------
        ValueError
            If a column in ``by`` does not exist in the frame.
        ValueError
            If the length of ``ascending`` does not match ``by``.
        ValueError
            If ``axis`` is not ``0`` or ``'index'``.
        ValueError
            If ``inplace`` is ``True``.
        ValueError
            If ``ignore_index`` is ``False``.
        NotImplementedError
            If ``kind`` or ``key`` is provided.

        See Also
        --------
        head : Return the first n rows.
        truncate : Select a range of rows by position.
        filter : Select columns by name, substring, or regex.

        Notes
        -----
        **Differences from pandas:**

        - The ``kind`` parameter (sort algorithm) is **not supported**.
          Sorting is delegated to the underlying Legend Engine.
        - The ``key`` parameter (per-element transform before sorting)
          is **not supported**.
        - ``inplace=True`` is **not supported**; always returns a new frame.
        - ``ignore_index`` must be ``True``; ``False`` is **not supported**
          because TDS frames do not have an index.
        - ``axis=1`` (sorting columns) is **not supported**; only row-wise
          sorting via ``axis=0`` is available.

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            # Sort by a single column (ascending by default)
            frame.sort_values("Ship Name").head(5).to_pandas()

            # Sort descending
            frame.sort_values("Order Id", ascending=False).head(5).to_pandas()

            # Sort by multiple columns with mixed directions
            frame.sort_values(
                by=["Ship Name", "Order Id"],
                ascending=[True, False]
            ).head(5).to_pandas()

        """
        pass  # pragma: no cover

    @abstractmethod
    def truncate(
            self,
            before: PyLegendUnion[date, str, int, None] = 0,
            after: PyLegendUnion[date, str, int, None] = None,
            axis: PyLegendUnion[str, int] = 0,
            copy: bool = True
    ) -> "PandasApiTdsFrame":
        """
        Select rows by positional index range.

        Return a new TDS frame containing rows from position ``before``
        (inclusive) to ``after`` (inclusive).

        Parameters
        ----------
        before : int or None, default 0
            Only ``int`` and ``None`` are supported.
            First row index to include (0-based, inclusive). Negative
            values are silently clamped to ``0``. ``None`` is treated
            as ``0``.
        after : int or None, default None
            Only ``int`` and ``None`` are supported.
            Last row index to include (0-based, inclusive). ``None``
            means no upper bound (all remaining rows are returned).
            Negative values result in an empty frame.
        axis : {{0, 'index'}}, default 0
            Axis to truncate along. Only ``0`` / ``'index'`` is
            supported.
        copy : bool, default True
            Must be ``True``. Setting to ``False`` raises
            ``NotImplementedError``.

        Returns
        -------
        PandasApiTdsFrame
            A new TDS frame containing only the rows in the specified
            positional range.

        Raises
        ------
        NotImplementedError
            If ``axis not ``0`` or ``'index'``.
            If ``copy`` is ``False``.
            If ``before`` or ``after`` is a non-integer type (e.g. a
            string or date).
            If ``before`` or ``after`` is a non-integer type (e.g. a
            string or date).
        ValueError
            If ``before`` is greater than ``after`` (after clamping).

        See Also
        --------
        head : Return the first n rows.
        sort_values : Sort the frame before truncating.
        filter : Select columns by name, substring, or regex.

        Notes
        -----
        **Differences from pandas:**

        - In pandas, ``truncate`` selects rows by **label** (index value).
          Here, it selects rows by **positional** (integer) index only
          (its translated to LIMIT and OFFSET of the underlying SQL engine).
          Passing ``date``, ``str``, or other label-based values for
          ``before`` / ``after`` raises ``NotImplementedError``.
        - ``copy=False`` is **not supported**; a new frame is always
          returned.
        - ``axis=1`` (truncating columns) is **not supported**.
        - Negative ``before`` values are **silently clamped to 0** rather
          than raising an error. Negative ``after`` values result in an
          **empty frame** (zero rows).
        - The ``after`` parameter is **inclusive** (row at position
          ``after`` is included), matching pandas behaviour.

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            # Get rows at positions 0 through 4 (inclusive)
            frame.truncate(before=0, after=4).to_pandas()

            # Skip first 5 rows, keep the rest
            frame.truncate(before=5).head(5).to_pandas()

            # Get rows at positions 2 through 6 (inclusive)
            frame.truncate(before=2, after=6).to_pandas()

        """
        pass  # pragma: no cover

    @abstractmethod
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
        """
        Remove columns from the TDS frame by label.

        Return a new TDS frame with the specified columns removed. Columns
        can be identified via ``labels`` (with ``axis=1``) or via the
        ``columns`` parameter directly. Accepts a single column name, a
        list, tuple, or set of names.

        Parameters
        ----------
        labels : str, sequence of str, or set of str, optional
            Column name(s) to drop. Mutually exclusive with ``columns``.
        axis : {{1, 'columns'}}, default 1
            The axis to drop along. Only column-axis (``1`` / ``'columns'``)
            is supported.
        index : None
            **Not supported.** Passing any value raises
            ``NotImplementedError``.
        columns : str, sequence of str, or set of str, optional
            Column name(s) to drop. Mutually exclusive with ``labels``.
        level : None
            **Not supported.** Passing any value raises
            ``NotImplementedError``.
        inplace : bool, default False
            Must be ``False``. In-place mutation is not supported.
        errors : {{'raise', 'ignore'}}, default 'raise'
            If ``'raise'``, a ``KeyError`` is raised when any label is
            not found. If ``'ignore'``, missing labels are silently
            skipped.

        Returns
        -------
        PandasApiTdsFrame
            A new TDS frame without the specified columns.

        Raises
        ------
        ValueError
            If both ``labels`` and ``columns`` are provided, or if
            neither is provided.
            If ``axis`` is an invalid value (not ``0``, ``1``,
            ``'index'``, or ``'columns'``).
        NotImplementedError
            If ``axis`` is ``0`` / ``'index'`` (row-level drop).
            If ``index`` or ``level`` is provided.
            If ``inplace`` is ``True``.
        KeyError
            If any specified column does not exist in the frame and
            ``errors='raise'``.
        TypeError
            If ``labels`` or ``columns`` is an unsupported type
            (e.g. a callable).

        See Also
        --------
        filter : Select columns by name, substring, or regex.
        assign : Add or overwrite columns.
        rename : Rename existing columns.

        Notes
        -----
        **Differences from pandas:**

        - In pandas, ``drop`` can remove **rows** (``axis=0``) or
          **columns** (``axis=1``). Here, **only column-axis dropping
          is supported** (``axis=1``). Passing ``axis=0`` raises
          ``NotImplementedError``.
        - The ``axis`` parameter defaults to ``1`` (columns), whereas in
          pandas it defaults to ``0`` (rows). This means bare
          ``frame.drop("col")`` drops a **column** here but would attempt
          to drop a row label in pandas.
        - The ``index`` and ``level`` parameters are **not supported**.
        - ``inplace=True`` is **not supported**; always returns a new
          frame.

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            # Drop a single column
            frame.drop(columns="Ship Name").head(3).to_pandas()

            # Drop multiple columns
            frame.drop(columns=["Ship Name", "Order Date"]).head(3).to_pandas()

            # Using labels parameter
            frame.drop(labels=["Ship Name"], axis=1).head(3).to_pandas()

            # Ignore missing columns instead of raising an error
            frame.drop(columns=["Ship Name", "NonExistent"], errors="ignore").head(3).to_pandas()

        """
        pass  # pragma: no cover

    @abstractmethod
    def aggregate(
            self,
            func: PyLegendAggInput,
            axis: PyLegendUnion[int, str] = 0,
            *args: PyLegendPrimitiveOrPythonPrimitive,
            **kwargs: PyLegendPrimitiveOrPythonPrimitive
    ) -> "PandasApiTdsFrame":
        """
        Aggregate the TDS frame using one or more operations.

        Apply one or more aggregation functions across all columns or
        specific columns, collapsing the frame into a single-row
        summary. Supported aggregation strings are ``'sum'``, ``'mean'``,
        ``'min'``, ``'max'``, ``'count'``, ``'std'``, ``'var'``, as well
        as aliases ``'len'``, ``'size'`` (both map to count), and
        ``'average'`` / ``'avg'`` (map to mean). Along with these,
        callables and numpy universal functions are supported.

        Parameters
        ----------
        func : str, callable, np.ufunc, list, or dict
            Aggregation specification. Accepted forms:

            - ``str`` : A named aggregation (e.g. ``'sum'``) applied to
              **every** column.
            - ``callable`` : A function that receives a column's Series
              proxy and returns an aggregated value
              (e.g. ``lambda x: x.sum()``), applied to every column.
            - ``np.ufunc`` : A NumPy universal function (e.g.
              ``np.sum``), applied to every column.
            - ``list`` : A list containing **one** of the above, applied
              to every column. Output column names are prefixed with the
              function name (e.g. ``'sum(col)'``).
            - ``dict`` : A mapping of column name → aggregation (str,
              callable, np.ufunc, or a list of these). Only the
              specified columns appear in the result.
        axis : {{0, 'index'}}, default 0
            Axis along which to aggregate. Only ``0`` / ``'index'``
            is supported.
        *args
            Not supported. Passing positional arguments raises
            ``NotImplementedError``.
        **kwargs
            Not supported. Passing keyword arguments raises
            ``NotImplementedError``.

        Returns
        -------
        PandasApiTdsFrame
            A new single-row TDS frame with the aggregated values.

        Raises
        ------
        NotImplementedError
            If ``axis`` is not ``0`` or ``'index'``.
            If extra ``*args`` or ``**kwargs`` are passed.
        TypeError
            If ``func`` is not a supported type (str, callable,
            np.ufunc, list, or dict).
            If dict keys are not strings, or dict/list values contain
            unsupported types.
        ValueError
            If a dict key refers to a column that does not exist in
            the frame.

        See Also
        --------
        agg : Alias for aggregate.
        groupby : Group rows before aggregating.
        sum : Convenience method for sum aggregation.
        mean : Convenience method for mean aggregation.

        Notes
        -----
        **Differences from pandas:**

        - In pandas, ``aggregate`` can return a multi-row result when
          multiple functions are applied (one row per function). Here,
          multiple functions per column produce **multiple columns**
          in a single-row result (e.g. ``{'col': ['min', 'max']}``
          yields columns ``'min(col)'`` and ``'max(col)'``).
        - Extra ``*args`` and ``**kwargs`` are **not forwarded** to the
          aggregation function; passing them raises
          ``NotImplementedError``.
        - ``axis=1`` (column-wise aggregation) is **not supported**.
        - When ``func`` is a list, it must contain **exactly one**
          element. Multi-element lists behave identically to a single-
          element list mapping applied to every column.

        Examples
        --------
        .. ipython:: python

            import pylegend
            import numpy as np
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            # Aggregate a single column with a string function
            frame.aggregate({"Order Id": "count"}).to_pandas()

            # Aggregate multiple columns with different functions
            frame.aggregate({"Order Id": "min", "Ship Name": "count"}).to_pandas()

            # Broadcast a single function to all columns
            frame.aggregate("count").to_pandas()

            # Use a lambda for custom aggregation
            frame.aggregate({
                "Order Id": lambda x: x.max(),
                "Order Date": np.min,
                "Order Date": np.max,
                "Shipped Date": "min"
            }).to_pandas()

        """
        pass  # pragma: no cover

    @abstractmethod
    def agg(
            self,
            func: PyLegendAggInput,
            axis: PyLegendUnion[int, str] = 0,
            *args: PyLegendPrimitiveOrPythonPrimitive,
            **kwargs: PyLegendPrimitiveOrPythonPrimitive
    ) -> "PandasApiTdsFrame":
        """
        Alias for :meth:`aggregate`. See ``aggregate`` for full
        documentation.

        """
        pass  # pragma: no cover

    @abstractmethod
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
        Merge this TDS frame with another using a database-style join.

        Combine two frames column-wise based on common columns or
        explicit key specifications. Supports inner, left, right,
        outer (full), and cross joins.

        Parameters
        ----------
        other : PandasApiTdsFrame
            The right TDS frame to merge with. Must be a different
            frame instance; merging a frame with itself raises
            ``NotImplementedError``.
        how : {{'inner', 'left', 'right', 'outer', 'cross'}}, default 'inner'
            Type of merge:

            - ``'inner'`` : Only rows with matching keys in both frames.
            - ``'left'`` : All rows from the left frame, NaN-filled for
              non-matching right rows.
            - ``'right'`` : All rows from the right frame, NaN-filled
              for non-matching left rows.
            - ``'outer'`` : All rows from both frames (``FULL OUTER
              JOIN``).
            - ``'cross'`` : Cartesian product of both frames. No join
              keys may be specified.
        on : str or list of str, optional
            Column name(s) to join on. Must exist in **both** frames.
            Mutually exclusive with ``left_on`` / ``right_on``.
        left_on : str or list of str, optional
            Column name(s) from the left frame to join on.
        right_on : str or list of str, optional
            Column name(s) from the right frame to join on. Must have
            the same length as ``left_on``.
        left_index : bool, default False
            **Not supported.** Setting to ``True`` raises
            ``NotImplementedError``.
        right_index : bool, default False
            **Not supported.** Setting to ``True`` raises
            ``NotImplementedError``.
        sort : bool, default False
            If ``True``, sort the result by the join keys in ascending
            order.
        suffixes : tuple of (str, str), default ('_x', '_y')
            Suffixes to apply to overlapping non-key column names from
            the left and right frames respectively. Use ``None`` to
            indicate that the column name from the respective frame
            should be left as-is (will raise if this causes duplicates).
        indicator : bool or str, default False
            **Not supported.** Setting to a truthy value raises
            ``NotImplementedError``.
        validate : str, optional
            **Not supported.** Passing any value raises
            ``NotImplementedError``.

        Returns
        -------
        PandasApiTdsFrame
            A new TDS frame containing the merged result.

        Raises
        ------
        TypeError
            If ``other`` is not a ``PandasApiTdsFrame``.
            If ``how``, ``on``, ``left_on``, ``right_on``, ``suffixes``,
            or ``sort`` have invalid types.
        ValueError
            If both ``on`` and ``left_on``/``right_on`` are specified.
            If ``left_on`` and ``right_on`` have different lengths.
            If no merge keys can be resolved and ``how`` is not
            ``'cross'``.
            If ``how='cross'`` is used with ``on``/``left_on``/
            ``right_on``.
            If ``how`` is not a recognised join method.
            If the resulting columns contain duplicates after suffix
            application.
        KeyError
            If a key specified in ``on``, ``left_on``, or ``right_on``
            does not exist in the corresponding frame.
        NotImplementedError
            If ``left_index=True``, ``right_index=True``,
            ``indicator`` is truthy, ``validate`` is set, or the frame
            is merged with itself.

        See Also
        --------
        join : Convenience wrapper around merge with simpler syntax.

        Notes
        -----
        **Differences from pandas:**

        - **Self-merge is not supported.** Merging a frame with itself
          raises ``NotImplementedError``.
        - **Index-based merging is not supported.** ``left_index`` and
          ``right_index`` must be ``False``.
        - **``indicator``** and **``validate``** parameters are **not
          supported**.
        - When no join keys are provided (and ``how`` is not
          ``'cross'``), the merge infers keys from the **intersection
          of column names** between the two frames. If no common
          columns exist, a ``ValueError`` is raised (unlike pandas,
          which would raise a ``MergeError``).
        - ``how='outer'`` maps to a ``FULL OUTER JOIN`` at the SQL
          level.
        - ``how='cross'`` is implemented as a ``CROSS JOIN`` in SQL,
          but mapped to ``JoinKind.INNER`` with a ``1==1`` condition
          in the PURE query representation.

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            # Create a second frame for joining
            frame2 = pylegend.samples.pandas_api.northwind_orders_frame()
            frame2 = frame2.rename({"Order Id": "Right Order Id"})

            # Inner merge on a common column
            frame.head(5).merge(
                frame2.head(5),
                how="inner",
                left_on="Order Id",
                right_on="Right Order Id"
            ).to_pandas()

        """
        pass  # pragma: no cover

    @abstractmethod
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
        Join this TDS frame with another on shared column(s).

        Convenience method that delegates to :meth:`merge`. The
        ``lsuffix`` and ``rsuffix`` parameters are mapped to the
        ``suffixes`` parameter of ``merge``, and ``on`` is passed
        directly.

        Parameters
        ----------
        other : PandasApiTdsFrame
            The right TDS frame to join with.
        on : str or list of str, optional
            Column name(s) to join on. Must exist in **both** frames.
            Unlike pandas ``join``, this parameter specifies **column
            names**, not index labels.
        how : {{'left', 'inner', 'right', 'outer', 'cross'}}, default 'left'
            Type of join. See :meth:`merge` for details.
        lsuffix : str, default ''
            Suffix to apply to overlapping column names from the left
            frame.
        rsuffix : str, default ''
            Suffix to apply to overlapping column names from the right
            frame.
        sort : bool, default False
            If ``True``, sort the result by the join keys.
        validate : str, optional
            **Not supported.** Passing any value raises
            ``NotImplementedError``.

        Returns
        -------
        PandasApiTdsFrame
            A new TDS frame containing the joined result.

        Raises
        ------
        ValueError
            If overlapping column names exist and ``lsuffix`` /
            ``rsuffix`` do not resolve the conflict.
        NotImplementedError
            If ``validate`` is set.

        See Also
        --------
        merge : The underlying merge method with full parameter control.

        Notes
        -----
        **Differences from pandas:**

        - In pandas, ``DataFrame.join`` joins on the **index** by
          default, optionally using ``on`` to specify a column in the
          *left* frame to match against the *right* frame's index.
          Here, ``join`` is purely **column-on-column** and delegates
          directly to ``merge(on=on)``. There is **no index-based
          joining**.
        - The ``lsuffix`` and ``rsuffix`` parameters correspond to
          ``suffixes=(lsuffix, rsuffix)`` in ``merge``. In pandas,
          default suffixes are empty strings (raising on conflict);
          here they also default to empty strings.
        - Because this delegates to ``merge``, all limitations of
          ``merge`` apply: no self-join, no ``left_index`` /
          ``right_index``, no ``indicator``, and no ``validate``.

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            # Create a second frame with renamed columns
            frame2 = pylegend.samples.pandas_api.northwind_orders_frame()
            frame2 = frame2.rename({"Order Id": "Right Order Id"})

            # Left join on a common key
            frame.head(5).join(
                frame2.head(5),
                on="Ship Name",
                how="left",
                lsuffix="_left",
                rsuffix="_right"
            ).to_pandas()

        """
        pass  # pragma: no cover

    @abstractmethod
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
        Rename columns of the TDS frame.

        Alter column labels using a mapping (dict) or a callable
        function applied to each column name.

        Parameters
        ----------
        mapper : dict or callable, optional
            Mapping of old column names to new column names, or a
            callable that transforms each column name (e.g.
            ``str.upper``). Used when ``axis=1`` (columns). Cannot be
            specified together with ``columns``.
        index : dict or callable, optional
            **Not supported.** Passing any value raises
            ``NotImplementedError``.
        columns : dict or callable, optional
            Alternative to ``mapper`` for renaming columns. Mutually
            exclusive with ``mapper`` when both are provided alongside
            ``axis``.
        axis : {{1, 'columns'}}, default 1
            Axis to target. Only ``1`` / ``'columns'`` is supported.
            ``0`` / ``'index'`` raises ``NotImplementedError``.
        inplace : bool, default False
            Must be ``False``. ``True`` raises ``NotImplementedError``.
        copy : bool, default True
            Must be ``True``. ``False`` raises ``NotImplementedError``.
        level : int or str, optional
            **Not supported.** Passing any value raises
            ``NotImplementedError``.
        errors : {{'ignore', 'raise'}}, default 'ignore'
            If ``'raise'``, raise a ``KeyError`` when a key in the
            mapping does not exist as a column name. If ``'ignore'``,
            silently skip non-existent keys.

        Returns
        -------
        PandasApiTdsFrame
            A new TDS frame with renamed columns.

        Raises
        ------
        TypeError
            If ``mapper`` or ``columns`` is not a dict or callable.
            If ``copy`` or ``inplace`` is not a bool.
        ValueError
            If both ``mapper`` (with ``axis``) and ``columns``/
            ``index`` are specified simultaneously.
            If ``axis`` is not a supported value.
            If ``errors`` is not ``'ignore'`` or ``'raise'``.
            If the rename produces duplicate column names.
        KeyError
            If ``errors='raise'`` and a key in the mapping does not
            exist in the frame's columns.
        NotImplementedError
            If ``axis=0``/``'index'``, ``index`` is set, ``level`` is
            set, ``copy=False``, or ``inplace=True``.

        See Also
        --------
        filter : Select columns by name.
        drop : Remove columns.
        assign : Add or overwrite columns.

        Notes
        -----
        **Differences from pandas:**

        - Only **column renaming** is supported (``axis=1``). Index
          renaming (``axis=0``) raises ``NotImplementedError``.
        - ``inplace=True`` is **not supported**; a new frame is always
          returned.
        - ``copy=False`` is **not supported**.
        - ``level`` (multi-level index) is **not supported**.
        - The ``index`` parameter is **not supported**.
        - When using a callable, it is applied to **every** column name
          (e.g. ``str.upper`` will uppercase all column names).
        - If ``errors='ignore'`` (the default), keys in the mapping
          that do not match any column are silently ignored, matching
          pandas behaviour.

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            # Rename with a dict
            frame.rename({"Order Id": "OrderId", "Ship Name": "ShipName"}).head(3).to_pandas()

            # Rename with a callable
            frame.rename(str.upper).head(3).to_pandas()

            # Rename via the columns parameter
            frame.rename(columns={"Order Id": "order_id"}).head(3).to_pandas()

        """
        pass  # pragma: no cover

    @abstractmethod
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
        """
        Group the TDS frame by one or more columns.

        Return a :class:`~pylegend.core.tds.pandas_api.frames.pandas_api_groupby_tds_frame.PandasApiGroupbyTdsFrame`
        object that can be used to apply aggregation functions
        (``sum``, ``mean``, ``min``, ``max``, ``std``, ``var``,
        ``count``, or the general ``aggregate``/``agg``) and OLAP
        window functions (``rank``) to each group. Column selection
        after grouping is supported via bracket notation
        (e.g. ``frame.groupby("A")["B"].sum()``).

        The groupby columns act as the ``PARTITION BY`` clause in the
        underlying SQL when window functions such as ``rank`` are used.

        Parameters
        ----------
        by : str or list of str
            Column name or list of column names to group by. All names
            must exist in the current frame.
        level : None
            **Not supported.** Passing any value raises
            ``NotImplementedError``. Use ``by`` instead.
        as_index : bool, default False
            Must be ``False``. Setting to ``True`` raises
            ``NotImplementedError``.
        sort : bool, default True
            Whether to sort the result by the grouping columns after
            aggregation.
        group_keys : bool, default False
            Must be ``False``. Setting to ``True`` raises
            ``NotImplementedError``.
        observed : bool, default False
            Must be ``False``. Setting to ``True`` raises
            ``NotImplementedError``.
        dropna : bool, default False
            Must be ``False``. Setting to ``True`` raises
            ``NotImplementedError``.

        Returns
        -------
        PandasApiGroupbyTdsFrame
            A groupby object on which aggregation and window methods
            can be called. See :class:`PandasApiGroupbyTdsFrame
            <pylegend.core.tds.pandas_api.frames.pandas_api_groupby_tds_frame.PandasApiGroupbyTdsFrame>`
            for the full list of available methods.

        Raises
        ------
        NotImplementedError
            If ``level``, ``as_index=True``, ``group_keys=True``,
            ``observed=True``, or ``dropna=True`` is provided.
        TypeError
            If ``by`` is not a string or list of strings.
        ValueError
            If ``by`` is an empty list.
        KeyError
            If any column in ``by`` does not exist in the frame.

        See Also
        --------
        aggregate : Aggregate without grouping.
        sum : Convenience shorthand for sum aggregation.
        count : Convenience shorthand for count aggregation.

        Notes
        -----
        **Differences from pandas:**

        - ``as_index`` defaults to ``False`` and **must** be ``False``.
          In pandas it defaults to ``True``. This means the grouping
          columns always appear as regular columns in the result, never
          as the index.
        - ``group_keys``, ``observed``, and ``dropna`` must all be
          ``False``; their ``True`` variants are **not supported**.
        - ``level`` (grouping by index level) is **not supported**.
        - The groupby object supports column selection via
          ``[col]`` (returns a ``GroupbySeries``) or ``[[col1, col2]]``
          (returns a narrowed ``PandasApiGroupbyTdsFrame``), matching
          the pandas pattern ``frame.groupby(...)["col"].sum()``.
        - When ``sort=True`` (default), the result is sorted by the
          grouping columns in ascending order after aggregation.

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            # Group by a single column and count
            frame.groupby("Ship Name")["Order Id"].count().head(5).to_pandas()

            # Group by a column and sum a numeric column
            frame.groupby("Ship Name")["Order Id"].sum().head(5).to_pandas()

            # Group by a column with dict-based aggregation
            frame.groupby("Ship Name").agg({"Order Id": "count"}).head(5).to_pandas()

        """
        pass  # pragma: no cover

    @abstractmethod
    def expanding(
            self,
            min_periods: int = 1,
            axis: PyLegendUnion[int, str] = 0,
            method: PyLegendOptional[str] = None,
            order_by: PyLegendOptional[PyLegendUnion[str, PyLegendSequence[str]]] = None,
            ascending: PyLegendUnion[bool, PyLegendSequence[bool]] = True,
    ) -> "PandasApiWindowTdsFrame":
        """
        Create an expanding window frame for window-aggregate computations.

        An expanding window includes all rows from the start of the partition
        up to the current row. This is useful for running totals, running
        averages, and similar cumulative calculations.

        Parameters
        ----------
        min_periods : int, default 1
            Minimum number of observations in the window required to have a
            value; otherwise, result is ``null``.
        axis : {{0, 'index'}}, default 0
            Only ``0`` / ``'index'`` is supported.
        method : str, optional
            Must be ``None`` or ``'python'``.
        order_by : str or list of str, optional
            Column(s) to order by within the window. Required for
            deterministic results.
        ascending : bool or list of bool, default True
            Sort order for the ``order_by`` columns.

        Returns
        -------
        PandasApiWindowTdsFrame
            A window frame on which window aggregates (``sum``, ``mean``,
            ``min``, ``max``, etc.) can be called.

        See Also
        --------
        rolling : Fixed-size sliding window.
        groupby : Group rows before applying window functions.

        Raises
        ------
        NotImplementedError
            If ``axis`` is not ``0``, ``method`` is not ``None`` or
            ``'python'``, or ``min_periods`` is less than ``1``.

        Notes
        -----
        **Differences from pandas:**

        - ``order_by`` and ``ascending`` are pylegend extensions not
          present in pandas. They control the ``ORDER BY`` clause
          inside the SQL ``OVER(...)`` window specification.
        - ``axis=1`` is **not supported**.
        - ``method='table'`` is **not supported**.

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            # Running sum of Order Id ordered by Order Id
            frame.filter(items=["Order Id"]).expanding(
                order_by="Order Id"
            ).sum().head(5).to_pandas()

        """
        pass  # pragma: no cover

    @abstractmethod
    def rolling(
            self,
            window: int,
            min_periods: PyLegendOptional[int] = None,
            center: bool = False,
            win_type: PyLegendOptional[str] = None,
            on: PyLegendOptional[str] = None,
            axis: PyLegendUnion[int, str] = 0,
            closed: PyLegendOptional[str] = None,
            step: PyLegendOptional[int] = None,
            method: PyLegendOptional[str] = None,
            order_by: PyLegendOptional[PyLegendUnion[str, PyLegendSequence[str]]] = None,
            ascending: PyLegendUnion[bool, PyLegendSequence[bool]] = True,
    ) -> "PandasApiWindowTdsFrame":
        """
        Create a fixed-size sliding window frame for window-aggregate computations.

        A rolling window includes a fixed number of preceding rows (and
        optionally the current row) for each row, enabling moving averages,
        moving sums, and similar calculations.

        Parameters
        ----------
        window : int
            Size of the moving window (number of rows).
        min_periods : int, optional
            Minimum number of observations in the window required to have a
            value. Defaults to ``window``.
        center : bool, default False
            Not supported. Must be ``False``.
        win_type : str, optional
            Not supported. Must be ``None``.
        on : str, optional
            Not supported. Must be ``None``.
        axis : {{0, 'index'}}, default 0
            Only ``0`` / ``'index'`` is supported.
        closed : str, optional
            Not supported. Must be ``None``.
        step : int, optional
            Not supported. Must be ``None``.
        method : str, optional
            Must be ``None`` or ``'python'``.
        order_by : str or list of str, optional
            Column(s) to order by within the window. Required for
            deterministic results.
        ascending : bool or list of bool, default True
            Sort order for the ``order_by`` columns.

        Returns
        -------
        PandasApiWindowTdsFrame
            A window frame on which window aggregates (``sum``, ``mean``,
            ``min``, ``max``, etc.) can be called.

        See Also
        --------
        expanding : Expanding (cumulative) window.
        groupby : Group rows before applying window functions.

        Raises
        ------
        NotImplementedError
            If ``center``, ``win_type``, ``on``, ``closed``, or ``step``
            are set to non-default values. Also raised if ``axis`` is not
            ``0`` or ``method`` is not ``None`` / ``'python'``.

        Notes
        -----
        **Differences from pandas:**

        - ``order_by`` and ``ascending`` are pylegend extensions not
          present in pandas. They control the ``ORDER BY`` clause
          inside the SQL ``OVER(...)`` window specification.
        - ``center``, ``win_type``, ``on``, ``closed``, ``step`` are
          **not supported**.
        - ``axis=1`` is **not supported**.

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            # 3-row moving average of Order Id ordered by Order Id
            frame.filter(items=["Order Id"]).rolling(
                window=3, order_by="Order Id"
            ).mean().head(5).to_pandas()

        """
        pass  # pragma: no cover

    @abstractmethod
    def sum(
            self,
            axis: PyLegendUnion[int, str] = 0,
            skipna: bool = True,
            numeric_only: bool = False,
            min_count: int = 0,
            **kwargs: PyLegendPrimitiveOrPythonPrimitive
    ) -> "PandasApiTdsFrame":
        """
        Compute the sum of each column.

        Convenience method equivalent to ``aggregate('sum')``. Returns a
        single-row TDS frame with the sum of every column.

        Parameters
        ----------
        axis : {{0, 'index'}}, default 0
            Only ``0`` / ``'index'`` is supported.
        skipna : bool, default True
            Must be ``True``. ``False`` is not supported.
        numeric_only : bool, default False
            Must be ``False``. ``True`` is not supported.
        min_count : int, default 0
            Must be ``0``. Non-zero values are not supported.
        **kwargs
            Not supported. Passing any keyword arguments raises
            ``NotImplementedError``.

        Returns
        -------
        PandasApiTdsFrame
            A single-row TDS frame with column sums.

        Raises
        ------
        NotImplementedError
            If ``axis``, ``skipna``, ``numeric_only``, ``min_count``,
            or ``**kwargs`` are set to unsupported values.

        See Also
        --------
        aggregate : General aggregation method.
        mean : Compute column means.
        count : Count non-null values per column.

        Notes
        -----
        **Differences from pandas:**

        - ``skipna=False``, ``numeric_only=True``, and non-zero
          ``min_count`` are **not supported**.
        - ``axis=1`` is **not supported**.
        - Internally delegates to ``aggregate('sum')``.

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            # Sum of all columns
            frame.filter(items=["Order Id"]).sum().to_pandas()

        """
        pass  # pragma: no cover

    @abstractmethod
    def mean(
            self,
            axis: PyLegendUnion[int, str] = 0,
            skipna: bool = True,
            numeric_only: bool = False,
            **kwargs: PyLegendPrimitiveOrPythonPrimitive
    ) -> "PandasApiTdsFrame":
        """
        Compute the mean of each column.

        Convenience method equivalent to ``aggregate('mean')``. Returns a
        single-row TDS frame with the arithmetic mean of every column.

        Parameters
        ----------
        axis : {{0, 'index'}}, default 0
            Only ``0`` / ``'index'`` is supported.
        skipna : bool, default True
            Must be ``True``. ``False`` is not supported.
        numeric_only : bool, default False
            Must be ``False``. ``True`` is not supported.
        **kwargs
            Not supported.

        Returns
        -------
        PandasApiTdsFrame
            A single-row TDS frame with column means.

        Raises
        ------
        NotImplementedError
            If any parameter is set to an unsupported value.

        See Also
        --------
        aggregate : General aggregation method.
        sum : Compute column sums.
        std : Compute column standard deviations.

        Notes
        -----
        Internally delegates to ``aggregate('mean')``. The same pandas
        deviations as :meth:`sum` apply (``skipna=False``,
        ``numeric_only=True``, ``axis=1`` are not supported).

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            # Mean of numeric columns
            frame.filter(items=["Order Id"]).mean().to_pandas()

        """
        pass  # pragma: no cover

    @abstractmethod
    def min(
            self,
            axis: PyLegendUnion[int, str] = 0,
            skipna: bool = True,
            numeric_only: bool = False,
            **kwargs: PyLegendPrimitiveOrPythonPrimitive
    ) -> "PandasApiTdsFrame":
        """
        Compute the minimum value of each column.

        Convenience method equivalent to ``aggregate('min')``. Returns a
        single-row TDS frame with the minimum value of every column.
        For string columns, returns the lexicographically smallest value.

        Parameters
        ----------
        axis : {{0, 'index'}}, default 0
            Only ``0`` / ``'index'`` is supported.
        skipna : bool, default True
            Must be ``True``. ``False`` is not supported.
        numeric_only : bool, default False
            Must be ``False``. ``True`` is not supported.
        **kwargs
            Not supported.

        Returns
        -------
        PandasApiTdsFrame
            A single-row TDS frame with column minimums.

        Raises
        ------
        NotImplementedError
            If any parameter is set to an unsupported value.

        See Also
        --------
        aggregate : General aggregation method.
        max : Compute column maximums.

        Notes
        -----
        Internally delegates to ``aggregate('min')``. The same pandas
        deviations as :meth:`sum` apply.

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            # Minimum of each column
            frame.filter(items=["Order Id"]).min().to_pandas()

        """
        pass  # pragma: no cover

    @abstractmethod
    def max(
            self,
            axis: PyLegendUnion[int, str] = 0,
            skipna: bool = True,
            numeric_only: bool = False,
            **kwargs: PyLegendPrimitiveOrPythonPrimitive
    ) -> "PandasApiTdsFrame":
        """
        Compute the maximum value of each column.

        Convenience method equivalent to ``aggregate('max')``. Returns a
        single-row TDS frame with the maximum value of every column.
        For string columns, returns the lexicographically largest value.

        Parameters
        ----------
        axis : {{0, 'index'}}, default 0
            Only ``0`` / ``'index'`` is supported.
        skipna : bool, default True
            Must be ``True``. ``False`` is not supported.
        numeric_only : bool, default False
            Must be ``False``. ``True`` is not supported.
        **kwargs
            Not supported.

        Returns
        -------
        PandasApiTdsFrame
            A single-row TDS frame with column maximums.

        Raises
        ------
        NotImplementedError
            If any parameter is set to an unsupported value.

        See Also
        --------
        aggregate : General aggregation method.
        min : Compute column minimums.

        Notes
        -----
        Internally delegates to ``aggregate('max')``. The same pandas
        deviations as :meth:`sum` apply.

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            # Maximum of each column
            frame.filter(items=["Order Id"]).max().to_pandas()

        """
        pass  # pragma: no cover

    @abstractmethod
    def std(
            self,
            axis: PyLegendUnion[int, str] = 0,
            skipna: bool = True,
            ddof: int = 1,
            numeric_only: bool = False,
            **kwargs: PyLegendPrimitiveOrPythonPrimitive
    ) -> "PandasApiTdsFrame":
        """
        Compute the standard deviation of each column.

        Convenience method equivalent to ``aggregate('std')`` (ddof=1) or
        ``aggregate('std_dev_population')`` (ddof=0). Returns a single-row
        TDS frame with the standard deviation of every column.

        Parameters
        ----------
        axis : {{0, 'index'}}, default 0
            Only ``0`` / ``'index'`` is supported.
        skipna : bool, default True
            Must be ``True``. ``False`` is not supported.
        ddof : int, default 1
            Degrees of freedom. ``1`` for sample standard deviation
            (``STDDEV_SAMP``), ``0`` for population standard deviation
            (``STDDEV_POP``).
        numeric_only : bool, default False
            Must be ``False``. ``True`` is not supported.
        **kwargs
            Not supported.

        Returns
        -------
        PandasApiTdsFrame
            A single-row TDS frame with column standard deviations.

        Raises
        ------
        NotImplementedError
            If ``ddof`` is not ``0`` or ``1``, or if any other
            parameter is set to an unsupported value.

        See Also
        --------
        aggregate : General aggregation method.
        var : Compute column variances.
        mean : Compute column means.

        Notes
        -----
        **Differences from pandas:**

        - Only ``ddof=0`` and ``ddof=1`` are supported.
        - Internally delegates to ``aggregate('std')`` (ddof=1, maps to
          ``STDDEV_SAMP``) or ``aggregate('std_dev_population')``
          (ddof=0, maps to ``STDDEV_POP``).

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            # Standard deviation of numeric columns
            frame.filter(items=["Order Id"]).std().to_pandas()

        """
        pass  # pragma: no cover

    @abstractmethod
    def var(
            self,
            axis: PyLegendUnion[int, str] = 0,
            skipna: bool = True,
            ddof: int = 1,
            numeric_only: bool = False,
            **kwargs: PyLegendPrimitiveOrPythonPrimitive
    ) -> "PandasApiTdsFrame":
        """
        Compute the variance of each column.

        Convenience method equivalent to ``aggregate('var')`` (ddof=1) or
        ``aggregate('variance_population')`` (ddof=0). Returns a single-row
        TDS frame with the variance of every column.

        Parameters
        ----------
        axis : {{0, 'index'}}, default 0
            Only ``0`` / ``'index'`` is supported.
        skipna : bool, default True
            Must be ``True``. ``False`` is not supported.
        ddof : int, default 1
            Degrees of freedom. ``1`` for sample variance
            (``VAR_SAMP``), ``0`` for population variance
            (``VAR_POP``).
        numeric_only : bool, default False
            Must be ``False``. ``True`` is not supported.
        **kwargs
            Not supported.

        Returns
        -------
        PandasApiTdsFrame
            A single-row TDS frame with column variances.

        Raises
        ------
        NotImplementedError
            If ``ddof`` is not ``0`` or ``1``, or if any other
            parameter is set to an unsupported value.

        See Also
        --------
        aggregate : General aggregation method.
        std : Compute column standard deviations.
        mean : Compute column means.

        Notes
        -----
        **Differences from pandas:**

        - Only ``ddof=0`` and ``ddof=1`` are supported.
        - Internally delegates to ``aggregate('var')`` (ddof=1, maps to
          ``VAR_SAMP``) or ``aggregate('variance_population')``
          (ddof=0, maps to ``VAR_POP``).

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            # Variance of numeric columns
            frame.filter(items=["Order Id"]).var().to_pandas()

        """
        pass  # pragma: no cover

    @abstractmethod
    def count(
            self,
            axis: PyLegendUnion[int, str] = 0,
            numeric_only: bool = False,
            **kwargs: PyLegendPrimitiveOrPythonPrimitive
    ) -> "PandasApiTdsFrame":
        """
        Count non-null values in each column.

        Convenience method equivalent to ``aggregate('count')``. Returns
        a single-row TDS frame with the count of non-null values for
        every column.

        Parameters
        ----------
        axis : {{0, 'index'}}, default 0
            Only ``0`` / ``'index'`` is supported.
        numeric_only : bool, default False
            Must be ``False``. ``True`` is not supported.
        **kwargs
            Not supported.

        Returns
        -------
        PandasApiTdsFrame
            A single-row TDS frame with non-null counts per column.

        Raises
        ------
        NotImplementedError
            If any parameter is set to an unsupported value.

        See Also
        --------
        aggregate : General aggregation method.
        sum : Compute column sums.

        Notes
        -----
        Internally delegates to ``aggregate('count')``. The same pandas
        deviations as :meth:`sum` apply (``axis=1``,
        ``numeric_only=True`` not supported). Unlike ``sum``, ``count``
        does not have a ``skipna`` parameter since counting is always
        of non-null values.

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            # Count non-null values in each column
            frame.count().to_pandas()

        """
        pass  # pragma: no cover

    @abstractmethod
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
        Apply a function to each column of the TDS frame.

        The callable receives a ``Series`` proxy for each column and
        must return a transformed value. The function is applied
        independently to **every** column, producing a new frame with
        the same column names but transformed values. Additional
        positional and keyword arguments can be forwarded to the
        callable via ``args`` and ``**kwargs``.

        Parameters
        ----------
        func : callable
            A function that takes a ``Series`` (column proxy) as its
            first argument and returns a primitive value or expression.
            String-based function names (e.g. ``'sum'``) are **not
            supported**; use :meth:`aggregate` for named aggregations.
        axis : {{0, 'index'}}, default 0
            Only column-wise application is supported (``axis=0`` or
            ``'index'``). Row-wise application (``axis=1``) raises
            ``ValueError``.
        raw : bool, default False
            Must be ``False``. ``True`` is not supported.
        result_type : None
            Must be ``None``. Any value raises
            ``NotImplementedError``.
        args : tuple, default ()
            Positional arguments to pass to ``func`` after the
            ``Series`` argument.
        by_row : {{False, 'compat'}}, default 'compat'
            Must be ``False`` or ``'compat'``. ``True`` raises
            ``NotImplementedError``.
        engine : str, default 'python'
            Must be ``'python'``. ``'numba'`` is not supported.
        engine_kwargs : None
            Must be ``None``. Not supported.
        **kwargs
            Additional keyword arguments forwarded to ``func``.

        Returns
        -------
        PandasApiTdsFrame
            A new TDS frame with the function applied to every column.

        Raises
        ------
        ValueError
            If ``axis`` is not ``0`` or ``'index'``.
        NotImplementedError
            If ``raw=True``, ``result_type`` is set, ``by_row=True``,
            ``engine='numba'``, ``engine_kwargs`` is set, or ``func``
            is a string.
        TypeError
            If ``func`` is not callable.

        See Also
        --------
        assign : Add or overwrite specific columns with callables.
        aggregate : Aggregate (reduce) columns to a single row.

        Notes
        -----
        **Differences from pandas:**

        - In pandas, ``apply`` with ``axis=0`` passes each column as
          a ``pandas.Series`` to the function, which can return a
          scalar (reducing the frame) or a Series (transforming it).
          Here, ``func`` receives a **column Series proxy** and must
          return a **scalar expression** that defines a row-level
          transformation. This means ``apply`` always produces a frame
          with the **same number of rows** — it cannot reduce the
          frame the way pandas ``apply`` can.
        - Row-wise application (``axis=1``) is **not supported**.
        - String function names (e.g. ``'sum'``) are **not supported**.
          Use :meth:`aggregate` instead.
        - ``raw=True``, ``result_type``, ``engine='numba'``, and
          ``engine_kwargs`` are **not supported**.

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            # Apply a lambda to every column
            frame.filter(items=["Order Id"]).apply(
                lambda x: x * 2
            ).head(5).to_pandas()

            # Apply a function with extra arguments
            def add_offset(series, offset, *, scale=1):
                return series * scale + offset

            frame.filter(items=["Order Id"]).apply(
                add_offset, args=(100,), scale=2
            ).head(5).to_pandas()

        """
        pass  # pragma: no cover

    @property
    @abstractmethod
    def iloc(self) -> "PandasApiIlocIndexer":
        """
        Purely integer-location based indexing for selection by position.

        Access rows and columns by integer position (0-based). Returns
        a ``PandasApiIlocIndexer`` that supports ``[]`` notation.

        Allowed inputs:

        - **An integer** — selects a single row (e.g. ``frame.iloc[5]``).
        - **A slice with ints** — selects a range of rows
          (e.g. ``frame.iloc[1:7]``). Only step=1 (or ``None``) is
          supported.
        - **A tuple of (rows, cols)** — selects rows and columns
          simultaneously (e.g. ``frame.iloc[1:5, 0:2]``). Each
          element can be an int or a slice.

        Returns
        -------
        PandasApiIlocIndexer
            An indexer object supporting ``[]`` notation that returns
            a new ``PandasApiTdsFrame``.

        Raises
        ------
        IndexError
            If more than two indexers are provided.
            If a column integer index is out of bounds.
        NotImplementedError
            If a slice step other than 1 is used for rows or columns.
            If a list, boolean array, or callable is used as an
            indexer.

        See Also
        --------
        loc : Label-based indexing (row filtering + column selection).
        head : Return the first n rows.
        truncate : Select rows by index range.
        filter : Select columns by name.

        Notes
        -----
        **Differences from pandas:**

        - Only **int** and **slice** indexers are supported. Lists of
          integers, boolean arrays, and callable indexers raise
          ``NotImplementedError``.
        - Slice steps other than 1 are **not supported**.
        - Negative integer indexing for rows is handled via
          ``truncate``, so it follows truncate's limitations.
        - When a single integer row index exceeds the number of rows,
          an **empty frame** is returned (no ``IndexError`` is raised,
          unlike pandas).

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            # Select a single row
            frame.iloc[0].to_pandas()

            # Select a range of rows and columns
            frame.iloc[1:4, 0:2].to_pandas()

            # Select a single row, all columns
            frame.iloc[2, :].to_pandas()

        """
        pass  # pragma: no cover

    @property
    @abstractmethod
    def loc(self) -> "PandasApiLocIndexer":
        """
        Access rows and columns by label-based indexing or boolean conditions.

        Returns a ``PandasApiLocIndexer`` that supports ``[]``
        notation for combined row filtering and column selection.

        **Row selection** (first indexer):

        - **Complete slice** ``:``: Select all rows.
        - **Boolean expression**: A ``PyLegendBoolean`` expression
          built from column comparisons
          (e.g. ``frame['col'] > 5``), used as a WHERE filter.
        - **Callable**: A function that receives the frame and returns
          a ``PyLegendBoolean`` expression
          (e.g. ``lambda x: x['col'] > 5``).

        **Column selection** (second indexer):

        - **``str``**: A single column name (e.g. ``'col1'``).
        - **``list of str``**: Multiple column names.
        - **``list of bool``**: Boolean mask over columns (must match
          the number of columns exactly).
        - **``slice of str``**: Label-based column slice
          (e.g. ``'col1':'col3'``), inclusive on both ends.
        - **Complete slice** ``:``: Select all columns.

        Returns
        -------
        PandasApiLocIndexer
            An indexer object supporting ``[]`` notation that returns
            a new ``PandasApiTdsFrame``.

        Raises
        ------
        IndexError
            If more than two indexers are provided.
            If a boolean column mask has the wrong length.
        TypeError
            If a label-based slice is used for rows (only ``:`` is
            allowed).
            If a list of integers, a set, or another unsupported type
            is used for row or column selection.
        KeyError
            If a column name in a list does not exist in the frame.

        See Also
        --------
        iloc : Integer-position based indexing.
        filter : Select columns by name.
        head : Return the first n rows.

        Notes
        -----
        **Differences from pandas:**

        - For **row selection**, only ``:``, boolean expressions, and
          callables are supported. Integer label selection, integer
          slicing, and list-of-integer selection are **not supported**.
        - Label-based **row slicing** (e.g. ``frame.loc[2:5]``) is
          **not supported** — only the complete slice ``:`` is
          allowed.
        - For **column selection**, string labels, lists of strings,
          boolean masks, and label-based slices are supported. Label
          slices use ``pandas.Index.slice_indexer`` internally, so
          slice semantics are **inclusive on both ends** (matching
          pandas ``loc`` behaviour).
        - If a label-based column slice resolves to an empty selection,
          an empty frame (zero rows) is returned via ``head(0)``.

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            # Select specific columns
            frame.loc[:, "Ship Name"].head(3).to_pandas()

            # Filter rows with a boolean condition and select columns
            frame.loc[frame["Order Id"] > 10300, ["Order Id", "Ship Name"]].head(5).to_pandas()

            # Filter rows with a callable
            frame.loc[
                lambda x: x["Ship Name"].startswith("A"),
                ["Order Id", "Ship Name"]
            ].head(5).to_pandas()

            # Boolean column mask
            frame.loc[:, [True, False]].head(3).to_pandas()

        """
        pass  # pragma: no cover

    @abstractmethod
    def head(self, n: int = 5) -> "PandasApiTdsFrame":
        """
        Return the first n rows of the TDS frame.

        This function returns the first ``n`` rows from the frame.
        It is useful for quickly inspecting the data without loading the entire dataset.

        Parameters
        ----------
        n : int, default 5
            Number of rows to return. Must be a non-negative integer.
            Passing a negative value raises ``NotImplementedError``.
            Passing a non-int type raises ``TypeError``.

        Returns
        -------
        PandasApiTdsFrame
            A new TDS frame containing only the first n rows.

        Raises
        ------
        TypeError
            If ``n`` is not an int.
        NotImplementedError
            If ``n`` is negative.

        See Also
        --------
        drop : Remove rows or columns by label.
        truncate : Truncate rows before and/or after some index value.
        iloc : Select rows by integer-location based indexing.

        Notes
        -----
        **Differences from pandas:**

        - **Negative values for ``n`` are not supported.** In pandas,
          ``head(-n)`` returns all rows except the last ``n``. Here,
          passing a negative value raises ``NotImplementedError``.
        - The operation is **lazy** — it builds a query
          rather than materialising rows in memory. Call
          ``to_pandas()`` or ``execute_frame_to_string()`` to
          materialise the result.

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            # Get first 5 rows (default)
            frame.head().to_pandas()

            # Get first 3 rows
            frame.head(3).to_pandas()

        """
        pass  # pragma: no cover

    @property
    @abstractmethod
    def shape(self) -> PyLegendTuple[int, int]:
        """
        Return the dimensionality of the TDS frame as ``(rows, columns)``.

        .. warning::

            Unlike ``pandas.DataFrame.shape``, this property **executes
            the frame** against the server to determine the row count.
            It issues a ``COUNT`` aggregation query, so every access
            incurs a round-trip to the database.

        Returns
        -------
        tuple of (int, int)
            A tuple ``(number_of_rows, number_of_columns)``.

        See Also
        --------
        head : Return the first n rows (lazy, no execution).
        count : Count non-null values per column (returns a frame).

        Notes
        -----
        **Differences from pandas:**

        - In pandas, ``DataFrame.shape`` is an **O(1) metadata lookup**
          that never triggers computation. Here, ``shape`` **executes
          the current frame** to obtain the row count via a ``COUNT``
          aggregation query. This means it requires a live connection
          to the database. This will fail on non-executable frames.
        - The result type is always ``(int, int)``; there is no lazy
          evaluation.

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            # Get the shape (triggers server execution)
            frame.head(5).shape

        """
        pass  # pragma: no cover

    @abstractmethod
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
        Remove rows with missing values.

        Return a new TDS frame with rows containing NA / null values
        removed. The check can be scoped to specific columns via
        ``subset`` and controlled via ``how``.

        Parameters
        ----------
        axis : {{0, 'index'}}, default 0
            Only ``0`` / ``'index'`` (drop rows) is supported.
            ``1`` / ``'columns'`` (drop columns) raises
            ``NotImplementedError``.
        how : {{'any', 'all'}}, default 'any'
            - ``'any'`` : Drop the row if **any** of the considered
              columns contain a null value.
            - ``'all'`` : Drop the row only if **all** of the
              considered columns are null.
        thresh : int, optional
            **Not supported.** Passing any value raises
            ``NotImplementedError``.
        subset : list-like of str, optional
            Column names to consider when checking for nulls. If
            ``None`` (default), all columns are considered. An empty
            list with ``how='any'`` keeps all rows; an empty list with
            ``how='all'`` drops all rows.
        inplace : bool, default False
            Must be ``False``. ``True`` raises ``NotImplementedError``.
        ignore_index : bool, default False
            Must be ``False``. ``True`` raises ``NotImplementedError``.

        Returns
        -------
        PandasApiTdsFrame
            A new TDS frame with rows containing nulls removed.

        Raises
        ------
        NotImplementedError
            If ``axis=1``, ``thresh`` is set, ``inplace=True``, or
            ``ignore_index=True``.
        ValueError
            If ``axis`` is not a recognised value or ``how`` is not
            ``'any'`` or ``'all'``.
        TypeError
            If ``subset`` is not a list, tuple, or set.
        KeyError
            If any column in ``subset`` does not exist in the frame.

        See Also
        --------
        fillna : Fill missing values instead of dropping rows.

        Notes
        -----
        **Differences from pandas:**

        - ``axis=1`` (dropping columns with nulls) is **not supported**.
        - ``thresh`` (minimum number of non-null values to keep a row)
          is **not supported**.
        - ``inplace=True`` is **not supported**; a new frame is always
          returned.
        - ``ignore_index=True`` is **not supported**.
        - Passing an empty ``subset=[]`` with ``how='any'`` is a
          no-op (all rows are kept). With ``how='all'``, an empty
          ``subset=[]`` drops **all rows** (the filter becomes
          ``false``).

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            # Drop rows where any column is null
            frame.dropna().head(5).to_pandas()

            # Drop rows where all columns are null
            frame.dropna(how="all").head(5).to_pandas()

            # Only consider specific columns
            frame.dropna(subset=["Ship Name"]).head(5).to_pandas()

        """
        pass  # pragma: no cover

    @abstractmethod
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
        Fill missing values with a specified value.

        Replace NA / null entries in the TDS frame. A scalar ``value``
        is applied to every column; a dict maps specific columns to
        their fill values (columns not present in the dict are left
        unchanged). Implemented via ``COALESCE`` at the SQL level.

        Parameters
        ----------
        value : scalar or dict
            Value(s) to replace nulls with. Accepted scalar types are
            ``int``, ``float``, ``str``, ``bool``, ``date``, and
            ``datetime``. When a dict is provided, keys must be column
            name strings and values must be scalars of the above types.
            Columns in the dict that do not exist in the frame are
            silently ignored. Omitting ``value`` entirely raises
            ``ValueError``.
        axis : {{0, 'index'}}, default 0
            Only ``0`` / ``'index'`` is supported. ``1`` /
            ``'columns'`` raises ``NotImplementedError``.
        inplace : bool, default False
            Must be ``False``. ``True`` raises ``NotImplementedError``.
        limit : int, optional
            **Not supported.** Passing any value raises
            ``NotImplementedError``.

        Returns
        -------
        PandasApiTdsFrame
            A new TDS frame with null values replaced.

        Raises
        ------
        ValueError
            If ``value`` is not provided.
            If ``axis`` is not a recognised value.
        TypeError
            If ``value`` is not a scalar or dict.
            If dict keys are not strings or dict values are not
            scalars.
        NotImplementedError
            If ``axis=1``, ``inplace=True``, or ``limit`` is set.

        See Also
        --------
        dropna : Remove rows with missing values.

        Notes
        -----
        **Differences from pandas:**

        - The ``method`` parameter (``'ffill'``, ``'bfill'``) available
          in older pandas versions is **not present**.
        - ``inplace=True`` is **not supported**; a new frame is always
          returned.
        - ``limit`` (maximum number of consecutive nulls to fill) is
          **not supported**.
        - ``axis=1`` (fill along columns) is **not supported**.

        Examples
        --------
        .. ipython:: python

            import datetime
            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()
            frame = frame.sort_values("Shipped Date")
            frame = frame.head()

            # check initial count of all the non-null values
            frame.to_pandas()

            # Fill all null values of the "Shipped Date" column with a fixed date
            frame = frame.fillna({
                "Shipped Date": datetime.date(1, 1, 1)
            })
            frame.to_pandas()

        """
        pass  # pragma: no cover

    @abstractmethod
    def rank(
            self,
            axis: PyLegendUnion[int, str] = 0,
            method: str = 'min',
            numeric_only: bool = False,
            na_option: str = 'bottom',
            ascending: bool = True,
            pct: bool = False
    ) -> "PandasApiTdsFrame":
        """
        Compute the rank of values in each column.

        Replace every column's values with their rank within that
        column. Each column is ranked independently using an SQL
        window function (``RANK``, ``DENSE_RANK``, ``ROW_NUMBER``, or
        ``PERCENT_RANK``).

        The result is a new frame with the **same column names** but
        all values replaced by their integer (or float when
        ``pct=True``) rank.

        Parameters
        ----------
        axis : {{0, 'index'}}, default 0
            Only ``0`` / ``'index'`` is supported. ``1`` raises
            ``NotImplementedError``.
        method : {{'min', 'first', 'dense'}}, default 'min'
            How to rank equal values:

            - ``'min'`` : Lowest rank in the group of ties (SQL
              ``RANK()``).
            - ``'first'`` : Ranks assigned in order of appearance
              (SQL ``ROW_NUMBER()``).
            - ``'dense'`` : Like ``'min'`` but ranks always increase
              by 1, no gaps (SQL ``DENSE_RANK()``).
        numeric_only : bool, default False
            If ``True``, only rank columns of numeric type (Integer,
            Float, Number). Non-numeric columns are excluded from the
            result.
        na_option : {{'bottom'}}, default 'bottom'
            How to rank null values. Only ``'bottom'`` is supported.
            ``'keep'`` and ``'top'`` raise ``NotImplementedError``.
        ascending : bool, default True
            Whether to rank in ascending order. ``False`` ranks in
            descending order.
        pct : bool, default False
            If ``True``, compute percentage ranks (SQL
            ``PERCENT_RANK()``). Result columns are of float type.
            Can only be used with ``method='min'``.

        Returns
        -------
        PandasApiTdsFrame
            A new TDS frame where every column contains integer ranks
            (or float when ``pct=True``).

        Raises
        ------
        NotImplementedError
            If ``axis`` is not ``0`` or ``'index'``.
            If ``method`` is not one of ``'min'``, ``'first'``,
            ``'dense'`` (e.g. ``'average'`` and ``'max'`` are not
            supported).
            If ``na_option`` is not ``'bottom'``.
            If ``pct=True`` with a method other than ``'min'``.

        See Also
        --------
        PandasApiGroupbyTdsFrame.rank : Rank within groups.
        sort_values : Sort the frame by column values.

        Notes
        -----
        **Differences from pandas:**

        - The ``'average'`` and ``'max'`` ranking methods are **not
          supported**. Only ``'min'``, ``'first'``, and ``'dense'``
          are available.
        - ``na_option`` only supports ``'bottom'``. ``'keep'`` and
          ``'top'`` raise ``NotImplementedError``.
        - ``pct=True`` is only supported with ``method='min'``
          (maps to ``PERCENT_RANK()``). Combining ``pct=True`` with
          other methods raises ``NotImplementedError``.
        - When applied to the full frame (not via a Series), **all
          columns** are replaced by their ranks. To append a rank
          column instead, use bracket assignment on a single-column
          Series: ``frame["rank_col"] = frame["col"].rank()``.
        - Combining multiple rank calls in a single expression is
          **not supported**
          (e.g. ``frame["col1"].rank() + frame["col2"].rank()``).
          Compute them in separate assignment steps instead.

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            # Rank all columns (replaces values with ranks)
            frame.filter(items=["Order Id"]).rank().head(5).to_pandas()

            # Append a percentage rank column via Series assignment
            frame["Order Rank"] = frame["Order Id"].rank(pct=True)
            frame.head(5).to_pandas()

        """
        pass  # pragma: no cover

    @abstractmethod
    def window_frame_legend_ext(
            self,
            frame_spec: "FrameSpec",
            order_by: PyLegendOptional[PyLegendUnion[str, PyLegendSequence[str]]] = None,
            ascending: PyLegendUnion[bool, PyLegendSequence[bool]] = True,
    ) -> "PandasApiWindowTdsFrame":
        """
        Create a custom window specification with explicit frame bounds.

        **PyLegend extension** — not present in pandas.

        Provides fine-grained control over the ``ROWS BETWEEN`` or
        ``RANGE BETWEEN`` clause used by window-aggregate computations.

        Parameters
        ----------
        frame_spec : RowsBetween or RangeBetween
            A window-frame specification created via
            :meth:`rows_between` or :meth:`range_between`.
        order_by : str or list of str, optional
            Column(s) to order by within the window. ``None`` means no
            explicit ordering (a fallback will be chosen automatically).
        ascending : bool or list of bool, default True
            Sort direction(s) for the ``order_by`` columns.

        Returns
        -------
        PandasApiWindowTdsFrame
            A window frame on which window aggregates (``sum``, ``mean``,
            ``min``, ``max``, etc.) can be called.

        Raises
        ------
        TypeError
            If ``frame_spec`` is not a ``RowsBetween`` or ``RangeBetween``.

        See Also
        --------
        expanding : Expanding (cumulative) window.
        rolling : Fixed-size sliding window.
        rows_between : Create a ``ROWS BETWEEN`` specification.
        range_between : Create a ``RANGE BETWEEN`` specification.

        Notes
        -----
        **Differences from pandas:**

        - This method has **no pandas equivalent**. It is a pylegend
          extension for explicit control over the SQL window frame.

        Examples
        --------
        .. ipython:: python

            import pylegend
            from pylegend.core.language.pandas_api.pandas_api_frame_spec import (
                RowsBetween,
            )
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            spec = RowsBetween(-2, 0)
            frame.filter(items=["Order Id"]).window_frame_legend_ext(
                spec, order_by="Order Id"
            ).sum().head(5).to_pandas()

        """
        pass  # pragma: no cover

    @abstractmethod
    def rows_between(
            self,
            start: PyLegendOptional[int] = None,
            end: PyLegendOptional[int] = None,
    ) -> "RowsBetween":
        """
        Create a ``ROWS BETWEEN`` window-frame specification.

        **PyLegend extension** — not present in pandas.

        Sign convention (same as legendQL):

        - ``None`` → UNBOUNDED (PRECEDING for *start*, FOLLOWING for *end*)
        - Negative → PRECEDING (e.g. ``-3`` → ``3 PRECEDING``)
        - ``0`` → CURRENT ROW
        - Positive → FOLLOWING (e.g. ``2`` → ``2 FOLLOWING``)

        Parameters
        ----------
        start : int, optional
            Lower bound of the frame. ``None`` means unbounded preceding.
        end : int, optional
            Upper bound of the frame. ``None`` means unbounded following.

        Returns
        -------
        RowsBetween
            A frame specification to pass to :meth:`window_frame_legend_ext`.

        Raises
        ------
        ValueError
            If ``start`` is greater than ``end``.

        See Also
        --------
        range_between : Create a ``RANGE BETWEEN`` specification.
        window_frame_legend_ext : Apply a custom window specification.

        Notes
        -----
        **Differences from pandas:**

        - This method has **no pandas equivalent**. It is a pylegend
          extension for constructing SQL ``ROWS BETWEEN`` clauses.

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            # 3-row trailing window (current row and 2 preceding)
            spec = frame.rows_between(-2, 0)

        """
        pass  # pragma: no cover

    @abstractmethod
    def range_between(
            self,
            start: PyLegendOptional[PyLegendUnion[int, float, PythonDecimal]] = None,
            end: PyLegendOptional[PyLegendUnion[int, float, PythonDecimal]] = None,
            *,
            duration_start: PyLegendOptional[PyLegendUnion[int, float, PythonDecimal, str]] = None,
            duration_start_unit: PyLegendOptional[str] = None,
            duration_end: PyLegendOptional[PyLegendUnion[int, float, PythonDecimal, str]] = None,
            duration_end_unit: PyLegendOptional[str] = None,
    ) -> "RangeBetween":
        """
        Create a ``RANGE BETWEEN`` window-frame specification.

        **PyLegend extension** — not present in pandas.

        Supports two calling styles:

        **Simple numeric bounds** (same sign convention as
        :meth:`rows_between`)::

            range_between(start=-100, end=0)
            # → RANGE BETWEEN 100 PRECEDING AND CURRENT ROW

        **Duration-based bounds** (for date/time ``ORDER BY`` columns)::

            range_between(
                duration_start=-1, duration_start_unit="DAYS",
                duration_end=1, duration_end_unit="MONTHS",
            )

        Parameters
        ----------
        start : int, float, or Decimal, optional
            Lower bound of the range. ``None`` means unbounded preceding.
        end : int, float, or Decimal, optional
            Upper bound of the range. ``None`` means unbounded following.
        duration_start : int, float, Decimal, or str, optional
            Duration-based lower bound. Pass ``"unbounded"`` for
            unbounded preceding.
        duration_start_unit : str, optional
            Time unit for ``duration_start`` (e.g. ``"DAYS"``,
            ``"MONTHS"``).
        duration_end : int, float, Decimal, or str, optional
            Duration-based upper bound.
        duration_end_unit : str, optional
            Time unit for ``duration_end``.

        Returns
        -------
        RangeBetween
            A frame specification to pass to :meth:`window_frame_legend_ext`.

        Raises
        ------
        ValueError
            If positional bounds and duration bounds are mixed, or if
            ``start`` is greater than ``end``.

        See Also
        --------
        rows_between : Create a ``ROWS BETWEEN`` specification.
        window_frame_legend_ext : Apply a custom window specification.

        Notes
        -----
        **Differences from pandas:**

        - This method has **no pandas equivalent**. It is a pylegend
          extension for constructing SQL ``RANGE BETWEEN`` clauses.

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            # Numeric range: 100 preceding to current row
            spec = frame.range_between(-100, 0)

        """
        pass  # pragma: no cover

    @abstractmethod
    def cume_dist_legend_ext(
            self,
            ascending: bool = True,
    ) -> "PandasApiTdsFrame":
        """
        Compute the cumulative distribution of each column.

        **PyLegend extension** — not present in pandas.

        Maps to SQL ``CUME_DIST() OVER (ORDER BY col)`` and Pure
        ``cumulativeDistribution``.

        Parameters
        ----------
        ascending : bool, default True
            Whether to order in ascending direction.

        Returns
        -------
        PandasApiTdsFrame
            A new TDS frame with cumulative distribution values
            (floats between 0 and 1) replacing every column.

        See Also
        --------
        rank : Compute column ranks.
        ntile_legend_ext : Assign rows to numbered buckets.

        Notes
        -----
        **Differences from pandas:**

        - This method has **no pandas equivalent**. ``CUME_DIST`` is
          exposed as a pylegend extension.

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            frame.filter(
                items=["Order Id"]
            ).cume_dist_legend_ext().head(5).to_pandas()

        """
        pass  # pragma: no cover

    @abstractmethod
    def ntile_legend_ext(
            self,
            num_buckets: int,
            ascending: bool = True,
    ) -> "PandasApiTdsFrame":
        """
        Assign rows to numbered buckets for each column.

        **PyLegend extension** — not present in pandas.

        Maps to SQL ``NTILE(n) OVER (ORDER BY col)`` and Pure ``ntile``.

        Parameters
        ----------
        num_buckets : int
            Number of buckets to distribute rows into.
        ascending : bool, default True
            Whether to order in ascending direction.

        Returns
        -------
        PandasApiTdsFrame
            A new TDS frame with integer bucket numbers (1-based)
            replacing every column.

        See Also
        --------
        rank : Compute column ranks.
        cume_dist_legend_ext : Cumulative distribution.

        Notes
        -----
        **Differences from pandas:**

        - This method has **no pandas equivalent**. ``NTILE`` is
          exposed as a pylegend extension.

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            frame.filter(
                items=["Order Id"]
            ).ntile_legend_ext(4).head(5).to_pandas()

        """
        pass  # pragma: no cover

    @abstractmethod
    def concat_legend_ext(
            self,
            other: "PandasApiTdsFrame",
    ) -> "PandasApiTdsFrame":
        """
        Concatenate this frame with another frame vertically.

        **PyLegend extension** — not present in pandas.

        Produces a SQL ``UNION ALL`` of the two frames. Both frames must
        have compatible schemas (same column names and types).

        Parameters
        ----------
        other : PandasApiTdsFrame
            The frame to concatenate below this one.

        Returns
        -------
        PandasApiTdsFrame
            A new TDS frame whose rows are the rows of ``self`` followed
            by the rows of ``other``.

        Raises
        ------
        TypeError
            If ``other`` is not a ``PandasApiBaseTdsFrame``.

        See Also
        --------
        merge : SQL join of two frames.

        Notes
        -----
        **Differences from pandas:**

        - In pandas, ``pd.concat`` is a top-level function that accepts
          a list of DataFrames. Here, ``concat_legend_ext`` is a method
          on a ``PandasApiTdsFrame`` and only supports vertical
          concatenation (``UNION ALL``) of two frames with the same
          schema.

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            top = frame.head(3)
            bottom = frame.head(3)
            top.concat_legend_ext(bottom).to_pandas()

        """
        pass  # pragma: no cover

    @abstractmethod
    def shift(
            self,
            order_by: PyLegendUnion[str, PyLegendSequence[str]],
            periods: PyLegendUnion[int, PyLegendSequence[int]] = 1,
            freq: PyLegendOptional[PyLegendUnion[str, int]] = None,
            axis: PyLegendUnion[int, str] = 0,
            fill_value: PyLegendOptional[PyLegendHashable] = None,
            suffix: PyLegendOptional[str] = None
    ) -> "PandasApiTdsFrame":
        """
        Shift values by desired number of periods.

        Replace every column's values with their shifted values. Because
        underlying TDS is inherently unordered, this requires
        an explicit ``order_by`` parameter to define the ordering for the
        window function (``LAG`` or ``LEAD``).

        Parameters
        ----------
        order_by : str or sequence of str
            Column name(s) to order the frame by before applying the shift.
            Unlike pandas, this is required to ensure deterministic output.
            All specified columns must be present in the base frame.
        periods : int or sequence of int, default 1
            Number of periods to shift. Currently, only ``1`` (shift down,
            equivalent to SQL ``LAG``) and ``-1`` (shift up, equivalent to SQL ``LEAD``) are supported.
            If a sequence is provided, it cannot contain duplicate values.
        freq : None
            Not supported. Must be ``None``.
        axis : {{0, 'index'}}, default 0
            Axis to shift along. Only ``0`` / ``'index'`` is supported.
        fill_value : None
            Not supported. Must be ``None``. Missing values introduced by
            the shift will always be null.
        suffix : str, default None
            If provided, renames the resulting shifted columns by appending
            this string to the original column names. This argument can
            only be used if ``periods`` is a sequence (not a single integer).

        Returns
        -------
        PandasApiTdsFrame
            A new TDS frame with the shifted columns.

        Raises
        ------
        NotImplementedError
            If ``periods`` contains any values other than ``1`` or ``-1``.
            If ``freq`` is not ``None``.
            If ``axis`` is not ``0`` or ``'index'``.
            If ``fill_value`` is not ``None``.
        ValueError
            If any column specified in ``order_by`` is not present in the frame.
            If ``periods`` contains duplicate values.
            If ``suffix`` is specified but ``periods`` is a single integer.

        See Also
        --------
        rank : Rank as ascending or descending.
        PandasApiGroupbyTdsFrame.shift : Shift values within groups.

        Notes
        -----
        **Differences from pandas:**

        - The ``order_by`` parameter is **mandatory**. In pandas, ``shift``
          relies on the implicit order of the dataframe's index. Here,
          an explicit order must be provided.
        - ``periods`` is strictly limited to ``1`` or ``-1``. Arbitrary
          integer shifts are **not supported**.
        - ``fill_value`` is **not supported** and must be ``None``.
        - The ``freq`` parameter is **not supported** and must be ``None``.
        - ``axis=1`` (shifting horizontally across columns) is **not
          supported**.

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            # Shift the entire frame down
            frame.head(5).shift(
                order_by="Order Date",
                periods=1
            ).to_pandas()

        """
        pass  # pragma: no cover

    @abstractmethod
    def diff(
            self,
            order_by: PyLegendUnion[str, PyLegendSequence[str]],
            periods: int = 1,
            axis: PyLegendUnion[int, str] = 0
    ) -> "PandasApiTdsFrame":
        """
        Compute the first discrete difference of each column.

        Calculates ``value - lag(value, periods)`` for each numeric
        column, using the SQL ``LAG`` window function.

        Parameters
        ----------
        order_by : str or list of str
            Column(s) that define row ordering. **Required** (pylegend
            extension).
        periods : int, default 1
            Number of periods to compute the difference over.
        axis : {{0, 'index'}}, default 0
            Only ``0`` / ``'index'`` is supported.

        Returns
        -------
        PandasApiTdsFrame
            A new TDS frame with differenced values.

        Raises
        ------
        NotImplementedError
            If ``axis`` is not ``0`` / ``'index'``.

        Notes
        -----
        **Differences from pandas:**

        - ``order_by`` is **required** and is a pylegend extension.
        - ``axis=1`` is **not supported**.

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            # First difference of Order Id
            frame.filter(items=["Order Id"]).diff(
                order_by="Order Id"
            ).head(5).to_pandas()

        """
        pass  # pragma: no cover

    @abstractmethod
    def pct_change(
            self,
            order_by: PyLegendUnion[str, PyLegendSequence[str]],
            periods: PyLegendUnion[int, PyLegendSequence[int]] = 1,
            freq: PyLegendOptional[PyLegendUnion[str, int]] = None,
            **kwargs: PyLegendPrimitiveOrPythonPrimitive
    ) -> "PandasApiTdsFrame":
        """
        Compute the fractional change between the current and a prior row.

        Calculates ``(value - lag(value, periods)) / lag(value, periods)``
        for each numeric column.

        Parameters
        ----------
        order_by : str or list of str
            Column(s) that define row ordering. **Required** (pylegend
            extension).
        periods : int or list of int, default 1
            Number of periods to compute the percentage change over.
        freq : str or int, optional
            Not supported. Must be ``None``.
        **kwargs
            Not supported.

        Returns
        -------
        PandasApiTdsFrame
            A new TDS frame with percentage-change values.

        Raises
        ------
        NotImplementedError
            If ``freq`` or ``**kwargs`` are set to unsupported values.

        Notes
        -----
        **Differences from pandas:**

        - ``order_by`` is **required** and is a pylegend extension.
        - ``freq`` is **not supported**.

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            # Percentage change of Order Id
            frame.filter(items=["Order Id"]).pct_change(
                order_by="Order Id"
            ).head(5).to_pandas()

        """
        pass  # pragma: no cover

    @abstractmethod
    def info(
            self,
            verbose: PyLegendOptional[bool] = None,
            buf: PyLegendOptional[PyLegendUnion["IO[str]", "StringIO"]] = None,
            max_cols: PyLegendOptional[int] = None,
            memory_usage: PyLegendOptional[PyLegendUnion[bool, str]] = None,
            show_counts: PyLegendOptional[bool] = None
    ) -> None:
        """
        Print a concise summary of the TDS frame.

        Displays the column names and their data types. This is a
        lightweight alternative to running a query — it uses only
        the metadata already available on the frame.

        Parameters
        ----------
        verbose : bool, optional
            Not supported. Ignored.
        buf : IO[str] or StringIO, optional
            Not supported. Output always goes to stdout.
        max_cols : int, optional
            Not supported. Ignored.
        memory_usage : bool or str, optional
            Not supported. Ignored.
        show_counts : bool, optional
            Not supported. Ignored.

        Returns
        -------
        None
            Prints to stdout; returns nothing.

        Notes
        -----
        **Differences from pandas:**

        - Only column names and types are shown.
        - ``memory_usage``, ``verbose``, ``buf``, ``max_cols``, and
          ``show_counts`` are accepted for API compatibility but
          **ignored**.

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            frame.info()

        """
        pass  # pragma: no cover

    @abstractmethod
    def drop_duplicates(
            self,
            subset: PyLegendOptional[PyLegendUnion[str, PyLegendList[str]]] = None,
            *,
            keep: str = 'first',
            inplace: bool = False,
            ignore_index: bool = False
    ) -> "PandasApiTdsFrame":
        """
        Remove duplicate rows.

        Returns a new TDS frame with duplicate rows removed, optionally
        considering only a subset of columns for identifying duplicates.

        Parameters
        ----------
        subset : str or list of str, optional
            Column label or list of labels to consider for identifying
            duplicates. If ``None``, all columns are used.
        keep : {{'first'}}, default 'first'
            Must be ``'first'``. Only keeping the first occurrence is
            supported.
        inplace : bool, default False
            Must be ``False``. In-place modification is not supported.
        ignore_index : bool, default False
            Must be ``False``. Not supported.

        Returns
        -------
        PandasApiTdsFrame
            A new TDS frame with duplicates removed.

        Raises
        ------
        NotImplementedError
            If ``keep`` is not ``'first'``, or ``inplace`` / ``ignore_index``
            are ``True``.

        Notes
        -----
        **Differences from pandas:**

        - Only ``keep='first'`` is supported. ``'last'`` and ``False``
          are **not supported**.
        - ``inplace=True`` and ``ignore_index=True`` are **not supported**.
        - Generates SQL ``SELECT DISTINCT ON ...`` or equivalent.

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            # Remove rows with duplicate Ship Name
            frame.drop_duplicates(subset=["Ship Name"]).head(5).to_pandas()

        """
        pass  # pragma: no cover

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
from typing import TYPE_CHECKING

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
    PyLegendDict
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
    from pylegend.core.language.pandas_api.pandas_api_series import Series
    from pylegend.core.tds.pandas_api.frames.pandas_api_groupby_tds_frame import PandasApiGroupbyTdsFrame
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
          bracket notation ``x["col"]``. Standard pandas Series operations
          are not available on the row object.
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
            Exact column names to keep. The output column order matches the
            order of this list. All names must exist in the frame.
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
            Also raised if ``items`` is a string instead of a list, or
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
          filtering is supported** (``axis=1`` or ``'columns'``). Passing
          ``axis=0`` or ``'index'`` raises ``ValueError``.
        - In pandas, ``items`` silently ignores names that do not exist in
          the frame. Here, **all names must exist**; unknown names raise a
          ``ValueError`` listing the missing and available columns.
        - In pandas, ``like`` and ``regex`` return an empty DataFrame when
          no columns match. Here, they **raise** ``ValueError`` when no
          columns match.
        - Calls can be chained to progressively narrow the column set
          (e.g. ``.filter(items=[...]).filter(like=...).filter(regex=...)``).

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
            If the length of ``ascending`` does not match ``by``.
            If ``axis`` is not ``0`` or ``'index'``.
            If ``inplace`` is ``True``.
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
          Sorting is delegated to the underlying SQL engine, which chooses
          its own algorithm.
        - The ``key`` parameter (per-element transform before sorting)
          is **not supported**.
        - ``inplace=True`` is **not supported**; always returns a new frame.
        - ``ignore_index`` must be ``True``; ``False`` is **not supported**
          because TDS frames do not have a meaningful integer index.
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
        (inclusive) to ``after`` (inclusive). This is useful for
        pagination or selecting a window of rows by position.

        Parameters
        ----------
        before : int or None, default 0
            First row index to include (0-based, inclusive). Negative
            values are silently clamped to ``0``. ``None`` is treated
            as ``0``.
        after : int or None, default None
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
            If ``axis`` is not ``0`` or ``'index'``.
            If ``copy`` is ``False``.
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
          Here, it selects rows by **positional** (integer) index only.
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
        index : optional
            **Not supported.** Passing any value raises
            ``NotImplementedError``.
        columns : str, sequence of str, or set of str, optional
            Column name(s) to drop. Mutually exclusive with ``labels``.
        level : optional
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
        - Calls can be chained to progressively drop columns
          (e.g. ``.drop(columns=["A"]).drop(columns=["B"])``).

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
        summary. Supported aggregation names are ``'sum'``, ``'mean'``,
        ``'min'``, ``'max'``, ``'count'``, ``'std'``, ``'var'``, as well
        as aliases ``'len'``, ``'size'`` (both map to count), and
        ``'average'`` / ``'avg'`` (map to mean).

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
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            # Aggregate a single column with a string function
            frame.aggregate({"Order Id": "count"}).to_pandas()

            # Aggregate multiple columns with different functions
            frame.aggregate({"Order Id": "min", "Ship Name": "count"}).to_pandas()

            # Broadcast a single function to all columns
            frame.aggregate("count").to_pandas()

            # Use a lambda for custom aggregation
            frame.aggregate({"Order Id": lambda x: x.max()}).to_pandas()

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
        Aggregate the TDS frame using one or more operations.

        Alias for :meth:`aggregate`. See ``aggregate`` for full
        documentation.

        Parameters
        ----------
        func : str, callable, np.ufunc, list, or dict
            Aggregation specification. See :meth:`aggregate`.
        axis : {{0, 'index'}}, default 0
            Axis along which to aggregate. Only ``0`` / ``'index'``
            is supported.
        *args
            Not supported.
        **kwargs
            Not supported.

        Returns
        -------
        PandasApiTdsFrame
            A new single-row TDS frame with the aggregated values.

        See Also
        --------
        aggregate : Equivalent method (canonical name).
        groupby : Group rows before aggregating.

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            # agg is an alias for aggregate
            frame.agg({"Order Id": "count"}).to_pandas()

            # Use a lambda
            frame.agg({"Order Id": lambda x: x.max()}).to_pandas()

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
        level : optional
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
        Compute the sample standard deviation of each column.

        Convenience method equivalent to ``aggregate('std')``. Returns a
        single-row TDS frame with the sample standard deviation
        (``ddof=1``) of every column.

        Parameters
        ----------
        axis : {{0, 'index'}}, default 0
            Only ``0`` / ``'index'`` is supported.
        skipna : bool, default True
            Must be ``True``. ``False`` is not supported.
        ddof : int, default 1
            Degrees of freedom. Must be ``1`` (sample standard
            deviation). Population standard deviation (``ddof=0``)
            is not supported.
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
            If any parameter is set to an unsupported value,
            including ``ddof != 1``.

        See Also
        --------
        aggregate : General aggregation method.
        var : Compute column variances.
        mean : Compute column means.

        Notes
        -----
        **Differences from pandas:**

        - Only ``ddof=1`` (sample standard deviation) is supported.
          ``ddof=0`` (population standard deviation) raises
          ``NotImplementedError``.
        - Internally delegates to ``aggregate('std')``, which maps to
          ``STDDEV_SAMP`` at the SQL level.

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
        Compute the sample variance of each column.

        Convenience method equivalent to ``aggregate('var')``. Returns a
        single-row TDS frame with the sample variance (``ddof=1``) of
        every column.

        Parameters
        ----------
        axis : {{0, 'index'}}, default 0
            Only ``0`` / ``'index'`` is supported.
        skipna : bool, default True
            Must be ``True``. ``False`` is not supported.
        ddof : int, default 1
            Degrees of freedom. Must be ``1`` (sample variance).
            Population variance (``ddof=0``) is not supported.
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
            If any parameter is set to an unsupported value,
            including ``ddof != 1``.

        See Also
        --------
        aggregate : General aggregation method.
        std : Compute column standard deviations.
        mean : Compute column means.

        Notes
        -----
        **Differences from pandas:**

        - Only ``ddof=1`` (sample variance) is supported. ``ddof=0``
          (population variance) raises ``NotImplementedError``.
        - Internally delegates to ``aggregate('var')``, which maps to
          ``VAR_SAMP`` at the SQL level.

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
        pass  # pragma: no cover

    @property
    @abstractmethod
    def iloc(self) -> "PandasApiIlocIndexer":
        pass  # pragma: no cover

    @property
    @abstractmethod
    def loc(self) -> "PandasApiLocIndexer":
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
            If ``n`` is not an int (e.g. a string).
        NotImplementedError
            If ``n`` is negative. Negative indexing is not supported yet
            in the Pandas API head.

        See Also
        --------
        drop : Remove rows or columns by label.
        truncate : Truncate rows before and/or after some index value.
        iloc : Select rows by integer-location based indexing.

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            # Get first 5 rows (default)
            frame.head().to_pandas()

            # Get first 3 rows
            frame.head(3).to_pandas()

            # Get zero rows (returns empty frame with columns preserved)
            frame.head(0).to_pandas()

        """
        pass  # pragma: no cover

    @property
    @abstractmethod
    def shape(self) -> PyLegendTuple[int, int]:
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
        pass  # pragma: no cover

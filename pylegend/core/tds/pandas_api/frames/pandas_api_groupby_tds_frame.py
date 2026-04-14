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
from pylegend.core.language.pandas_api.pandas_api_frame_spec import FrameSpec, RowsBetween
from pylegend.core.language.shared.primitives.primitive import PyLegendPrimitiveOrPythonPrimitive
from pylegend.core.tds.pandas_api.frames.helpers.series_helper import get_groupby_series_from_col_type
from pylegend.core.tds.pandas_api.frames.pandas_api_base_tds_frame import PandasApiBaseTdsFrame
from pylegend.core.tds.tds_column import TdsColumn

if TYPE_CHECKING:
    from pylegend.core.tds.pandas_api.frames.pandas_api_tds_frame import PandasApiTdsFrame
    from pylegend.core.language.pandas_api.pandas_api_groupby_series import GroupbySeries
    from pylegend.core.tds.pandas_api.frames.pandas_api_window_tds_frame import PandasApiWindowTdsFrame


class PandasApiGroupbyTdsFrame:
    """
    Groupby object for applying aggregation and window operations per group.

    Created by calling :meth:`PandasApiTdsFrame.groupby
    <pylegend.core.tds.pandas_api.frames.pandas_api_tds_frame.PandasApiTdsFrame.groupby>`.
    Supports column selection via bracket notation before calling an
    aggregation method, mirroring the pandas
    ``frame.groupby(...)["col"].agg(...)`` pattern.

    The groupby columns also serve as the ``PARTITION BY`` clause when
    OLAP window functions such as ``rank`` are applied.

    See Also
    --------
    PandasApiTdsFrame.groupby : Create this object from a TDS frame.
    """
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
        """
        Aggregate each group using one or more operations.

        Apply aggregation function(s) to each group defined by the
        preceding ``groupby`` call. The grouping columns always appear
        in the result alongside the aggregated values. When
        ``sort=True`` was passed to ``groupby`` (the default), the
        result is sorted by the grouping columns.

        Parameters
        ----------
        func : str, callable, np.ufunc, list, or dict
            Aggregation specification. Accepted forms:

            - ``str`` : A named aggregation (e.g. ``'sum'``) applied to
              all non-grouping columns (or selected columns if bracket
              notation was used after ``groupby``).
            - ``callable`` : A function that receives a column Series
              proxy and returns an aggregated value
              (e.g. ``lambda x: x.sum()``).
            - ``np.ufunc`` : A NumPy universal function (e.g.
              ``np.sum``).
            - ``list`` : A list of the above, producing one output
              column per function per input column.
            - ``dict`` : A mapping of column name → aggregation(s).
              Only the specified columns appear in the result.
        axis : {{0, 'index'}}, default 0
            Only ``0`` / ``'index'`` is supported.
        *args
            Not supported.
        **kwargs
            Not supported.

        Returns
        -------
        PandasApiTdsFrame
            A new TDS frame with one row per group and the aggregated
            columns.

        Raises
        ------
        TypeError
            If ``func`` is not a valid aggregation specification.
        KeyError
            If a column name in a dict-based ``func`` does not exist in
            the frame.
        NotImplementedError
            If ``axis`` is not ``0`` / ``'index'``, or if extra
            ``*args`` / ``**kwargs`` are passed.

        See Also
        --------
        agg : Alias for aggregate.
        PandasApiTdsFrame.aggregate : Frame-level aggregation (no grouping).

        Notes
        -----
        **Differences from pandas:**

        - The result always contains the grouping columns as regular
          columns (never as the index), because ``as_index`` is always
          ``False``.
        - Extra ``*args`` / ``**kwargs`` are **not forwarded** to the
          aggregation function.

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            # Dict-based aggregation on groups
            frame.groupby("Ship Name").aggregate(
                {"Order Id": "count"}
            ).head(5).to_pandas()

            # Multiple aggregations per column
            frame.groupby("Ship Name").aggregate(
                {"Order Id": ["min", "max"]}
            ).head(5).to_pandas()

            # Broadcast a single function to all non-grouping columns
            frame.groupby("Ship Name", sort=False).aggregate("count").head(5).to_pandas()

        """
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
        """
        Aggregate each group using one or more operations.

        Alias for :meth:`aggregate`. See ``aggregate`` for full
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
        engine: PyLegendOptional[str] = None,
        engine_kwargs: PyLegendOptional[PyLegendDict[str, bool]] = None,
    ) -> "PandasApiTdsFrame":
        """
        Compute the sum of values within each group.

        Convenience method equivalent to ``aggregate('sum')`` on the
        groupby object.

        Parameters
        ----------
        numeric_only : bool, default False
            Must be ``False``. ``True`` is not supported.
        min_count : int, default 0
            Must be ``0``. Non-zero values are not supported.
        engine : None
            Not supported. Must be ``None``.
        engine_kwargs : None
            Not supported. Must be ``None``.

        Returns
        -------
        PandasApiTdsFrame
            A new TDS frame with one row per group.

        Raises
        ------
        NotImplementedError
            If any parameter is set to an unsupported value.

        See Also
        --------
        aggregate : General grouped aggregation.
        PandasApiTdsFrame.sum : Frame-level sum (no grouping).

        Notes
        -----
        **Differences from the frame-level** :meth:`PandasApiTdsFrame.sum`:

        - No ``axis``, ``skipna``, or ``**kwargs`` parameters. The
          groupby convenience methods follow the pandas
          ``DataFrameGroupBy`` signature, which omits these.

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            frame.groupby("Ship Name")["Order Id"].sum().head(5).to_pandas()

        """
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
        """
        Compute the mean of values within each group.

        Convenience method equivalent to ``aggregate('mean')`` on the
        groupby object.

        Parameters
        ----------
        numeric_only : bool, default False
            Must be ``False``. ``True`` is not supported.
        engine : None
            Not supported. Must be ``None``.
        engine_kwargs : None
            Not supported. Must be ``None``.

        Returns
        -------
        PandasApiTdsFrame
            A new TDS frame with one row per group.

        Raises
        ------
        NotImplementedError
            If any parameter is set to an unsupported value.

        See Also
        --------
        aggregate : General grouped aggregation.
        PandasApiTdsFrame.mean : Frame-level mean (no grouping).

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            frame.groupby("Ship Name")["Order Id"].mean().head(5).to_pandas()

        """
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
        """
        Compute the minimum value within each group.

        Convenience method equivalent to ``aggregate('min')`` on the
        groupby object. For string columns, returns the
        lexicographically smallest value per group.

        Parameters
        ----------
        numeric_only : bool, default False
            Must be ``False``. ``True`` is not supported.
        min_count : int, default -1
            Must be ``-1``. Other values are not supported.
        engine : None
            Not supported. Must be ``None``.
        engine_kwargs : None
            Not supported. Must be ``None``.

        Returns
        -------
        PandasApiTdsFrame
            A new TDS frame with one row per group.

        Raises
        ------
        NotImplementedError
            If any parameter is set to an unsupported value.

        See Also
        --------
        max : Compute group maximums.
        aggregate : General grouped aggregation.
        PandasApiTdsFrame.min : Frame-level min (no grouping).

        Notes
        -----
        **Differences from the frame-level** :meth:`PandasApiTdsFrame.min`:

        - The ``min_count`` parameter defaults to ``-1`` (matching
          the pandas ``DataFrameGroupBy.min`` default) rather than
          being absent.

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            frame.groupby("Ship Name")["Order Id"].min().head(5).to_pandas()

        """
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
        """
        Compute the maximum value within each group.

        Convenience method equivalent to ``aggregate('max')`` on the
        groupby object. For string columns, returns the
        lexicographically largest value per group.

        Parameters
        ----------
        numeric_only : bool, default False
            Must be ``False``. ``True`` is not supported.
        min_count : int, default -1
            Must be ``-1``. Other values are not supported.
        engine : None
            Not supported. Must be ``None``.
        engine_kwargs : None
            Not supported. Must be ``None``.

        Returns
        -------
        PandasApiTdsFrame
            A new TDS frame with one row per group.

        Raises
        ------
        NotImplementedError
            If any parameter is set to an unsupported value.

        See Also
        --------
        min : Compute group minimums.
        aggregate : General grouped aggregation.
        PandasApiTdsFrame.max : Frame-level max (no grouping).

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            frame.groupby("Ship Name")["Order Id"].max().head(5).to_pandas()

        """
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
        """
        Compute the standard deviation within each group.

        Convenience method equivalent to ``aggregate('std')`` on the
        groupby object. Supports both ``ddof=1`` (sample, maps to
        ``STDDEV_SAMP``) and ``ddof=0`` (population, maps to
        ``STDDEV_POP``) at the SQL level.

        Parameters
        ----------
        ddof : int, default 1
            Degrees of freedom. ``1`` for sample standard deviation
            (``STDDEV_SAMP``), ``0`` for population standard deviation
            (``STDDEV_POP``).
        engine : None
            Not supported. Must be ``None``.
        engine_kwargs : None
            Not supported. Must be ``None``.
        numeric_only : bool, default False
            Must be ``False``. ``True`` is not supported.

        Returns
        -------
        PandasApiTdsFrame
            A new TDS frame with one row per group.

        Raises
        ------
        NotImplementedError
            If ``ddof`` is not ``0`` or ``1``, or if ``engine``,
            ``engine_kwargs``, or ``numeric_only`` are set to unsupported
            values.

        See Also
        --------
        var : Compute group variances.
        aggregate : General grouped aggregation.
        PandasApiTdsFrame.std : Frame-level std (no grouping).

        Notes
        -----
        **Differences from pandas:**

        - Only ``ddof=0`` and ``ddof=1`` are supported. Other values
          raise ``NotImplementedError``.
        - ``engine``, ``engine_kwargs``, and ``numeric_only`` are
          **not supported**.

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            frame.groupby("Ship Name")["Order Id"].std().head(5).to_pandas()

        """
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
        """
        Compute the variance within each group.

        Convenience method equivalent to ``aggregate('var')`` on the
        groupby object. Supports both ``ddof=1`` (sample, maps to
        ``VAR_SAMP``) and ``ddof=0`` (population, maps to ``VAR_POP``)
        at the SQL level.

        Parameters
        ----------
        ddof : int, default 1
            Degrees of freedom. ``1`` for sample variance
            (``VAR_SAMP``), ``0`` for population variance (``VAR_POP``).
        engine : None
            Not supported. Must be ``None``.
        engine_kwargs : None
            Not supported. Must be ``None``.
        numeric_only : bool, default False
            Must be ``False``. ``True`` is not supported.

        Returns
        -------
        PandasApiTdsFrame
            A new TDS frame with one row per group.

        Raises
        ------
        NotImplementedError
            If ``ddof`` is not ``0`` or ``1``, or if ``engine``,
            ``engine_kwargs``, or ``numeric_only`` are set to unsupported
            values.

        See Also
        --------
        std : Compute group standard deviations.
        aggregate : General grouped aggregation.
        PandasApiTdsFrame.var : Frame-level var (no grouping).

        Notes
        -----
        **Differences from pandas:**

        - Only ``ddof=0`` and ``ddof=1`` are supported. Other values
          raise ``NotImplementedError``.
        - ``engine``, ``engine_kwargs``, and ``numeric_only`` are
          **not supported**.

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            frame.groupby("Ship Name")["Order Id"].var().head(5).to_pandas()

        """
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
        """
        Count non-null values within each group.

        Convenience method equivalent to ``aggregate('count')`` on the
        groupby object. Returns the number of non-null values per column
        for each group.

        Returns
        -------
        PandasApiTdsFrame
            A new TDS frame with one row per group.

        See Also
        --------
        sum : Compute group sums.
        aggregate : General grouped aggregation.
        PandasApiTdsFrame.count : Frame-level count (no grouping).

        Notes
        -----
        **Differences from the frame-level** :meth:`PandasApiTdsFrame.count`:

        - The groupby ``count`` takes **no parameters** (no ``axis``,
          ``numeric_only``, or ``**kwargs``), matching the pandas
          ``DataFrameGroupBy.count`` signature.

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            frame.groupby("Ship Name")["Order Id"].count().head(5).to_pandas()

        """
        return self.aggregate("count", 0)

    def median(self) -> "PandasApiTdsFrame":
        """
        Compute the median of each numeric column within each group.

        Applies ``PERCENTILE_CONT(0.5)`` at the SQL level for each
        numeric column. Non-numeric columns are excluded automatically.

        Returns
        -------
        PandasApiTdsFrame
            A new TDS frame with one row per group.

        See Also
        --------
        mean : Compute group means.
        aggregate : General grouped aggregation.

        Notes
        -----
        **Differences from pandas:**

        - Only numeric columns are included; non-numeric columns are
          silently skipped.
        - The pandas ``numeric_only`` parameter is not available; the
          behaviour is always ``numeric_only=True``.

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            frame.groupby("Ship Name")["Order Id"].median().head(5).to_pandas()

        """
        numeric_func_map = self._numeric_only_func_map("median")
        return self.aggregate(numeric_func_map, 0)

    def mode(self) -> "PandasApiTdsFrame":
        """
        Compute the mode of each numeric column within each group.

        Returns the most frequently occurring value per numeric column
        within each group. Maps to ``MODE()`` at the SQL level.

        Returns
        -------
        PandasApiTdsFrame
            A new TDS frame with one row per group.

        See Also
        --------
        median : Compute group medians.
        aggregate : General grouped aggregation.

        Notes
        -----
        **Differences from pandas:**

        - Only numeric columns are included; non-numeric columns are
          silently skipped.
        - Returns a single value per group (SQL ``MODE``). Pandas may
          return multiple rows when there are ties.

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            frame.groupby("Ship Name")["Order Id"].mode().head(5).to_pandas()

        """
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
        """
        Compute the rank of values within each group.

        Rank each value within its group defined by the preceding
        ``groupby`` call. The grouping columns act as the
        ``PARTITION BY`` clause in the underlying SQL window function.
        Only the ranked (non-grouping) columns appear in the result.

        Parameters
        ----------
        method : {{'min', 'first', 'dense'}}, default 'min'
            How to rank equal values:

            - ``'min'`` : Lowest rank in the group of ties (SQL
              ``RANK()``).
            - ``'first'`` : Ranks assigned in order of appearance
              within the group (SQL ``ROW_NUMBER()``).
            - ``'dense'`` : Like ``'min'`` but ranks always increase
              by 1, no gaps (SQL ``DENSE_RANK()``).
        ascending : bool, default True
            Whether to rank in ascending order. ``False`` ranks in
            descending order.
        na_option : {{'bottom'}}, default 'bottom'
            How to rank null values. Only ``'bottom'`` is supported.
            ``'keep'`` and ``'top'`` raise ``NotImplementedError``.
        pct : bool, default False
            If ``True``, compute percentage ranks (SQL
            ``PERCENT_RANK()``). Result columns are of float type.
            Can only be used with ``method='min'``.
        axis : {{0, 'index'}}, default 0
            Only ``0`` / ``'index'`` is supported.

        Returns
        -------
        PandasApiTdsFrame
            A new TDS frame containing only the ranked columns (the
            grouping columns are **not** included in the output). Each
            column contains integer ranks (or float when
            ``pct=True``).

        Raises
        ------
        NotImplementedError
            If ``method`` is not one of ``'min'``, ``'first'``,
            ``'dense'``.
            If ``na_option`` is not ``'bottom'``.
            If ``pct=True`` with a method other than ``'min'``.
            If ``axis`` is not ``0`` or ``'index'``.

        See Also
        --------
        PandasApiTdsFrame.rank : Frame-level rank (no partitioning).
        aggregate : Grouped aggregation.

        Notes
        -----
        **Differences from pandas:**

        - The ``'average'`` and ``'max'`` ranking methods are **not
          supported**.
        - ``na_option`` only supports ``'bottom'``.
        - ``pct=True`` is only supported with ``method='min'``.
        - The result contains **only the ranked columns**, not the
          grouping columns. In pandas, ``DataFrameGroupBy.rank``
          returns a frame with the same shape as the input, preserving
          all columns. Here, grouping columns are excluded from the
          output. To preserve all columns, use bracket assignment
          with a single-column selection:
          ``frame["rank"] = frame.groupby("grp")["col"].rank()``.
        - ``numeric_only`` is not exposed in the groupby ``rank``
          signature (it is always ``False``).
        - Combining multiple rank calls in a single expression is
          **not supported**. Compute them in separate steps.

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            # Rank within groups (only ranked columns in output)
            frame.groupby("Ship Name")[["Order Id"]].rank().head(5).to_pandas()

            # Append a grouped rank column to the frame
            frame["Order Rank"] = frame.groupby(
                "Ship Name"
            )["Order Id"].rank()
            frame.head(5).to_pandas()

            # Dense rank descending within groups
            frame.groupby("Ship Name")[["Order Id"]].rank(
                method="dense", ascending=False
            ).head(5).to_pandas()

        """
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

    def cume_dist_legend_ext(
            self,
            ascending: bool = True,
    ) -> "PandasApiTdsFrame":
        """
        PyLegend extension (not present in pandas).

        Compute the cumulative distribution within each group, equivalent to
        SQL ``CUME_DIST() OVER (PARTITION BY ... ORDER BY col)`` and Pure
        ``cumulativeDistribution``.
        """
        from pylegend.core.tds.pandas_api.frames.pandas_api_applied_function_tds_frame import (
            PandasApiAppliedFunctionTdsFrame
        )
        from pylegend.core.tds.pandas_api.frames.functions.rank_function import RankFunction
        return PandasApiAppliedFunctionTdsFrame(RankFunction(
            base_frame=self,
            axis=0,
            method='cume_dist',
            numeric_only=False,
            na_option='bottom',
            ascending=ascending,
            pct=False,
        ))

    def ntile_legend_ext(
            self,
            num_buckets: int,
            ascending: bool = True,
    ) -> "PandasApiTdsFrame":
        """
        PyLegend extension (not present in pandas).

        Compute the NTILE bucket within each group, equivalent to
        SQL ``NTILE(n) OVER (PARTITION BY ... ORDER BY col)`` and Pure
        ``ntile``.
        """
        from pylegend.core.tds.pandas_api.frames.pandas_api_applied_function_tds_frame import (
            PandasApiAppliedFunctionTdsFrame
        )
        from pylegend.core.tds.pandas_api.frames.functions.rank_function import RankFunction
        return PandasApiAppliedFunctionTdsFrame(RankFunction(
            base_frame=self,
            axis=0,
            method='ntile',
            numeric_only=False,
            na_option='bottom',
            ascending=ascending,
            pct=False,
            num_buckets=num_buckets,
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
        """
        Shift values by desired number of periods within each group.

        Replace every column's values with their shifted values, computing
        the shift independently for each group. Because the underlying TDS
        is inherently unordered, this requires an explicit
        ``order_by`` parameter to define the ordering for the window
        function partitioned by the group keys.

        Parameters
        ----------
        order_by : str or sequence of str
            Column name(s) to order the frame by within each group before
            applying the shift. Unlike pandas, this is required to ensure
            deterministic output. All specified columns must be present in
            the base frame.
        periods : int or sequence of int, default 1
            Number of periods to shift. Currently, only ``1`` (shift down,
            SQL ``LAG``) and ``-1`` (shift up, SQL ``LEAD``) are supported.
            If a sequence is provided, it cannot contain duplicate values.
        freq : None
            Not supported. Must be ``None``.
        axis : {0, 'index'}, default 0
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
            A new TDS frame with the shifted columns computed per group.

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
        PandasApiTdsFrame.shift : Shift values for the entire frame.

        Notes
        -----
        **Differences from pandas:**

        - The ``order_by`` parameter is **mandatory**. In pandas, ``shift``
          relies on the implicit order of the dataframe's index. Here,
          because it translates to SQL, an explicit order must be provided.
        - ``periods`` is strictly limited to ``1`` or ``-1``. Arbitrary
          integer shifts are **not supported**.
        - ``fill_value`` is **not supported** and must remain ``None``.
        - The ``freq`` parameter is **not supported** and must be ``None``.
        - ``axis=1`` (shifting horizontally across columns) is **not
          supported**.

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            # Shift the entire frame down by 1 row within each 'Ship Name' group,
            frame.groupby("Ship Name")[["Order Date", "Shipped Date"]].shift(
                order_by="Order Date",
                periods=1
            ).head(3).to_pandas()
        """
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
            frame_spec: PyLegendOptional[FrameSpec] = RowsBetween(None, None),
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
            ``None`` means no frame clause (just PARTITION BY + ORDER BY).
        order_by:
            Column name(s) to use for ORDER BY within the window.
        ascending:
            Sort direction(s) for the ORDER BY columns.
        """
        from pylegend.core.tds.pandas_api.frames.pandas_api_window_tds_frame import PandasApiWindowTdsFrame

        if frame_spec is not None and not isinstance(frame_spec, FrameSpec):
            raise TypeError(
                f"frame_spec must be a RowsBetween or RangeBetween, got {type(frame_spec).__name__}"
            )

        return PandasApiWindowTdsFrame(
            base_frame=self,
            order_by=order_by,
            frame_spec=frame_spec,
            ascending=ascending,
        )

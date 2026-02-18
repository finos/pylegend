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

from abc import ABCMeta
from pylegend._typing import (
    PyLegendSequence,
    PyLegendTypeVar,
    PyLegendCallable,
    PyLegendUnion,
    PyLegendList,
    PyLegendTuple,
    PyLegendOptional,
)
from pylegend.core.language import PyLegendBoolean, PyLegendPrimitiveOrPythonPrimitive, PyLegendPrimitiveCollection, \
    PyLegendPrimitive
from pylegend.core.language.legendql_api.legendql_api_custom_expressions import (
    LegendQLApiPrimitive,
    LegendQLApiSortInfo,
    LegendQLApiWindow,
    LegendQLApiPartialFrame,
    LegendQLApiWindowReference,
    LegendQLApiWindowFrame,
    LegendQLApiWindowFrameMode,
    LegendQLApiWindowFrameBound,
    LegendQLApiWindowFrameBoundType,
    LegendQLApiDurationUnit
)
from pylegend.core.language.legendql_api.legendql_api_tds_row import LegendQLApiTdsRow
from pylegend.core.tds.abstract.frames.base_tds_frame import BaseTdsFrame
from pylegend.core.tds.legendql_api.frames.legendql_api_tds_frame import LegendQLApiTdsFrame
from pylegend.core.tds.tds_column import TdsColumn

__all__: PyLegendSequence[str] = [
    "LegendQLApiBaseTdsFrame"
]

R = PyLegendTypeVar('R')


class LegendQLApiBaseTdsFrame(LegendQLApiTdsFrame, BaseTdsFrame, metaclass=ABCMeta):
    def __init__(self, columns: PyLegendSequence[TdsColumn]) -> None:
        BaseTdsFrame.__init__(self, columns=columns)

    def head(self, row_count: int = 5) -> "LegendQLApiTdsFrame":
        """
        Return the first n rows of the TDS frame.

        This function returns the first `row_count` rows from the frame.
        It is useful for quickly inspecting the data without loading the entire dataset.

        Parameters
        ----------
        row_count : int, default 5
            Number of rows to return.

        Returns
        -------
        LegendQLApiTdsFrame
            A new TDS frame containing only the first n rows.

        See Also
        --------
        limit : Alias for head.
        drop : Skip the first n rows.
        slice : Return a subset of rows by index range.

        Examples
        --------
        .. ipython:: python

            import pylegend
            from legendql_api_local_tds_client import legendql_api_local_tds_client

            tds_client = legendql_api_local_tds_client()
            frame = tds_client.legend_service_frame(
                service_pattern="/allOrders",
                group_id="org.finos.legend.pylegend",
                artifact_id="pylegend-northwind-models",
                version="0.0.1-SNAPSHOT"
            )

            # View the schema to see available columns
            frame.schema()

            # Get first 5 rows (default)
            frame.head().to_pandas()

            # Get first 3 rows
            frame.head(3).to_pandas()

        """
        from pylegend.core.tds.legendql_api.frames.legendql_api_applied_function_tds_frame import (
            LegendQLApiAppliedFunctionTdsFrame
        )
        from pylegend.core.tds.legendql_api.frames.functions.legendql_api_head_function import (
            LegendQLApiHeadFunction
        )
        return LegendQLApiAppliedFunctionTdsFrame(LegendQLApiHeadFunction(self, row_count))

    def limit(self, row_count: int = 5) -> "LegendQLApiTdsFrame":
        """
        Return the first n rows of the TDS frame.

        Alias for :meth:`head`. This function returns the first `row_count` rows
        from the frame.

        Parameters
        ----------
        row_count : int, default 5
            Number of rows to return.

        Returns
        -------
        LegendQLApiTdsFrame
            A new TDS frame containing only the first n rows.

        See Also
        --------
        head : Equivalent method.
        drop : Skip the first n rows.

        Examples
        --------
        .. ipython:: python

            import pylegend
            from legendql_api_local_tds_client import legendql_api_local_tds_client

            tds_client = legendql_api_local_tds_client()
            frame = tds_client.legend_service_frame(
                service_pattern="/allOrders",
                group_id="org.finos.legend.pylegend",
                artifact_id="pylegend-northwind-models",
                version="0.0.1-SNAPSHOT"
            )

            # View the schema to see available columns
            frame.schema()

            # Limit to 2 rows
            frame.limit(2).to_pandas()

        """
        return self.head(row_count=row_count)

    def distinct(
            self,
            columns: PyLegendOptional[PyLegendUnion[
                str,
                PyLegendList[str],
                PyLegendCallable[
                    [LegendQLApiTdsRow],
                    PyLegendUnion[LegendQLApiPrimitive, PyLegendList[LegendQLApiPrimitive]]
                ]
            ]] = None) -> "LegendQLApiTdsFrame":
        """
        Return unique rows based on specified columns.

        Remove duplicate rows from the TDS frame. If columns are specified,
        uniqueness is determined based on those columns only. If no columns
        are specified, all columns are used to determine uniqueness.

        Parameters
        ----------
        columns : str, list of str, callable, or None, default None
            Column(s) to consider for identifying duplicates:

            - ``None`` : Use all columns to determine uniqueness.
            - ``str`` : Single column name.
            - ``list of str`` : List of column names.
            - ``callable`` : A function that takes a TDS row and returns
              column(s) to use for uniqueness.

        Returns
        -------
        LegendQLApiTdsFrame
            A new TDS frame with duplicate rows removed.

        See Also
        --------
        select : Select specific columns.
        filter : Filter rows based on a condition.

        Examples
        --------
        .. ipython:: python

            import pylegend
            from legendql_api_local_tds_client import legendql_api_local_tds_client

            tds_client = legendql_api_local_tds_client()
            frame = tds_client.legend_service_frame(
                service_pattern="/allOrders",
                group_id="org.finos.legend.pylegend",
                artifact_id="pylegend-northwind-models",
                version="0.0.1-SNAPSHOT"
            )

            # View the schema to see available columns
            frame.schema()

            # Get distinct values for a single column
            frame.distinct("Ship Name").head(10).to_pandas()

            # Get distinct combinations of multiple columns
            frame.distinct(["Ship Name", "Order Date"]).head(10).to_pandas()

            # Using a callable to specify columns
            frame.distinct(lambda r: [r["Ship Name"], r["Order Date"]]).head(10).to_pandas()

        """
        from pylegend.core.tds.legendql_api.frames.legendql_api_applied_function_tds_frame import (
            LegendQLApiAppliedFunctionTdsFrame
        )
        from pylegend.core.tds.legendql_api.frames.functions.legendql_api_distinct_function import (
            LegendQLApiDistinctFunction
        )
        return LegendQLApiAppliedFunctionTdsFrame(LegendQLApiDistinctFunction(self, columns))

    def select(
            self,
            columns: PyLegendUnion[
                str,
                PyLegendList[str],
                PyLegendCallable[
                    [LegendQLApiTdsRow],
                    PyLegendUnion[LegendQLApiPrimitive, PyLegendList[LegendQLApiPrimitive]]
                ]
            ]
    ) -> "LegendQLApiTdsFrame":
        """
        Select specific columns from the TDS frame.

        Return a new TDS frame containing only the specified columns.
        This is useful for reducing the data to only the columns of interest.

        Parameters
        ----------
        columns : str, list of str, or callable
            Column(s) to select:

            - ``str`` : Single column name.
            - ``list of str`` : List of column names.
            - ``callable`` : A function that takes a TDS row and returns
              the column(s) to select.

        Returns
        -------
        LegendQLApiTdsFrame
            A new TDS frame containing only the selected columns.

        See Also
        --------
        distinct : Get unique rows.
        project : Create new computed columns.
        extend : Add new columns while keeping existing ones.

        Examples
        --------
        .. ipython:: python

            import pylegend
            from legendql_api_local_tds_client import legendql_api_local_tds_client

            tds_client = legendql_api_local_tds_client()
            frame = tds_client.legend_service_frame(
                service_pattern="/allOrders",
                group_id="org.finos.legend.pylegend",
                artifact_id="pylegend-northwind-models",
                version="0.0.1-SNAPSHOT"
            )

            # View the schema to see available columns
            frame.schema()

            # Select a single column
            frame.select("Ship Name").head(5).to_pandas()

            # Select multiple columns
            frame.select(["Order Id", "Ship Name", "Order Date"]).head(5).to_pandas()

            # Using a callable to select columns
            frame.select(lambda r: [r["Order Id"], r["Ship Name"]]).head(5).to_pandas()

        """
        from pylegend.core.tds.legendql_api.frames.legendql_api_applied_function_tds_frame import (
            LegendQLApiAppliedFunctionTdsFrame
        )
        from pylegend.core.tds.legendql_api.frames.functions.legendql_api_select_function import (
            LegendQLApiSelectFunction
        )
        return LegendQLApiAppliedFunctionTdsFrame(LegendQLApiSelectFunction(self, columns))

    def drop(self, count: int = 5) -> "LegendQLApiTdsFrame":
        """
        Skip the first n rows of the TDS frame.

        Return a new TDS frame with the first `count` rows removed.
        This is useful for pagination or skipping header-like rows.

        Parameters
        ----------
        count : int, default 5
            Number of rows to skip from the beginning.

        Returns
        -------
        LegendQLApiTdsFrame
            A new TDS frame with the first n rows removed.

        See Also
        --------
        head : Return the first n rows.
        slice : Return a subset of rows by index range.

        Examples
        --------
        .. ipython:: python

            import pylegend
            from legendql_api_local_tds_client import legendql_api_local_tds_client

            tds_client = legendql_api_local_tds_client()
            frame = tds_client.legend_service_frame(
                service_pattern="/allOrders",
                group_id="org.finos.legend.pylegend",
                artifact_id="pylegend-northwind-models",
                version="0.0.1-SNAPSHOT"
            )

            # View the schema to see available columns
            frame.schema()

            # Skip first 5 rows (default), then show next 5
            frame.drop().head(5).to_pandas()

            # Skip first 10 rows
            frame.drop(10).head(5).to_pandas()

        """
        from pylegend.core.tds.legendql_api.frames.legendql_api_applied_function_tds_frame import (
            LegendQLApiAppliedFunctionTdsFrame
        )
        from pylegend.core.tds.legendql_api.frames.functions.legendql_api_drop_function import (
            LegendQLApiDropFunction
        )
        return LegendQLApiAppliedFunctionTdsFrame(LegendQLApiDropFunction(self, count))

    def slice(self, start_row: int, end_row_exclusive: int) -> "LegendQLApiTdsFrame":
        """
        Return a subset of rows by index range.

        Select rows from `start_row` (inclusive) to `end_row_exclusive` (exclusive).
        This is similar to Python's list slicing behavior.

        Parameters
        ----------
        start_row : int
            Starting row index (0-based, inclusive).
        end_row_exclusive : int
            Ending row index (0-based, exclusive).

        Returns
        -------
        LegendQLApiTdsFrame
            A new TDS frame containing only the rows in the specified range.

        See Also
        --------
        head : Return the first n rows.
        drop : Skip the first n rows.

        Examples
        --------
        .. ipython:: python

            import pylegend
            from legendql_api_local_tds_client import legendql_api_local_tds_client

            tds_client = legendql_api_local_tds_client()
            frame = tds_client.legend_service_frame(
                service_pattern="/allOrders",
                group_id="org.finos.legend.pylegend",
                artifact_id="pylegend-northwind-models",
                version="0.0.1-SNAPSHOT"
            )

            # View the schema to see available columns
            frame.schema()

            # Get rows 0-4 (first 5 rows)
            frame.slice(0, 5).to_pandas()

            # Get rows 5-9
            frame.slice(5, 10).to_pandas()

            # Get rows 2-6
            frame.slice(2, 7).to_pandas()

        """
        from pylegend.core.tds.legendql_api.frames.legendql_api_applied_function_tds_frame import (
            LegendQLApiAppliedFunctionTdsFrame
        )
        from pylegend.core.tds.legendql_api.frames.functions.legendql_api_slice_function import (
            LegendQLApiSliceFunction
        )
        return LegendQLApiAppliedFunctionTdsFrame(LegendQLApiSliceFunction(self, start_row, end_row_exclusive))

    def sort(
            self,
            sort_infos: PyLegendUnion[
                str,
                PyLegendList[str],
                PyLegendCallable[
                    [LegendQLApiTdsRow],
                    PyLegendUnion[
                        LegendQLApiPrimitive,
                        LegendQLApiSortInfo,
                        PyLegendList[PyLegendUnion[LegendQLApiPrimitive, LegendQLApiSortInfo]],
                    ]
                ]
            ]
    ) -> "LegendQLApiTdsFrame":
        """
        Sort the TDS frame by specified columns.

        Return a new TDS frame sorted by the given column(s). Supports
        ascending and descending sort orders.

        Parameters
        ----------
        sort_infos : str, list of str, or callable
            Column(s) to sort by:

            - ``str`` : Single column name (ascending order).
            - ``list of str`` : List of column names (all ascending order).
            - ``callable`` : A function that takes a TDS row and returns:
                - A column (ascending order by default).
                - A ``LegendQLApiSortInfo`` with direction (use ``.ascending()``
                  or ``.descending()`` on columns).
                - A list of columns and/or ``LegendQLApiSortInfo`` objects.

        Returns
        -------
        LegendQLApiTdsFrame
            A new TDS frame with rows sorted according to the specified criteria.

        See Also
        --------
        head : Return the first n rows.
        filter : Filter rows based on a condition.

        Examples
        --------
        .. ipython:: python

            import pylegend
            from legendql_api_local_tds_client import legendql_api_local_tds_client

            tds_client = legendql_api_local_tds_client()
            frame = tds_client.legend_service_frame(
                service_pattern="/allOrders",
                group_id="org.finos.legend.pylegend",
                artifact_id="pylegend-northwind-models",
                version="0.0.1-SNAPSHOT"
            )

            # View the schema to see available columns
            frame.schema()

            # Sort by single column (ascending by default)
            frame.sort("Ship Name").head(5).to_pandas()

            # Sort by multiple columns (all ascending)
            frame.sort(["Ship Name", "Order Date"]).head(5).to_pandas()

            # Sort with explicit direction using callable
            frame.sort(lambda r: r["Ship Name"].descending()).head(5).to_pandas()

            # Sort by multiple columns with different directions
            frame.sort(lambda r: [r["Ship Name"].ascending(), r["Order Id"].descending()]).head(5).to_pandas()

        """
        from pylegend.core.tds.legendql_api.frames.legendql_api_applied_function_tds_frame import (
            LegendQLApiAppliedFunctionTdsFrame
        )
        from pylegend.core.tds.legendql_api.frames.functions.legendql_api_sort_function import (
            LegendQLApiSortFunction
        )
        return LegendQLApiAppliedFunctionTdsFrame(LegendQLApiSortFunction(self, sort_infos))

    def concatenate(self, other: "LegendQLApiTdsFrame") -> "LegendQLApiTdsFrame":
        """
        Concatenate two TDS frames vertically.

        Append the rows of another TDS frame to the current frame.
        Both frames must have compatible schemas (same columns).

        Parameters
        ----------
        other : LegendQLApiTdsFrame
            The TDS frame to append to the current frame.

        Returns
        -------
        LegendQLApiTdsFrame
            A new TDS frame containing all rows from both frames.

        See Also
        --------
        join : Combine frames horizontally based on a condition.

        Examples
        --------
        .. ipython:: python

            import pylegend
            from legendql_api_local_tds_client import legendql_api_local_tds_client

            tds_client = legendql_api_local_tds_client()
            frame = tds_client.legend_service_frame(
                service_pattern="/allOrders",
                group_id="org.finos.legend.pylegend",
                artifact_id="pylegend-northwind-models",
                version="0.0.1-SNAPSHOT"
            )

            # View the schema to see available columns
            frame.schema()

            # Get first 3 rows and rows 10-12, then concatenate
            first_rows = frame.head(3)
            other_rows = frame.slice(10, 13)
            first_rows.concatenate(other_rows).to_pandas()

        """
        from pylegend.core.tds.legendql_api.frames.legendql_api_applied_function_tds_frame import (
            LegendQLApiAppliedFunctionTdsFrame
        )
        from pylegend.core.tds.legendql_api.frames.functions.legendql_api_concatenate_function import (
            LegendQLApiConcatenateFunction
        )
        return LegendQLApiAppliedFunctionTdsFrame(LegendQLApiConcatenateFunction(self, other))

    def filter(
            self,
            filter_function: PyLegendCallable[[LegendQLApiTdsRow], PyLegendUnion[bool, PyLegendBoolean]]
    ) -> "LegendQLApiTdsFrame":
        """
        Filter rows based on a condition.

        Return a new TDS frame containing only the rows that satisfy
        the given condition.

        Parameters
        ----------
        filter_function : callable
            A function that takes a TDS row and returns a boolean value.
            Rows where the function returns ``True`` are kept.

        Returns
        -------
        LegendQLApiTdsFrame
            A new TDS frame containing only the rows that match the condition.

        See Also
        --------
        distinct : Remove duplicate rows.
        head : Return the first n rows.

        Examples
        --------
        .. ipython:: python

            import pylegend
            from legendql_api_local_tds_client import legendql_api_local_tds_client

            tds_client = legendql_api_local_tds_client()
            frame = tds_client.legend_service_frame(
                service_pattern="/allOrders",
                group_id="org.finos.legend.pylegend",
                artifact_id="pylegend-northwind-models",
                version="0.0.1-SNAPSHOT"
            )

            # View the schema to see available columns
            frame.schema()

            # Filter by equality
            frame.filter(lambda r: r["Ship Name"] == "Wolski Zajazd").head(5).to_pandas()

            # Filter with comparison operators
            frame.filter(lambda r: r["Order Id"] > 10500).head(5).to_pandas()

            # Filter with multiple conditions (AND)
            frame.filter(lambda r: (r["Order Id"] > 10250) & (r["Order Id"] < 10560)).head(5).to_pandas()

        """
        from pylegend.core.tds.legendql_api.frames.legendql_api_applied_function_tds_frame import (
            LegendQLApiAppliedFunctionTdsFrame
        )
        from pylegend.core.tds.legendql_api.frames.functions.legendql_api_filter_function import (
            LegendQLApiFilterFunction
        )
        return LegendQLApiAppliedFunctionTdsFrame(LegendQLApiFilterFunction(self, filter_function))

    def rename(
            self,
            column_renames: PyLegendUnion[
                PyLegendTuple[str, str],
                PyLegendList[PyLegendTuple[str, str]],
                PyLegendCallable[
                    [LegendQLApiTdsRow],
                    PyLegendUnion[
                        PyLegendTuple[LegendQLApiPrimitive, str],
                        PyLegendList[PyLegendTuple[LegendQLApiPrimitive, str]]
                    ]
                ]
            ]
    ) -> "LegendQLApiTdsFrame":
        """
        Rename columns in the TDS frame.

        Return a new TDS frame with specified columns renamed.

        Parameters
        ----------
        column_renames : tuple, list of tuples, or callable
            Column rename specification:

            - ``tuple (str, str)`` : Single rename as (old_name, new_name).
            - ``list of tuples`` : Multiple renames as [(old1, new1), (old2, new2), ...].
            - ``callable`` : A function that takes a TDS row and returns
              tuple(s) of (column, new_name).

        Returns
        -------
        LegendQLApiTdsFrame
            A new TDS frame with the specified columns renamed.

        See Also
        --------
        select : Select specific columns.
        extend : Add new columns.
        project : Create new computed columns.

        Examples
        --------
        .. ipython:: python

            import pylegend
            from legendql_api_local_tds_client import legendql_api_local_tds_client

            tds_client = legendql_api_local_tds_client()
            frame = tds_client.legend_service_frame(
                service_pattern="/allOrders",
                group_id="org.finos.legend.pylegend",
                artifact_id="pylegend-northwind-models",
                version="0.0.1-SNAPSHOT"
            )

            # View the schema to see available columns
            frame.schema()

            # Rename a single column
            frame.rename(("Ship Name", "Vessel Name")).head(3).to_pandas()

            # Rename multiple columns
            frame.rename([("Ship Name", "Vessel Name"), ("Order Date", "Date Ordered")]).head(3).to_pandas()

            # Using a callable to rename columns
            frame.rename(lambda r: (r["Ship Name"], "Vessel Name")).head(3).to_pandas()

        """
        from pylegend.core.tds.legendql_api.frames.legendql_api_applied_function_tds_frame import (
            LegendQLApiAppliedFunctionTdsFrame
        )
        from pylegend.core.tds.legendql_api.frames.functions.legendql_api_rename_function import (
            LegendQLApiRenameFunction
        )
        return LegendQLApiAppliedFunctionTdsFrame(LegendQLApiRenameFunction(self, column_renames))

    def extend(
            self,
            extend_columns: PyLegendUnion[
                PyLegendTuple[
                    str,
                    PyLegendCallable[[LegendQLApiTdsRow], PyLegendPrimitiveOrPythonPrimitive]
                ],
                PyLegendTuple[
                    str,
                    PyLegendCallable[[LegendQLApiTdsRow], PyLegendPrimitiveOrPythonPrimitive],
                    PyLegendCallable[[PyLegendPrimitiveCollection], PyLegendPrimitive]
                ],
                PyLegendList[
                    PyLegendUnion[
                        PyLegendTuple[
                            str,
                            PyLegendCallable[[LegendQLApiTdsRow], PyLegendPrimitiveOrPythonPrimitive]
                        ],
                        PyLegendTuple[
                            str,
                            PyLegendCallable[[LegendQLApiTdsRow], PyLegendPrimitiveOrPythonPrimitive],
                            PyLegendCallable[[PyLegendPrimitiveCollection], PyLegendPrimitive]
                        ]
                    ]
                ]
            ]
    ) -> "LegendQLApiTdsFrame":
        """
        Add new computed columns to the TDS frame.

        Return a new TDS frame with additional columns computed from
        existing data. The original columns are preserved.

        Parameters
        ----------
        extend_columns : tuple or list of tuples
            Column extension specification:

            - ``tuple (name, func)`` : Add a column with given name using the
              function to compute values from each row.
            - ``tuple (name, func, agg_func)`` : Add a column with an aggregation
              function applied.
            - ``list of tuples`` : Add multiple columns.

        Returns
        -------
        LegendQLApiTdsFrame
            A new TDS frame with the additional computed columns.

        See Also
        --------
        project : Create new columns (replacing existing).
        rename : Rename existing columns.
        select : Select specific columns.

        Examples
        --------
        .. ipython:: python

            import pylegend
            from legendql_api_local_tds_client import legendql_api_local_tds_client

            tds_client = legendql_api_local_tds_client()
            frame = tds_client.legend_service_frame(
                service_pattern="/allOrders",
                group_id="org.finos.legend.pylegend",
                artifact_id="pylegend-northwind-models",
                version="0.0.1-SNAPSHOT"
            )

            # View the schema to see available columns
            frame.schema()

            # Add a single computed column
            frame.extend(("Ship Name Upper", lambda r: r["Ship Name"].upper())).select(["Ship Name", "Ship Name Upper"]).head(5).to_pandas()

            # Add multiple computed columns
            frame.extend([
                ("Ship Name Upper", lambda r: r["Ship Name"].upper()),
                ("Ship Name Lower", lambda r: r["Ship Name"].lower())
            ]).select(["Ship Name", "Ship Name Upper", "Ship Name Lower"]).head(5).to_pandas()

        """
        from pylegend.core.tds.legendql_api.frames.legendql_api_applied_function_tds_frame import (
            LegendQLApiAppliedFunctionTdsFrame
        )
        from pylegend.core.tds.legendql_api.frames.functions.legendql_api_extend_function import (
            LegendQLApiExtendFunction
        )
        return LegendQLApiAppliedFunctionTdsFrame(LegendQLApiExtendFunction(self, extend_columns))

    def join(
            self,
            other: "LegendQLApiTdsFrame",
            join_condition: PyLegendCallable[
                [LegendQLApiTdsRow, LegendQLApiTdsRow], PyLegendUnion[bool, PyLegendBoolean]
            ],
            join_type: str = 'LEFT_OUTER'
    ) -> "LegendQLApiTdsFrame":
        """
        Join two TDS frames based on a condition.

        Combine columns from two TDS frames based on a join condition.

        Parameters
        ----------
        other : LegendQLApiTdsFrame
            The right TDS frame to join with.
        join_condition : callable
            A function that takes two TDS rows (left, right) and returns
            a boolean indicating whether the rows should be joined.
        join_type : str, default 'LEFT_OUTER'
            Type of join to perform:

            - ``'INNER'`` : Only matching rows from both frames.
            - ``'LEFT_OUTER'`` : All rows from left, matching from right.
            - ``'RIGHT_OUTER'`` : All rows from right, matching from left.

        Returns
        -------
        LegendQLApiTdsFrame
            A new TDS frame with columns from both frames.

        See Also
        --------
        inner_join : Convenience method for inner join.
        left_join : Convenience method for left outer join.
        right_join : Convenience method for right outer join.
        as_of_join : Join based on temporal proximity.
        concatenate : Combine frames vertically.

        Examples
        --------
        .. ipython:: python

            import pylegend
            from legendql_api_local_tds_client import legendql_api_local_tds_client

            tds_client = legendql_api_local_tds_client()
            orders = tds_client.legend_service_frame(
                service_pattern="/allOrders",
                group_id="org.finos.legend.pylegend",
                artifact_id="pylegend-northwind-models",
                version="0.0.1-SNAPSHOT"
            )

            # View the schema to see available columns
            orders.schema()

            # Self-join example: join orders with a filtered version
            # Rename duplicate columns to avoid conflicts
            filtered_orders = orders.filter(lambda r: r["Order Id"] > 10300).select(["Order Id", "Ship Name"]).rename([("Order Id", "Right Order Id"), ("Ship Name", "Filtered Ship Name")])
            orders.join(filtered_orders, lambda l, r: l["Order Id"] == r["Right Order Id"], "INNER").head(5).to_pandas()

        """
        from pylegend.core.tds.legendql_api.frames.legendql_api_applied_function_tds_frame import (
            LegendQLApiAppliedFunctionTdsFrame
        )
        from pylegend.core.tds.legendql_api.frames.functions.legendql_api_join_function import (
            LegendQLApiJoinFunction
        )
        return LegendQLApiAppliedFunctionTdsFrame(LegendQLApiJoinFunction(self, other, join_condition, join_type))

    def inner_join(
            self,
            other: "LegendQLApiTdsFrame",
            join_condition: PyLegendCallable[
                [LegendQLApiTdsRow, LegendQLApiTdsRow], PyLegendUnion[bool, PyLegendBoolean]
            ]
    ) -> "LegendQLApiTdsFrame":
        """
        Perform an inner join with another TDS frame.

        Return only the rows that have matching values in both frames.

        Parameters
        ----------
        other : LegendQLApiTdsFrame
            The right TDS frame to join with.
        join_condition : callable
            A function that takes two TDS rows (left, right) and returns
            a boolean indicating whether the rows should be joined.

        Returns
        -------
        LegendQLApiTdsFrame
            A new TDS frame containing only matching rows from both frames.

        See Also
        --------
        join : General join method with configurable join type.
        left_join : Left outer join.
        right_join : Right outer join.

        Examples
        --------
        .. ipython:: python

            import pylegend
            from legendql_api_local_tds_client import legendql_api_local_tds_client

            tds_client = legendql_api_local_tds_client()
            orders = tds_client.legend_service_frame(
                service_pattern="/allOrders",
                group_id="org.finos.legend.pylegend",
                artifact_id="pylegend-northwind-models",
                version="0.0.1-SNAPSHOT"
            )

            # View the schema to see available columns
            frame.schema()

            # Inner join with filtered data
            # Rename duplicate columns to avoid conflicts
            filtered = orders.filter(lambda r: r["Order Id"] > 10300).select(["Order Id", "Ship Name"]).rename([("Order Id", "Right Order Id"), ("Ship Name", "Filtered Ship Name")])
            orders.inner_join(filtered, lambda l, r: l["Order Id"] == r["Right Order Id"]).head(5).to_pandas()

        """
        return self.join(other, join_condition, "INNER")

    def left_join(
            self,
            other: "LegendQLApiTdsFrame",
            join_condition: PyLegendCallable[
                [LegendQLApiTdsRow, LegendQLApiTdsRow], PyLegendUnion[bool, PyLegendBoolean]
            ]
    ) -> "LegendQLApiTdsFrame":
        """
        Perform a left outer join with another TDS frame.

        Return all rows from the left frame, with matching rows from the
        right frame. Non-matching rows will have null values for the
        right frame columns.

        Parameters
        ----------
        other : LegendQLApiTdsFrame
            The right TDS frame to join with.
        join_condition : callable
            A function that takes two TDS rows (left, right) and returns
            a boolean indicating whether the rows should be joined.

        Returns
        -------
        LegendQLApiTdsFrame
            A new TDS frame with all left rows and matching right rows.

        See Also
        --------
        join : General join method with configurable join type.
        inner_join : Inner join.
        right_join : Right outer join.

        Examples
        --------
        .. ipython:: python

            import pylegend
            from legendql_api_local_tds_client import legendql_api_local_tds_client

            tds_client = legendql_api_local_tds_client()
            orders = tds_client.legend_service_frame(
                service_pattern="/allOrders",
                group_id="org.finos.legend.pylegend",
                artifact_id="pylegend-northwind-models",
                version="0.0.1-SNAPSHOT"
            )

            # View the schema to see available columns
            frame.schema()

            # Left join - all orders with filtered info where available
            # Rename duplicate columns to avoid conflicts
            filtered_info = orders.filter(lambda r: r["Order Id"] > 10300).select(["Order Id", "Ship Name"]).rename([("Order Id", "Right Order Id"), ("Ship Name", "Filtered Ship Name")]).head(10)
            orders.head(15).left_join(filtered_info, lambda l, r: l["Order Id"] == r["Right Order Id"]).to_pandas()

        """
        return self.join(other, join_condition, "LEFT_OUTER")

    def right_join(
            self,
            other: "LegendQLApiTdsFrame",
            join_condition: PyLegendCallable[
                [LegendQLApiTdsRow, LegendQLApiTdsRow], PyLegendUnion[bool, PyLegendBoolean]
            ]
    ) -> "LegendQLApiTdsFrame":
        """
        Perform a right outer join with another TDS frame.

        Return all rows from the right frame, with matching rows from the
        left frame. Non-matching rows will have null values for the
        left frame columns.

        Parameters
        ----------
        other : LegendQLApiTdsFrame
            The right TDS frame to join with.
        join_condition : callable
            A function that takes two TDS rows (left, right) and returns
            a boolean indicating whether the rows should be joined.

        Returns
        -------
        LegendQLApiTdsFrame
            A new TDS frame with all right rows and matching left rows.

        See Also
        --------
        join : General join method with configurable join type.
        inner_join : Inner join.
        left_join : Left outer join.

        Examples
        --------
        .. ipython:: python

            import pylegend
            from legendql_api_local_tds_client import legendql_api_local_tds_client

            tds_client = legendql_api_local_tds_client()
            orders = tds_client.legend_service_frame(
                service_pattern="/allOrders",
                group_id="org.finos.legend.pylegend",
                artifact_id="pylegend-northwind-models",
                version="0.0.1-SNAPSHOT"
            )

            # View the schema to see available columns
            frame.schema()

            # Right join example
            # Rename duplicate columns to avoid conflicts
            filtered_info = orders.filter(lambda r: r["Order Id"] > 10300).select(["Order Id", "Ship Name"]).rename([("Order Id", "Right Order Id"), ("Ship Name", "Filtered Ship Name")]).head(10)
            orders.head(5).right_join(filtered_info, lambda l, r: l["Order Id"] == r["Right Order Id"]).to_pandas()

        """
        return self.join(other, join_condition, "RIGHT_OUTER")

    def as_of_join(
            self,
            other: "LegendQLApiTdsFrame",
            match_function: PyLegendCallable[
                [LegendQLApiTdsRow, LegendQLApiTdsRow], PyLegendUnion[bool, PyLegendBoolean]
            ],
            join_condition: PyLegendOptional[
                PyLegendCallable[[LegendQLApiTdsRow, LegendQLApiTdsRow], PyLegendUnion[bool, PyLegendBoolean]]
            ] = None
    ) -> "LegendQLApiTdsFrame":
        """
        Perform an as-of join with another TDS frame.

        Join based on temporal proximity, finding the closest matching row
        from the right frame for each row in the left frame. Useful for
        time-series data where exact matches may not exist.

        Parameters
        ----------
        other : LegendQLApiTdsFrame
            The right TDS frame to join with.
        match_function : callable
            A function that defines the temporal matching condition.
            Takes two TDS rows (left, right) and returns a boolean.
        join_condition : callable, optional
            An additional filter condition for the join.
            Takes two TDS rows (left, right) and returns a boolean.

        Returns
        -------
        LegendQLApiTdsFrame
            A new TDS frame with as-of joined data.

        See Also
        --------
        join : General join method.
        inner_join : Inner join.
        left_join : Left outer join.

        Examples
        --------
        .. ipython:: python

            import pylegend
            from legendql_api_local_tds_client import legendql_api_local_tds_client

            tds_client = legendql_api_local_tds_client()
            orders = tds_client.legend_service_frame(
                service_pattern="/allOrders",
                group_id="org.finos.legend.pylegend",
                artifact_id="pylegend-northwind-models",
                version="0.0.1-SNAPSHOT"
            )

            # View the schema to see available columns
            orders.schema()

            # As-of join: match orders with closest order ID
            # Rename duplicate columns to avoid conflicts
            reference = orders.select(["Order Id", "Ship Name"]).rename([("Order Id", "Ref Order Id"), ("Ship Name", "Ref Ship Name")]).head(10)
            orders.head(5).as_of_join(reference, lambda l, r: l["Order Id"] >= r["Ref Order Id"]).to_pandas()

        """
        from pylegend.core.tds.legendql_api.frames.legendql_api_applied_function_tds_frame import (
            LegendQLApiAppliedFunctionTdsFrame
        )
        from pylegend.core.tds.legendql_api.frames.functions.legendql_api_asofjoin_function import (
            LegendQLApiAsOfJoinFunction
        )
        return LegendQLApiAppliedFunctionTdsFrame(LegendQLApiAsOfJoinFunction(self, other, match_function, join_condition))

    def group_by(
            self,
            grouping_columns: PyLegendUnion[
                str,
                PyLegendList[str],
                PyLegendCallable[
                    [LegendQLApiTdsRow],
                    PyLegendUnion[LegendQLApiPrimitive, PyLegendList[LegendQLApiPrimitive]]
                ]
            ],
            aggregate_specifications: PyLegendUnion[
                PyLegendTuple[
                    str,
                    PyLegendCallable[[LegendQLApiTdsRow], PyLegendPrimitiveOrPythonPrimitive],
                    PyLegendCallable[[PyLegendPrimitiveCollection], PyLegendPrimitive]
                ],
                PyLegendList[
                    PyLegendTuple[
                        str,
                        PyLegendCallable[[LegendQLApiTdsRow], PyLegendPrimitiveOrPythonPrimitive],
                        PyLegendCallable[[PyLegendPrimitiveCollection], PyLegendPrimitive]
                    ]
                ]
            ]
    ) -> "LegendQLApiTdsFrame":
        """
        Group rows and apply aggregate functions.

        Group the TDS frame by the specified columns and compute
        aggregate values for each group.

        Parameters
        ----------
        grouping_columns : str, list of str, or callable
            Column(s) to group by:

            - ``str`` : Single column name.
            - ``list of str`` : Multiple column names.
            - ``callable`` : A function returning column(s) to group by.

        aggregate_specifications : tuple or list of tuples
            Aggregation specification as tuple(s) of:
            ``(result_column_name, column_selector, aggregate_function)``

            Common aggregate functions from ``pylegend.agg``:

            - ``count()`` : Count of values.
            - ``sum()`` : Sum of values.
            - ``avg()`` : Average of values.
            - ``min()`` : Minimum value.
            - ``max()`` : Maximum value.

        Returns
        -------
        LegendQLApiTdsFrame
            A new TDS frame with grouped and aggregated data.

        See Also
        --------
        distinct : Get unique rows.
        filter : Filter rows before grouping.

        Examples
        --------
        .. ipython:: python

            import pylegend
            from legendql_api_local_tds_client import legendql_api_local_tds_client

            tds_client = legendql_api_local_tds_client()
            frame = tds_client.legend_service_frame(
                service_pattern="/allOrders",
                group_id="org.finos.legend.pylegend",
                artifact_id="pylegend-northwind-models",
                version="0.0.1-SNAPSHOT"
            )

            # View the schema to see available columns
            frame.schema()

            # Group by single column with count using lambda syntax
            frame.group_by(
                lambda r: r["Ship Name"],
                ("Order Count", lambda r: r["Order Id"], lambda col: col.count())
            ).head(10).to_pandas()

            # Group by single column with multiple aggregations using lambda syntax
            frame.group_by(
                lambda r: r["Ship Name"],
                [
                    ("Order Count", lambda r: r["Order Id"], lambda col: col.count()),
                    ("Min Order Id", lambda r: r["Order Id"], lambda col: col.min()),
                    ("Max Order Id", lambda r: r["Order Id"], lambda col: col.max())
                ]
            ).head(10).to_pandas()

            # Chained group_by operations - grouping on result of previous aggregation
            frame2 = frame.group_by(
                lambda r: r["Ship Name"],
                ("Order Count", lambda r: r["Order Id"], lambda col: col.count())
            )
            frame2.group_by(
                lambda r: r["Ship Name"],
                ("Count of Counts", lambda r: r["Order Count"], lambda col: col.count())
            ).head(10).to_pandas()

        """
        from pylegend.core.tds.legendql_api.frames.legendql_api_applied_function_tds_frame import (
            LegendQLApiAppliedFunctionTdsFrame
        )
        from pylegend.core.tds.legendql_api.frames.functions.legendql_api_groupby_function import (
            LegendQLApiGroupByFunction
        )
        return LegendQLApiAppliedFunctionTdsFrame(
            LegendQLApiGroupByFunction(self, grouping_columns, aggregate_specifications)
        )

    def rows(
            self,
            start: PyLegendUnion[str, int],
            end: PyLegendUnion[str, int]) -> LegendQLApiWindowFrame:
        """
        Create a row-based window frame specification.

        Define a window frame based on row offsets relative to the current row.
        Used with :meth:`window` and :meth:`window_extend` for window functions.

        Parameters
        ----------
        start : str or int
            Start boundary of the window:

            - ``"unbounded"`` : From the first row of the partition.
            - ``0`` : Current row.
            - Negative int : That many rows before current row.
            - Positive int : That many rows after current row.

        end : str or int
            End boundary of the window:

            - ``"unbounded"`` : To the last row of the partition.
            - ``0`` : Current row.
            - Negative int : That many rows before current row.
            - Positive int : That many rows after current row.

        Returns
        -------
        LegendQLApiWindowFrame
            A window frame specification for use with window functions.

        See Also
        --------
        range : Create a range-based window frame.
        window : Create a window specification.
        window_extend : Apply window functions.

        """
        return LegendQLApiWindowFrame(
            LegendQLApiWindowFrameMode.ROWS,
            _infer_window_frame_bound(start, is_start_bound=True),
            _infer_window_frame_bound(end)
        )

    def range(
            self,
            *,
            number_start: PyLegendOptional[PyLegendUnion[str, int, float]] = None,
            number_end: PyLegendOptional[PyLegendUnion[str, int, float]] = None,
            duration_start: PyLegendOptional[PyLegendUnion[str, int, float]] = None,
            duration_start_unit: PyLegendOptional[str] = None,
            duration_end: PyLegendOptional[PyLegendUnion[str, int, float]] = None,
            duration_end_unit: PyLegendOptional[str] = None) -> LegendQLApiWindowFrame:
        """
        Create a range-based window frame specification.

        Define a window frame based on value ranges (numeric or duration)
        relative to the current row's order-by column value.
        Used with :meth:`window` and :meth:`window_extend` for window functions.

        Parameters
        ----------
        number_start : str, int, or float, optional
            Numeric start boundary. Use ``"unbounded"`` for no lower limit.
        number_end : str, int, or float, optional
            Numeric end boundary. Use ``"unbounded"`` for no upper limit.
        duration_start : str, int, or float, optional
            Duration start boundary for time-based ranges.
        duration_start_unit : str, optional
            Time unit for duration_start (e.g., ``"DAYS"``, ``"HOURS"``).
        duration_end : str, int, or float, optional
            Duration end boundary for time-based ranges.
        duration_end_unit : str, optional
            Time unit for duration_end.

        Returns
        -------
        LegendQLApiWindowFrame
            A window frame specification for use with window functions.

        Raises
        ------
        ValueError
            If neither numeric nor duration range is provided, or if both are provided.

        See Also
        --------
        rows : Create a row-based window frame.
        window : Create a window specification.
        window_extend : Apply window functions.

        """
        has_number = number_start is not None or number_end is not None
        has_duration = any([
            duration_start is not None,
            duration_end is not None,
            duration_start_unit is not None,
            duration_end_unit is not None,
        ])

        if not has_number and not has_duration:
            raise ValueError(
                "Either numeric range or duration range must be provided. "
                "Specify number_start and number_end, or duration_start and duration_end "
                "(with duration_start_unit and duration_end_unit as needed)."
            )

        if has_number and has_duration:
            raise ValueError(
                "Numeric range and duration range cannot be used together. "
                "Use either (number_start, number_end) or (duration_start, duration_end)."
                "(with duration_start_unit and duration_end_unit as needed)."
            )

        if has_number:
            if number_start is None or number_end is None:
                raise ValueError(
                    "Both number_start and number_end must be provided together."
                )

            return LegendQLApiWindowFrame(
                LegendQLApiWindowFrameMode.RANGE,
                _infer_window_frame_bound(number_start, is_start_bound=True),
                _infer_window_frame_bound(number_end),
            )

        if duration_start is None or duration_end is None:
            raise ValueError(
                "Both duration_start and duration_end must be provided."
                "(with duration_start_unit and duration_end_unit as needed).")

        def is_unbounded(value: object) -> bool:
            return isinstance(value, str) and value.lower() == "unbounded"

        if not is_unbounded(duration_start) and duration_start_unit is None:
            raise ValueError("duration_start_unit is required for bounded duration_start.")

        if not is_unbounded(duration_end) and duration_end_unit is None:
            raise ValueError("duration_end_unit is required for bounded duration_end.")

        return LegendQLApiWindowFrame(
            LegendQLApiWindowFrameMode.RANGE,
            _infer_window_frame_bound(duration_start, is_start_bound=True, duration_unit=duration_start_unit),
            _infer_window_frame_bound(duration_end, duration_unit=duration_end_unit)
        )

    def window(
            self,
            partition_by: PyLegendOptional[
                PyLegendUnion[
                    str,
                    PyLegendList[str],
                    PyLegendCallable[
                        [LegendQLApiTdsRow],
                        PyLegendUnion[LegendQLApiPrimitive, PyLegendList[LegendQLApiPrimitive]]
                    ]
                ]
            ] = None,
            order_by: PyLegendOptional[
                PyLegendUnion[
                    str,
                    PyLegendList[str],
                    PyLegendCallable[
                        [LegendQLApiTdsRow],
                        PyLegendUnion[
                            LegendQLApiPrimitive,
                            LegendQLApiSortInfo,
                            PyLegendList[PyLegendUnion[LegendQLApiPrimitive, LegendQLApiSortInfo]],
                        ]
                    ]
                ]
            ] = None,
            frame: PyLegendOptional[LegendQLApiWindowFrame] = None
    ) -> "LegendQLApiWindow":
        """
        Create a window specification for window functions.

        Define how rows are partitioned, ordered, and framed for
        window function calculations.

        Parameters
        ----------
        partition_by : str, list of str, callable, or None, optional
            Column(s) to partition the data by. Each partition is processed
            independently for window calculations.

            - ``None`` : Entire dataset is one partition.
            - ``str`` : Single column name.
            - ``list of str`` : Multiple column names.
            - ``callable`` : Function returning column(s).

        order_by : str, list of str, callable, or None, optional
            Column(s) to order rows within each partition.

            - ``None`` : No specific order.
            - ``str`` : Single column name (ascending).
            - ``list of str`` : Multiple columns (ascending).
            - ``callable`` : Function returning column(s) with optional
              ``.ascending()`` or ``.descending()`` direction.

        frame : LegendQLApiWindowFrame, optional
            Window frame specification created by :meth:`rows` or :meth:`range`.
            Defines which rows relative to the current row are included
            in the calculation.

        Returns
        -------
        LegendQLApiWindow
            A window specification for use with :meth:`window_extend`.

        See Also
        --------
        rows : Create a row-based window frame.
        range : Create a range-based window frame.
        window_extend : Apply window functions using the window specification.

        """
        from pylegend.core.tds.legendql_api.frames.functions.legendql_api_function_helpers import (
            infer_columns_from_frame,
            infer_sorts_from_frame,
        )
        return LegendQLApiWindow(
            partition_by=(
                None if partition_by is None else
                infer_columns_from_frame(self, partition_by, "'window' function partition_by")
            ),
            order_by=(
                None if order_by is None else
                infer_sorts_from_frame(self, order_by, "'window' function order_by")
            ),
            frame=frame
        )

    def window_extend(
            self,
            window: LegendQLApiWindow,
            extend_columns: PyLegendUnion[
                PyLegendTuple[
                    str,
                    PyLegendCallable[
                        [LegendQLApiPartialFrame, LegendQLApiWindowReference, LegendQLApiTdsRow],
                        PyLegendPrimitiveOrPythonPrimitive
                    ]
                ],
                PyLegendTuple[
                    str,
                    PyLegendCallable[
                        [LegendQLApiPartialFrame, LegendQLApiWindowReference, LegendQLApiTdsRow],
                        PyLegendPrimitiveOrPythonPrimitive
                    ],
                    PyLegendCallable[[PyLegendPrimitiveCollection], PyLegendPrimitive]
                ],
                PyLegendList[
                    PyLegendUnion[
                        PyLegendTuple[
                            str,
                            PyLegendCallable[
                                [LegendQLApiPartialFrame, LegendQLApiWindowReference, LegendQLApiTdsRow],
                                PyLegendPrimitiveOrPythonPrimitive
                            ]
                        ],
                        PyLegendTuple[
                            str,
                            PyLegendCallable[
                                [LegendQLApiPartialFrame, LegendQLApiWindowReference, LegendQLApiTdsRow],
                                PyLegendPrimitiveOrPythonPrimitive
                            ],
                            PyLegendCallable[[PyLegendPrimitiveCollection], PyLegendPrimitive]
                        ]
                    ]
                ]
            ]
    ) -> "LegendQLApiTdsFrame":
        """
        Add columns computed using window functions.

        Extend the TDS frame with new columns calculated using window
        functions over the specified window.

        Parameters
        ----------
        window : LegendQLApiWindow
            Window specification created by :meth:`window`.
        extend_columns : tuple or list of tuples
            Column specification(s) as:

            - ``tuple (name, func)`` : Column name and function that receives
              (partial_frame, window_ref, row) and returns a value.
            - ``tuple (name, func, agg_func)`` : With aggregation function.
            - ``list of tuples`` : Multiple window columns.

            The function parameters:

            - ``partial_frame`` : Access to partitioned frame data.
            - ``window_ref`` : Window reference for functions like ``row_number()``,
              ``rank()``, ``lead()``, ``lag()``.
            - ``row`` : Current row data.

        Returns
        -------
        LegendQLApiTdsFrame
            A new TDS frame with additional window-computed columns.

        See Also
        --------
        window : Create a window specification.
        rows : Create a row-based window frame.
        range : Create a range-based window frame.
        extend : Add computed columns without window functions.

        Examples
        --------
        .. ipython:: python

            import pylegend
            from legendql_api_local_tds_client import legendql_api_local_tds_client

            tds_client = legendql_api_local_tds_client()
            frame = tds_client.legend_service_frame(
                service_pattern="/allOrders",
                group_id="org.finos.legend.pylegend",
                artifact_id="pylegend-northwind-models",
                version="0.0.1-SNAPSHOT"
            )

            # View the schema to see available columns
            frame.schema()

            # Add a single window function column
            win = frame.window(partition_by="Ship Name", order_by="Order Id")
            frame.window_extend(
                win,
                ("RowNumber", lambda p, w, r: p.row_number(r))
            ).select(["Ship Name", "Order Id", "RowNumber"]).head(5).to_pandas()

            # Add basic window function columns
            frame.window_extend(
                frame.window(partition_by="Ship Name", order_by="Order Id"),
                [
                    ("RowNumber", lambda p, w, r: p.row_number(r)),
                    ("Rank", lambda p, w, r: p.rank(w, r))
                ]
            ).select(["Ship Name", "Order Id", "RowNumber", "Rank"]).head(5).to_pandas()

        """
        from pylegend.core.tds.legendql_api.frames.legendql_api_applied_function_tds_frame import (
            LegendQLApiAppliedFunctionTdsFrame
        )
        from pylegend.core.tds.legendql_api.frames.functions.legendql_api_window_extend_function import (
            LegendQLApiWindowExtendFunction
        )
        return LegendQLApiAppliedFunctionTdsFrame(LegendQLApiWindowExtendFunction(self, window, extend_columns))

    def project(
            self,
            project_columns: PyLegendUnion[
                PyLegendTuple[
                    str,
                    PyLegendCallable[[LegendQLApiTdsRow], PyLegendPrimitiveOrPythonPrimitive]
                ],
                PyLegendList[
                    PyLegendTuple[
                        str,
                        PyLegendCallable[[LegendQLApiTdsRow], PyLegendPrimitiveOrPythonPrimitive]
                    ]
                ]
            ]
    ) -> "LegendQLApiTdsFrame":
        """
        Create a new TDS frame with specified computed columns.

        Unlike :meth:`extend`, which adds columns to existing ones,
        ``project`` creates a new frame with only the specified columns.

        Parameters
        ----------
        project_columns : tuple or list of tuples
            Column specification(s) as ``(column_name, function)``:

            - ``tuple (str, callable)`` : Single column with name and
              computation function.
            - ``list of tuples`` : Multiple columns.

            The function takes a TDS row and returns the column value.

        Returns
        -------
        LegendQLApiTdsFrame
            A new TDS frame containing only the projected columns.

        See Also
        --------
        extend : Add columns while keeping existing ones.
        select : Select existing columns.
        rename : Rename existing columns.

        Examples
        --------
        .. ipython:: python

            import pylegend
            from legendql_api_local_tds_client import legendql_api_local_tds_client

            tds_client = legendql_api_local_tds_client()
            frame = tds_client.legend_service_frame(
                service_pattern="/allOrders",
                group_id="org.finos.legend.pylegend",
                artifact_id="pylegend-northwind-models",
                version="0.0.1-SNAPSHOT"
            )

            # View the schema to see available columns
            frame.schema()

            # Project single column (copy with new name)
            frame.project(("Order Number", lambda r: r["Order Id"])).head(5).to_pandas()

            # Project multiple columns with transformations
            frame.project([
                ("Order Number", lambda r: r["Order Id"]),
                ("Vessel", lambda r: r["Ship Name"].upper()),
                ("Vessel Lower", lambda r: r["Ship Name"].lower())
            ]).head(5).to_pandas()

            # Project with existing column values
            frame.project([
                ("ID", lambda r: r["Order Id"]),
                ("Name", lambda r: r["Ship Name"]),
                ("Date", lambda r: r["Order Date"])
            ]).head(5).to_pandas()

        """
        from pylegend.core.tds.legendql_api.frames.legendql_api_applied_function_tds_frame import (
            LegendQLApiAppliedFunctionTdsFrame
        )
        from pylegend.core.tds.legendql_api.frames.functions.legendql_api_project_function import (
            LegendQLApiProjectFunction
        )
        return LegendQLApiAppliedFunctionTdsFrame(LegendQLApiProjectFunction(self, project_columns))


def _infer_window_frame_bound(
        value: PyLegendOptional[
            PyLegendUnion[str, int, float]
        ] = None,
        *,
        is_start_bound: bool = False,
        duration_unit: PyLegendOptional[str] = None,
) -> LegendQLApiWindowFrameBound:
    if isinstance(value, str):
        if value.lower() != "unbounded":
            raise ValueError(
                f"Invalid window frame boundary '{value}'. "
                "Only 'unbounded' is supported as a string. "
                "Otherwise, provide a numeric offset where "
                "positive = FOLLOWING, negative = PRECEDING, "
                "and 0 = CURRENT ROW."
            )

        bound_type = (
            LegendQLApiWindowFrameBoundType.UNBOUNDED_PRECEDING
            if is_start_bound
            else LegendQLApiWindowFrameBoundType.UNBOUNDED_FOLLOWING
        )

        return LegendQLApiWindowFrameBound(bound_type)

    if not isinstance(value, (int, float)):
        raise TypeError(
            f"Invalid type for window frame boundary: {type(value).__name__}. "
            "Expected 'unbounded' (str) or numeric offset (int | float)."
        )

    duration_unit_enum = (
        LegendQLApiDurationUnit.from_string(duration_unit)
        if duration_unit
        else None
    )

    if value == 0:
        return LegendQLApiWindowFrameBound(
            LegendQLApiWindowFrameBoundType.CURRENT_ROW,
            row_offset=None,
            duration_unit=duration_unit_enum
        )

    if value > 0:
        bound_type = LegendQLApiWindowFrameBoundType.FOLLOWING
    else:
        bound_type = LegendQLApiWindowFrameBoundType.PRECEDING

    return LegendQLApiWindowFrameBound(bound_type, value, duration_unit_enum)

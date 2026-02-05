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
        from pylegend.core.tds.legendql_api.frames.legendql_api_applied_function_tds_frame import (
            LegendQLApiAppliedFunctionTdsFrame
        )
        from pylegend.core.tds.legendql_api.frames.functions.legendql_api_head_function import (
            LegendQLApiHeadFunction
        )
        return LegendQLApiAppliedFunctionTdsFrame(LegendQLApiHeadFunction(self, row_count))

    def limit(self, row_count: int = 5) -> "LegendQLApiTdsFrame":
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
        from pylegend.core.tds.legendql_api.frames.legendql_api_applied_function_tds_frame import (
            LegendQLApiAppliedFunctionTdsFrame
        )
        from pylegend.core.tds.legendql_api.frames.functions.legendql_api_select_function import (
            LegendQLApiSelectFunction
        )
        return LegendQLApiAppliedFunctionTdsFrame(LegendQLApiSelectFunction(self, columns))

    def drop(self, count: int = 5) -> "LegendQLApiTdsFrame":
        from pylegend.core.tds.legendql_api.frames.legendql_api_applied_function_tds_frame import (
            LegendQLApiAppliedFunctionTdsFrame
        )
        from pylegend.core.tds.legendql_api.frames.functions.legendql_api_drop_function import (
            LegendQLApiDropFunction
        )
        return LegendQLApiAppliedFunctionTdsFrame(LegendQLApiDropFunction(self, count))

    def slice(self, start_row: int, end_row_exclusive: int) -> "LegendQLApiTdsFrame":
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
            Sort the TDS frame based on the given columns.

            Parameters
            ----------
            sort_infos: Union[str, List[str], Callable[[LegendQLApiTdsRow],
             Union[LegendQLApiPrimitive, LegendQLApiSortInfo, List[Union[LegendQLApiPrimitive, LegendQLApiSortInfo]]]]]
                A string or list of strings representing column names to sort by in ascending order.
                Alternatively, a callable that takes a TDS row and returns a column,
                 a SortInfo object (for specifying direction),
                or a list of columns/SortInfo objects to sort by.

            Returns
            -------
            LegendQLApiTdsFrame
                A new TDS frame with the sort operation applied.

            Examples
            --------
            .. ipython:: python

               import pylegend

               tds_client = pylegend.legendql_api_local_tds_client()  # for local testing
               frame = tds_client.legend_service_frame(
                   service_pattern="/allOrders",
                   group_id="org.finos.legend.pylegend",
                   artifact_id="pylegend-northwind-models",
                   version="0.0.1-SNAPSHOT"
               )

               frame.schema()
               frame = frame.sort(lambda r: [r["Ship Name"].descending(), r["Order Id"].ascending()])
               frame.to_pandas_df()

        """
        from pylegend.core.tds.legendql_api.frames.legendql_api_applied_function_tds_frame import (
            LegendQLApiAppliedFunctionTdsFrame
        )
        from pylegend.core.tds.legendql_api.frames.functions.legendql_api_sort_function import (
            LegendQLApiSortFunction
        )
        return LegendQLApiAppliedFunctionTdsFrame(LegendQLApiSortFunction(self, sort_infos))

    def concatenate(self, other: "LegendQLApiTdsFrame") -> "LegendQLApiTdsFrame":
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
        return self.join(other, join_condition, "INNER")

    def left_join(
            self,
            other: "LegendQLApiTdsFrame",
            join_condition: PyLegendCallable[
                [LegendQLApiTdsRow, LegendQLApiTdsRow], PyLegendUnion[bool, PyLegendBoolean]
            ]
    ) -> "LegendQLApiTdsFrame":
        return self.join(other, join_condition, "LEFT_OUTER")

    def right_join(
            self,
            other: "LegendQLApiTdsFrame",
            join_condition: PyLegendCallable[
                [LegendQLApiTdsRow, LegendQLApiTdsRow], PyLegendUnion[bool, PyLegendBoolean]
            ]
    ) -> "LegendQLApiTdsFrame":
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

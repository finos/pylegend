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

    def distinct(self) -> "LegendQLApiTdsFrame":
        from pylegend.core.tds.legendql_api.frames.legendql_api_applied_function_tds_frame import (
            LegendQLApiAppliedFunctionTdsFrame
        )
        from pylegend.core.tds.legendql_api.frames.functions.legendql_api_distinct_function import (
            LegendQLApiDistinctFunction
        )
        return LegendQLApiAppliedFunctionTdsFrame(LegendQLApiDistinctFunction(self))

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
            ] = None
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
            frame=None
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

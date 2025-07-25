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

from abc import ABCMeta

from pylegend._typing import (
    PyLegendSequence,
    PyLegendTypeVar,
    PyLegendList,
    PyLegendOptional,
    PyLegendCallable,
    PyLegendUnion,
)
from pylegend.core.language import (
    LegacyApiTdsRow,
    PyLegendBoolean,
    PyLegendPrimitiveOrPythonPrimitive,
    LegacyApiAggregateSpecification,
)
from pylegend.core.tds.abstract.frames.base_tds_frame import BaseTdsFrame
from pylegend.core.tds.legacy_api.frames.legacy_api_tds_frame import LegacyApiTdsFrame
from pylegend.core.tds.tds_column import TdsColumn

__all__: PyLegendSequence[str] = [
    "LegacyApiBaseTdsFrame"
]

R = PyLegendTypeVar('R')


class LegacyApiBaseTdsFrame(LegacyApiTdsFrame, BaseTdsFrame, metaclass=ABCMeta):

    def __init__(self, columns: PyLegendSequence[TdsColumn]) -> None:
        BaseTdsFrame.__init__(self, columns=columns)

    def head(self, row_count: int = 5) -> "LegacyApiTdsFrame":
        from pylegend.core.tds.legacy_api.frames.legacy_api_applied_function_tds_frame import (
            LegacyApiAppliedFunctionTdsFrame
        )
        from pylegend.core.tds.legacy_api.frames.functions.legacy_api_head_function import (
            LegacyApiHeadFunction
        )
        return LegacyApiAppliedFunctionTdsFrame(LegacyApiHeadFunction(self, row_count))

    def take(self, row_count: int = 5) -> "LegacyApiTdsFrame":
        return self.head(row_count=row_count)

    def limit(self, row_count: int = 5) -> "LegacyApiTdsFrame":
        return self.head(row_count=row_count)

    def drop(self, row_count: int = 5) -> "LegacyApiTdsFrame":
        from pylegend.core.tds.legacy_api.frames.legacy_api_applied_function_tds_frame import (
            LegacyApiAppliedFunctionTdsFrame
        )
        from pylegend.core.tds.legacy_api.frames.functions.legacy_api_drop_function import (
            LegacyApiDropFunction
        )
        return LegacyApiAppliedFunctionTdsFrame(LegacyApiDropFunction(self, row_count))

    def slice(self, start_row: int, end_row_exclusive: int) -> "LegacyApiTdsFrame":
        from pylegend.core.tds.legacy_api.frames.legacy_api_applied_function_tds_frame import (
            LegacyApiAppliedFunctionTdsFrame
        )
        from pylegend.core.tds.legacy_api.frames.functions.legacy_api_slice_function import (
            LegacyApiSliceFunction
        )
        return LegacyApiAppliedFunctionTdsFrame(LegacyApiSliceFunction(self, start_row, end_row_exclusive))

    def distinct(self) -> "LegacyApiTdsFrame":
        from pylegend.core.tds.legacy_api.frames.legacy_api_applied_function_tds_frame import (
            LegacyApiAppliedFunctionTdsFrame
        )
        from pylegend.core.tds.legacy_api.frames.functions.legacy_api_distinct_function import (
            LegacyApiDistinctFunction
        )
        return LegacyApiAppliedFunctionTdsFrame(LegacyApiDistinctFunction(self))

    def restrict(self, column_name_list: PyLegendList[str]) -> "LegacyApiTdsFrame":
        from pylegend.core.tds.legacy_api.frames.legacy_api_applied_function_tds_frame import (
            LegacyApiAppliedFunctionTdsFrame
        )
        from pylegend.core.tds.legacy_api.frames.functions.legacy_api_restrict_function import (
            LegacyApiRestrictFunction
        )
        return LegacyApiAppliedFunctionTdsFrame(LegacyApiRestrictFunction(self, column_name_list))

    def sort(
            self,
            column_name_list: PyLegendList[str],
            direction_list: PyLegendOptional[PyLegendList[str]] = None
    ) -> "LegacyApiTdsFrame":
        from pylegend.core.tds.legacy_api.frames.legacy_api_applied_function_tds_frame import (
            LegacyApiAppliedFunctionTdsFrame
        )
        from pylegend.core.tds.legacy_api.frames.functions.legacy_api_sort_function import (
            LegacyApiSortFunction
        )
        return LegacyApiAppliedFunctionTdsFrame(LegacyApiSortFunction(self, column_name_list, direction_list))

    def concatenate(self, other: "LegacyApiTdsFrame") -> "LegacyApiTdsFrame":
        from pylegend.core.tds.legacy_api.frames.legacy_api_applied_function_tds_frame import (
            LegacyApiAppliedFunctionTdsFrame
        )
        from pylegend.core.tds.legacy_api.frames.functions.legacy_api_concatenate_function import (
            LegacyApiConcatenateFunction
        )
        return LegacyApiAppliedFunctionTdsFrame(LegacyApiConcatenateFunction(self, other))

    def rename_columns(
            self,
            column_names: PyLegendList[str],
            renamed_column_names: PyLegendList[str]
    ) -> "LegacyApiTdsFrame":
        from pylegend.core.tds.legacy_api.frames.legacy_api_applied_function_tds_frame import (
            LegacyApiAppliedFunctionTdsFrame
        )
        from pylegend.core.tds.legacy_api.frames.functions.legacy_api_rename_columns_function import (
            LegacyApiRenameColumnsFunction
        )
        return LegacyApiAppliedFunctionTdsFrame(LegacyApiRenameColumnsFunction(self, column_names, renamed_column_names))

    def filter(
            self,
            filter_function: PyLegendCallable[[LegacyApiTdsRow], PyLegendUnion[bool, PyLegendBoolean]]
    ) -> "LegacyApiTdsFrame":
        from pylegend.core.tds.legacy_api.frames.legacy_api_applied_function_tds_frame import (
            LegacyApiAppliedFunctionTdsFrame
        )
        from pylegend.core.tds.legacy_api.frames.functions.legacy_api_filter_function import (
            LegacyApiFilterFunction
        )
        return LegacyApiAppliedFunctionTdsFrame(LegacyApiFilterFunction(self, filter_function))

    def extend(
            self,
            functions_list: PyLegendList[PyLegendCallable[[LegacyApiTdsRow], PyLegendPrimitiveOrPythonPrimitive]],
            column_names_list: PyLegendList[str]
    ) -> "LegacyApiTdsFrame":
        from pylegend.core.tds.legacy_api.frames.legacy_api_applied_function_tds_frame import (
            LegacyApiAppliedFunctionTdsFrame
        )
        from pylegend.core.tds.legacy_api.frames.functions.legacy_api_extend_function import (
            LegacyApiExtendFunction
        )
        return LegacyApiAppliedFunctionTdsFrame(LegacyApiExtendFunction(self, functions_list, column_names_list))

    def join(
            self,
            other: "LegacyApiTdsFrame",
            join_condition: PyLegendCallable[[LegacyApiTdsRow, LegacyApiTdsRow], PyLegendUnion[bool, PyLegendBoolean]],
            join_type: str = 'LEFT_OUTER'
    ) -> "LegacyApiTdsFrame":
        from pylegend.core.tds.legacy_api.frames.legacy_api_applied_function_tds_frame import (
            LegacyApiAppliedFunctionTdsFrame
        )
        from pylegend.core.tds.legacy_api.frames.functions.legacy_api_join_function import (
            LegacyApiJoinFunction
        )
        return LegacyApiAppliedFunctionTdsFrame(
            LegacyApiJoinFunction(self, other, join_condition, join_type)
        )

    def join_by_columns(
            self,
            other: "LegacyApiTdsFrame",
            self_columns: PyLegendList[str],
            other_columns: PyLegendList[str],
            join_type: str = 'LEFT_OUTER'
    ) -> "LegacyApiTdsFrame":
        from pylegend.core.tds.legacy_api.frames.legacy_api_applied_function_tds_frame import (
            LegacyApiAppliedFunctionTdsFrame
        )
        from pylegend.core.tds.legacy_api.frames.functions.legacy_api_join_by_columns_function import (
            LegacyApiJoinByColumnsFunction
        )
        return LegacyApiAppliedFunctionTdsFrame(
            LegacyApiJoinByColumnsFunction(self, other, self_columns, other_columns, join_type)
        )

    def group_by(
            self,
            grouping_columns: PyLegendList[str],
            aggregations: PyLegendList[LegacyApiAggregateSpecification],
    ) -> "LegacyApiTdsFrame":
        from pylegend.core.tds.legacy_api.frames.legacy_api_applied_function_tds_frame import (
            LegacyApiAppliedFunctionTdsFrame
        )
        from pylegend.core.tds.legacy_api.frames.functions.legacy_api_group_by_function import (
            LegacyApiGroupByFunction
        )
        return LegacyApiAppliedFunctionTdsFrame(
            LegacyApiGroupByFunction(self, grouping_columns, aggregations)
        )

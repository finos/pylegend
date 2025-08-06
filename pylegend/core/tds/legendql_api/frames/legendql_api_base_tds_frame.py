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
)
from pylegend.core.language import PyLegendBoolean
from pylegend.core.language.legendql_api.legendql_api_custom_expressions import (
    LegendQLApiPrimitive,
    LegendQLApiSortInfo,
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
            columns_function: PyLegendCallable[[LegendQLApiTdsRow], PyLegendUnion[
                LegendQLApiPrimitive,
                str,
                PyLegendList[PyLegendUnion[str, LegendQLApiPrimitive]]
            ]]
    ) -> "LegendQLApiTdsFrame":
        from pylegend.core.tds.legendql_api.frames.legendql_api_applied_function_tds_frame import (
            LegendQLApiAppliedFunctionTdsFrame
        )
        from pylegend.core.tds.legendql_api.frames.functions.legendql_api_select_function import (
            LegendQLApiSelectFunction
        )
        return LegendQLApiAppliedFunctionTdsFrame(LegendQLApiSelectFunction(self, columns_function))

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
            sort_infos_function: PyLegendCallable[[LegendQLApiTdsRow], PyLegendUnion[
                LegendQLApiPrimitive,
                str,
                LegendQLApiSortInfo,
                PyLegendList[PyLegendUnion[LegendQLApiPrimitive, str, LegendQLApiSortInfo]],
            ]]
    ) -> "LegendQLApiTdsFrame":
        from pylegend.core.tds.legendql_api.frames.legendql_api_applied_function_tds_frame import (
            LegendQLApiAppliedFunctionTdsFrame
        )
        from pylegend.core.tds.legendql_api.frames.functions.legendql_api_sort_function import (
            LegendQLApiSortFunction
        )
        return LegendQLApiAppliedFunctionTdsFrame(LegendQLApiSortFunction(self, sort_infos_function))

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

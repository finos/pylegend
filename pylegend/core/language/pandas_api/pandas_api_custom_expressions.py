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

from pylegend.core.language import (
    PyLegendInteger,
    PyLegendFloat,
)
from pylegend._typing import (
    PyLegendSequence,
    TYPE_CHECKING,
)
from pylegend.core.language.shared.pylegend_custom_expressions import (
    PyLegendCustomPrimitive as PandasApiPrimitive,
    PyLegendCustomBoolean as PandasApiBoolean,
    PyLegendCustomString as PandasApiString,
    PyLegendCustomNumber as PandasApiNumber,
    PyLegendCustomInteger as PandasApiInteger,
    PyLegendCustomFloat as PandasApiFloat,
    PyLegendCustomDate as PandasApiDate,
    PyLegendCustomDateTime as PandasApiDateTime,
    PyLegendCustomStrictDate as PandasApiStrictDate,
    PyLegendSortDirection as PandasApiSortDirection,
    PyLegendSortInfo as PandasApiSortInfo,
    PyLegendDurationUnit as PandasApiDurationUnit,
    PyLegendFrameBoundType as PandasApiFrameBoundType,
    PyLegendFrameBound as PandasApiFrameBound,
    PyLegendWindowFrameMode as PandasApiWindowFrameMode,
    PyLegendWindowFrame as PandasApiWindowFrame,
    PyLegendWindow as PandasApiWindow,
    PyLegendPartialFrame,
    PyLegendWindowReference as PandasApiWindowReference,
    PyLegendRowNumberExpression as PandasApiRowNumberExpression,
    PyLegendRankExpression as PandasApiRankExpression,
    PyLegendDenseRankExpression as PandasApiDenseRankExpression,
    PyLegendPercentRankExpression as PandasApiPercentRankExpression,
)

__all__: PyLegendSequence[str] = [
    "PandasApiPrimitive",
    "PandasApiBoolean",
    "PandasApiString",
    "PandasApiNumber",
    "PandasApiInteger",
    "PandasApiFloat",
    "PandasApiDate",
    "PandasApiDateTime",
    "PandasApiStrictDate",
    "PandasApiSortInfo",
    "PandasApiSortDirection",
    "PandasApiDurationUnit",
    "PandasApiWindow",
    "PandasApiWindowReference",
    "PandasApiWindowFrame",
    "PandasApiFrameBoundType",
    "PandasApiFrameBound",
    "PandasApiWindowFrameMode",
    "PandasApiRankExpression",
    "PandasApiDenseRankExpression",
    "PandasApiRowNumberExpression",
    "PandasApiPartialFrame",
    "PandasApiPercentRankExpression",
]


class PandasApiPartialFrame(PyLegendPartialFrame):
    if TYPE_CHECKING:
        from pylegend.core.tds.pandas_api.frames.pandas_api_base_tds_frame import PandasApiBaseTdsFrame
        from pylegend.core.language.pandas_api.pandas_api_tds_row import PandasApiTdsRow

    __base_frame: "PandasApiBaseTdsFrame"

    def __init__(self, base_frame: "PandasApiBaseTdsFrame", var_name: str) -> None:
        super().__init__(var_name)
        self.__base_frame = base_frame

    def get_base_frame(self) -> "PandasApiBaseTdsFrame":
        return self.__base_frame

    def row_number(
            self,
            row: "PandasApiTdsRow"
    ) -> PyLegendInteger:
        return PyLegendInteger(PandasApiRowNumberExpression(self, row))

    def rank(
            self,
            window: "PandasApiWindowReference",
            row: "PandasApiTdsRow"
    ) -> PyLegendInteger:
        return PyLegendInteger(PandasApiRankExpression(self, window, row))

    def dense_rank(
            self,
            window: "PandasApiWindowReference",
            row: "PandasApiTdsRow"
    ) -> PyLegendInteger:
        return PyLegendInteger(PandasApiDenseRankExpression(self, window, row))

    def percent_rank(
            self,
            window: "PandasApiWindowReference",
            row: "PandasApiTdsRow"
    ) -> PyLegendFloat:
        return PyLegendFloat(PandasApiPercentRankExpression(self, window, row))

    def lead(
            self,
            row: "PandasApiTdsRow",
            num_rows_to_lead_by: int = 1
    ) -> "PandasApiTdsRow":
        from pylegend.core.language.pandas_api.pandas_api_tds_row import PandasApiLeadRow
        return PandasApiLeadRow(self, row, num_rows_to_lead_by)

    def lag(
            self,
            row: "PandasApiTdsRow",
            num_rows_to_lag_by: int = 1
    ) -> "PandasApiTdsRow":
        from pylegend.core.language.pandas_api.pandas_api_tds_row import PandasApiLagRow
        return PandasApiLagRow(self, row, num_rows_to_lag_by)

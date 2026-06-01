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

from pylegend._typing import (
    PyLegendList,
    PyLegendSequence,
    PyLegendCallable,
    PyLegendUnion,
)
from pylegend.core.language.legendql_api.legendql_api_custom_expressions import LegendQLApiPrimitive
from pylegend.core.language.legendql_api.legendql_api_tds_row import LegendQLApiTdsRow
from pylegend.core.tds.legendql_api.frames.functions.legendql_api_function_helpers import infer_columns_from_frame
from pylegend.core.tds.legendql_api.frames.legendql_api_applied_function_tds_frame import LegendQLApiAppliedFunction
from pylegend.core.tds.tds_column import TdsColumn
from pylegend.core.tds.tds_frame import FrameToPureConfig
from pylegend.core.tds.legendql_api.frames.legendql_api_base_tds_frame import LegendQLApiBaseTdsFrame
from pylegend.core.language.shared.helpers import escape_column_name


__all__: PyLegendSequence[str] = [
    "LegendQLApiSelectFunction"
]


class LegendQLApiSelectFunction(LegendQLApiAppliedFunction):
    __base_frame: LegendQLApiBaseTdsFrame
    __column_name_list: PyLegendList[str]

    @classmethod
    def name(cls) -> str:
        return "select"

    def __init__(
            self,
            base_frame: LegendQLApiBaseTdsFrame,
            columns: PyLegendUnion[
                str,
                PyLegendList[str],
                PyLegendCallable[
                    [LegendQLApiTdsRow],
                    PyLegendUnion[LegendQLApiPrimitive, PyLegendList[LegendQLApiPrimitive]]
                ]
            ]
    ) -> None:
        self.__base_frame = base_frame
        self.__column_name_list = infer_columns_from_frame(base_frame, columns, "'select' function 'columns'")

    def to_pure(self, config: FrameToPureConfig) -> str:
        escaped_columns = []
        for col_name in self.__column_name_list:
            escaped_columns.append(escape_column_name(col_name))
        return (f"{self.__base_frame.to_pure(config)}{config.separator(1)}" +
                f"->select(~[{', '.join(escaped_columns)}])")

    def base_frame(self) -> LegendQLApiBaseTdsFrame:
        return self.__base_frame

    def tds_frame_parameters(self) -> PyLegendList["LegendQLApiBaseTdsFrame"]:
        return []

    def calculate_columns(self) -> PyLegendSequence["TdsColumn"]:
        base_columns = self.__base_frame.columns()
        new_columns = []
        for c in self.__column_name_list:
            for base_col in base_columns:
                if base_col.get_name() == c:
                    new_columns.append(base_col.copy())
                    break
        return new_columns

    def validate(self) -> bool:
        base_columns = self.__base_frame.columns()
        for c in self.__column_name_list:
            found_col = False
            for base_col in base_columns:
                if base_col.get_name() == c:
                    found_col = True
                    break
            if not found_col:
                raise ValueError(
                    f"Column - '{c}' in select columns list doesn't exist in the current frame. "
                    f"Current frame columns: {[x.get_name() for x in base_columns]}"
                )
        return True

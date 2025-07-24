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

from pylegend._typing import (
    PyLegendList,
    PyLegendSequence
)
from pylegend.core.tds.legendql_api.frames.legendql_api_input_tds_frame import LegendQLApiNonExecutableInputTdsFrame
from pylegend.core.tds.tds_column import TdsColumn
from pylegend.extensions.tds.abstract.table_spec_input_frame import TableSpecInputFrameAbstract


__all__: PyLegendSequence[str] = [
    "LegendQLApiTableSpecInputFrame"
]


class LegendQLApiTableSpecInputFrame(TableSpecInputFrameAbstract, LegendQLApiNonExecutableInputTdsFrame):

    def __init__(self, table_name_parts: PyLegendList[str], columns: PyLegendSequence[TdsColumn]) -> None:
        TableSpecInputFrameAbstract.__init__(self, table_name_parts=table_name_parts)
        LegendQLApiNonExecutableInputTdsFrame.__init__(self, columns=columns)

    def __str__(self) -> str:
        return f"LegendQLApiTableSpecInputFrame({'.'.join(self.table.parts)})"

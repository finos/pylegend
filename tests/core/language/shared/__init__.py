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

from pylegend.core.language.shared.tds_row import AbstractTdsRow
from pylegend.core.tds.tds_column import TdsColumn
from pylegend.core.tds.tds_frame import FrameToPureConfig, PyLegendTdsFrame
from pylegend.extensions.tds.abstract.table_spec_input_frame import TableSpecInputFrameAbstract
from pylegend._typing import (
    PyLegendList,
    PyLegendSequence,
)


__all__: PyLegendSequence[str] = [
    "TestTableSpecInputFrame",
    "TestTdsRow",
]


class TestTableSpecInputFrame(TableSpecInputFrameAbstract):
    __columns: PyLegendSequence[TdsColumn]

    def to_pure_query(self, config: FrameToPureConfig = FrameToPureConfig()) -> str:
        return f"#Table({'.'.join(self.table.parts)})#"

    def columns(self) -> PyLegendSequence[TdsColumn]:
        return self.__columns

    def __init__(self, table_name_parts: PyLegendList[str], columns: PyLegendSequence[TdsColumn]) -> None:
        TableSpecInputFrameAbstract.__init__(self, table_name_parts=table_name_parts)
        self.__columns = columns

    def __str__(self) -> str:
        return f"TestTableSpecInputFrame({'.'.join(self.table.parts)})"


class TestTdsRow(AbstractTdsRow):
    def __init__(self, frame_name: str, frame: PyLegendTdsFrame) -> None:
        super().__init__(frame_name, frame)

    @staticmethod
    def from_tds_frame(frame_name: str, frame: PyLegendTdsFrame) -> "TestTdsRow":
        return TestTdsRow(frame_name=frame_name, frame=frame)

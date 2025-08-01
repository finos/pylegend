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
    PyLegendSequence,
)
from pylegend.core.language.shared.tds_row import AbstractTdsRow
from pylegend.core.tds.tds_frame import PyLegendTdsFrame
from pylegend.core.language import PyLegendPrimitive

__all__: PyLegendSequence[str] = [
    "LegendQLApiTdsRow",
]


class LegendQLApiTdsRow(AbstractTdsRow):

    def __init__(self, frame_name: str, frame: PyLegendTdsFrame) -> None:
        super().__init__(frame_name, frame)

    @staticmethod
    def from_tds_frame(frame_name: str, frame: PyLegendTdsFrame) -> "LegendQLApiTdsRow":
        return LegendQLApiTdsRow(frame_name=frame_name, frame=frame)

    def __getattr__(self, key: str) -> PyLegendPrimitive:
        return self[key]

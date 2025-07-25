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
    PyLegendSequence
)
from pylegend.core.request.legend_client import LegendClient
from pylegend.core.tds.abstract.frames.input_tds_frame import (
    InputTdsFrame,
    ExecutableInputTdsFrame,
    NonExecutableInputTdsFrame,
)
from pylegend.core.tds.legendql_api.frames.legendql_api_base_tds_frame import LegendQLApiBaseTdsFrame
from pylegend.core.tds.tds_column import TdsColumn

__all__: PyLegendSequence[str] = [
    "LegendQLApiExecutableInputTdsFrame",
    "LegendQLApiNonExecutableInputTdsFrame",
    "LegendQLApiInputTdsFrame"
]


class LegendQLApiInputTdsFrame(LegendQLApiBaseTdsFrame, InputTdsFrame, metaclass=ABCMeta):
    def __init__(self, columns: PyLegendSequence[TdsColumn]) -> None:
        LegendQLApiBaseTdsFrame.__init__(self, columns=columns)
        InputTdsFrame.__init__(self, columns=columns)


class LegendQLApiExecutableInputTdsFrame(LegendQLApiInputTdsFrame, ExecutableInputTdsFrame, metaclass=ABCMeta):
    def __init__(self, legend_client: LegendClient, columns: PyLegendSequence[TdsColumn]) -> None:
        LegendQLApiInputTdsFrame.__init__(self, columns=columns)
        ExecutableInputTdsFrame.__init__(self, legend_client=legend_client, columns=columns)


class LegendQLApiNonExecutableInputTdsFrame(LegendQLApiInputTdsFrame, NonExecutableInputTdsFrame, metaclass=ABCMeta):
    def __init__(self, columns: PyLegendSequence[TdsColumn]) -> None:
        LegendQLApiInputTdsFrame.__init__(self, columns=columns)
        NonExecutableInputTdsFrame.__init__(self, columns=columns)

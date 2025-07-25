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
)
from pylegend.core.tds.abstract.frames.input_tds_frame import (
    InputTdsFrame,
    ExecutableInputTdsFrame,
    NonExecutableInputTdsFrame,
)
from pylegend.core.tds.tds_column import TdsColumn
from pylegend.core.tds.legacy_api.frames.legacy_api_base_tds_frame import LegacyApiBaseTdsFrame
from pylegend.core.request.legend_client import LegendClient


__all__: PyLegendSequence[str] = [
    "LegacyApiExecutableInputTdsFrame",
    "LegacyApiNonExecutableInputTdsFrame",
    "LegacyApiInputTdsFrame"
]


class LegacyApiInputTdsFrame(LegacyApiBaseTdsFrame, InputTdsFrame, metaclass=ABCMeta):
    def __init__(self, columns: PyLegendSequence[TdsColumn]) -> None:
        LegacyApiBaseTdsFrame.__init__(self, columns=columns)
        InputTdsFrame.__init__(self, columns=columns)


class LegacyApiExecutableInputTdsFrame(LegacyApiInputTdsFrame, ExecutableInputTdsFrame, metaclass=ABCMeta):
    def __init__(self, legend_client: LegendClient, columns: PyLegendSequence[TdsColumn]) -> None:
        LegacyApiInputTdsFrame.__init__(self, columns=columns)
        ExecutableInputTdsFrame.__init__(self, legend_client=legend_client, columns=columns)


class LegacyApiNonExecutableInputTdsFrame(LegacyApiInputTdsFrame, NonExecutableInputTdsFrame, metaclass=ABCMeta):
    def __init__(self, columns: PyLegendSequence[TdsColumn]) -> None:
        LegacyApiInputTdsFrame.__init__(self, columns=columns)
        NonExecutableInputTdsFrame.__init__(self, columns=columns)

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
    PyLegendList
)
from pylegend.core.tds.tds_column import TdsColumn
from pylegend.core.tds.pandas_api.frames.pandas_api_base_tds_frame import PandasApiBaseTdsFrame
from pylegend.core.request.legend_client import LegendClient


__all__: PyLegendSequence[str] = [
    "PandasApiExecutableInputTdsFrame",
    "PandasApiNonExecutableInputTdsFrame",
    "PandasApiInputTdsFrame",
]


class PandasApiInputTdsFrame(PandasApiBaseTdsFrame, metaclass=ABCMeta):

    def __init__(self, columns: PyLegendSequence[TdsColumn]) -> None:
        super().__init__(columns=columns)

    def get_all_tds_frames(self) -> PyLegendList["PandasApiBaseTdsFrame"]:
        return [self]


class PandasApiExecutableInputTdsFrame(PandasApiInputTdsFrame, metaclass=ABCMeta):
    __legend_client: LegendClient

    def __init__(self, legend_client: LegendClient, columns: PyLegendSequence[TdsColumn]) -> None:
        super().__init__(columns=columns)
        self.__legend_client = legend_client

    def get_legend_client(self) -> LegendClient:
        return self.__legend_client


class PandasApiNonExecutableInputTdsFrame(PandasApiInputTdsFrame, metaclass=ABCMeta):

    def __init__(self, columns: PyLegendSequence[TdsColumn]) -> None:
        super().__init__(columns=columns)

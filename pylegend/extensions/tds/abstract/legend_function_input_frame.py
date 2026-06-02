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
from pylegend.core.tds.tds_frame import (
    PyLegendTdsFrame,
    FrameToPureConfig,
)
from pylegend.core.project_cooridnates import ProjectCoordinates

__all__: PyLegendSequence[str] = [
    "LegendFunctionInputFrameAbstract",
]


class LegendFunctionInputFrameAbstract(PyLegendTdsFrame, metaclass=ABCMeta):
    __path: str
    __project_coordinates: ProjectCoordinates
    __initialized: bool = False

    def __init__(
            self,
            path: str,
            project_coordinates: ProjectCoordinates,
    ) -> None:
        self.__path = path
        self.__project_coordinates = project_coordinates

    def to_pure(self, config: FrameToPureConfig) -> str:
        # The path is the fully-qualified Pure function name
        # e.g. 'pylegend::test::function::SimplePersonFunction__TabularDataSet_1_'
        # Wrap in lambda prefix '|' and append '()' to call the function
        return f"|{self.get_path()}()"

    def get_path(self) -> str:
        return self.__path

    def get_project_coordinates(self) -> ProjectCoordinates:
        return self.__project_coordinates

    def set_initialized(self, val: bool) -> None:
        self.__initialized = val

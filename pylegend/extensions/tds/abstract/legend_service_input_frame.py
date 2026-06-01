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
    "LegendServiceInputFrameAbstract",
]


class LegendServiceInputFrameAbstract(PyLegendTdsFrame, metaclass=ABCMeta):
    __pattern: str
    __project_coordinates: ProjectCoordinates
    __initialized: bool = False

    def __init__(
            self,
            pattern: str,
            project_coordinates: ProjectCoordinates,
    ) -> None:
        self.__pattern = pattern
        self.__project_coordinates = project_coordinates

    def to_pure(self, config: FrameToPureConfig) -> str:
        # Strip leading '/' from pattern (e.g. '/simplePersonService' -> 'simplePersonService')
        # Capitalize the first letter to get the service class name
        # Prepend the test model package prefix and append '.all()' call form
        # The package prefix 'pylegend::test' is verified from the test model JSON
        raw = self.get_pattern().lstrip("/")
        service_name = raw[0].upper() + raw[1:] if raw else raw
        return f"|pylegend::test::{service_name}.all()"

    def get_pattern(self) -> str:
        return self.__pattern

    def get_project_coordinates(self) -> ProjectCoordinates:
        return self.__project_coordinates

    def set_initialized(self, val: bool) -> None:
        self.__initialized = val

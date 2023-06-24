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

from abc import abstractmethod
from pylegend.core.tds.tds_frame import (
    PyLegendTdsFrame
)
from pylegend._typing import (
    PyLegendSequence,
    PyLegendList,
    PyLegendOptional,
)

__all__: PyLegendSequence[str] = [
    "LegendApiTdsFrame"
]


class LegendApiTdsFrame(PyLegendTdsFrame):

    @abstractmethod
    def head(self, count: int = 5) -> "LegendApiTdsFrame":
        pass

    @abstractmethod
    def take(self, count: int = 5) -> "LegendApiTdsFrame":
        pass

    @abstractmethod
    def limit(self, count: int = 5) -> "LegendApiTdsFrame":
        pass

    @abstractmethod
    def drop(self, count: int = 5) -> "LegendApiTdsFrame":
        pass

    @abstractmethod
    def slice(self, start_row: int, end_row_exclusive: int) -> "LegendApiTdsFrame":
        pass

    @abstractmethod
    def distinct(self) -> "LegendApiTdsFrame":
        pass

    @abstractmethod
    def restrict(self, column_name_list: PyLegendList[str]) -> "LegendApiTdsFrame":
        pass

    @abstractmethod
    def sort(
            self,
            column_name_list: PyLegendList[str],
            direction_list: PyLegendOptional[PyLegendList[str]] = None
    ) -> "LegendApiTdsFrame":
        pass

    @abstractmethod
    def concatenate(self, other: "LegendApiTdsFrame") -> "LegendApiTdsFrame":
        pass

    @abstractmethod
    def rename_columns(
            self,
            column_names: PyLegendList[str],
            renamed_column_names: PyLegendList[str]
    ) -> "LegendApiTdsFrame":
        pass

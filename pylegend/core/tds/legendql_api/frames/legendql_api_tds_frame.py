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

from abc import ABCMeta, abstractmethod

from pylegend.core.language import PyLegendPrimitive
from pylegend.core.language.legendql_api.legendql_api_tds_row import LegendQLApiTdsRow
from pylegend.core.tds.tds_frame import (
    PyLegendTdsFrame
)
from pylegend._typing import (
    PyLegendSequence,
    PyLegendCallable,
    PyLegendUnion,
    PyLegendList,
)

__all__: PyLegendSequence[str] = [
    "LegendQLApiTdsFrame"
]


class LegendQLApiTdsFrame(PyLegendTdsFrame, metaclass=ABCMeta):

    @abstractmethod
    def head(self, count: int = 5) -> "LegendQLApiTdsFrame":
        pass  # pragma: no cover

    @abstractmethod
    def drop(self, count: int = 5) -> "LegendQLApiTdsFrame":
        pass  # pragma: no cover

    @abstractmethod
    def slice(self, start_row: int, end_row_exclusive: int) -> "LegendQLApiTdsFrame":
        pass  # pragma: no cover

    @abstractmethod
    def distinct(self) -> "LegendQLApiTdsFrame":
        pass  # pragma: no cover

    @abstractmethod
    def select(
            self,
            columns_function: PyLegendCallable[[LegendQLApiTdsRow], PyLegendUnion[
                PyLegendPrimitive,
                PyLegendList[PyLegendPrimitive],
                str,
                PyLegendList[str]
            ]]
    ) -> "LegendQLApiTdsFrame":
        pass  # pragma: no cover

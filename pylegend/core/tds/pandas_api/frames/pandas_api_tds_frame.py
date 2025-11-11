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
from datetime import date, datetime

from pylegend._typing import (
    PyLegendSequence,
    PyLegendUnion,
    PyLegendOptional,
    PyLegendList,
    PyLegendCallable,
)
from pylegend.core.language.shared.tds_row import AbstractTdsRow
from pylegend.core.tds.tds_frame import PyLegendTdsFrame
from pylegend.core.language import (
    LegacyApiTdsRow,
    PyLegendPrimitive,
)
from pylegend.core.language.shared.primitives.integer import PyLegendInteger
from pylegend.core.tds.tds_frame import PyLegendTdsFrame

__all__: PyLegendSequence[str] = [
    "PandasApiTdsFrame"
]


class PandasApiTdsFrame(PyLegendTdsFrame):

    @abstractmethod
    def assign(
            self,
            **kwargs: PyLegendCallable[
                [LegacyApiTdsRow],
                PyLegendUnion[int, float, bool, str, date, datetime, PyLegendPrimitive]
            ],
    ) -> "PandasApiTdsFrame":
        pass  # pragma: no cover

    @abstractmethod
    def filter(
            self,
            items: PyLegendOptional[PyLegendList[str]] = None,
            like: PyLegendOptional[str] = None,
            regex: PyLegendOptional[str] = None,
            axis: PyLegendOptional[PyLegendUnion[str, int, PyLegendInteger]] = None
    ) -> "PandasApiTdsFrame":
        pass  # pragma: no cover

    @abstractmethod
    def sort_values(
            self,
            by: PyLegendUnion[str, PyLegendList[str]],
            axis: PyLegendUnion[str, int] = 0,
            ascending: PyLegendUnion[bool, PyLegendList[bool]] = True,
            inplace: bool = False,
            kind: PyLegendOptional[str] = None,
            na_position: str = 'last',
            ignore_index: bool = True,
            key: PyLegendOptional[PyLegendCallable[[AbstractTdsRow], AbstractTdsRow]] = None
    ) -> "PandasApiTdsFrame":
        pass  # pragma: no cover

    @abstractmethod
    def truncate(
            self,
            before: PyLegendUnion[str, int] = 0,
            after: PyLegendUnion[str, int] = int('inf'),
            axis: PyLegendUnion[str, int] = 0,
            copy: bool = True
    ) -> "PandasApiTdsFrame":
        pass  # pragma: no cover

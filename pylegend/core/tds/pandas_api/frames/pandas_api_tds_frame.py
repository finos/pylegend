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
    PyLegendCallable,
    PyLegendSequence,
    PyLegendUnion,
    PyLegendOptional,
    PyLegendList,
    PyLegendSet
)
from pylegend.core.language import (
    PyLegendPrimitive,
)
from pylegend.core.language.pandas_api.pandas_api_tds_row import PandasApiTdsRow
from pylegend.core.language.shared.primitives.boolean import PyLegendBoolean
from pylegend.core.language.shared.primitives.integer import PyLegendInteger
from pylegend.core.language.shared.tds_row import AbstractTdsRow
from pylegend.core.tds.tds_frame import PyLegendTdsFrame

__all__: PyLegendSequence[str] = [
    "PandasApiTdsFrame"
]

DropArg = PyLegendOptional[PyLegendUnion[str, PyLegendSequence[str], PyLegendSet[str]]]


class PandasApiTdsFrame(PyLegendTdsFrame):

    @abstractmethod
    def assign(
            self,
            **kwargs: PyLegendCallable[
                [PandasApiTdsRow],
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
            before: PyLegendUnion[date, str, int, None] = 0,
            after: PyLegendUnion[date, str, int, None] = None,
            axis: PyLegendUnion[str, int] = 0,
            copy: bool = True
    ) -> "PandasApiTdsFrame":
        pass  # pragma: no cover

    @abstractmethod
    def drop(
            self,
            labels: DropArg = None,
            axis: PyLegendUnion[str, int, PyLegendInteger] = 1,
            index: DropArg = None,
            columns: DropArg = None,
            level: PyLegendOptional[PyLegendUnion[int, PyLegendInteger, str]] = None,
            inplace: PyLegendUnion[bool, PyLegendBoolean] = True,
            errors: str = "raise",
    ) -> "PandasApiTdsFrame":
        pass  # pragma: no cover

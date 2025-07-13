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
)
from pylegend.core.tds.tds_frame import PyLegendTdsFrame
from pylegend.core.language import (
    LegacyApiTdsRow,
    PyLegendPrimitive,
)

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

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

from pylegend._typing import (
    PyLegendSequence,
)
from pylegend.core.language import (
    PyLegendBoolean,
    PyLegendString,
    PyLegendInteger,
    PyLegendFloat,
    PyLegendNumber,
    PyLegendStrictDate,
    PyLegendDateTime,
    PyLegendDate,
)
from pylegend.core.language.pandas_api.pandas_api_custom_expressions import (
    PandasApiBoolean,
    PandasApiString,
    PandasApiInteger,
    PandasApiFloat,
    PandasApiNumber,
    PandasApiStrictDate,
    PandasApiDateTime,
    PandasApiDate,
    PandasApiPrimitive,
)
from pylegend.core.language.shared.tds_row import AbstractTdsRow
from pylegend.core.tds.tds_frame import PyLegendTdsFrame

__all__: PyLegendSequence[str] = [
    "PandasApiTdsRow",
]


class PandasApiTdsRow(AbstractTdsRow):
    def __init__(self, frame_name: str, frame: PyLegendTdsFrame) -> None:
        super().__init__(frame_name, frame)

    @staticmethod
    def from_tds_frame(frame_name: str, frame: PyLegendTdsFrame) -> "PandasApiTdsRow":
        return PandasApiTdsRow(frame_name=frame_name, frame=frame)

    def __getitem__(self, item: str) -> PandasApiPrimitive:
        res = super().__getitem__(item)
        if isinstance(res, PyLegendBoolean):
            return PandasApiBoolean(res)
        if isinstance(res, PyLegendString):
            return PandasApiString(res)
        if isinstance(res, PyLegendInteger):
            return PandasApiInteger(res)
        if isinstance(res, PyLegendFloat):
            return PandasApiFloat(res)
        if isinstance(res, PyLegendNumber):
            return PandasApiNumber(res)
        if isinstance(res, PyLegendStrictDate):
            return PandasApiStrictDate(res)
        if isinstance(res, PyLegendDateTime):
            return PandasApiDateTime(res)
        if isinstance(res, PyLegendDate):
            return PandasApiDate(res)

        raise RuntimeError(f"Unhandled primitive type {type(res)} in Pandas Api")  # pragma: no cover

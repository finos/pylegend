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
from pylegend.core.language.legendql_api.legendql_api_custom_expressions import (
    LegendQLApiBoolean,
    LegendQLApiString,
    LegendQLApiInteger,
    LegendQLApiFloat,
    LegendQLApiNumber,
    LegendQLApiStrictDate,
    LegendQLApiDateTime,
    LegendQLApiDate, LegendQLApiPrimitive,
)
from pylegend.core.language.shared.tds_row import AbstractTdsRow
from pylegend.core.tds.tds_frame import PyLegendTdsFrame

__all__: PyLegendSequence[str] = [
    "LegendQLApiTdsRow",
]


class LegendQLApiTdsRow(AbstractTdsRow):

    def __init__(self, frame_name: str, frame: PyLegendTdsFrame) -> None:
        super().__init__(frame_name, frame)

    @staticmethod
    def from_tds_frame(frame_name: str, frame: PyLegendTdsFrame) -> "LegendQLApiTdsRow":
        return LegendQLApiTdsRow(frame_name=frame_name, frame=frame)

    def __getattr__(self, key: str) -> LegendQLApiPrimitive:
        return self[key]

    def get_boolean(self, column: str) -> LegendQLApiBoolean:
        return LegendQLApiBoolean(super().get_boolean(column))

    def get_string(self, column: str) -> LegendQLApiString:
        return LegendQLApiString(super().get_string(column))

    def get_number(self, column: str) -> LegendQLApiNumber:
        return LegendQLApiNumber(super().get_number(column))

    def get_integer(self, column: str) -> LegendQLApiInteger:
        return LegendQLApiInteger(super().get_integer(column))

    def get_float(self, column: str) -> LegendQLApiFloat:
        return LegendQLApiFloat(super().get_float(column))

    def get_date(self, column: str) -> LegendQLApiDate:
        return LegendQLApiDate(super().get_date(column))

    def get_datetime(self, column: str) -> LegendQLApiDateTime:
        return LegendQLApiDateTime(super().get_datetime(column))

    def get_strictdate(self, column: str) -> LegendQLApiStrictDate:
        return LegendQLApiStrictDate(super().get_strictdate(column))

    def __getitem__(self, item: str) -> LegendQLApiPrimitive:
        res = super().__getitem__(item)
        if isinstance(res, PyLegendBoolean):
            return LegendQLApiBoolean(res)
        if isinstance(res, PyLegendString):
            return LegendQLApiString(res)
        if isinstance(res, PyLegendInteger):
            return LegendQLApiInteger(res)
        if isinstance(res, PyLegendFloat):
            return LegendQLApiFloat(res)
        if isinstance(res, PyLegendNumber):
            return LegendQLApiNumber(res)
        if isinstance(res, PyLegendStrictDate):
            return LegendQLApiStrictDate(res)
        if isinstance(res, PyLegendDateTime):
            return LegendQLApiDateTime(res)
        if isinstance(res, PyLegendDate):
            return LegendQLApiDate(res)

        raise RuntimeError(f"Unhandled primitive type {type(res)} in LegendQL Api")

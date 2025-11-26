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

from abc import ABCMeta
from pylegend.core.language import (
    PyLegendPrimitive,
    PyLegendBoolean,
    PyLegendString,
    PyLegendNumber,
    PyLegendInteger,
    PyLegendFloat,
    PyLegendDate,
    PyLegendDateTime,
    PyLegendStrictDate,
)
from pylegend._typing import (
    PyLegendSequence,
)

__all__: PyLegendSequence[str] = [
    "PandasApiPrimitive",
    "PandasApiBoolean",
    "PandasApiString",
    "PandasApiNumber",
    "PandasApiInteger",
    "PandasApiFloat",
    "PandasApiDate",
    "PandasApiDateTime",
    "PandasApiStrictDate",
]


class PandasApiPrimitive(PyLegendPrimitive, metaclass=ABCMeta):
    pass


class PandasApiBoolean(PandasApiPrimitive, PyLegendBoolean):
    def __init__(self, expr: PyLegendBoolean):
        PyLegendBoolean.__init__(self, expr.value())


class PandasApiString(PandasApiPrimitive, PyLegendString):
    def __init__(self, expr: PyLegendString):
        PyLegendString.__init__(self, expr.value())


class PandasApiNumber(PandasApiPrimitive, PyLegendNumber):
    def __init__(self, expr: PyLegendNumber):
        PyLegendNumber.__init__(self, expr.value())


class PandasApiInteger(PandasApiPrimitive, PyLegendInteger):
    def __init__(self, expr: PyLegendInteger):
        PyLegendInteger.__init__(self, expr.value())


class PandasApiFloat(PandasApiPrimitive, PyLegendFloat):
    def __init__(self, expr: PyLegendFloat):
        PyLegendFloat.__init__(self, expr.value())


class PandasApiDate(PandasApiPrimitive, PyLegendDate):
    def __init__(self, expr: PyLegendDate):
        PyLegendDate.__init__(self, expr.value())


class PandasApiDateTime(PandasApiPrimitive, PyLegendDateTime):
    def __init__(self, expr: PyLegendDateTime):
        PyLegendDateTime.__init__(self, expr.value())


class PandasApiStrictDate(PandasApiPrimitive, PyLegendStrictDate):
    def __init__(self, expr: PyLegendStrictDate):
        PyLegendStrictDate.__init__(self, expr.value())

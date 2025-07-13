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

from pylegend._typing import (
    PyLegendSequence,
    PyLegendCallable
)
from pylegend.core.language.shared.primitive_collection import PyLegendPrimitiveCollection
from pylegend.core.language import (
    LegacyApiTdsRow,
    PyLegendPrimitive,
    PyLegendPrimitiveOrPythonPrimitive,
)

__all__: PyLegendSequence[str] = [
    "LegacyApiAggregateSpecification",
    "agg",
]


class LegacyApiAggregateSpecification:
    __map_fn: PyLegendCallable[[LegacyApiTdsRow], PyLegendPrimitiveOrPythonPrimitive]
    __aggregate_fn: PyLegendCallable[[PyLegendPrimitiveCollection], PyLegendPrimitive]
    __name: str

    def __init__(
            self,
            map_fn: PyLegendCallable[[LegacyApiTdsRow], PyLegendPrimitiveOrPythonPrimitive],
            aggregate_fn: PyLegendCallable[[PyLegendPrimitiveCollection], PyLegendPrimitive],
            name: str
    ) -> None:
        self.__map_fn = map_fn
        self.__aggregate_fn = aggregate_fn
        self.__name = name

    def get_name(self) -> str:
        return self.__name

    def get_map_fn(self) -> PyLegendCallable[[LegacyApiTdsRow], PyLegendPrimitiveOrPythonPrimitive]:
        return self.__map_fn

    def get_aggregate_fn(self) -> PyLegendCallable[[PyLegendPrimitiveCollection], PyLegendPrimitive]:
        return self.__aggregate_fn


def agg(
    map_fn: PyLegendCallable[[LegacyApiTdsRow], PyLegendPrimitiveOrPythonPrimitive],
    aggregate_fn: PyLegendCallable[[PyLegendPrimitiveCollection], PyLegendPrimitive],
    name: str
) -> LegacyApiAggregateSpecification:
    return LegacyApiAggregateSpecification(map_fn=map_fn, aggregate_fn=aggregate_fn, name=name)

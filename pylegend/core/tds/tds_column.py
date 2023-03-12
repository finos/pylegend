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
    PyLegendList,
    PyLegendSequence
)


class PyLegendTdsColumn:
    __name: str
    __type: str

    __valid_types: PyLegendList[str] = [
        "Integer",
        "Float",
        "String",
        "Boolean"
    ]

    def __init__(self, name: str, _type: str) -> None:
        self.__name = name
        if _type not in self.__valid_types:
            raise ValueError("Unknown type provided for TDS column: '" + _type + "'. " +
                             "Known types are: " + ", ".join(self.__valid_types))
        self.__type = _type

    def get_name(self) -> str:
        return self.__name

    def __str__(self) -> str:
        return "PyLegendTdsColumn(Name: " + self.__name + ", Type: " + self.__type + ")"


__all__: PyLegendSequence[str] = [
    "PyLegendTdsColumn"
]

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
    PyLegendType,
    PyLegendTypeVar
)


T = PyLegendTypeVar("T")


def find_sub_classes(clazz: PyLegendType[T], recursive: bool = True) -> PyLegendList[PyLegendType[T]]:
    subclasses = clazz.__subclasses__()
    if recursive:
        subclasses = subclasses + [c for s in subclasses for c in find_sub_classes(s, True)]
    return subclasses

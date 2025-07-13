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
from pylegend.core.tds.tds_frame import (
    PyLegendTdsFrame
)
from pylegend._typing import (
    PyLegendSequence,
    PyLegendList,
    PyLegendOptional,
    PyLegendCallable,
    PyLegendUnion,
)
from pylegend.core.language import (
    LegacyApiTdsRow,
    PyLegendBoolean,
    PyLegendPrimitiveOrPythonPrimitive,
    LegacyApiAggregateSpecification,
)

__all__: PyLegendSequence[str] = [
    "LegacyApiTdsFrame"
]


class LegacyApiTdsFrame(PyLegendTdsFrame):

    @abstractmethod
    def head(self, count: int = 5) -> "LegacyApiTdsFrame":
        pass  # pragma: no cover

    @abstractmethod
    def take(self, count: int = 5) -> "LegacyApiTdsFrame":
        pass  # pragma: no cover

    @abstractmethod
    def limit(self, count: int = 5) -> "LegacyApiTdsFrame":
        pass  # pragma: no cover

    @abstractmethod
    def drop(self, count: int = 5) -> "LegacyApiTdsFrame":
        pass  # pragma: no cover

    @abstractmethod
    def slice(self, start_row: int, end_row_exclusive: int) -> "LegacyApiTdsFrame":
        pass  # pragma: no cover

    @abstractmethod
    def distinct(self) -> "LegacyApiTdsFrame":
        pass  # pragma: no cover

    @abstractmethod
    def restrict(self, column_name_list: PyLegendList[str]) -> "LegacyApiTdsFrame":
        pass  # pragma: no cover

    @abstractmethod
    def sort(
            self,
            column_name_list: PyLegendList[str],
            direction_list: PyLegendOptional[PyLegendList[str]] = None
    ) -> "LegacyApiTdsFrame":
        pass  # pragma: no cover

    @abstractmethod
    def concatenate(self, other: "LegacyApiTdsFrame") -> "LegacyApiTdsFrame":
        pass  # pragma: no cover

    @abstractmethod
    def rename_columns(
            self,
            column_names: PyLegendList[str],
            renamed_column_names: PyLegendList[str]
    ) -> "LegacyApiTdsFrame":
        pass  # pragma: no cover

    @abstractmethod
    def filter(
            self,
            filter_function: PyLegendCallable[[LegacyApiTdsRow], PyLegendUnion[bool, PyLegendBoolean]]
    ) -> "LegacyApiTdsFrame":
        pass  # pragma: no cover

    @abstractmethod
    def extend(
            self,
            functions_list: PyLegendList[PyLegendCallable[[LegacyApiTdsRow], PyLegendPrimitiveOrPythonPrimitive]],
            column_names_list: PyLegendList[str]
    ) -> "LegacyApiTdsFrame":
        pass  # pragma: no cover

    @abstractmethod
    def join(
            self,
            other: "LegacyApiTdsFrame",
            join_condition: PyLegendCallable[[LegacyApiTdsRow, LegacyApiTdsRow], PyLegendUnion[bool, PyLegendBoolean]],
            join_type: str = 'LEFT_OUTER'
    ) -> "LegacyApiTdsFrame":
        pass  # pragma: no cover

    @abstractmethod
    def join_by_columns(
            self,
            other: "LegacyApiTdsFrame",
            self_columns: PyLegendList[str],
            other_columns: PyLegendList[str],
            join_type: str = 'LEFT_OUTER'
    ) -> "LegacyApiTdsFrame":
        pass  # pragma: no cover

    def join_by_function(
            self,
            other: "LegacyApiTdsFrame",
            join_condition: PyLegendCallable[[LegacyApiTdsRow, LegacyApiTdsRow], PyLegendUnion[bool, PyLegendBoolean]],
            join_type: str = 'LEFT_OUTER'
    ) -> "LegacyApiTdsFrame":
        return self.join(other, join_condition, join_type)

    @abstractmethod
    def group_by(
            self,
            grouping_columns: PyLegendList[str],
            aggregations: PyLegendList[LegacyApiAggregateSpecification],
    ) -> "LegacyApiTdsFrame":
        pass  # pragma: no cover

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

from abc import ABCMeta, abstractmethod
from pylegend._typing import (
    PyLegendSequence,
    PyLegendCallable,
    PyLegendIterator,
    PyLegendTypeVar,
)
from pylegend.core.tds.tds_column import TdsColumn

__all__: PyLegendSequence[str] = [
    "PyLegendTdsFrame",
    "FrameToSqlConfig"
]


class FrameToSqlConfig:
    database_type: str
    pretty: bool

    def __init__(
            self,
            database_type: str = "Postgres",
            pretty: bool = True
    ) -> None:
        self.database_type = database_type
        self.pretty = pretty


R = PyLegendTypeVar('R')


class PyLegendTdsFrame(metaclass=ABCMeta):

    @abstractmethod
    def columns(self) -> PyLegendSequence[TdsColumn]:
        pass

    @abstractmethod
    def to_sql_query(self, config: FrameToSqlConfig = FrameToSqlConfig()) -> str:
        pass

    @abstractmethod
    def execute_frame(
            self,
            result_handler: PyLegendCallable[[PyLegendIterator[bytes]], R],
            chunk_size: int = 1024
    ) -> R:
        pass

    @abstractmethod
    def execute_frame_to_string(
            self,
            chunk_size: int = 1024
    ) -> str:
        pass

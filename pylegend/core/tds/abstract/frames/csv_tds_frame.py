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
from pylegend._typing import (
    PyLegendSequence
)
from pylegend.core.tds.tds_column import TdsColumn
from pylegend.core.tds.abstract.frames.base_tds_frame import BaseTdsFrame
from pylegend.core.tds.tds_frame import FrameToPureConfig, FrameToSqlConfig
from pylegend.core.sql.metamodel import (
    QuerySpecification,
)

__all__: PyLegendSequence[str] = [
    "CsvTdsFrame"
]


class CsvTdsFrame(BaseTdsFrame, metaclass=ABCMeta):
    __csv_string: str

    def __init__(
            self,
            csv_string: str,
            columns: PyLegendSequence[TdsColumn]) -> None:
        super().__init__(columns=columns)
        self.__csv_string = csv_string

    def get_all_tds_frames(self) -> PyLegendSequence["BaseTdsFrame"]:
        return [self]

    def to_sql_query_object(self, config: FrameToSqlConfig) -> QuerySpecification:
        raise RuntimeError("SQL generation for csv tds frames is not supported yet.")

    def to_pure(self, config: FrameToPureConfig) -> str:
        return f"#TDS\n{self.__csv_string}#"

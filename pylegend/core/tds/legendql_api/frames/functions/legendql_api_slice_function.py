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
    PyLegendList,
    PyLegendSequence
)
from pylegend.core.tds.legendql_api.frames.legendql_api_applied_function_tds_frame import LegendQLApiAppliedFunction
from pylegend.core.tds.sql_query_helpers import copy_query, create_sub_query
from pylegend.core.sql.metamodel import (
    QuerySpecification,
    LongLiteral,
)
from pylegend.core.tds.tds_column import TdsColumn
from pylegend.core.tds.tds_frame import FrameToSqlConfig
from pylegend.core.tds.tds_frame import FrameToPureConfig
from pylegend.core.tds.legendql_api.frames.legendql_api_base_tds_frame import LegendQLApiBaseTdsFrame


__all__: PyLegendSequence[str] = [
    "LegendQLApiSliceFunction"
]


class LegendQLApiSliceFunction(LegendQLApiAppliedFunction):
    __base_frame: LegendQLApiBaseTdsFrame
    __start_row: int
    __end_row: int

    @classmethod
    def name(cls) -> str:
        return "slice"

    def __init__(self, base_frame: LegendQLApiBaseTdsFrame, start_row: int, end_row: int) -> None:
        self.__base_frame = base_frame
        self.__start_row = start_row
        self.__end_row = end_row

    def to_sql(self, config: FrameToSqlConfig) -> QuerySpecification:
        base_query = self.__base_frame.to_sql_query_object(config)
        should_create_sub_query = (base_query.offset is not None) or (base_query.limit is not None)
        new_query = (
            create_sub_query(base_query, config, "root") if should_create_sub_query else
            copy_query(base_query)
        )
        new_query.offset = LongLiteral(self.__start_row)
        new_query.limit = LongLiteral(self.__end_row - self.__start_row)
        return new_query

    def to_pure(self, config: FrameToPureConfig) -> str:
        return (f"{self.__base_frame.to_pure(config)}{config.separator(1)}"
                f"->slice({self.__start_row}, {self.__end_row})")

    def base_frame(self) -> LegendQLApiBaseTdsFrame:
        return self.__base_frame

    def tds_frame_parameters(self) -> PyLegendList["LegendQLApiBaseTdsFrame"]:
        return []

    def calculate_columns(self) -> PyLegendSequence["TdsColumn"]:
        return [c.copy() for c in self.__base_frame.columns()]

    def validate(self) -> bool:
        if self.__start_row < 0:
            raise ValueError(
                "Start row argument of slice function cannot be negative. Start row: " + str(self.__start_row)
            )
        if self.__end_row <= self.__start_row:
            raise ValueError("End row argument of slice function cannot be less than or equal to start row argument. "
                             f"Start row: {self.__start_row}, End row: {self.__end_row}")
        return True

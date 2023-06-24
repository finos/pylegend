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
from pylegend.core.tds.legend_api.frames.legend_api_applied_function_tds_frame import (
    AppliedFunction,
    create_sub_query,
    copy_query
)
from pylegend.core.sql.metamodel import (
    QuerySpecification,
    LongLiteral,
)
from pylegend.core.tds.tds_column import TdsColumn
from pylegend.core.tds.tds_frame import FrameToSqlConfig
from pylegend.core.tds.legend_api.frames.legend_api_base_tds_frame import LegendApiBaseTdsFrame


__all__: PyLegendSequence[str] = [
    "SliceFunction"
]


class SliceFunction(AppliedFunction):
    __base_frame: LegendApiBaseTdsFrame
    __start_row: int
    __end_row: int

    @classmethod
    def name(cls) -> str:
        return "slice"

    def __init__(self, base_frame: LegendApiBaseTdsFrame, start_row: int, end_row: int) -> None:
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

    def base_frame(self) -> LegendApiBaseTdsFrame:
        return self.__base_frame

    def tds_frame_parameters(self) -> PyLegendList["LegendApiBaseTdsFrame"]:
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
                             "Start row: {s}, End row: {e}".format(s=self.__start_row, e=self.__end_row))
        return True

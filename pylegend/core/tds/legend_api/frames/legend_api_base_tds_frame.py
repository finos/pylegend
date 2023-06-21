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
    PyLegendTypeVar,
    PyLegendIterator,
    PyLegendList,
    PyLegendOptional,
)
from pylegend.core.sql.metamodel import QuerySpecification
from pylegend.core.databse.sql_to_string import (
    SqlToStringConfig,
    SqlToStringFormat
)
from pylegend.core.tds.tds_column import TdsColumn
from pylegend.core.tds.tds_frame import FrameToSqlConfig
from pylegend.core.tds.legend_api.frames.legend_api_tds_frame import LegendApiTdsFrame

__all__: PyLegendSequence[str] = [
    "LegendApiBaseTdsFrame"
]

R = PyLegendTypeVar('R')


class LegendApiBaseTdsFrame(LegendApiTdsFrame, metaclass=ABCMeta):
    __columns: PyLegendSequence[TdsColumn]

    def __init__(self, columns: PyLegendSequence[TdsColumn]) -> None:
        col_names = [c.get_name() for c in columns]
        if len(col_names) != len(set(col_names)):
            raise ValueError("TdsFrame cannot have duplicated column names. Passed columns: {cols}".format(
                cols="[" + ", ".join([str(c) for c in columns]) + "]"
            ))
        self.__columns = [c.copy() for c in columns]

    def columns(self) -> PyLegendSequence[TdsColumn]:
        return [c.copy() for c in self.__columns]

    def head(self, row_count: int = 5) -> "LegendApiTdsFrame":
        from pylegend.core.tds.legend_api.frames.legend_api_applied_function_tds_frame import (
            LegendApiAppliedFunctionTdsFrame
        )
        from pylegend.core.tds.legend_api.frames.functions.head_function import (
            HeadFunction
        )
        return LegendApiAppliedFunctionTdsFrame(HeadFunction(self, row_count))

    def take(self, row_count: int = 5) -> "LegendApiTdsFrame":
        return self.head(row_count=row_count)

    def limit(self, row_count: int = 5) -> "LegendApiTdsFrame":
        return self.head(row_count=row_count)

    def drop(self, row_count: int = 5) -> "LegendApiTdsFrame":
        from pylegend.core.tds.legend_api.frames.legend_api_applied_function_tds_frame import (
            LegendApiAppliedFunctionTdsFrame
        )
        from pylegend.core.tds.legend_api.frames.functions.drop_function import (
            DropFunction
        )
        return LegendApiAppliedFunctionTdsFrame(DropFunction(self, row_count))

    def distinct(self) -> "LegendApiTdsFrame":
        from pylegend.core.tds.legend_api.frames.legend_api_applied_function_tds_frame import (
            LegendApiAppliedFunctionTdsFrame
        )
        from pylegend.core.tds.legend_api.frames.functions.distinct_function import (
            DistinctFunction
        )
        return LegendApiAppliedFunctionTdsFrame(DistinctFunction(self))

    def restrict(self, column_name_list: PyLegendList[str]) -> "LegendApiTdsFrame":
        from pylegend.core.tds.legend_api.frames.legend_api_applied_function_tds_frame import (
            LegendApiAppliedFunctionTdsFrame
        )
        from pylegend.core.tds.legend_api.frames.functions.restrict_function import (
            RestrictFunction
        )
        return LegendApiAppliedFunctionTdsFrame(RestrictFunction(self, column_name_list))

    def sort(
            self,
            column_name_list: PyLegendList[str],
            direction_list: PyLegendOptional[PyLegendList[str]] = None
    ) -> "LegendApiTdsFrame":
        from pylegend.core.tds.legend_api.frames.legend_api_applied_function_tds_frame import (
            LegendApiAppliedFunctionTdsFrame
        )
        from pylegend.core.tds.legend_api.frames.functions.sort_function import (
            SortFunction
        )
        return LegendApiAppliedFunctionTdsFrame(SortFunction(self, column_name_list, direction_list))

    @abstractmethod
    def to_sql_query_object(self, config: FrameToSqlConfig) -> QuerySpecification:
        pass

    @abstractmethod
    def get_all_tds_frames(self) -> PyLegendList["LegendApiBaseTdsFrame"]:
        pass

    def to_sql_query(self, config: FrameToSqlConfig = FrameToSqlConfig()) -> str:
        query = self.to_sql_query_object(config)
        sql_to_string_config = SqlToStringConfig(
            format_=SqlToStringFormat(
                pretty=config.pretty
            )
        )
        return config.sql_to_string_generator().generate_sql_string(query, sql_to_string_config)

    def execute_frame(
            self,
            result_handler: PyLegendCallable[[PyLegendIterator[bytes]], R],
            chunk_size: int = 1024
    ) -> R:
        from pylegend.core.tds.legend_api.frames.legend_api_input_tds_frame import (
            LegendApiInputTdsFrame,
            LegendApiExecutableInputTdsFrame
        )

        tds_frames = self.get_all_tds_frames()
        input_frames = [x for x in tds_frames if isinstance(x, LegendApiInputTdsFrame)]

        non_exec_frames = [x for x in input_frames if not isinstance(x, LegendApiExecutableInputTdsFrame)]
        if non_exec_frames:
            raise ValueError(
                "Cannot execute frame as its built on top of non-executable input frames: [" +
                (", ".join([str(f) for f in non_exec_frames]) + "]")
            )

        exec_frames = [x for x in input_frames if isinstance(x, LegendApiExecutableInputTdsFrame)]

        all_legend_clients = []
        for e in exec_frames:
            c = e.get_legend_client()
            if c not in all_legend_clients:
                all_legend_clients.append(c)
        if len(all_legend_clients) > 1:
            raise ValueError(
                "Found tds frames with multiple legend_clients (which is not supported): [" +
                (", ".join([str(f) for f in all_legend_clients]) + "]")
            )
        legend_client = all_legend_clients[0]
        result = legend_client.execute_sql_string(self.to_sql_query(), chunk_size=chunk_size)
        return result_handler(result)

    def execute_frame_to_string(
            self,
            chunk_size: int = 1024
    ) -> str:
        return self.execute_frame(lambda res: b"".join(res).decode("utf-8"), chunk_size)

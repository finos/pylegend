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
    PyLegendList
)
from pylegend.core.sql.metamodel import QuerySpecification
from pylegend.core.databse.sql_to_string import (
    SqlToStringGenerator,
    SqlToStringConfig,
    SqlToStringFormat
)
from pylegend.core.tds.tds_frame import FrameToSqlConfig
from pylegend.core.tds.legend_api.frames.legend_api_tds_frame import LegendApiTdsFrame

__all__: PyLegendSequence[str] = [
    "LegendApiBaseTdsFrame"
]

R = PyLegendTypeVar('R')


class LegendApiBaseTdsFrame(LegendApiTdsFrame, metaclass=ABCMeta):
    def head(self, row_count: int = 5) -> "LegendApiTdsFrame":
        from pylegend.core.tds.legend_api.frames.legend_api_applied_function_tds_frame import (
            LegendApiAppliedFunctionTdsFrame
        )
        from pylegend.core.tds.legend_api.frames.functions.head_function import (
            HeadFunction
        )
        return LegendApiAppliedFunctionTdsFrame(self, HeadFunction(row_count))

    @abstractmethod
    def to_sql_query_object(self, config: FrameToSqlConfig) -> QuerySpecification:
        pass

    @abstractmethod
    def get_all_tds_frames(self) -> PyLegendList["LegendApiBaseTdsFrame"]:
        pass

    def to_sql_query(self, config: FrameToSqlConfig = FrameToSqlConfig()) -> str:
        query = self.to_sql_query_object(config)
        sql_to_string_generator = SqlToStringGenerator.find_sql_to_string_generator_for_db_type(config.database_type)
        sql_to_string_config = SqlToStringConfig(
            format_=SqlToStringFormat(
                pretty=config.pretty
            )
        )
        return sql_to_string_generator.generate_sql_string(query, sql_to_string_config)

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

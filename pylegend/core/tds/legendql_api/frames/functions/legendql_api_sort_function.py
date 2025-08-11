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
    PyLegendSequence,
    PyLegendCallable,
    PyLegendUnion,
)
from pylegend.core.language.legendql_api.legendql_api_custom_expressions import (
    LegendQLApiPrimitive,
    LegendQLApiSortInfo,
)
from pylegend.core.language.legendql_api.legendql_api_tds_row import LegendQLApiTdsRow
from pylegend.core.sql.metamodel import (
    QuerySpecification,
)
from pylegend.core.tds.legendql_api.frames.functions.legendql_api_function_helpers import infer_sorts_from_frame
from pylegend.core.tds.legendql_api.frames.legendql_api_applied_function_tds_frame import LegendQLApiAppliedFunction
from pylegend.core.tds.legendql_api.frames.legendql_api_base_tds_frame import LegendQLApiBaseTdsFrame
from pylegend.core.tds.sql_query_helpers import copy_query, create_sub_query
from pylegend.core.tds.tds_column import TdsColumn
from pylegend.core.tds.tds_frame import FrameToPureConfig
from pylegend.core.tds.tds_frame import FrameToSqlConfig

__all__: PyLegendSequence[str] = [
    "LegendQLApiSortFunction"
]


class LegendQLApiSortFunction(LegendQLApiAppliedFunction):
    __base_frame: LegendQLApiBaseTdsFrame
    __sort_infos: PyLegendList[LegendQLApiSortInfo]

    @classmethod
    def name(cls) -> str:
        return "sort"

    def __init__(
            self,
            base_frame: LegendQLApiBaseTdsFrame,
            sort_infos: PyLegendUnion[
                str,
                PyLegendList[str],
                PyLegendCallable[
                    [LegendQLApiTdsRow],
                    PyLegendUnion[
                        LegendQLApiPrimitive,
                        LegendQLApiSortInfo,
                        PyLegendList[PyLegendUnion[LegendQLApiPrimitive, LegendQLApiSortInfo]],
                    ]
                ]
            ]
    ) -> None:
        self.__base_frame = base_frame
        self.__sort_infos = infer_sorts_from_frame(base_frame, sort_infos, "'sort' function sort_infos")

    def to_sql(self, config: FrameToSqlConfig) -> QuerySpecification:
        base_query = self.__base_frame.to_sql_query_object(config)
        should_create_sub_query = (base_query.offset is not None) or (base_query.limit is not None)
        new_query = (
            create_sub_query(base_query, config, "root") if should_create_sub_query else
            copy_query(base_query)
        )
        new_query.orderBy = [i.to_sql_node(query=new_query, config=config) for i in self.__sort_infos]
        return new_query

    def to_pure(self, config: FrameToPureConfig) -> str:
        return (f"{self.__base_frame.to_pure(config)}{config.separator(1)}" +
                f"->sort([{', '.join([i.to_pure_expression(config) for i in self.__sort_infos])}])")

    def base_frame(self) -> LegendQLApiBaseTdsFrame:
        return self.__base_frame

    def tds_frame_parameters(self) -> PyLegendList["LegendQLApiBaseTdsFrame"]:
        return []

    def calculate_columns(self) -> PyLegendSequence["TdsColumn"]:
        return [c.copy() for c in self.__base_frame.columns()]

    def validate(self) -> bool:
        return True

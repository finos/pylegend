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
from pylegend.core.language import PyLegendColumnExpression
from pylegend.core.language.legendql_api.legendql_api_custom_expressions import (
    LegendQLApiPrimitive,
    LegendQLApiSortInfo,
    LegendQLApiSortDirection,
)
from pylegend.core.language.legendql_api.legendql_api_tds_row import LegendQLApiTdsRow
from pylegend.core.sql.metamodel import (
    QuerySpecification,
)
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
            sort_infos_function: PyLegendCallable[[LegendQLApiTdsRow], PyLegendUnion[
                LegendQLApiPrimitive,
                str,
                LegendQLApiSortInfo,
                PyLegendList[PyLegendUnion[LegendQLApiPrimitive, str, LegendQLApiSortInfo]],
            ]]
    ) -> None:
        self.__base_frame = base_frame
        tds_row = LegendQLApiTdsRow.from_tds_frame("frame", self.__base_frame)
        if not isinstance(sort_infos_function, type(lambda x: 0)) or (sort_infos_function.__code__.co_argcount != 1):
            raise TypeError("Sort sort_infos_function should be a lambda which takes one argument (TDSRow)")

        try:
            result = sort_infos_function(tds_row)
        except Exception as e:
            raise RuntimeError(
                "Sort lambda incompatible. Error occurred while evaluating. Message: " + str(e)
            ) from e

        list_result: PyLegendList[PyLegendUnion[LegendQLApiPrimitive, str, LegendQLApiSortInfo]]
        if isinstance(result, list):
            list_result = result
        elif isinstance(result, tuple):
            list_result = list(result)
        else:
            list_result = [result]

        sort_info_list = []
        for (i, r) in enumerate(list_result):
            if isinstance(r, str):
                col_expr1: PyLegendColumnExpression = tds_row[r].value()
                sort_info_list.append(LegendQLApiSortInfo(col_expr1, LegendQLApiSortDirection.ASC))
            elif isinstance(r, LegendQLApiPrimitive) and isinstance(r.value(), PyLegendColumnExpression):
                col_expr2: PyLegendColumnExpression = r.value()
                sort_info_list.append(LegendQLApiSortInfo(col_expr2, LegendQLApiSortDirection.ASC))
            elif isinstance(r, LegendQLApiSortInfo):
                sort_info_list.append(r)
            else:
                raise RuntimeError(
                    "Sort lambda incompatible. Columns can either be strings (lambda r: ['c1', 'c2']) or "
                    "simple column expressions (lambda r: [r.c1, r.c2]) or "
                    "sort infos (lambda r: [r.c1.ascending(), r['c2'].descending()]). "
                    f"Element at index {i} in the list is incompatible."
                )

        self.__sort_infos = sort_info_list

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

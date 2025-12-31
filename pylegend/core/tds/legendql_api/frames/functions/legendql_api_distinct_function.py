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
    PyLegendUnion,
    PyLegendOptional,
    PyLegendCallable
)
from pylegend.core.tds.legendql_api.frames.legendql_api_applied_function_tds_frame import LegendQLApiAppliedFunction
from pylegend.core.tds.sql_query_helpers import copy_query, create_sub_query
from pylegend.core.sql.metamodel import (
    QuerySpecification,
    SingleColumn,
)
from pylegend.core.tds.tds_column import TdsColumn
from pylegend.core.tds.tds_frame import FrameToSqlConfig
from pylegend.core.tds.tds_frame import FrameToPureConfig
from pylegend.core.tds.legendql_api.frames.legendql_api_base_tds_frame import LegendQLApiBaseTdsFrame
from pylegend.core.language.legendql_api.legendql_api_custom_expressions import LegendQLApiPrimitive
from pylegend.core.language.legendql_api.legendql_api_tds_row import LegendQLApiTdsRow
from pylegend.core.tds.legendql_api.frames.functions.legendql_api_function_helpers import infer_columns_from_frame
from pylegend.core.language.shared.helpers import escape_column_name


__all__: PyLegendSequence[str] = [
    "LegendQLApiDistinctFunction"
]


class LegendQLApiDistinctFunction(LegendQLApiAppliedFunction):
    __base_frame: LegendQLApiBaseTdsFrame
    __column_name_list: PyLegendList[str]

    @classmethod
    def name(cls) -> str:
        return "distinct"

    def __init__(
            self,
            base_frame: LegendQLApiBaseTdsFrame,
            columns: PyLegendOptional[PyLegendUnion[
                str,
                PyLegendList[str],
                PyLegendCallable[
                    [LegendQLApiTdsRow],
                    PyLegendUnion[LegendQLApiPrimitive, PyLegendList[LegendQLApiPrimitive]]
                ]
            ]] = None
    ) -> None:
        self.__base_frame = base_frame
        self.__column_name_list = infer_columns_from_frame(base_frame, columns,
                                                           "'distinct' function 'columns'") if columns is not None else []

    def to_sql(self, config: FrameToSqlConfig) -> QuerySpecification:
        base_query = self.__base_frame.to_sql_query_object(config)
        should_create_sub_query = (base_query.offset is not None) or (base_query.limit is not None)
        new_query = (
            create_sub_query(base_query, config, "root") if should_create_sub_query else
            copy_query(base_query)
        )
        new_query.select.distinct = True
        if self.__column_name_list:
            db_extension = config.sql_to_string_generator().get_db_extension()
            quoted_cols = {
                db_extension.quote_identifier(col)
                for col in self.__column_name_list
            }

            new_query.select.selectItems = [
                item
                for item in new_query.select.selectItems
                if isinstance(item, SingleColumn) and item.alias in quoted_cols
            ]
        return new_query

    def to_pure(self, config: FrameToPureConfig) -> str:
        columns_expr = (
            f"~[{', '.join(map(escape_column_name, self.__column_name_list))}]"
            if self.__column_name_list else ""
        )
        return (f"{self.__base_frame.to_pure(config)}{config.separator(1)}"
                f"->distinct({columns_expr})")

    def base_frame(self) -> LegendQLApiBaseTdsFrame:
        return self.__base_frame

    def tds_frame_parameters(self) -> PyLegendList[LegendQLApiBaseTdsFrame]:
        return []

    def calculate_columns(self) -> PyLegendSequence[TdsColumn]:
        return [c.copy() for c in self.__base_frame.columns()]

    def validate(self) -> bool:
        return True

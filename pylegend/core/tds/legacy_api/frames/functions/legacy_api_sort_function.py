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
    PyLegendSequence,
    PyLegendOptional,
)
from pylegend.core.tds.legacy_api.frames.legacy_api_applied_function_tds_frame import LegacyApiAppliedFunction
from pylegend.core.tds.sql_query_helpers import copy_query, create_sub_query
from pylegend.core.sql.metamodel import (
    QuerySpecification,
    SortItem,
    SortItemOrdering,
    SortItemNullOrdering,
    SingleColumn,
    Expression,
)
from pylegend.core.tds.tds_column import TdsColumn
from pylegend.core.tds.tds_frame import FrameToSqlConfig
from pylegend.core.tds.tds_frame import FrameToPureConfig
from pylegend.core.tds.legacy_api.frames.legacy_api_base_tds_frame import LegacyApiBaseTdsFrame
from pylegend.core.language.shared.helpers import escape_column_name


__all__: PyLegendSequence[str] = [
    "LegacyApiSortFunction"
]


class LegacyApiSortFunction(LegacyApiAppliedFunction):
    __base_frame: LegacyApiBaseTdsFrame
    __column_name_list: PyLegendList[str]
    __directions: PyLegendOptional[PyLegendList[str]]

    @classmethod
    def name(cls) -> str:
        return "sort"

    def __init__(
            self,
            base_frame: LegacyApiBaseTdsFrame,
            column_name_list: PyLegendList[str],
            directions: PyLegendOptional[PyLegendList[str]]
    ) -> None:
        self.__base_frame = base_frame
        self.__column_name_list = column_name_list
        self.__directions = directions

    def to_sql(self, config: FrameToSqlConfig) -> QuerySpecification:
        base_query = self.__base_frame.to_sql_query_object(config)
        db_extension = config.sql_to_string_generator().get_db_extension()

        def find_column_expression(col: str, query: QuerySpecification) -> Expression:
            filtered = [
                s for s in query.select.selectItems
                if (isinstance(s, SingleColumn) and
                    s.alias == db_extension.quote_identifier(col))
            ]
            if len(filtered) == 0:
                raise RuntimeError("Cannot find column: " + col)  # pragma: no cover
            return filtered[0].expression

        should_create_sub_query = (base_query.offset is not None) or (base_query.limit is not None)
        new_query = (
            create_sub_query(base_query, config, "root") if should_create_sub_query else
            copy_query(base_query)
        )

        direction_list = self._build_directions_list()

        new_query.orderBy = [
            SortItem(
                sortKey=find_column_expression(self.__column_name_list[i], new_query),
                ordering=(SortItemOrdering.ASCENDING if direction_list[i].upper() == "ASC"
                          else SortItemOrdering.DESCENDING),
                nullOrdering=SortItemNullOrdering.UNDEFINED
            )
            for i in range(len(self.__column_name_list))
        ]
        return new_query

    def to_pure(self, config: FrameToPureConfig) -> str:
        direction_list = self._build_directions_list()
        sort_infos = []
        for col_name, direction in zip(self.__column_name_list, direction_list):
            escaped = escape_column_name(col_name)
            sort_infos.append(f"{'ascending' if direction.upper() == 'ASC' else 'descending'}(~{escaped})")
        return (f"{self.__base_frame.to_pure(config)}{config.separator(1)}" +
                f"->sort([{', '.join(sort_infos)}])")

    def base_frame(self) -> LegacyApiBaseTdsFrame:
        return self.__base_frame

    def tds_frame_parameters(self) -> PyLegendList["LegacyApiBaseTdsFrame"]:
        return []

    def calculate_columns(self) -> PyLegendSequence["TdsColumn"]:
        return [c.copy() for c in self.__base_frame.columns()]

    def validate(self) -> bool:
        base_cols = [c.get_name() for c in self.__base_frame.columns()]
        for c in self.__column_name_list:
            if c not in base_cols:
                raise ValueError(
                    f"Column - '{c}' in sort columns list doesn't exist in the current frame. "
                    f"Current frame columns: {base_cols}"
                )

        if (self.__directions is not None) and (len(self.__directions) > 0):
            if len(self.__column_name_list) != len(self.__directions):
                cols = self.__column_name_list
                dirs = self.__directions
                raise ValueError(
                    "Sort directions (ASC/DESC) provided need to be in sync with columns or left empty to "
                    f"choose defaults. Passed column list: {cols}, directions: {dirs}"
                )

            for d in self.__directions:
                if d.upper() not in (["ASC", "DESC"]):
                    raise ValueError(
                        "Sort direction can be ASC/DESC (case insensitive). Passed unknown value: " + d
                    )

        return True

    def _build_directions_list(self) -> PyLegendList[str]:
        if (self.__directions is None) or (len(self.__directions) == 0):
            return ["ASC" for _ in self.__column_name_list]
        else:
            # Already validated that directions are all ASC/DESC and length matches with column list
            direction_list = []
            for d in self.__directions:
                direction_list.append(d.upper())
            return direction_list

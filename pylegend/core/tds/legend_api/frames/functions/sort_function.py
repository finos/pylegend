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
from pylegend.core.databse.sql_to_string import SqlToStringGenerator
from pylegend.core.tds.legend_api.frames.legend_api_applied_function_tds_frame import (
    AppliedFunction,
    create_sub_query,
    copy_query
)
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
from pylegend.core.tds.legend_api.frames.legend_api_base_tds_frame import LegendApiBaseTdsFrame


__all__: PyLegendSequence[str] = [
    "SortFunction"
]


class SortFunction(AppliedFunction):
    base_frame: LegendApiBaseTdsFrame
    column_name_list: PyLegendList[str]
    direction_list: PyLegendList[str]

    @classmethod
    def name(cls) -> str:
        return "restrict"

    def __init__(
            self,
            base_frame: LegendApiBaseTdsFrame,
            column_name_list: PyLegendList[str],
            directions: PyLegendOptional[PyLegendList[str]]
    ) -> None:
        self.base_frame = base_frame

        base_cols = [c.get_name() for c in self.base_frame.columns()]
        for c in column_name_list:
            if c not in base_cols:
                raise ValueError(
                    "Column - '{col}' in sort columns list doesn't exist in the current frame. "
                    "Current frame columns: {cols}".format(
                        col=c,
                        cols=base_cols
                    )
                )
        self.column_name_list = column_name_list

        if (directions is None) or (len(directions) == 0):
            self.direction_list = ["ASC" for _ in self.column_name_list]
        else:
            if len(self.column_name_list) != len(directions):
                raise ValueError(
                    "Sort directions (ASC/DESC) provided need to be in sync with columns or left empty to "
                    "choose defaults. Passed column list: {cols}, directions: {dirs}".format(
                        cols=column_name_list,
                        dirs=directions
                    )
                )

            self.direction_list = []
            for d in directions:
                if d.upper() not in (["ASC", "DESC"]):
                    raise ValueError(
                        "Sort direction can be ASC/DESC (case insensitive). Passed unknown value: " + d
                    )
                self.direction_list.append(d.upper())

    def to_sql(self, base_query: QuerySpecification, config: FrameToSqlConfig) -> QuerySpecification:
        generator = SqlToStringGenerator.find_sql_to_string_generator_for_db_type(config.database_type)
        db_extension = generator.get_db_extension()

        def find_column_expression(col: str, query: QuerySpecification) -> Expression:
            filtered = [
                s for s in query.select.selectItems
                if isinstance(s, SingleColumn) and s.alias == db_extension.quote_identifier(col)
            ]
            if len(filtered) == 0:
                raise RuntimeError("Cannot find column: " + col)  # pragma: no cover
            return filtered[0].expression

        if (base_query.offset is not None) or (base_query.limit is not None):
            new_query = create_sub_query(base_query, config, "root")
        else:
            new_query = copy_query(base_query)

        new_query.orderBy = [
            SortItem(
                sortKey=find_column_expression(self.column_name_list[i], new_query),
                ordering=(SortItemOrdering.ASCENDING if self.direction_list[i].upper() == "ASC"
                          else SortItemOrdering.DESCENDING),
                nullOrdering=SortItemNullOrdering.UNDEFINED
            )
            for i in range(len(self.column_name_list))
        ]
        return new_query

    def tds_frame_parameters(self) -> PyLegendList["LegendApiBaseTdsFrame"]:
        return []

    def calculate_columns(self, base_frame: "LegendApiBaseTdsFrame") -> PyLegendSequence["TdsColumn"]:
        return [c.copy() for c in base_frame.columns()]

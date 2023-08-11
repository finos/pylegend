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
import select

from pylegend._typing import (
    PyLegendList,
    PyLegendSequence,
)
from pylegend.core.tds.legend_api.frames.legend_api_applied_function_tds_frame import LegendApiAppliedFunction
from pylegend.core.tds.legend_api.frames.legend_api_base_tds_frame import LegendApiBaseTdsFrame

from pylegend.core.tds.pandas_api.frames.pandas_api_applied_function_tds_frame import PandasApiAppliedFunction

from pylegend.core.sql.metamodel import (
    QuerySpecification,
    LogicalBinaryExpression, SingleColumn, Expression, AllColumns, SubqueryExpression, Join, JoinType, Relation,
    JoinCriteria, JoinOn, JoinUsing, Select, QualifiedNameReference, QualifiedName, AliasedRelation, TableSubquery,
    Query, QueryBody
)
from pylegend.core.tds.pandas_api.frames.pandas_api_base_tds_frame import PandasApiBaseTdsFrame

from pylegend.core.tds.sql_query_helpers import create_sub_query
from pylegend.core.tds.tds_column import TdsColumn
from pylegend.core.tds.tds_frame import FrameToSqlConfig

from typing import Callable, Union, Optional


class MergeFunction(PandasApiAppliedFunction, LegendApiAppliedFunction):
    __base_frame: Union[PandasApiBaseTdsFrame, LegendApiBaseTdsFrame]
    __other_frame: Union[PandasApiBaseTdsFrame, LegendApiBaseTdsFrame]
    join_type: JoinType
    join_criteria: JoinCriteria

    @classmethod
    def name(cls) -> str:
        return "join"

    def __init__(self, join_type: JoinType, base_frame: "Union[PandasApiBaseTdsFrame, LegendApiBaseTdsFrame]",
                 right_frame: Union[PandasApiBaseTdsFrame, LegendApiBaseTdsFrame], join_criteria: Union[JoinOn, JoinUsing]) -> None:
        self.join_type = join_type
        self.__base_frame = base_frame
        self.__other_frame = right_frame
        self.join_criteria = join_criteria

    def to_sql(self, config: FrameToSqlConfig) -> QuerySpecification:
        base_query = self.__base_frame.to_sql_query_object(config)
        other_query = self.__other_frame.to_sql_query_object(config)
        new_base_query = create_sub_query(base_query, config, "root1")
        new_other_query = create_sub_query(other_query, config, "root2")

        db_extension = config.sql_to_string_generator().get_db_extension()
        base_frame_columns = [db_extension.quote_identifier(c.get_name()) for c in self.__base_frame.columns()]
        other_frame_columns = [db_extension.quote_identifier(c.get_name()) for c in self.__other_frame.columns()]

        base_frame_root_alias = db_extension.quote_identifier("root1")
        other_frame_root_alias = db_extension.quote_identifier("root2")

        join_func = Join(self.join_type, new_base_query, new_other_query, self.join_criteria)

        query = QuerySpecification(
            select=Select(
                selectItems=[
                    SingleColumn(
                        alias=x,
                        expression=QualifiedNameReference(name=QualifiedName(parts=[base_frame_root_alias, x]))
                    )
                    for x in base_frame_columns],
                distinct=False
            ),
            from_=[join_func],
            where=None,
            groupBy=[],
            having=None,
            orderBy=[],
            limit=None,
            offset=None
        )

        for x in other_frame_columns:
            new_col = SingleColumn(alias=x, expression=QualifiedNameReference(name=QualifiedName(
                parts=[other_frame_root_alias, x]))
)
            query.select.distinct = query.select.selectItems.append(new_col)

        return query

    def base_frame(self) -> PandasApiBaseTdsFrame:
        return self.__base_frame

    def tds_frame_parameters(self) -> PyLegendList["PandasApiBaseTdsFrame"]:
        return []

    def calculate_columns(self) -> PyLegendSequence["TdsColumn"]:
        return [c.copy() for c in self.__base_frame.columns()]

    def validate(self) -> bool:  # type: ignore
        pass
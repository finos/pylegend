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
    PyLegendList,
    PyLegendOptional
)
from pylegend.core.databse.sql_to_string import SqlToStringGenerator
from pylegend.core.sql.metamodel import (
    QuerySpecification,
    SingleColumn,
    QualifiedName,
    QualifiedNameReference,
    AliasedRelation,
    Select,
    TableSubquery,
    Query
)
from pylegend.core.tds.tds_frame import FrameToSqlConfig
from pylegend.core.tds.legend_api.frames.legend_api_base_tds_frame import LegendApiBaseTdsFrame


__all__: PyLegendSequence[str] = [
    "LegendApiAppliedFunctionTdsFrame",
    "AppliedFunction",
    "create_sub_query",
    "copy_query"
]


class AppliedFunction(metaclass=ABCMeta):
    @classmethod
    @abstractmethod
    def name(cls) -> str:
        pass

    @abstractmethod
    def to_sql(self, base_query: QuerySpecification, config: FrameToSqlConfig) -> QuerySpecification:
        pass

    @abstractmethod
    def tds_frame_parameters(self) -> PyLegendList["LegendApiBaseTdsFrame"]:
        pass


def create_sub_query(
        base_query: QuerySpecification,
        config: FrameToSqlConfig,
        alias: str,
        columns_to_retain: PyLegendOptional[PyLegendList[str]] = None
) -> QuerySpecification:
    query = copy_query(base_query)
    generator = SqlToStringGenerator.find_sql_to_string_generator_for_db_type(config.database_type)
    db_extension = generator.get_db_extension()
    table_alias = db_extension.quote_identifier(alias)

    columns = []
    for col in query.select.selectItems:
        if not isinstance(col, SingleColumn):
            raise ValueError("Subquery creation not supported for queries "
                             "with columns other than SingleColumn")  # pragma: no cover
        if col.alias is None:
            raise ValueError("Subquery creation not supported for queries "
                             "with SingleColumns with missing alias")  # pragma: no cover
        columns.append(col.alias)

    outer_query_columns = columns_to_retain if columns_to_retain else columns

    return QuerySpecification(
        select=Select(
            selectItems=[
                SingleColumn(
                    alias=x,
                    expression=QualifiedNameReference(name=QualifiedName(parts=[table_alias, x]))
                )
                for x in columns if x in outer_query_columns
            ],
            distinct=False
        ),
        from_=[
            AliasedRelation(
                relation=TableSubquery(query=Query(queryBody=query, limit=None, offset=None, orderBy=[])),
                alias=table_alias,
                columnNames=columns
            )
        ],
        where=None,
        groupBy=[],
        having=None,
        orderBy=[],
        limit=None,
        offset=None
    )


def copy_query(query: QuerySpecification) -> QuerySpecification:
    return QuerySpecification(
        select=copy_select(query.select),
        from_=query.from_,
        where=query.where,
        groupBy=query.groupBy,
        having=query.having,
        orderBy=query.orderBy,
        limit=query.limit,
        offset=query.offset
    )


def copy_select(select: Select) -> Select:
    return Select(
        distinct=select.distinct,
        selectItems=[s for s in select.selectItems]
    )


class LegendApiAppliedFunctionTdsFrame(LegendApiBaseTdsFrame):
    base_frame: LegendApiBaseTdsFrame
    applied_function: AppliedFunction

    def __init__(self, base_frame: LegendApiBaseTdsFrame, applied_function: AppliedFunction):
        self.base_frame = base_frame
        self.applied_function = applied_function

    def to_sql_query_object(self, config: FrameToSqlConfig) -> QuerySpecification:
        base_query = self.base_frame.to_sql_query_object(config)
        return self.applied_function.to_sql(base_query, config)

    def get_all_tds_frames(self) -> PyLegendList["LegendApiBaseTdsFrame"]:
        return [
            y
            for x in [self.base_frame] + self.applied_function.tds_frame_parameters()
            for y in x.get_all_tds_frames()
        ] + [self]

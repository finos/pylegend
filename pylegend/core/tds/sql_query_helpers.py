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
    PyLegendSequence,
    PyLegendOptional,
    PyLegendList,
)
from pylegend.core.sql.metamodel import (
    QuerySpecification,
    SelectItem,
    SingleColumn,
    QualifiedName,
    QualifiedNameReference,
    AliasedRelation,
    Select,
    TableSubquery,
    Query
)
from pylegend.core.tds.tds_frame import FrameToSqlConfig

__all__: PyLegendSequence[str] = [
    "create_sub_query",
    "copy_query",
    "extract_columns_for_subquery",
]


def create_sub_query(
        base_query: QuerySpecification,
        config: FrameToSqlConfig,
        alias: str,
        columns_to_retain: PyLegendOptional[PyLegendList[str]] = None
) -> QuerySpecification:
    query = copy_query(base_query)
    table_alias = config.sql_to_string_generator().get_db_extension().quote_identifier(alias)

    columns = extract_columns_for_subquery(query)
    outer_query_columns = columns_to_retain if columns_to_retain else columns
    unordered_select_items_with_index = [
        (
            outer_query_columns.index(x),
            SingleColumn(
                alias=x,
                expression=QualifiedNameReference(name=QualifiedName(parts=[table_alias, x]))
            )
        )
        for x in columns if x in outer_query_columns
    ]
    ordered_select_items: PyLegendList[SelectItem] = [
        y[1] for y in sorted(unordered_select_items_with_index, key=lambda x: x[0])
    ]

    return QuerySpecification(
        select=Select(
            selectItems=ordered_select_items,
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


def extract_columns_for_subquery(query: QuerySpecification) -> PyLegendList[str]:
    columns = []
    for col in query.select.selectItems:
        if not isinstance(col, SingleColumn):
            raise ValueError("Subquery creation not supported for queries "
                             "with columns other than SingleColumn")  # pragma: no cover
        if col.alias is None:
            raise ValueError("Subquery creation not supported for queries "
                             "with SingleColumns with missing alias")  # pragma: no cover
        columns.append(col.alias)
    return columns


def copy_select(select: Select) -> Select:
    return Select(
        distinct=select.distinct,
        selectItems=[s for s in select.selectItems]
    )

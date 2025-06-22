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

from abc import ABCMeta
from pylegend._typing import (
    PyLegendSequence,
    PyLegendList,
)
from pylegend.core.sql.metamodel import (
    QualifiedName,
    QualifiedNameReference,
    QuerySpecification,
    Select,
    SingleColumn,
    Table,
    AliasedRelation
)
from pylegend.core.tds.tds_frame import PyLegendTdsFrame
from pylegend.core.tds.tds_frame import FrameToSqlConfig
from pylegend.core.tds.tds_frame import FrameToPureConfig

__all__: PyLegendSequence[str] = [
    "TableSpecInputFrameAbstract",
]


class TableSpecInputFrameAbstract(PyLegendTdsFrame, metaclass=ABCMeta):
    table: QualifiedName

    def __init__(self, table_name_parts: PyLegendList[str]) -> None:
        self.table = QualifiedName(table_name_parts)

    def to_sql_query_object(self, config: FrameToSqlConfig) -> QuerySpecification:
        db_extension = config.sql_to_string_generator().get_db_extension()
        root_alias = db_extension.quote_identifier("root")
        return QuerySpecification(
            select=Select(
                selectItems=[
                    SingleColumn(
                        alias=db_extension.quote_identifier(x.get_name()),
                        expression=QualifiedNameReference(name=QualifiedName(parts=[root_alias, x.get_name()]))
                    )
                    for x in self.columns()
                ],
                distinct=False
            ),
            from_=[
                AliasedRelation(
                    relation=Table(name=self.table),
                    alias=root_alias,
                    columnNames=[x.get_name() for x in self.columns()]
                )
            ],
            where=None,
            groupBy=[],
            having=None,
            orderBy=[],
            limit=None,
            offset=None
        )

    def to_pure(self, config: FrameToPureConfig) -> str:
        return f"#Table({'.'.join(self.table.parts)})#"

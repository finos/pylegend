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
)
from pylegend.core.sql.metamodel import QuerySpecification
from pylegend.core.databse.sql_to_string import SqlToStringGenerator
from pylegend.core.tds.frames.input_tds_frame import ExecutableInputTdsFrame
from pylegend.core.tds.tds_frame import FrameToSqlConfig
from pylegend.core.request.legend_client import LegendClient
from pylegend.core.sql.metamodel import (
    TableFunction,
    Select,
    AllColumns,
    FunctionCall,
    QualifiedName,
    NamedArgumentExpression,
    StringLiteral,
    AliasedRelation,
    SingleColumn,
    QualifiedNameReference
)

__all__: PyLegendSequence[str] = [
    "LegendServiceFrame",
    "simple_person_service_frame"
]


class LegendServiceFrame(ExecutableInputTdsFrame):

    def __init__(
            self,
            legend_client: LegendClient,
            pattern: str,
            group_id: str,
            artifact_id: str,
            version: str
    ) -> None:
        super().__init__(legend_client)
        self.pattern = pattern
        self.group_id = group_id
        self.artifact_id = artifact_id
        self.version = version
        self.columns = None
        self.columns = legend_client.get_sql_string_schema(self.to_sql_query())

    def to_sql_query_object(self, config: FrameToSqlConfig) -> QuerySpecification:
        generator = SqlToStringGenerator.find_sql_to_string_generator_for_db_type(config.database_type)
        db_extension = generator.get_db_extension()
        root_alias = db_extension.quote_identifier("root")
        func_call = FunctionCall(
            name=QualifiedName(["legend_service"]),
            distinct=False,
            filter_=None,
            window=None,
            arguments=[
                StringLiteral(self.pattern, False),
                NamedArgumentExpression(name="groupId", expression=StringLiteral(self.group_id, False)),
                NamedArgumentExpression(name="artifactId", expression=StringLiteral(self.artifact_id, False)),
                NamedArgumentExpression(name="version", expression=StringLiteral(self.version, False)),
            ]
        )

        return QuerySpecification(
            select=Select(
                selectItems=[
                    SingleColumn(
                        alias=db_extension.quote_identifier(x.get_name()),
                        expression=QualifiedNameReference(
                            name=QualifiedName(parts=[root_alias, db_extension.quote_identifier(x.get_name())])
                        )
                    )
                    for x in self.columns
                ] if self.columns else [AllColumns(prefix=root_alias)],
                distinct=False
            ),
            from_=[
                AliasedRelation(
                    relation=TableFunction(functionCall=func_call),
                    alias=root_alias,
                    columnNames=[x.get_name() for x in self.columns] if self.columns else []
                )
            ],
            where=None,
            groupBy=[],
            having=None,
            orderBy=[],
            limit=None,
            offset=None
        )


def simple_person_service_frame(engine_port: int) -> LegendServiceFrame:
    legend_client = LegendClient("localhost", engine_port, False)
    return LegendServiceFrame(
        legend_client=legend_client,
        pattern="/simplePersonService",
        group_id="org.finos.legend.pylegend",
        artifact_id="pylegend-test-models",
        version="0.0.1-SNAPSHOT"
    )

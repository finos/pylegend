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
    PyLegendList,
    PyLegendSequence,
)
from pylegend.core.tds.tds_frame import (
    PyLegendTdsFrame,
    FrameToSqlConfig,
    FrameToPureConfig,
)
from pylegend.core.project_cooridnates import ProjectCoordinates
from pylegend.core.sql.metamodel import (
    QuerySpecification,
    TableFunction,
    Select,
    AllColumns,
    FunctionCall,
    QualifiedName,
    NamedArgumentExpression,
    StringLiteral,
    AliasedRelation,
    SingleColumn,
    QualifiedNameReference,
    Expression,
)

__all__: PyLegendSequence[str] = [
    "LegendServiceInputFrameAbstract",
]


class LegendServiceInputFrameAbstract(PyLegendTdsFrame, metaclass=ABCMeta):
    __pattern: str
    __project_coordinates: ProjectCoordinates
    __initialized: bool = False

    def __init__(
            self,
            pattern: str,
            project_coordinates: ProjectCoordinates,
    ) -> None:
        self.__pattern = pattern
        self.__project_coordinates = project_coordinates

    def to_sql_query_object(self, config: FrameToSqlConfig) -> QuerySpecification:
        db_extension = config.sql_to_string_generator().get_db_extension()
        root_alias = db_extension.quote_identifier("root")
        args: PyLegendList[Expression] = [
            NamedArgumentExpression(
                name="pattern",
                expression=StringLiteral(value=self.__pattern, quoted=False)
            )
        ]
        args += self.__project_coordinates.sql_params()
        func_call = FunctionCall(
            name=QualifiedName(["service"]),
            distinct=False,
            filter_=None,
            window=None,
            arguments=args
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
                    for x in self.columns()
                ] if self.__initialized else [AllColumns(prefix=root_alias)],
                distinct=False
            ),
            from_=[
                AliasedRelation(
                    relation=TableFunction(functionCall=func_call),
                    alias=root_alias,
                    columnNames=[x.get_name() for x in self.columns()] if self.__initialized else []
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
        raise RuntimeError("to_pure is not supported for LegendServiceInputFrame")

    def get_pattern(self) -> str:
        return self.__pattern

    def set_initialized(self, val: bool) -> None:
        self.__initialized = val

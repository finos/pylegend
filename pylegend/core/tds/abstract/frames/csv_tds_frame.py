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
from abc import ABCMeta
from pylegend._typing import (
    PyLegendSequence
)
from pylegend.core.tds.tds_column import TdsColumn
from pylegend.core.tds.abstract.frames.base_tds_frame import BaseTdsFrame
from pylegend.core.tds.tds_frame import FrameToPureConfig, FrameToSqlConfig
from pylegend.core.sql.metamodel import (
    QualifiedName,
    QualifiedNameReference,
    QuerySpecification,
    Select,
    SingleColumn,
    AliasedRelation,
    AllColumns,
    TableFunction,
    FunctionCall,
    StringLiteral
)

__all__: PyLegendSequence[str] = [
    "CsvTdsFrame"
]


class CsvTdsFrame(BaseTdsFrame, metaclass=ABCMeta):
    __initialized: bool = False
    __csv_string: str

    def __init__(
            self,
            csv_string: str,
            columns: PyLegendSequence[TdsColumn]) -> None:
        super().__init__(columns=columns)
        self.__csv_string = csv_string

    def get_all_tds_frames(self) -> PyLegendSequence["BaseTdsFrame"]:
        return [self]

    def to_sql_query_object(self, config: FrameToSqlConfig) -> QuerySpecification:
        db_extension = config.sql_to_string_generator().get_db_extension()
        root_alias = db_extension.quote_identifier("root")
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
                    relation=TableFunction(functionCall=FunctionCall(
                        name=QualifiedName(["CSV"]),
                        distinct=False,
                        filter_=None,
                        arguments=[
                            StringLiteral(self.__csv_string, quoted=False)
                        ],
                        window=None
                    )), alias="root", columnNames=[]
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
        return f"#TDS\n{self.__csv_string}#"

    def set_initialized(self, val: bool) -> None:
        self.__initialized = val

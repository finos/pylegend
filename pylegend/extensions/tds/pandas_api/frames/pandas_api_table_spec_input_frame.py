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
    PyLegendSequence
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
from pylegend.core.tds.pandas_api.frames.pandas_api_input_tds_frame import PandasApiNonExecutableInputTdsFrame
from pylegend.core.tds.tds_column import TdsColumn
from pylegend.core.tds.tds_frame import FrameToSqlConfig, FrameToPureConfig
from pylegend.extensions.tds.abstract.table_spec_input_frame import TableSpecInputFrameAbstract

__all__: PyLegendSequence[str] = [
    "PandasApiTableSpecInputFrame"
]


class PandasApiTableSpecInputFrame(TableSpecInputFrameAbstract, PandasApiNonExecutableInputTdsFrame):
    table: QualifiedName

    def __init__(self, table_name_parts: PyLegendList[str], columns: PyLegendSequence[TdsColumn]) -> None:
        TableSpecInputFrameAbstract.__init__(self, table_name_parts=table_name_parts)
        PandasApiNonExecutableInputTdsFrame.__init__(self, columns=columns)

    def __str__(self) -> str:
        return f"PandasApiTableSpecInputFrame({'.'.join(self.table.parts)})"  # pragma: no cover

    def to_sql_query_object(self, config: FrameToSqlConfig) -> QuerySpecification:
        if self._extra_frame is None:
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
        else:
            return self._extra_frame.to_sql_query_object(config)

    def to_pure(self, config: FrameToPureConfig) -> str:
        if self._extra_frame is None:
            return f"#Table({'.'.join(self.table.parts)})#"
        else:
            return self._extra_frame.to_pure(config)

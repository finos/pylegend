# Copyright 2026 Goldman Sachs
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
from pylegend.core.tds.pandas_api.frames.pandas_api_applied_function_tds_frame import PandasApiAppliedFunction
from pylegend.core.sql.metamodel import (
    QuerySpecification,
    Union,
    AliasedRelation,
    SingleColumn,
    Select,
    QualifiedNameReference,
    QualifiedName,
    TableSubquery,
    Query,
)
from pylegend.core.tds.sql_query_helpers import create_sub_query
from pylegend.core.tds.tds_column import TdsColumn
from pylegend.core.tds.tds_frame import FrameToSqlConfig, FrameToPureConfig
from pylegend.core.tds.pandas_api.frames.pandas_api_base_tds_frame import PandasApiBaseTdsFrame

__all__: PyLegendSequence[str] = [
    "PandasApiConcatFunction"
]


class PandasApiConcatFunction(PandasApiAppliedFunction):
    __base_frame: PandasApiBaseTdsFrame
    __other_frame: PandasApiBaseTdsFrame

    @classmethod
    def name(cls) -> str:
        return "concat_legend_ext"

    def __init__(self, base_frame: PandasApiBaseTdsFrame, other: PandasApiBaseTdsFrame) -> None:
        self.__base_frame = base_frame
        self.__other_frame = other

    def to_sql(self, config: FrameToSqlConfig) -> QuerySpecification:
        base_query = self.__base_frame.to_sql_query_object(config)
        other_query = self.__other_frame.to_sql_query_object(config)

        db_extension = config.sql_to_string_generator().get_db_extension()
        root_alias = db_extension.quote_identifier("root")
        columns = [db_extension.quote_identifier(c.get_name()) for c in self.__base_frame.columns()]

        return QuerySpecification(
            select=Select(
                selectItems=[
                    SingleColumn(
                        alias=x,
                        expression=QualifiedNameReference(name=QualifiedName(parts=[root_alias, x]))
                    )
                    for x in columns
                ],
                distinct=False
            ),
            from_=[
                AliasedRelation(
                    relation=TableSubquery(
                        query=Query(
                            queryBody=Union(
                                left=create_sub_query(base_query, config, "left"),
                                right=create_sub_query(other_query, config, "right"),
                                distinct=False
                            ),
                            limit=None, offset=None, orderBy=[]
                        )
                    ),
                    alias=root_alias,
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

    def to_pure(self, config: FrameToPureConfig) -> str:
        return (f"{self.__base_frame.to_pure(config)}{config.separator(1)}"
                f"->concatenate({config.separator(2)}"
                f"{self.__other_frame.to_pure(config.push_indent(2))}"
                f"{config.separator(1)})")

    def base_frame(self) -> PandasApiBaseTdsFrame:
        return self.__base_frame

    def tds_frame_parameters(self) -> PyLegendList["PandasApiBaseTdsFrame"]:
        return [self.__other_frame]

    def calculate_columns(self) -> PyLegendSequence["TdsColumn"]:
        return [c.copy() for c in self.__base_frame.columns()]

    def validate(self) -> bool:
        if not isinstance(self.__other_frame, PandasApiBaseTdsFrame):
            raise TypeError(
                f"Can only concatenate TdsFrame objects, a {type(self.__other_frame)} was passed"
            )

        base_frame_cols = self.__base_frame.columns()
        other_frame_cols = self.__other_frame.columns()

        if len(base_frame_cols) != len(other_frame_cols):
            cols1 = "[" + ", ".join([str(c) for c in base_frame_cols]) + "]"
            cols2 = "[" + ", ".join([str(c) for c in other_frame_cols]) + "]"
            raise ValueError(
                "Cannot concatenate two Tds Frames with different column counts. \n"
                f"Frame 1 cols - (Count: {len(base_frame_cols)}) - {cols1} \n"
                f"Frame 2 cols - (Count: {len(other_frame_cols)}) - {cols2} \n"
            )

        for i in range(0, len(base_frame_cols)):
            base_col = base_frame_cols[i]
            other_col = other_frame_cols[i]

            if (base_col.get_name() != other_col.get_name()) or (base_col.get_type() != other_col.get_type()):
                raise ValueError(
                    f"Column name/type mismatch when concatenating Tds Frames at index {i}. "
                    f"Frame 1 column - {base_col}, Frame 2 column - {other_col}"
                )

        return True

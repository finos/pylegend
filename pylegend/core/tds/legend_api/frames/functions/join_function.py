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
    PyLegendUnion,
    PyLegendCallable,
)
from pylegend.core.tds.legend_api.frames.legend_api_applied_function_tds_frame import LegendApiAppliedFunction
from pylegend.core.tds.sql_query_helpers import copy_query, create_sub_query, extract_columns_for_subquery
from pylegend.core.sql.metamodel import (
    QuerySpecification,
    Select,
    SelectItem,
    SingleColumn,
    AliasedRelation,
    TableSubquery,
    Query,
    Join,
    JoinType,
    JoinOn,
    QualifiedNameReference,
    QualifiedName,
)
from pylegend.core.tds.tds_column import TdsColumn
from pylegend.core.tds.tds_frame import FrameToSqlConfig
from pylegend.core.tds.legend_api.frames.legend_api_base_tds_frame import LegendApiBaseTdsFrame
from pylegend.core.tds.legend_api.frames.legend_api_tds_frame import LegendApiTdsFrame
from pylegend.core.language import (
    PyLegendBoolean,
    TdsRow,
    PyLegendBooleanLiteralExpression,
)


__all__: PyLegendSequence[str] = [
    "JoinFunction"
]


class JoinFunction(LegendApiAppliedFunction):
    __base_frame: LegendApiBaseTdsFrame
    __other_frame: LegendApiBaseTdsFrame
    __join_condition: PyLegendCallable[[TdsRow, TdsRow], PyLegendUnion[bool, PyLegendBoolean]]
    __join_type: str

    @classmethod
    def name(cls) -> str:
        return "join"

    def __init__(
            self,
            base_frame: LegendApiBaseTdsFrame,
            other_frame: LegendApiTdsFrame,
            join_condition: PyLegendCallable[[TdsRow, TdsRow], PyLegendUnion[bool, PyLegendBoolean]],
            join_type: str
    ) -> None:
        self.__base_frame = base_frame
        if not isinstance(other_frame, LegendApiBaseTdsFrame):
            raise ValueError("Expected LegendApiBaseTdsFrame")  # pragma: no cover
        self.__other_frame = other_frame
        self.__join_condition = join_condition
        self.__join_type = join_type

    def to_sql(self, config: FrameToSqlConfig) -> QuerySpecification:
        db_extension = config.sql_to_string_generator().get_db_extension()
        base_query = copy_query(self.__base_frame.to_sql_query_object(config))
        other_query = copy_query(self.__other_frame.to_sql_query_object(config))

        join_type = (
            JoinType.INNER if self.__join_type.lower() == 'inner' else (
                JoinType.LEFT if self.__join_type.lower() in ('left_outer', 'leftouter') else
                JoinType.RIGHT
            )
        )

        left_row = TdsRow.from_tds_frame('left', self.__base_frame)
        right_row = TdsRow.from_tds_frame('right', self.__other_frame)

        join_expr = self.__join_condition(left_row, right_row)
        if isinstance(join_expr, bool):
            join_expr = PyLegendBoolean(PyLegendBooleanLiteralExpression(join_expr))
        join_sql_expr = join_expr.to_sql_expression(
            {
                'left': create_sub_query(base_query, config, 'left'),
                'right': create_sub_query(other_query, config, 'right'),
            },
            config
        )

        left_alias = db_extension.quote_identifier('left')
        right_alias = db_extension.quote_identifier('right')
        new_select_items: PyLegendList[SelectItem] = []
        for c in self.__base_frame.columns():
            q = db_extension.quote_identifier(c.get_name())
            new_select_items.append(SingleColumn(q, QualifiedNameReference(name=QualifiedName(parts=[left_alias, q]))))
        for c in self.__other_frame.columns():
            q = db_extension.quote_identifier(c.get_name())
            new_select_items.append(SingleColumn(q, QualifiedNameReference(name=QualifiedName(parts=[right_alias, q]))))

        join_query = QuerySpecification(
            select=Select(
                selectItems=new_select_items,
                distinct=False
            ),
            from_=[
                Join(
                    type_=join_type,
                    left=AliasedRelation(
                        relation=TableSubquery(query=Query(queryBody=base_query, limit=None, offset=None, orderBy=[])),
                        alias=left_alias,
                        columnNames=extract_columns_for_subquery(base_query)
                    ),
                    right=AliasedRelation(
                        relation=TableSubquery(query=Query(queryBody=other_query, limit=None, offset=None, orderBy=[])),
                        alias=right_alias,
                        columnNames=extract_columns_for_subquery(other_query)
                    ),
                    criteria=JoinOn(expression=join_sql_expr)
                )
            ],
            where=None,
            groupBy=[],
            having=None,
            orderBy=[],
            limit=None,
            offset=None
        )

        wrapped_join_query = create_sub_query(join_query, config, "root")
        return wrapped_join_query

    def base_frame(self) -> LegendApiBaseTdsFrame:
        return self.__base_frame

    def tds_frame_parameters(self) -> PyLegendList["LegendApiBaseTdsFrame"]:
        return [self.__other_frame]

    def calculate_columns(self) -> PyLegendSequence["TdsColumn"]:
        return (
            [c.copy() for c in self.__base_frame.columns()] +
            [c.copy() for c in self.__other_frame.columns()]
        )

    def validate(self) -> bool:
        copy = self.__join_condition  # For MyPy
        if not isinstance(copy, type(lambda x: 0)) or (copy.__code__.co_argcount != 2):
            raise TypeError("Join condition function should be a lambda which takes two arguments (TDSRow, TDSRow)")

        left_row = TdsRow.from_tds_frame("left", self.__base_frame)
        right_row = TdsRow.from_tds_frame("right", self.__other_frame)

        try:
            result = self.__join_condition(left_row, right_row)
        except Exception as e:
            raise RuntimeError(
                "Join condition function incompatible. Error occurred while evaluating. Message: " + str(e)
            ) from e

        if not isinstance(result, (bool, PyLegendBoolean)):
            raise RuntimeError("Join condition function incompatible. Returns non boolean - " + str(type(result)))

        left_cols = [c.get_name() for c in self.__base_frame.columns()]
        right_cols = [c.get_name() for c in self.__other_frame.columns()]

        final_cols = left_cols + right_cols
        if len(final_cols) != len(set(final_cols)):
            raise ValueError(
                "Found duplicate columns in joined frames. Either use join_by_columns function if joining on shared "
                "columns or use rename_columns function to ensure there are no duplicate columns in joined frames. "
                f"Columns - Left Frame: {left_cols}, Right Frame: {right_cols}"
            )

        if self.__join_type.lower() not in ('inner', 'left_outer', 'right_outer', 'leftouter', 'rightouter'):
            raise ValueError(
                f"Unknown join type - {self.__join_type}. Supported types are - INNER, LEFT_OUTER, RIGHT_OUTER"
            )

        return True

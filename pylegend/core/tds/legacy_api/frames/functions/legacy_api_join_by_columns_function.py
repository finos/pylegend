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

import functools
from pylegend._typing import (
    PyLegendList,
    PyLegendSequence,
)
from pylegend.core.tds.legacy_api.frames.legacy_api_applied_function_tds_frame import LegacyApiAppliedFunction
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
    LogicalBinaryExpression,
    LogicalBinaryType,
    ComparisonExpression,
    ComparisonOperator,
    Expression,
)
from pylegend.core.tds.tds_column import TdsColumn
from pylegend.core.tds.tds_frame import FrameToSqlConfig
from pylegend.core.tds.tds_frame import FrameToPureConfig
from pylegend.core.tds.legacy_api.frames.legacy_api_base_tds_frame import LegacyApiBaseTdsFrame
from pylegend.core.tds.legacy_api.frames.legacy_api_tds_frame import LegacyApiTdsFrame
from pylegend.core.language.shared.helpers import generate_pure_lambda, escape_column_name


__all__: PyLegendSequence[str] = [
    "LegacyApiJoinByColumnsFunction"
]


class LegacyApiJoinByColumnsFunction(LegacyApiAppliedFunction):
    __base_frame: LegacyApiBaseTdsFrame
    __other_frame: LegacyApiBaseTdsFrame
    __column_names_self: PyLegendList[str]
    __column_names_other: PyLegendList[str]
    __join_type: str

    @classmethod
    def name(cls) -> str:
        return "join_by_columns"

    def __init__(
            self,
            base_frame: LegacyApiBaseTdsFrame,
            other_frame: LegacyApiTdsFrame,
            column_names_self: PyLegendList[str],
            column_names_other: PyLegendList[str],
            join_type: str
    ) -> None:
        self.__base_frame = base_frame
        if not isinstance(other_frame, LegacyApiBaseTdsFrame):
            raise ValueError("Expected LegacyApiBaseTdsFrame")  # pragma: no cover
        self.__other_frame = other_frame
        self.__column_names_self = column_names_self
        self.__column_names_other = column_names_other
        self.__join_type = join_type

    def to_sql(self, config: FrameToSqlConfig) -> QuerySpecification:
        db_extension = config.sql_to_string_generator().get_db_extension()
        base_query = copy_query(self.__base_frame.to_sql_query_object(config))
        other_query = copy_query(self.__other_frame.to_sql_query_object(config))
        left_alias = db_extension.quote_identifier('left')
        right_alias = db_extension.quote_identifier('right')

        join_type = (
            JoinType.INNER if self.__join_type.lower() == 'inner' else (
                JoinType.LEFT if self.__join_type.lower() in ('left_outer', 'leftouter') else
                JoinType.RIGHT
            )
        )

        def logical_and_expr(left: Expression, right: Expression) -> Expression:
            return LogicalBinaryExpression(
                type_=LogicalBinaryType.AND,
                left=left,
                right=right
            )
        join_expr = functools.reduce(
            logical_and_expr,  # type: ignore
            [
                ComparisonExpression(
                    left=QualifiedNameReference(
                        name=QualifiedName(parts=[left_alias, db_extension.quote_identifier(x)])
                    ),
                    right=QualifiedNameReference(
                        name=QualifiedName(parts=[right_alias, db_extension.quote_identifier(y)])
                    ),
                    operator=ComparisonOperator.EQUAL
                )
                for x, y in zip(self.__column_names_self, self.__column_names_other)
            ]
        )

        common_join_cols = [x for x, y in zip(self.__column_names_self, self.__column_names_other) if x == y]
        common_join_cols.sort()
        new_select_items: PyLegendList[SelectItem] = []
        for c in (c for c in self.__base_frame.columns() if c.get_name() not in common_join_cols):
            q = db_extension.quote_identifier(c.get_name())
            new_select_items.append(SingleColumn(q, QualifiedNameReference(name=QualifiedName(parts=[left_alias, q]))))
        for c in (c for c in self.__base_frame.columns() if c.get_name() in common_join_cols):
            q = db_extension.quote_identifier(c.get_name())
            common_col_alias = right_alias if self.__join_type.lower() == 'right_outer' else left_alias
            new_select_items.append(
                SingleColumn(q, QualifiedNameReference(name=QualifiedName(parts=[common_col_alias, q])))
            )
        for c in (c for c in self.__other_frame.columns() if c.get_name() not in common_join_cols):
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
                    criteria=JoinOn(expression=join_expr)
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

    def to_pure(self, config: FrameToPureConfig) -> str:
        common_join_cols = [x for x, y in zip(self.__column_names_self, self.__column_names_other) if x == y]
        other_frame = (self.__other_frame if (len(common_join_cols) == 0) else
                       self.__other_frame.rename_columns(common_join_cols, [c + "_gen_r" for c in common_join_cols]))
        sub_expressions = []
        for x, y in zip(self.__column_names_self, self.__column_names_other):
            left = "$l." + escape_column_name(x)
            y = (y + "_gen_r") if y in common_join_cols else y
            right = "$r." + escape_column_name(y)
            sub_expressions.append(f"({left} == {right})")

        join_expr_string = " && ".join(sub_expressions)
        join_kind = (
            "INNER" if self.__join_type.lower() == 'inner' else
            "LEFT" if self.__join_type.lower() in ('left_outer', 'leftouter') else
            "RIGHT" if self.__join_type.lower() in ('right_outer', 'rightouter') else
            "FULL"
        )
        return (f"{self.__base_frame.to_pure(config)}{config.separator(1)}" +
                f"->join({config.separator(2)}"
                f"{other_frame.to_pure(config.push_indent(2))},{config.separator(2, True)}"  # type: ignore
                f"JoinKind.{join_kind},{config.separator(2, True)}"
                f"{generate_pure_lambda('l, r', join_expr_string)}{config.separator(1)}"
                f")")

    def base_frame(self) -> LegacyApiBaseTdsFrame:
        return self.__base_frame

    def tds_frame_parameters(self) -> PyLegendList["LegacyApiBaseTdsFrame"]:
        return [self.__other_frame]

    def calculate_columns(self) -> PyLegendSequence["TdsColumn"]:
        common_join_cols = [x for x, y in zip(self.__column_names_self, self.__column_names_other) if x == y]
        common_join_cols.sort()
        return (
            [c.copy() for c in self.__base_frame.columns() if c.get_name() not in common_join_cols] +
            [c.copy() for c in self.__base_frame.columns() if c.get_name() in common_join_cols] +
            [c.copy() for c in self.__other_frame.columns() if c.get_name() not in common_join_cols]
        )

    def validate(self) -> bool:
        left_cols = [c.get_name() for c in self.__base_frame.columns()]
        for c in self.__column_names_self:
            if c not in left_cols:
                raise ValueError(
                    f"Column - '{c}' in join columns list doesn't exist in the left frame being joined. "
                    f"Current left frame columns: {left_cols}"
                )

        right_cols = [c.get_name() for c in self.__other_frame.columns()]
        for c in self.__column_names_other:
            if c not in right_cols:
                raise ValueError(
                    f"Column - '{c}' in join columns list doesn't exist in the right frame being joined. "
                    f"Current right frame columns: {right_cols}"
                )

        if len(self.__column_names_self) != len(self.__column_names_other):
            raise ValueError(
                "For join_by_columns function, column lists should be of same size. "
                f"Passed column list sizes -  Left: {len(self.__column_names_self)}, Right: {len(self.__column_names_other)}"
            )

        if len(self.__column_names_self) == 0:
            raise ValueError("For join_by_columns function, column lists should not be empty")

        common_join_cols = []
        for (x, y) in zip(self.__column_names_self, self.__column_names_other):
            left_col = list(filter(lambda c: c.get_name() == x, self.__base_frame.columns()))[0]
            right_col = list(filter(lambda c: c.get_name() == y, self.__other_frame.columns()))[0]

            if left_col.get_type() != right_col.get_type():
                raise ValueError(
                    f"Trying to join on columns with different types -  Left Col: {left_col}, Right Col: {right_col}"
                )

            if x == y:
                common_join_cols.append(x)

        final_cols = (
                [x for x in left_cols if x not in common_join_cols] +
                [x for x in right_cols if x not in common_join_cols] +
                common_join_cols
        )

        if len(final_cols) != len(set(final_cols)):
            raise ValueError(
                "Found duplicate columns in joined frames (which are not join keys). "
                f"Columns -  Left Frame: {left_cols}, Right Frame: {right_cols}, Common Join Keys: {common_join_cols}"
            )

        if self.__join_type.lower() not in ('inner', 'left_outer', 'right_outer', 'leftouter', 'rightouter'):
            raise ValueError(
                f"Unknown join type - {self.__join_type}. Supported types are - INNER, LEFT_OUTER, RIGHT_OUTER"
            )

        return True

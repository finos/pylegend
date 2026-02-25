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
    PyLegendSequence,
    PyLegendDict,
    PyLegendCallable,
    PyLegendList,
    PyLegendOptional
)
from pylegend.core.language.shared.expression import (
    PyLegendExpression,
)
from pylegend.core.language.shared.helpers import expr_has_matching_start_and_end_parentheses
from pylegend.core.sql.metamodel import (
    Expression,
    QuerySpecification,
)
from pylegend.core.tds.tds_frame import FrameToSqlConfig
from pylegend.core.tds.tds_frame import FrameToPureConfig

__all__: PyLegendSequence[str] = [
    "PyLegendNaryExpression"
]


class PyLegendNaryExpression(PyLegendExpression, metaclass=ABCMeta):
    __operands: PyLegendList[PyLegendExpression]
    __to_sql_func: PyLegendCallable[
        [PyLegendList[Expression], PyLegendDict[str, QuerySpecification], FrameToSqlConfig],
        Expression
    ]
    __to_pure_func: PyLegendCallable[
        [PyLegendList[str], FrameToPureConfig],
        str
    ]
    __non_nullable: bool
    __operands_non_nullable_flags: PyLegendList[bool]

    def __init__(
            self,
            operands: PyLegendList[PyLegendExpression],
            to_sql_func: PyLegendCallable[
                [PyLegendList[Expression], PyLegendDict[str, QuerySpecification], FrameToSqlConfig],
                Expression
            ],
            to_pure_func: PyLegendCallable[
                [PyLegendList[str], FrameToPureConfig],
                str
            ],
            non_nullable: bool = False,
            operands_non_nullable_flags: PyLegendOptional[PyLegendList[bool]] = None
    ) -> None:
        self.__operands = operands
        self.__to_sql_func = to_sql_func
        self.__to_pure_func = to_pure_func
        self.__non_nullable = non_nullable
        self.__operands_non_nullable_flags = (
            operands_non_nullable_flags
            if operands_non_nullable_flags is not None
            else [False] * len(operands)
        )

    def to_sql_expression(
            self,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        sql_operands = [
            operand.to_sql_expression(frame_name_to_base_query_map, config)
            for operand in self.__operands
        ]
        return self.__to_sql_func(sql_operands, frame_name_to_base_query_map, config)

    def to_pure_expression(self, config: FrameToPureConfig) -> str:
        from pylegend.core.language.pandas_api.pandas_api_series import Series
        from pylegend.core.language.pandas_api.pandas_api_groupby_series import GroupbySeries
        pure_operands: PyLegendList[str] = []

        for operand, must_be_non_nullable in zip(self.__operands, self.__operands_non_nullable_flags):
            expr = operand.to_pure_expression(config)

            if must_be_non_nullable:
                expr = (
                    expr if operand.is_non_nullable()
                    or (isinstance(operand, (Series, GroupbySeries)) and operand.expr is not None) else
                    f"toOne({expr[1:-1] if expr_has_matching_start_and_end_parentheses(expr) else expr})"
                )

            pure_operands.append(expr)

        return self.__to_pure_func(pure_operands, config)

    def is_non_nullable(self) -> bool:
        return self.__non_nullable

    def get_sub_expressions(self) -> PyLegendSequence["PyLegendExpression"]:
        sub_expressions: PyLegendList["PyLegendExpression"] = []
        for operand in self.__operands:
            sub_expressions += operand.get_sub_expressions()
        return sub_expressions

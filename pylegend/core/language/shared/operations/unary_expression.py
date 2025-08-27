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
    PyLegendDict,
    PyLegendCallable,
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
    "PyLegendUnaryExpression"
]


class PyLegendUnaryExpression(PyLegendExpression, metaclass=ABCMeta):
    __operand: PyLegendExpression
    __to_sql_func: PyLegendCallable[
        [Expression, PyLegendDict[str, QuerySpecification], FrameToSqlConfig],
        Expression
    ]
    __to_pure_func: PyLegendCallable[[str, FrameToPureConfig], str]
    __non_nullable: bool
    __operand_needs_to_be_non_nullable: bool

    def __init__(
            self,
            operand: PyLegendExpression,
            to_sql_func: PyLegendCallable[
                [Expression, PyLegendDict[str, QuerySpecification], FrameToSqlConfig],
                Expression
            ],
            to_pure_func: PyLegendCallable[[str, FrameToPureConfig], str],
            non_nullable: bool = False,
            operand_needs_to_be_non_nullable: bool = False,
    ) -> None:
        self.__operand = operand
        self.__to_sql_func = to_sql_func
        self.__to_pure_func = to_pure_func
        self.__non_nullable = non_nullable
        self.__operand_needs_to_be_non_nullable = operand_needs_to_be_non_nullable

    def to_sql_expression(
            self,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        op_expr = self.__operand.to_sql_expression(frame_name_to_base_query_map, config)
        return self.__to_sql_func(
            op_expr,
            frame_name_to_base_query_map,
            config
        )

    def to_pure_expression(self, config: FrameToPureConfig) -> str:
        op_expr = self.__operand.to_pure_expression(config)
        if self.__operand_needs_to_be_non_nullable:
            op_expr = (
                op_expr if self.__operand.is_non_nullable() else
                f"toOne({op_expr[1:-1] if expr_has_matching_start_and_end_parentheses(op_expr) else op_expr})"
            )
        return self.__to_pure_func(op_expr, config)

    def is_non_nullable(self) -> bool:
        return self.__non_nullable

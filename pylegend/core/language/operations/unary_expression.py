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
from pylegend.core.language.expression import (
    PyLegendExpression,
)
from pylegend.core.sql.metamodel import (
    Expression,
    QuerySpecification,
)
from pylegend.core.tds.tds_frame import FrameToSqlConfig


__all__: PyLegendSequence[str] = [
    "PyLegendUnaryExpression"
]


class PyLegendUnaryExpression(PyLegendExpression, metaclass=ABCMeta):
    __operand: PyLegendExpression
    __to_sql_func: PyLegendCallable[
        [Expression, PyLegendDict[str, QuerySpecification], FrameToSqlConfig],
        Expression
    ]

    def __init__(
            self,
            operand: PyLegendExpression,
            to_sql_func: PyLegendCallable[
                [Expression, PyLegendDict[str, QuerySpecification], FrameToSqlConfig],
                Expression
            ]
    ) -> None:
        self.__operand = operand
        self.__to_sql_func = to_sql_func

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

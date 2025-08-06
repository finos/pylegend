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

from pylegend._typing import (
    PyLegendList,
    PyLegendSequence,
    PyLegendCallable,
    PyLegendUnion,
)
from pylegend.core.language.legendql_api.legendql_api_tds_row import LegendQLApiTdsRow
from pylegend.core.tds.legendql_api.frames.legendql_api_applied_function_tds_frame import LegendQLApiAppliedFunction
from pylegend.core.tds.sql_query_helpers import copy_query, create_sub_query
from pylegend.core.sql.metamodel import (
    QuerySpecification,
    LogicalBinaryType,
    LogicalBinaryExpression,
)
from pylegend.core.tds.tds_column import TdsColumn
from pylegend.core.tds.tds_frame import FrameToSqlConfig
from pylegend.core.tds.tds_frame import FrameToPureConfig
from pylegend.core.tds.legendql_api.frames.legendql_api_base_tds_frame import LegendQLApiBaseTdsFrame
from pylegend.core.language import (
    PyLegendBoolean,
    PyLegendBooleanLiteralExpression,
    PyLegendPrimitive,
    convert_literal_to_literal_expression,
)
from pylegend.core.language.shared.helpers import generate_pure_lambda

__all__: PyLegendSequence[str] = [
    "LegendQLApiFilterFunction"
]


class LegendQLApiFilterFunction(LegendQLApiAppliedFunction):
    __base_frame: LegendQLApiBaseTdsFrame
    __filter_function: PyLegendCallable[[LegendQLApiTdsRow], PyLegendUnion[bool, PyLegendBoolean]]

    @classmethod
    def name(cls) -> str:
        return "filter"

    def __init__(
            self,
            base_frame: LegendQLApiBaseTdsFrame,
            filter_function: PyLegendCallable[[LegendQLApiTdsRow], PyLegendUnion[bool, PyLegendBoolean]]
    ) -> None:
        self.__base_frame = base_frame
        self.__filter_function = filter_function

    def to_sql(self, config: FrameToSqlConfig) -> QuerySpecification:
        base_query = self.__base_frame.to_sql_query_object(config)
        should_create_sub_query = (len(base_query.groupBy) > 0) or \
                                  (base_query.offset is not None) or (base_query.limit is not None)
        new_query = (
            create_sub_query(base_query, config, "root") if should_create_sub_query else
            copy_query(base_query)
        )

        tds_row = LegendQLApiTdsRow.from_tds_frame("frame", self.__base_frame)
        filter_expr = self.__filter_function(tds_row)
        if isinstance(filter_expr, bool):
            filter_expr = PyLegendBoolean(PyLegendBooleanLiteralExpression(filter_expr))
        filter_sql_expr = filter_expr.to_sql_expression(
            {"frame": new_query},
            config
        )

        if new_query.where is None:
            new_query.where = filter_sql_expr
        else:
            new_query.where = LogicalBinaryExpression(LogicalBinaryType.AND, new_query.where, filter_sql_expr)

        return new_query

    def to_pure(self, config: FrameToPureConfig) -> str:
        tds_row = LegendQLApiTdsRow.from_tds_frame("r", self.__base_frame)
        filter_expr = self.__filter_function(tds_row)
        filter_expr_string = (filter_expr.to_pure_expression(config) if isinstance(filter_expr, PyLegendPrimitive) else
                              convert_literal_to_literal_expression(filter_expr).to_pure_expression(config))
        return (f"{self.__base_frame.to_pure(config)}{config.separator(1)}" +
                f"->filter({generate_pure_lambda('r', filter_expr_string)})")

    def base_frame(self) -> LegendQLApiBaseTdsFrame:
        return self.__base_frame

    def tds_frame_parameters(self) -> PyLegendList["LegendQLApiBaseTdsFrame"]:
        return []

    def calculate_columns(self) -> PyLegendSequence["TdsColumn"]:
        return [c.copy() for c in self.__base_frame.columns()]

    def validate(self) -> bool:
        tds_row = LegendQLApiTdsRow.from_tds_frame("frame", self.__base_frame)

        copy = self.__filter_function  # For MyPy
        if not isinstance(copy, type(lambda x: 0)) or (copy.__code__.co_argcount != 1):
            raise TypeError("Filter function should be a lambda which takes one argument (TDSRow)")

        try:
            result = self.__filter_function(tds_row)
        except Exception as e:
            raise RuntimeError(
                "Filter function incompatible. Error occurred while evaluating. Message: " + str(e)
            ) from e

        if not isinstance(result, (bool, PyLegendBoolean)):
            raise RuntimeError("Filter function incompatible. Returns non boolean - " + str(type(result)))

        return True

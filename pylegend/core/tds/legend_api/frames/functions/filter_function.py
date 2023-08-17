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
    PyLegendCallable,
    PyLegendUnion,
)
from pylegend.core.tds.legend_api.frames.legend_api_applied_function_tds_frame import LegendApiAppliedFunction
from pylegend.core.tds.sql_query_helpers import copy_query, create_sub_query
from pylegend.core.sql.metamodel import (
    QuerySpecification,
    LogicalBinaryType,
    LogicalBinaryExpression,
)
from pylegend.core.tds.tds_column import TdsColumn
from pylegend.core.tds.tds_frame import FrameToSqlConfig
from pylegend.core.tds.legend_api.frames.legend_api_base_tds_frame import LegendApiBaseTdsFrame
from pylegend.core.language import (
    TdsRow,
    PyLegendBoolean,
    PyLegendBooleanLiteralExpression,
)

__all__: PyLegendSequence[str] = [
    "FilterFunction"
]


class FilterFunction(LegendApiAppliedFunction):
    __base_frame: LegendApiBaseTdsFrame
    __filter_function: PyLegendCallable[[TdsRow], PyLegendUnion[bool, PyLegendBoolean]]

    @classmethod
    def name(cls) -> str:
        return "filter"

    def __init__(
            self,
            base_frame: LegendApiBaseTdsFrame,
            filter_function: PyLegendCallable[[TdsRow], PyLegendUnion[bool, PyLegendBoolean]]
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

        tds_row = TdsRow.from_tds_frame("frame", self.__base_frame)
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

    def base_frame(self) -> LegendApiBaseTdsFrame:
        return self.__base_frame

    def tds_frame_parameters(self) -> PyLegendList["LegendApiBaseTdsFrame"]:
        return []

    def calculate_columns(self) -> PyLegendSequence["TdsColumn"]:
        return [c.copy() for c in self.__base_frame.columns()]

    def validate(self) -> bool:
        tds_row = TdsRow.from_tds_frame("frame", self.__base_frame)

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

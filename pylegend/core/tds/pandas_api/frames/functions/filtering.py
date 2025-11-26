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
    PyLegendSequence,
    PyLegendList
)
from pylegend.core.language import PyLegendBoolean
from pylegend.core.sql.metamodel import (
    QuerySpecification,
    LogicalBinaryExpression,
    LogicalBinaryType,
)
from pylegend.core.tds.pandas_api.frames.pandas_api_applied_function_tds_frame import PandasApiAppliedFunction
from pylegend.core.tds.pandas_api.frames.pandas_api_base_tds_frame import PandasApiBaseTdsFrame
from pylegend.core.tds.sql_query_helpers import copy_query, create_sub_query
from pylegend.core.tds.tds_column import TdsColumn
from pylegend.core.tds.tds_frame import FrameToSqlConfig, FrameToPureConfig

__all__: PyLegendSequence[str] = ["PandasApiFilteringFunction"]


class PandasApiFilteringFunction(PandasApiAppliedFunction):
    __base_frame: PandasApiBaseTdsFrame
    __filter_expr: PyLegendBoolean

    @classmethod
    def name(cls) -> str:
        return "boolean_filter"  # pragma: no cover

    def __init__(
            self,
            base_frame: PandasApiBaseTdsFrame,
            filter_expr: PyLegendBoolean
    ) -> None:
        self.__base_frame = base_frame
        self.__filter_expr = filter_expr

    def to_sql(self, config: FrameToSqlConfig) -> QuerySpecification:
        base_query = self.__base_frame.to_sql_query_object(config)
        should_create_sub_query = (len(base_query.groupBy) > 0) or \
                                  (base_query.offset is not None) or (base_query.limit is not None)
        new_query = (
            create_sub_query(base_query, config, "root") if should_create_sub_query else
            copy_query(base_query)
        )

        sql_expr = self.__filter_expr.to_sql_expression({"c": new_query}, config)

        if new_query.where is None:
            new_query.where = sql_expr
        else:
            new_query.where = LogicalBinaryExpression(
                type_=LogicalBinaryType.AND,
                left=new_query.where,
                right=sql_expr
            )
        return new_query

    def to_pure(self, config: FrameToPureConfig) -> str:
        pure_expr = self.__filter_expr.to_pure_expression(config)
        return f"{self.__base_frame.to_pure(config)}{config.separator(1)}->filter(c|{pure_expr})"

    def base_frame(self) -> PandasApiBaseTdsFrame:
        return self.__base_frame

    def tds_frame_parameters(self) -> PyLegendList["PandasApiBaseTdsFrame"]:
        return []

    def calculate_columns(self) -> PyLegendSequence["TdsColumn"]:
        return [c.copy() for c in self.__base_frame.columns()]

    def validate(self) -> bool:
        return True

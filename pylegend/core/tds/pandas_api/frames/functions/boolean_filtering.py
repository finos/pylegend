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

from datetime import date, datetime

from pylegend.core.language.pandas_api.pandas_api_custom_expressions import PandasApiLogicalExpression
from pylegend.core.language.shared.expression import PyLegendExpressionBooleanReturn

from pylegend.core.language.shared.literal_expressions import convert_literal_to_literal_expression

from pylegend.core.language.shared.primitive_collection import create_primitive_collection

from pylegend._typing import (
    PyLegendList,
    PyLegendSequence,
    PyLegendDict,
    PyLegendCallable,
    PyLegendUnion,
)
from pylegend.core.tds.pandas_api.frames.functions.comparator_filtering import PandasApiComparatorFiltering

from pylegend.core.tds.pandas_api.frames.pandas_api_applied_function_tds_frame import PandasApiAppliedFunction
from pylegend.core.tds.sql_query_helpers import copy_query, create_sub_query
from pylegend.core.sql.metamodel import (
    QuerySpecification,
    SingleColumn, Select, QualifiedNameReference, QualifiedName, ComparisonExpression, LogicalBinaryExpression,
    LogicalBinaryType,
)
from pylegend.core.tds.pandas_api.frames.pandas_api_base_tds_frame import PandasApiBaseTdsFrame
from pylegend.core.tds.tds_column import TdsColumn, PrimitiveTdsColumn
from pylegend.core.tds.tds_frame import FrameToSqlConfig, FrameToPureConfig
from pylegend.core.language import (
    LegacyApiTdsRow,
    PyLegendPrimitive,
    PyLegendInteger,
    PyLegendFloat,
    PyLegendNumber,
    PyLegendBoolean,
    PyLegendString,
)
from pylegend._typing import *


class PandasApiBooleanFilteringFunction:
    __base_frame: PandasApiBaseTdsFrame
    __filter_expr: PyLegendUnion[PandasApiComparatorFiltering, PandasApiLogicalExpression]

    @classmethod
    def name(cls) -> str:
        return "boolean_filter"  # pragma : no cover

    def __init__(
            self,
            base_frame: PandasApiBaseTdsFrame,
            filter_expr: PyLegendUnion[PandasApiComparatorFiltering, PandasApiLogicalExpression]
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

        sql_expr = self.__filter_expr.to_sql(config)

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
        pure_expr = self.__filter_expr.to_pure(config)
        return f"{self.__base_frame.to_pure(config)}->filter(c|{pure_expr})"

    def base_frame(self) -> PandasApiBaseTdsFrame:
        return self.__base_frame

    def tds_frame_parameters(self) -> PyLegendList["PandasApiBaseTdsFrame"]:
        return []

    def calculate_columns(self) -> PyLegendSequence["TdsColumn"]:
        return [c.copy() for c in self.__base_frame.columns()]

    def validate(self) -> bool:
        return True

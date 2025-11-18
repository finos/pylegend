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
from pylegend.core.language.shared.operations.primitive_operation_expressions import PyLegendPrimitiveEqualsExpression, \
    PyLegendPrimitiveNotEqualsExpression, PyLegendPrimitiveLessThanExpression, \
    PyLegendPrimitiveLessThanOrEqualExpression, PyLegendPrimitiveGreaterThanExpression, \
    PyLegendPrimitiveGreaterThanOrEqualExpression

from pylegend.core.language.shared.literal_expressions import convert_literal_to_literal_expression

from pylegend.core.language.shared.primitive_collection import create_primitive_collection

from pylegend._typing import (
    PyLegendList,
    PyLegendSequence,
    PyLegendDict,
    PyLegendCallable,
    PyLegendUnion,
)

from pylegend.core.tds.pandas_api.frames.pandas_api_applied_function_tds_frame import PandasApiAppliedFunction
from pylegend.core.tds.sql_query_helpers import copy_query, create_sub_query
from pylegend.core.sql.metamodel import (
    QuerySpecification,
    SingleColumn, Select, QualifiedNameReference, QualifiedName, ComparisonExpression, LogicalBinaryExpression,
    LogicalBinaryType, ComparisonOperator,
)
from pylegend.core.tds.pandas_api.frames.pandas_api_base_tds_frame import PandasApiBaseTdsFrame
from pylegend.core.tds.tds_column import TdsColumn, PrimitiveTdsColumn, PandasApiTdsColumn
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

class PandasApiComparatorFiltering(PandasApiAppliedFunction):
    __base_frame: PandasApiBaseTdsFrame
    __column: PandasApiTdsColumn
    __operator: ComparisonOperator
    __value: PyLegendPrimitive

    @classmethod
    def name(cls) -> str:
        return "comparator_filter"  # pragma : no cover

    def __init__(
            self,
            base_frame: PandasApiBaseTdsFrame,
            column: PandasApiTdsColumn,
            operator: ComparisonOperator,
            value: PyLegendPrimitive
    ) -> None:
        self.__base_frame = base_frame
        self.__column = column
        self.__operator = operator
        self.__value = value

    def __and__(self, other):
        if not isinstance(other, (PandasApiComparatorFiltering, PandasApiLogicalExpression)):
            raise TypeError(f"Unsupported operand type(s) for &: '{type(self)}' and '{type(other)}'")
        return PandasApiLogicalExpression(self, LogicalBinaryType.AND, other)

    def __or__(self, other):
        if not isinstance(other, (PandasApiComparatorFiltering, PandasApiLogicalExpression)):
            raise TypeError(f"Unsupported operand type(s) for |: '{type(self)}' and '{type(other)}'")
        return PandasApiLogicalExpression(self, LogicalBinaryType.OR, other)

    def __invert__(self):
        return PandasApiLogicalExpression(self, LogicalBinaryType.NOT)

    def to_sql(self, config: FrameToSqlConfig) -> QuerySpecification:
        if self.__operator == ComparisonOperator.EQUAL:
            return PyLegendPrimitiveEqualsExpression._PyLegendPrimitiveEqualsExpression__to_sql_func(
                QualifiedNameReference(QualifiedName(['"root"', self.__column.get_name()])),
                convert_literal_to_literal_expression(self.__value).to_sql_expression({}, config),
                {},
                config
            )
        elif self.__operator == ComparisonOperator.NOT_EQUAL:
            return PyLegendPrimitiveNotEqualsExpression._PyLegendPrimitiveNotEqualsExpression__to_sql_func(
                QualifiedNameReference(QualifiedName(['"root"', self.__column.get_name()])),
                convert_literal_to_literal_expression(self.__value).to_sql_expression({}, config),
                {},
                config
            )
        elif self.__operator == ComparisonOperator.LESS_THAN:
            return PyLegendPrimitiveLessThanExpression._PyLegendPrimitiveLessThanExpression__to_sql_func(
                QualifiedNameReference(QualifiedName(['"root"', self.__column.get_name()])),
                convert_literal_to_literal_expression(self.__value).to_sql_expression({}, config),
                {},
                config
            )
        elif self.__operator == ComparisonOperator.LESS_THAN_OR_EQUAL:
            return PyLegendPrimitiveLessThanOrEqualExpression._PyLegendPrimitiveLessThanOrEqualExpression__to_sql_func(
                QualifiedNameReference(QualifiedName(['"root"', self.__column.get_name()])),
                convert_literal_to_literal_expression(self.__value).to_sql_expression({}, config),
                {},
                config
            )
        elif self.__operator == ComparisonOperator.GREATER_THAN:
            return PyLegendPrimitiveGreaterThanExpression._PyLegendPrimitiveGreaterThanExpression__to_sql_func(
                QualifiedNameReference(QualifiedName(['"root"', self.__column.get_name()])),
                convert_literal_to_literal_expression(self.__value).to_sql_expression({}, config),
                {},
                config
            )
        elif self.__operator == ComparisonOperator.GREATER_THAN_OR_EQUAL:
            return PyLegendPrimitiveGreaterThanOrEqualExpression._PyLegendPrimitiveGreaterThanOrEqualExpression__to_sql_func(
                QualifiedNameReference(QualifiedName(['"root"', self.__column.get_name()])),
                convert_literal_to_literal_expression(self.__value).to_sql_expression({}, config),
                {},
                config
            )
        else:
            raise ValueError(f"Unsupported operator: {self.__operator} for Pandas API")

    def to_pure(self, config: FrameToPureConfig) -> str:
        if self.__operator == ComparisonOperator.EQUAL:
            return PyLegendPrimitiveEqualsExpression._PyLegendPrimitiveEqualsExpression__to_pure_func(
                self.__column.to_pure_expression(),
                convert_literal_to_literal_expression(self.__value).to_pure_expression(config),
                config
            )
        elif self.__operator == ComparisonOperator.NOT_EQUAL:
            return PyLegendPrimitiveNotEqualsExpression._PyLegendPrimitiveNotEqualsExpression__to_pure_func(
                self.__column.to_pure_expression(),
                convert_literal_to_literal_expression(self.__value).to_pure_expression(config),
                config
            )
        elif self.__operator == ComparisonOperator.LESS_THAN:
            return PyLegendPrimitiveLessThanExpression._PyLegendPrimitiveLessThanExpression__to_pure_func(
                self.__column.to_pure_expression(),
                convert_literal_to_literal_expression(self.__value).to_pure_expression(config),
                config
            )
        elif self.__operator == ComparisonOperator.LESS_THAN_OR_EQUAL:
            return PyLegendPrimitiveLessThanOrEqualExpression._PyLegendPrimitiveLessThanOrEqualExpression__to_pure_func(
                self.__column.to_pure_expression(),
                convert_literal_to_literal_expression(self.__value).to_pure_expression(config),
                config
            )
        elif self.__operator == ComparisonOperator.GREATER_THAN:
            return PyLegendPrimitiveGreaterThanExpression._PyLegendPrimitiveGreaterThanExpression__to_pure_func(
                self.__column.to_pure_expression(),
                convert_literal_to_literal_expression(self.__value).to_pure_expression(config),
                config
            )
        elif self.__operator == ComparisonOperator.GREATER_THAN_OR_EQUAL:
            return PyLegendPrimitiveGreaterThanOrEqualExpression._PyLegendPrimitiveGreaterThanOrEqualExpression__to_pure_func(
                self.__column.to_pure_expression(),
                convert_literal_to_literal_expression(self.__value).to_pure_expression(config),
                config
            )
        else:
            raise ValueError(f"Unsupported operator: {self.__operator} for Pandas API")

    def base_frame(self) -> PandasApiBaseTdsFrame:
        return self.__base_frame

    def tds_frame_parameters(self) -> PyLegendList["PandasApiBaseTdsFrame"]:
        return []

    def calculate_columns(self) -> PyLegendSequence["TdsColumn"]:
        new_cols = [c.copy() for c in self.__base_frame.columns()]
        return new_cols

    def validate(self) -> bool:
        return True


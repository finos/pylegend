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

from pylegend.core.language.shared.operations.boolean_operation_expressions import PyLegendBooleanAndExpression, \
    PyLegendBooleanOrExpression, PyLegendBooleanNotExpression
from pylegend.core.sql.metamodel import LogicalBinaryExpression, LogicalBinaryType, NotExpression

from pylegend.core.tds.tds_frame import FrameToSqlConfig, FrameToPureConfig


class PandasApiLogicalExpression:
    def __init__(self, left, operator=None, right=None):
        self.left = left
        self.operator = operator
        self.right = right

    def __and__(self, other):
        from pylegend.core.tds.pandas_api.frames.functions.comparator_filtering import PandasApiComparatorFiltering
        if not isinstance(other, (PandasApiComparatorFiltering, PandasApiLogicalExpression)):
            raise TypeError(f"Unsupported operand type(s) for &: '{type(self)}' and '{type(other)}'")
        return PandasApiLogicalExpression(self, LogicalBinaryType.AND, other)

    def __or__(self, other):
        from pylegend.core.tds.pandas_api.frames.functions.comparator_filtering import PandasApiComparatorFiltering
        if isinstance(other, (PandasApiLogicalExpression, PandasApiComparatorFiltering)):
            return PandasApiLogicalExpression(self, LogicalBinaryType.OR, other)
        else:
            raise TypeError(f"Unsupported operand type(s) for |: '{type(self)}' and '{type(other)}'")

    def __invert__(self):
        return PandasApiLogicalExpression(self, LogicalBinaryType.NOT)

    def to_sql(self, config: FrameToSqlConfig) -> LogicalBinaryExpression:
        if self.operator == LogicalBinaryType.AND:
            return LogicalBinaryExpression(
                LogicalBinaryType.AND,
                self.left.to_sql(config),
                self.right.to_sql(config)
            )
        elif self.operator == LogicalBinaryType.OR:
            return LogicalBinaryExpression(
                LogicalBinaryType.OR,
                self.left.to_sql(config),
                self.right.to_sql(config)
            )
        elif self.operator == LogicalBinaryType.NOT:
            return NotExpression(self.left.to_sql(config))
        else:
            raise SyntaxError("invalid syntax")

    def to_pure(self, config: FrameToPureConfig) -> str:
        if self.operator == LogicalBinaryType.AND:
            return PyLegendBooleanAndExpression._PyLegendBooleanAndExpression__to_pure_func(
                self.left.to_pure(config),
                self.right.to_pure(config),
                config
            )
        elif self.operator == LogicalBinaryType.OR:
            return PyLegendBooleanOrExpression._PyLegendBooleanOrExpression__to_pure_func(
                self.left.to_pure(config),
                self.right.to_pure(config),
                config
            )
        elif self.operator == LogicalBinaryType.NOT:
            return PyLegendBooleanNotExpression._PyLegendBooleanNotExpression__to_pure_func(
                self.left.to_pure(config),
                config
            )
        else:
            raise SyntaxError("invalid syntax")
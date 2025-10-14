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
from pylegend.core.sql.metamodel import LogicalBinaryExpression, LogicalBinaryType

from pylegend.core.tds.tds_frame import FrameToSqlConfig, FrameToPureConfig


class PandasApiLogicalExpression:
    def __init__(self, left, operator, right):
        self.left = left
        self.operator = operator  # LogicalBinaryType.AND or LogicalBinaryType.OR
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

    def to_sql(self, config: FrameToSqlConfig) -> LogicalBinaryExpression:
        return LogicalBinaryExpression(
            self.operator,
            self.left.to_sql(config),
            self.right.to_sql(config)
        )

    def to_pure(self, config: FrameToPureConfig) -> str:
        left_pure = self.left.to_pure(config)
        right_pure = self.right.to_pure(config)
        operator_str = " && " if self.operator == LogicalBinaryType.AND else " || "
        return f"({left_pure}{operator_str}{right_pure})"
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

from enum import Enum
from typing import List

from legendql.dialect.purerelation.dialect import PureRelationExpressionVisitor
from legendql.model.metamodel import ExecutionVisitor, Clause


class DialectType(Enum):
    PURE_RELATION = "pure_relation"

    def to_string(self, clauses: List[Clause]) -> str:
        if self == DialectType.PURE_RELATION:
            visitor = PureRelationExpressionVisitor()
            return "->".join(map(lambda clause: clause.visit(visitor, ""), clauses)) + "->from(legendql::Runtime)"

        raise ValueError(f"Unsupported dialect type: {self}")

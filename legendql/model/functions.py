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

from dataclasses import dataclass
from typing import TypeVar

from legendql.model.metamodel import Function, ExecutionVisitor

P = TypeVar("P")
T = TypeVar("T")

@dataclass
class AggregationFunction(Function):
    # sum(), min(), max(), count(), avg()
    def visit(self, visitor: ExecutionVisitor, parameter: P) -> T:
        raise NotImplementedError()

@dataclass
class ScalarFunction(Function):
    # date_diff(), left(), abs() ..
    def visit(self, visitor: ExecutionVisitor, parameter: P) -> T:
        raise NotImplementedError()

@dataclass
class WindowFunction(Function):
    # rank(), row_number(), first(), last() ..
    def visit(self, visitor: ExecutionVisitor, parameter: P) -> T:
        raise NotImplementedError()

@dataclass
class RankFunction(WindowFunction):
    def visit(self, visitor: ExecutionVisitor, parameter: P) -> T:
        raise NotImplementedError()

@dataclass
class RowNumberFunction(WindowFunction):
    def visit(self, visitor: ExecutionVisitor, parameter: P) -> T:
        raise NotImplementedError()

@dataclass
class LeadFunction(WindowFunction):
    def visit(self, visitor: ExecutionVisitor, parameter: P) -> T:
        raise NotImplementedError()

@dataclass
class LagFunction(WindowFunction):
    def visit(self, visitor: ExecutionVisitor, parameter: P) -> T:
        raise NotImplementedError()

@dataclass
class LeftFunction(ScalarFunction):
    def visit(self, visitor: ExecutionVisitor, parameter: P) -> T:
        raise NotImplementedError()

@dataclass
class StringConcatFunction(ScalarFunction):
    def visit(self, visitor: ExecutionVisitor, parameter: P) -> T:
        raise NotImplementedError()

@dataclass
class AvgFunction(AggregationFunction):
    def visit(self, visitor: ExecutionVisitor, parameter: P) -> T:
        raise NotImplementedError()

@dataclass
class CountFunction(AggregationFunction):
    def visit(self, visitor: ExecutionVisitor, parameter: P) -> T:
        return visitor.visit_count_function(self, parameter)

@dataclass
class SumFunction(AggregationFunction):
    def visit(self, visitor: ExecutionVisitor, parameter: P) -> T:
        return visitor.visit_sum_function(self, parameter)

@dataclass
class OverFunction(ScalarFunction):
    def visit(self, visitor: ExecutionVisitor, parameter: P) -> T:
        raise NotImplementedError()

@dataclass
class RowsFunction(ScalarFunction):
    def visit(self, visitor: ExecutionVisitor, parameter: P) -> T:
        raise NotImplementedError()

@dataclass
class RangeFunction(ScalarFunction):
    def visit(self, visitor: ExecutionVisitor, parameter: P) -> T:
        raise NotImplementedError()

@dataclass
class UnboundedFunction(ScalarFunction):
    def visit(self, visitor: ExecutionVisitor, parameter: P) -> T:
        raise NotImplementedError()

@dataclass
class AggregateFunction(ScalarFunction):
    def visit(self, visitor: ExecutionVisitor, parameter: P) -> T:
        raise NotImplementedError()

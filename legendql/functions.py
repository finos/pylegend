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

import ast
from abc import ABC
from dataclasses import dataclass
from typing import Union, Optional


class AggregationFunction:
    pass

class AvgFunction(AggregationFunction, float):
    pass

class CountFunction(AggregationFunction, int):
    pass

class SumFunction(AggregationFunction, float):
    pass

class LeftFunction(str):
    def __init__(self, expr: ast.Expr, count_: int):
        pass

class WindowFunction:
    pass

class RankFunction(WindowFunction, int):
    pass

class RowNumberFunction(WindowFunction, int):
    pass

class LeadFunction(WindowFunction, int):
    def __init__(self,
        expression: ast.Expr,
        offset: Optional[int] = 1,
        default: Optional[ast.Expr] = None):
        pass

class LagFunction(WindowFunction, int):
    def __init__(self,
        expression: ast.Expr,
        offset: Optional[int] = 1,
        default: Optional[ast.Expr] = None):
        pass

@dataclass
class AggregateFunction:
    def __init__(self,
        columns: Union[ast.expr, list[ast.expr]],
        functions: Union[AggregationFunction, list[AggregationFunction]],
        having: Optional[bool] = None):
        pass

@dataclass
class UnboundedFunction:
    def __init__(self):
        pass


@dataclass
class Frame(ABC):
    pass


@dataclass
class RowsFunction(Frame):
    def __init__(self, start: Union[int, UnboundedFunction], end: Union[int, UnboundedFunction]):
        super().__init__(start, end)


@dataclass
class RangeFunction(Frame):
    def __init__(self, start: Union[int, UnboundedFunction], end: Union[int, UnboundedFunction]):
        super().__init__(start, end)

@dataclass
class OverFunction:
    def __init__(self,
        columns: Union[ast.expr, list[ast.expr]],
        functions: Union[AggregationFunction, WindowFunction, list[AggregationFunction], list[WindowFunction]],
        sort: Optional[Union[ast.expr, list[ast.expr]]] = None,
        frame: Optional[Frame] = None,
        qualify: Optional[bool] = None):
        pass

aggregate = AggregateFunction
over = OverFunction
unbounded = UnboundedFunction
rows = RowsFunction
range = RangeFunction
left = LeftFunction
avg = AvgFunction
count = CountFunction
sum = SumFunction
rank = RankFunction
lead = LeadFunction
lag = LagFunction
row_number = RowNumberFunction

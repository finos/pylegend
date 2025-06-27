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

from __future__ import annotations
from abc import ABC, abstractmethod
from datetime import date
from typing import List, Optional, TypeVar
from dataclasses import dataclass

T = TypeVar("T")
P = TypeVar("P")

class Literal(ABC):
    @abstractmethod
    def value(self) -> T:
        pass

    @abstractmethod
    def visit(self, visitor: ExecutionVisitor, parameter: P) -> T:
        pass

@dataclass
class IntegerLiteral(Literal):
    val: int
    def __init__(self, val):
        self.val = val

    def value(self) -> int:
        return self.val

    def visit(self, visitor: ExecutionVisitor, parameter: P) -> T:
        return visitor.visit_integer_literal(self, parameter)

@dataclass
class StringLiteral(Literal):
    val: str
    def __init__(self, val):
        self.val = val

    def value(self) -> str:
        return self.val

    def visit(self, visitor: ExecutionVisitor, parameter: P) -> T:
        return visitor.visit_string_literal(self, parameter)

@dataclass
class DateLiteral(Literal):
    val: date
    def __init__(self, val):
        self.val = val

    def value(self) -> date:
        return self.val

    def visit(self, visitor: ExecutionVisitor, parameter: P) -> T:
        return visitor.visit_date_literal(self, parameter)

@dataclass
class BooleanLiteral(Literal):
    val: bool
    def __init__(self, val):
        self.val = val

    def value(self) -> bool:
        return self.val

    def visit(self, visitor: ExecutionVisitor, parameter: P) -> T:
        return visitor.visit_boolean_literal(self, parameter)

class Function(ABC):
    @abstractmethod
    def visit(self, visitor: ExecutionVisitor, parameter: P) -> T:
        pass

@dataclass
class CountFunction(Function):
    def visit(self, visitor: ExecutionVisitor, parameter: P) -> T:
        return visitor.visit_count_function(self, parameter)

@dataclass
class AverageFunction(Function):
    def visit(self, visitor: ExecutionVisitor, parameter: P) -> T:
        return visitor.visit_average_function(self, parameter)

@dataclass
class ModuloFunction(Function):
    def visit(self, visitor: ExecutionVisitor, parameter: P) -> T:
        return visitor.visit_modulo_function(self, parameter)

@dataclass
class ExponentFunction(Function):
    def visit(self, visitor: ExecutionVisitor, parameter: P) -> T:
        return visitor.visit_exponent_function(self, parameter)

class Expression(ABC):
    @abstractmethod
    def visit(self, visitor: ExecutionVisitor, parameter: P) -> T:
        pass

class Operator(ABC):
    @abstractmethod
    def visit(self, visitor: ExecutionVisitor, parameter: P) -> T:
        pass

class UnaryOperator(Operator, ABC):
    pass

@dataclass
class NotUnaryOperator(UnaryOperator):
    def visit(self, visitor: ExecutionVisitor, parameter: P) -> T:
        return visitor.visit_not_unary_operator(self, parameter)

class BinaryOperator(Operator, ABC):
    pass

@dataclass
class EqualsBinaryOperator(BinaryOperator):
    def visit(self, visitor: ExecutionVisitor, parameter: P) -> T:
        return visitor.visit_equals_binary_operator(self, parameter)

@dataclass
class NotEqualsBinaryOperator(BinaryOperator):
    def visit(self, visitor: ExecutionVisitor, parameter: P) -> T:
        return visitor.visit_not_equals_binary_operator(self, parameter)

@dataclass
class GreaterThanBinaryOperator(BinaryOperator):
    def visit(self, visitor: ExecutionVisitor, parameter: P) -> T:
        return visitor.visit_greater_than_binary_operator(self, parameter)

@dataclass
class GreaterThanEqualsBinaryOperator(BinaryOperator):
    def visit(self, visitor: ExecutionVisitor, parameter: P) -> T:
        return visitor.visit_greater_than_equals_operator(self, parameter)

@dataclass
class LessThanBinaryOperator(BinaryOperator):
    def visit(self, visitor: ExecutionVisitor, parameter: P) -> T:
        return visitor.visit_less_than_binary_operator(self, parameter)

@dataclass
class LessThanEqualsBinaryOperator(BinaryOperator):
    def visit(self, visitor: ExecutionVisitor, parameter: P) -> T:
        return visitor.visit_less_than_equals_binary_operator(self, parameter)

@dataclass
class InBinaryOperator(BinaryOperator):
    def visit(self, visitor: ExecutionVisitor, parameter: P) -> T:
        return visitor.visit_in_binary_operator(self, parameter)

@dataclass
class NotInBinaryOperator(BinaryOperator):
    def visit(self, visitor: ExecutionVisitor, parameter: P) -> T:
        return visitor.visit_not_in_binary_operator(self, parameter)

@dataclass
class IsBinaryOperator(BinaryOperator):
    def visit(self, visitor: ExecutionVisitor, parameter: P) -> T:
        return visitor.visit_is_binary_operator(self, parameter)

@dataclass
class IsNotBinaryOperator(BinaryOperator):
    def visit(self, visitor: ExecutionVisitor, parameter: P) -> T:
        return visitor.visit_is_not_binary_operator(self, parameter)

@dataclass
class AndBinaryOperator(BinaryOperator):
    def visit(self, visitor: ExecutionVisitor, parameter: P) -> T:
        return visitor.visit_and_binary_operator(self, parameter)

@dataclass
class OrBinaryOperator(BinaryOperator):
    def visit(self, visitor: ExecutionVisitor, parameter: P) -> T:
        return visitor.visit_or_binary_operator(self, parameter)

@dataclass
class AddBinaryOperator(BinaryOperator):
    def visit(self, visitor: ExecutionVisitor, parameter: P) -> T:
        return visitor.visit_add_binary_operator(self, parameter)

@dataclass
class MultiplyBinaryOperator(BinaryOperator):
    def visit(self, visitor: ExecutionVisitor, parameter: P) -> T:
        return visitor.visit_multiply_binary_operator(self, parameter)

@dataclass
class SubtractBinaryOperator(BinaryOperator):
    def visit(self, visitor: ExecutionVisitor, parameter: P) -> T:
        return visitor.visit_subtract_binary_operator(self, parameter)

@dataclass
class DivideBinaryOperator(BinaryOperator):
    def visit(self, visitor: ExecutionVisitor, parameter: P) -> T:
        return visitor.visit_divide_binary_operator(self, parameter)

@dataclass
class BitwiseAndBinaryOperator(BinaryOperator):
    def visit(self, visitor: ExecutionVisitor, parameter: P) -> T:
        return visitor.visit_bitwise_and_binary_operator(self, parameter)

@dataclass
class BitwiseOrBinaryOperator(BinaryOperator):
    def visit(self, visitor: ExecutionVisitor, parameter: P) -> T:
        return visitor.visit_bitwise_or_binary_operator(self, parameter)

@dataclass
class OperandExpression(Expression):
    expression: Expression

    def visit(self, visitor: ExecutionVisitor, parameter: P) -> T:
        return visitor.visit_operand_expression(self, parameter)

@dataclass
class UnaryExpression(Expression):
    operator: UnaryOperator
    expression: OperandExpression

    def visit(self, visitor: ExecutionVisitor, parameter: P) -> T:
        return visitor.visit_unary_expression(self, parameter)

@dataclass
class BinaryExpression(Expression):
    left: OperandExpression
    right: OperandExpression
    operator: BinaryOperator

    def visit(self, visitor: ExecutionVisitor, parameter: P) -> T:
        return visitor.visit_binary_expression(self, parameter)

@dataclass
class LiteralExpression(Expression):
    literal: Literal

    def visit(self, visitor: ExecutionVisitor, parameter: P) -> T:
        return visitor.visit_literal_expression(self, parameter)

@dataclass
class AliasExpression(Expression, ABC):
    alias: str = None

@dataclass
class VariableAliasExpression(AliasExpression):
    def visit(self, visitor: ExecutionVisitor, parameter: P) -> T:
        return visitor.visit_variable_alias_expression(self, parameter)

@dataclass
class ColumnReferenceExpression(Expression):
    name: str
    def visit(self, visitor: ExecutionVisitor, parameter: P) -> T:
        return visitor.visit_column_reference_expression(self, parameter)

@dataclass
class ColumnAliasExpression(AliasExpression):
    reference: ColumnReferenceExpression = None
    def visit(self, visitor: ExecutionVisitor, parameter: P) -> T:
        return visitor.visit_column_alias_expression(self, parameter)

@dataclass
class ComputedColumnAliasExpression(AliasExpression):
    expression: Optional[Expression] = None

    def visit(self, visitor: ExecutionVisitor, parameter: P) -> T:
        return visitor.visit_computed_column_alias_expression(self, parameter)

@dataclass
class IfExpression(Expression):
    test: Expression
    body: Expression
    orelse: Expression

    def visit(self, visitor: ExecutionVisitor, parameter: P) -> T:
        return visitor.visit_if_expression(self, parameter)

class OrderType(Expression, ABC):
    pass

@dataclass
class AscendingOrderType(OrderType):
    def visit(self, visitor: ExecutionVisitor, parameter: P) -> T:
        return visitor.visit_ascending_order_type(self, parameter)

@dataclass
class DescendingOrderType(OrderType):
    def visit(self, visitor: ExecutionVisitor, parameter: P) -> T:
        return visitor.visit_descending_order_type(self, parameter)

@dataclass
class OrderByExpression(Expression):
    direction: OrderType
    expression: Expression

    def visit(self, visitor: ExecutionVisitor, parameter: P) -> T:
        return visitor.visit_order_by_expression(self, parameter)

@dataclass
class FunctionExpression(Expression):
    function: Function
    parameters: List[Expression]

    def visit(self, visitor: ExecutionVisitor, parameter: P) -> T:
        return visitor.visit_function_expression(self, parameter)

@dataclass
class MapReduceExpression(Expression):
    map_expression: Expression
    reduce_expression: Expression
    def visit(self, visitor: ExecutionVisitor, parameter: P) -> T:
        return visitor.visit_map_reduce_expression(self, parameter)

@dataclass
class LambdaExpression(Expression):
    parameters: List[str]
    expression: Expression
    def visit(self, visitor: ExecutionVisitor, parameter: P) -> T:
        return visitor.visit_lambda_expression(self, parameter)

class Clause(ABC):
    pass

@dataclass
class RenameClause(Clause):
    columnAliases: List[ColumnAliasExpression]

    def visit(self, visitor: ExecutionVisitor, parameter: P) -> T:
        return visitor.visit_rename_clause(self, parameter)


@dataclass
class FilterClause(Clause):
    expression: Expression

    def visit(self, visitor: ExecutionVisitor, parameter: P) -> T:
        return visitor.visit_filter_clause(self, parameter)

@dataclass
class SelectionClause(Clause):
    expressions: List[Expression]

    def visit(self, visitor: ExecutionVisitor, parameter: P) -> T:
        return visitor.visit_selection_clause(self, parameter)

@dataclass
class ExtendClause(Clause):
    expressions: List[Expression]

    def visit(self, visitor: ExecutionVisitor, parameter: P) -> T:
        return visitor.visit_extend_clause(self, parameter)

@dataclass
class GroupByClause(Clause):
    expression: Expression

    def visit(self, visitor: ExecutionVisitor, parameter: P) -> T:
        return visitor.visit_group_by_clause(self, parameter)

@dataclass
class GroupByExpression(Expression):
    selections: List[Expression]
    expressions: List[Expression]

    def visit(self, visitor: ExecutionVisitor, parameter: P) -> T:
        return visitor.visit_group_by_expression(self, parameter)

@dataclass
class DistinctClause(Clause):
    expressions: List[Expression]

    def visit(self, visitor: ExecutionVisitor, parameter: P) -> T:
        return visitor.visit_distinct_clause(self, parameter)

@dataclass
class OrderByClause(Clause):
    ordering: List[OrderType]

    def visit(self, visitor: ExecutionVisitor, parameter: P) -> T:
        return visitor.visit_order_by_clause(self, parameter)

@dataclass
class LimitClause(Clause):
    value: IntegerLiteral

    def visit(self, visitor: ExecutionVisitor, parameter: P) -> T:
        return visitor.visit_limit_clause(self, parameter)

class FromClause(Clause, ABC):
    @abstractmethod
    def get_from_clause(self) -> str:
        pass

    def visit(self, visitor: ExecutionVisitor, parameter: P) -> T:
        return visitor.visit_from_clause(self, parameter)

@dataclass
class DatabaseFromClause(FromClause):
    database: str
    table: str
    schema: str = None

    def get_from_clause(self) -> str:
        return "#>{" + self.database + "." + (self.schema + "." if self.schema else "") + self.table + "}#"

class JoinType(ABC):
    @abstractmethod
    def visit(self, visitor: ExecutionVisitor, parameter: P) -> T:
        pass

@dataclass
class InnerJoinType(JoinType):
    def visit(self, visitor: ExecutionVisitor, parameter: P) -> T:
        return visitor.visit_inner_join_type(self, parameter)

@dataclass
class LeftJoinType(JoinType):
    def visit(self, visitor: ExecutionVisitor, parameter: P) -> T:
        return visitor.visit_left_join_type(self, parameter)

@dataclass
class JoinExpression(Expression):
    on: Expression

    def visit(self, visitor: ExecutionVisitor, parameter: P) -> T:
        return visitor.visit_join_expression(self, parameter)

@dataclass
class JoinClause(Clause):
    from_clause: List[Clause]
    join_type: JoinType
    on_clause: JoinExpression

    def visit(self, visitor: ExecutionVisitor, parameter: P) -> T:
        return visitor.visit_join_clause(self, parameter)

@dataclass
class OffsetClause(Clause):
    value: IntegerLiteral

    def visit(self, visitor: ExecutionVisitor, parameter: P) -> T:
        return visitor.visit_offset_clause(self, parameter)


class ExecutionVisitor(ABC):

    @abstractmethod
    def visit_from_clause(self, val: FromClause, parameter: P) -> T:
        raise NotImplementedError()

    @abstractmethod
    def visit_integer_literal(self, val: IntegerLiteral, parameter: P) -> T:
        raise NotImplementedError()

    @abstractmethod
    def visit_string_literal(self, val: StringLiteral, parameter: P) -> T:
        raise NotImplementedError()

    @abstractmethod
    def visit_date_literal(self, val: DateLiteral, parameter: P) -> T:
        raise NotImplementedError()

    @abstractmethod
    def visit_boolean_literal(self, val: BooleanLiteral, parameter: P) -> T:
        raise NotImplementedError()

    @abstractmethod
    def visit_operand_expression(self, val: OperandExpression, parameter: P) -> T:
        raise NotImplementedError()

    @abstractmethod
    def visit_not_unary_operator(self, val: NotUnaryOperator, parameter: P) -> T:
        raise NotImplementedError()

    @abstractmethod
    def visit_equals_binary_operator(self, val: EqualsBinaryOperator, parameter: P) -> T:
        raise NotImplementedError()

    @abstractmethod
    def visit_not_equals_binary_operator(self, val: NotEqualsBinaryOperator, parameter: P) -> T:
        raise NotImplementedError()

    @abstractmethod
    def visit_greater_than_binary_operator(self, val: GreaterThanBinaryOperator, parameter: P) -> T:
        raise NotImplementedError()

    @abstractmethod
    def visit_greater_than_equals_operator(self, val: GreaterThanEqualsBinaryOperator, parameter: P) -> T:
        raise NotImplementedError()

    @abstractmethod
    def visit_less_than_binary_operator(self, val: LessThanBinaryOperator, parameter: P) -> T:
        raise NotImplementedError()

    @abstractmethod
    def visit_less_than_equals_binary_operator(self, val: LessThanEqualsBinaryOperator, parameter: P) -> T:
        raise NotImplementedError()

    @abstractmethod
    def visit_and_binary_operator(self, val: AndBinaryOperator, parameter: P) -> T:
        raise NotImplementedError()

    @abstractmethod
    def visit_or_binary_operator(self, val: OrBinaryOperator, parameter: P) -> T:
        raise NotImplementedError()

    @abstractmethod
    def visit_add_binary_operator(self, val: AddBinaryOperator, parameter: P) -> T:
        raise NotImplementedError()

    @abstractmethod
    def visit_multiply_binary_operator(self, val: MultiplyBinaryOperator, parameter: P) -> T:
        raise NotImplementedError()

    @abstractmethod
    def visit_subtract_binary_operator(self, val: SubtractBinaryOperator, parameter: P) -> T:
        raise NotImplementedError()

    @abstractmethod
    def visit_divide_binary_operator(self, val: DivideBinaryOperator, parameter: P) -> T:
        raise NotImplementedError()

    @abstractmethod
    def visit_literal_expression(self, val: LiteralExpression, parameter: P) -> T:
        raise NotImplementedError()

    @abstractmethod
    def visit_unary_expression(self, val: UnaryExpression, parameter: P) -> T:
        raise NotImplementedError()

    @abstractmethod
    def visit_binary_expression(self, val: BinaryExpression, parameter: P) -> T:
        raise NotImplementedError()

    @abstractmethod
    def visit_variable_alias_expression(self, val: VariableAliasExpression, parameter: P) -> T:
        raise NotImplementedError()

    @abstractmethod
    def visit_computed_column_alias_expression(self, val: ComputedColumnAliasExpression, parameter: P) -> T:
        raise NotImplementedError()

    @abstractmethod
    def visit_column_alias_expression(self, val: ColumnAliasExpression, parameter: P) -> T:
        raise NotImplementedError()

    @abstractmethod
    def visit_function_expression(self, val: FunctionExpression, parameter: P) -> T:
        raise NotImplementedError()

    @abstractmethod
    def visit_sum_function(self, val: FunctionExpression, parameter: P) -> T:
        raise NotImplementedError()

    @abstractmethod
    def visit_map_reduce_expression(self, val: MapReduceExpression, parameter: P) -> T:
        raise NotImplementedError()

    @abstractmethod
    def visit_lambda_expression(self, val: LambdaExpression, parameter: P) -> T:
        raise NotImplementedError()

    @abstractmethod
    def visit_count_function(self, val: CountFunction, parameter: P) -> T:
        raise NotImplementedError()

    @abstractmethod
    def visit_average_function(self, val: AverageFunction, parameter: P) -> T:
        raise NotImplementedError()

    def visit_modulo_function(self, val: ModuloFunction, parameter: P) -> T:
        raise NotImplementedError()

    def visit_exponent_function(self, val: ExponentFunction, parameter: P) -> T:
        raise NotImplementedError()

    @abstractmethod
    def visit_filter_clause(self, val: FilterClause, parameter: P) -> T:
        raise NotImplementedError()

    @abstractmethod
    def visit_selection_clause(self, val: SelectionClause, parameter: P) -> T:
        raise NotImplementedError()

    @abstractmethod
    def visit_extend_clause(self, val: ExtendClause, parameter: P) -> T:
        raise NotImplementedError()

    @abstractmethod
    def visit_group_by_clause(self, val: GroupByClause, parameter: P) -> T:
        raise NotImplementedError()

    @abstractmethod
    def visit_group_by_expression(self, val: GroupByExpression, parameter: P) -> T:
        raise NotImplementedError()

    @abstractmethod
    def visit_distinct_clause(self, val: DistinctClause, parameter: P) -> T:
        raise NotImplementedError()

    @abstractmethod
    def visit_order_by_clause(self, val: OrderByClause, parameter: P) -> T:
        raise NotImplementedError()

    @abstractmethod
    def visit_limit_clause(self, val: LimitClause, parameter: P) -> T:
        raise NotImplementedError()

    @abstractmethod
    def visit_join_expression(self, val: JoinExpression, parameter: P) -> T:
        raise NotImplementedError()

    @abstractmethod
    def visit_join_clause(self, val: JoinClause, parameter: P) -> T:
        raise NotImplementedError()

    @abstractmethod
    def visit_inner_join_type(self, val: InnerJoinType, parameter: P) -> T:
        raise NotImplementedError()

    @abstractmethod
    def visit_left_join_type(self, val: LeftJoinType, parameter: P) -> T:
        raise NotImplementedError()

    @abstractmethod
    def visit_column_reference_expression(self, val: ColumnReferenceExpression, parameter: P) -> T:
        raise NotImplementedError()

    @abstractmethod
    def visit_if_expression(self, val: IfExpression, parameter: P) -> T:
        raise NotImplementedError()

    @abstractmethod
    def visit_order_by_expression(self, val: OrderByExpression, parameter: P) -> T:
        raise NotImplementedError()

    @abstractmethod
    def visit_ascending_order_type(self, val: AscendingOrderType, parameter: P) -> T:
        raise NotImplementedError()

    @abstractmethod
    def visit_descending_order_type(self, val: DescendingOrderType, parameter: P) -> T:
        raise NotImplementedError()

    @abstractmethod
    def visit_rename_clause(self, val: RenameClause, parameter: P) -> T:
        raise NotImplementedError()

    @abstractmethod
    def visit_offset_clause(self, val: OffsetClause, parameter: P) -> T:
        raise NotImplementedError()

    @abstractmethod
    def visit_in_binary_operator(self, self1, parameter: P) -> T:
        raise NotImplementedError()

    @abstractmethod
    def visit_not_in_binary_operator(self, self1, parameter: P) -> T:
        raise NotImplementedError()

    @abstractmethod
    def visit_is_binary_operator(self, self1, parameter: P) -> T:
        raise NotImplementedError()

    @abstractmethod
    def visit_is_not_binary_operator(self, self1, parameter: P) -> T:
        raise NotImplementedError()

    @abstractmethod
    def visit_bitwise_and_binary_operator(self, self1, parameter: P) -> T:
        raise NotImplementedError()

    @abstractmethod
    def visit_bitwise_or_binary_operator(self, self1, parameter: P) -> T:
        raise NotImplementedError()

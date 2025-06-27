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

"""
Lambda parser for the cloud-dataframe DSL.

This module provides utilities for parsing Python lambda functions
and converting them to SQL expressions.
"""
import ast
import importlib
import inspect
from _ast import operator, arg
from enum import Enum
from typing import Callable, List, Union, Dict, Tuple, TypeVar
import pyarrow as pa

from legendql.model.functions import StringConcatFunction
from legendql.model.metamodel import Expression, BinaryExpression, BinaryOperator, \
    ColumnReferenceExpression, BooleanLiteral, IfExpression, OrderByExpression, \
    FunctionExpression, \
    OperandExpression, AndBinaryOperator, OrBinaryOperator, IntegerLiteral, StringLiteral, EqualsBinaryOperator, \
    NotEqualsBinaryOperator, LessThanBinaryOperator, LessThanEqualsBinaryOperator, GreaterThanBinaryOperator, \
    GreaterThanEqualsBinaryOperator, InBinaryOperator, NotInBinaryOperator, IsBinaryOperator, IsNotBinaryOperator, \
    AddBinaryOperator, SubtractBinaryOperator, MultiplyBinaryOperator, DivideBinaryOperator, BitwiseOrBinaryOperator, \
    BitwiseAndBinaryOperator, DateLiteral, GroupByExpression, \
    DescendingOrderType, ComputedColumnAliasExpression, AscendingOrderType, ColumnAliasExpression, LambdaExpression, \
    VariableAliasExpression, MapReduceExpression, UnaryExpression, NotUnaryOperator, ModuloFunction, ExponentFunction, \
    JoinExpression
from legendql.model.schema import Table

E = TypeVar("E", bound=Expression)

class ParseType(Enum):
    extend = "extend"
    join = "join"
    rename = "rename"
    select = "select"
    group_by = "group_by"
    filter = "filter"
    order_by = "order_by"
    lambda_body = "lambda_body"
    over = "over"

class Parser:

    @staticmethod
    def parse(func: Callable, tables: [Table], ptype: ParseType) -> Tuple[Union[E, List[E]], Table]:
        """
        Parse a lambda function and convert it to an Expression or list of Expressions.

        Args:
            func: The lambda function to parse. Can be:
                - A lambda that returns a boolean expression (e.g., lambda x: x.age > 30)
                - A lambda that returns a column reference (e.g., lambda x: x.name)
                - A lambda that returns an array of column references (e.g., lambda x: [x.name, x.age])
                - A lambda that returns tuples with sort direction (e.g., lambda x: [(x.department, Sort.DESC)])
            tables: the current LegendQL query context
            ptype: The LegendQL function calling this parser (extend, join, select ..)

        Returns:
            An Expression or list of Expressions representing the lambda function
        """
        # Get the source code of the lambda function
        lambda_node = func if isinstance(func, ast.Lambda) else Parser._get_lambda_node(func)
        lambda_args = lambda_node.args.args

        src_table = Table(tables[0].table, tables[0].columns.copy())
        new_table = Table(tables[0].table, tables[0].columns.copy())

        if ptype == ParseType.select:
            Parser._validate_lambda_args_length(lambda_args, 1)
            return Parser._parse_select(lambda_node.body, src_table, new_table), new_table
        if ptype == ParseType.filter:
            Parser._validate_lambda_args_length(lambda_args, 1)
            return Parser._parse_filter(lambda_node.body, lambda_args, src_table, new_table), new_table
        if ptype == ParseType.extend:
            Parser._validate_lambda_args_length(lambda_args, 1)
            return Parser._parse_extend(lambda_node.body, lambda_args, src_table, new_table), new_table
        if ptype == ParseType.join:
            Parser._validate_lambda_args_length(lambda_args, 2)
            new_name = "_".join(map(lambda t: t.table, tables))
            src_table = Table(new_name, [])
            new_table = Table(new_name, [])
            for s in tables:
                src_table.columns = src_table.columns + s.columns.copy()
                new_table.columns = new_table.columns + s.columns.copy()
            return Parser._parse_join(lambda_node.body, lambda_args, src_table, new_table), new_table
        if ptype == ParseType.rename:
            Parser._validate_lambda_args_length(lambda_args, 1)
            return Parser._parse_rename(lambda_node.body, src_table, new_table), new_table
        if ptype == ParseType.group_by:
            Parser._validate_lambda_args_length(lambda_args, 1)
            return Parser._parse_group_by(lambda_node.body, lambda_args, src_table, new_table), new_table
        if ptype == ParseType.order_by:
            Parser._validate_lambda_args_length(lambda_args, 1)
            return Parser._parse_order_by(lambda_node.body, lambda_args), new_table
        if ptype == ParseType.over:
            Parser._validate_lambda_args_length(lambda_args, 1)
            return Parser._parse_over(lambda_node.body, lambda_args, src_table, new_table), new_table
        raise ValueError(f"Unknown ParseType: {ptype}")

    @staticmethod
    def _get_lambda_node(func):
        source_lines, _ = inspect.getsourcelines(func)
        source_text = ''.join(source_lines).strip().replace("\n", "")

        try:
            # if it is lambda on own line this should work
            source_ast = ast.parse(source_text)
        except:
            # fluent api way
            idx = source_text.find("lambda")
            source_text = source_text[idx:len(source_text) - 1]

            try:
                # fluent api way
                source_ast = ast.parse(source_text)
            except:
                # is it on the last line? try to strip out one more
                source_text = source_text[:len(source_text) - 1]

                try:
                    source_ast = ast.parse(source_text)
                except:
                    raise ValueError(f"Could not get Lambda func: {source_text}")

        return next((node for node in ast.walk(source_ast) if isinstance(node, ast.Lambda)), None)

    @staticmethod
    def _validate_lambda_args_length(args: [arg], length: int) -> None:
        if len(args) != length:
            raise ValueError(f"Lambda MUST have exactly {length} argument(s): {args}")

    @staticmethod
    def _parse_select(node: ast.AST, src_table: Table, new_table: Table) -> [ColumnReferenceExpression]:
        new_table.columns = []
        if isinstance(node, ast.List):
            return [item for sublist in map(lambda n: Parser._parse_single_select(n, src_table, new_table), node.elts) for item in sublist]

        if isinstance(node, ast.Attribute) and isinstance(node.value, ast.Name):
            return Parser._parse_single_select(node, src_table, new_table)

        raise ValueError(f"Unsupported Column Reference {node.value}")

    @staticmethod
    def _parse_single_select(node: ast.Attribute, src_table: Table, new_table: Table) -> [ColumnReferenceExpression]:
        src_column = src_table.column(node.attr)
        new_table.add_column(pa.field(node.attr, src_column.type, src_column.nullable, src_column.metadata))
        return [ColumnReferenceExpression(name=node.attr)]

    @staticmethod
    def _parse_filter(node: ast.AST, args: [arg], src_table: Table, new_table: Table) -> LambdaExpression:
        return LambdaExpression(list(map(lambda a: a.arg, args)), Parser._parse_lambda_body(node, ParseType.lambda_body, src_table, new_table))

    @staticmethod
    def _parse_extend(node: ast.AST, args: [arg], src_table: Table, new_table: Table) -> List[ComputedColumnAliasExpression]:
        if isinstance(node, ast.List):
            new_implicit_aliases = {}
            extends = []
            for elt in node.elts:
                node_and_new_aliases = Parser._parse_single_extend(elt, args, new_implicit_aliases, src_table, new_table)
                new_implicit_aliases.update(node_and_new_aliases[1])
                extends.append(node_and_new_aliases[0])
            return extends

        return [Parser._parse_single_extend(node, args, {}, src_table, new_table)[0]]

    @staticmethod
    def _parse_single_extend(node: ast.AST, args: [arg], implicit_aliases: Dict[str, str], src_table: Table, new_table: Table) -> Tuple[ComputedColumnAliasExpression, Dict[str, str]]:
        if isinstance(node, ast.NamedExpr) and isinstance(node.target, ast.Name):
            # TODO: AJH: need to infer column type
            new_table.add_column(pa.field(node.target.id, pa.null()))
            return (ComputedColumnAliasExpression(node.target.id, LambdaExpression(list(map(lambda a: a.arg, args)), Parser._parse_lambda_body(node.value, args, src_table, new_table, implicit_aliases))), {node.target.id: args[0].arg})

        raise ValueError(f"Not a valid extend statement {node.value}")

    @staticmethod
    def _parse_join(node: ast.AST, args: [arg], src_table: Table, new_table: Table) -> List[ColumnReferenceExpression]:
        return JoinExpression(LambdaExpression(list(map(lambda a: a.arg, args)), Parser._parse_lambda_body(node, ParseType.lambda_body, src_table, new_table)))

    @staticmethod
    def _parse_rename(node: ast.AST, src_table: Table, new_table: Table) -> List[ColumnReferenceExpression]:
        if isinstance(node, ast.List) or isinstance(node, ast.Tuple):
            return [item for sublist in map(lambda n: Parser._parse_rename(n, src_table, new_table), node.elts) for item in sublist]

        if isinstance(node, ast.NamedExpr) and isinstance(node.target, ast.Name) and isinstance(node.value, ast.Attribute):
            src_column = src_table.column(node.value.attr)
            src_table.add_column(pa.field(node.target.id, src_column.type, src_column.nullable, src_column.metadata))
            new_table.add_column(pa.field(node.target.id, src_column.type, src_column.nullable, src_column.metadata))
            return [ColumnAliasExpression(node.target.id, ColumnReferenceExpression(node.value.attr))]

        raise ValueError(f"Unsupported Rename {node}")

    @staticmethod
    def _parse_group_by(node: ast.AST, args: [arg], src_table: Table, new_table: Table) -> List[GroupByExpression | Expression]:
        if isinstance(node, ast.Call) and ((isinstance(node.func, ast.Name) and node.func.id == "aggregate") or (isinstance(node.func, ast.Attribute) and node.func.attr == "aggregate")):
            if len(node.args) != 2 and len(node.args) != 3:
                raise ValueError(f"An aggregate function requires 2 or 3 arguments: {node.args}")

            # capture the columns we're going to group by
            selections = Parser._parse_select(node.args[0], src_table, new_table)
            expressions = list(map(lambda n: Parser._parse_group_by_map_aggregate(n, args, src_table, new_table), node.args[1].elts if isinstance(node.args[1], ast.List) or isinstance(node.args[1], ast.Tuple) else [node.args[1]]))
            group_by = GroupByExpression(selections, expressions)
            if len(node.args) == 3:
                having = Parser._parse_filter(node.args[2], args, src_table, new_table) if len(node.args) == 3 else None
                return [group_by, having]
            return [group_by]

        raise ValueError(f"Unsupported GroupBy expression {ast.dump(node)}")

    @staticmethod
    def _parse_group_by_map_aggregate(node: ast.AST, args: [arg], src_table: Table, new_table: Table) -> Expression:
        if isinstance(node, ast.NamedExpr):
            # TODO: AJH: need to infer type for the computed column
            computed_column = node.target.id
            new_table.add_column(pa.field(computed_column, pa.null()))
            src_table.add_column(pa.field(computed_column, pa.null()))
            # since pure groupBy's map/reduce is really map/apply_function, we need to assume the first node is a Call
            # note that there is only one arg for this lambda
            if isinstance(node.value, ast.Call):
                map_expression = LambdaExpression(list(map(lambda a: a.arg, args)), Parser._parse_lambda_body(node.value.args[0], args, src_table, new_table))
                # very brittle, lots more checks needed here
                module = importlib.import_module("legendql.model.functions")
                class_ = getattr(module, f"{node.value.func.id.title()}Function")
                function_instance = class_()
                function_argument = args[0].arg
                reduce_expression = LambdaExpression(list(map(lambda a: a.arg, args)), FunctionExpression(function_instance, [VariableAliasExpression(function_argument)]))
                return ComputedColumnAliasExpression(computed_column, MapReduceExpression(map_expression, reduce_expression))

        raise ValueError(f"Unsupported GroupBy expression {node}")

    @staticmethod
    def _parse_order_by(node: ast.AST, args: [arg]) -> [OrderByExpression]:
        if isinstance(node, ast.List):
            return [item for sublist in map(lambda n: Parser._parse_order_by(n, args), node.elts) for item in sublist]

        order = AscendingOrderType()
        clause = node
        if isinstance(node, ast.UnaryOp):
            clause = node.operand
            if isinstance(node.op, ast.UAdd):
                order = AscendingOrderType()
            if isinstance(node.op, ast.USub):
                order = DescendingOrderType()

        if isinstance(clause, ast.Attribute):
            return [OrderByExpression(order, ColumnReferenceExpression(clause.attr))]

        raise ValueError(f"Not a valid sort statement {clause}")

    @staticmethod
    def _parse_over(node: ast.AST, args: [arg], src_table: Table, new_table: Table) -> Expression:
        print(ast.dump(node))
        # window = lambda r: (avg_val :=
        #                     over(r.location, avg(r.salary), sort=[r.emp_name, -r.location], frame=rows(0, unbounded())))

        # columns: Union[ast.expr, list[ast.expr]],
        # functions: Union[AggregationFunction, WindowFunction, list[AggregationFunction], list[WindowFunction]],
        # sort: Optional[Union[ast.expr, list[ast.expr]]] = None,
        # frame: Optional[Frame] = None,
        # qualify: Optional[bool] = None):

        if isinstance(node, ast.NamedExpr):
            alias = node.target.id
            if isinstance(node.value, ast.Call) and node.value.func.id == "over":
                print(ast.dump(node.value.args))

        raise NotImplementedError()

    @staticmethod
    def _parse_lambda_body(node: ast.AST, args: [arg], src_table: Table, new_table: Table, implicit_aliases: dict[str, str] = None) -> Expression:
        if node is None:
            raise ValueError("node in Parser._parse_expression is None")

        if isinstance(node, ast.NamedExpr):
            src_column = src_table.column(node.target.id)
            src_table.add_column(pa.field(node.target.id, src_column.type, src_column.nullable, src_column.metadata))
            new_table.add_column(pa.field(node.target.id, src_column.type, src_column.nullable, src_column.metadata))
            return ColumnAliasExpression(node.target.id, Parser._parse_lambda_body(node.value, args, src_table, new_table, implicit_aliases))

        if isinstance(node, ast.Compare):
            # Handle comparison operations (e.g., x > 5, y == 'value')
            left = Parser._parse_lambda_body(node.left, args, src_table, new_table, implicit_aliases)

            # We only handle the first comparator for simplicity
            # In a real implementation, we would handle multiple comparators
            op = node.ops[0]
            right = Parser._parse_lambda_body(node.comparators[0], args, src_table, new_table, implicit_aliases)

            comp_op = Parser._get_comparison_operator(op)

            # Ensure left and right are Expression objects, not lists or tuples
            if isinstance(left, list) or isinstance(left, tuple):
                raise ValueError(f"Unsupported Compare object {left}")
            if isinstance(right, list) or isinstance(right, tuple):
                raise ValueError(f"Unsupported Compare object {right}")

            return BinaryExpression(left=OperandExpression(left), operator=comp_op, right=OperandExpression(right))

        elif isinstance(node, ast.BinOp):
            # Handle binary operations (e.g., x + y, x - y, x * y)
            left = Parser._parse_lambda_body(node.left, args, src_table, new_table, implicit_aliases)
            right = Parser._parse_lambda_body(node.right, args, src_table, new_table, implicit_aliases)

            if isinstance(node.op, ast.Mod):
                return FunctionExpression(parameters=[left, right], function=ModuloFunction())
            elif isinstance(node.op, ast.Pow):
                return FunctionExpression(parameters=[left, right], function=ExponentFunction())

            comp_op = Parser._get_binary_operator(node.op)

            # Ensure left and right are Expression objects, not lists or tuples
            if isinstance(left, list) or isinstance(left, tuple):
                raise ValueError(f"Unsupported BinOp object {left}")
            if isinstance(right, list) or isinstance(right, tuple):
                raise ValueError(f"Unsupported BinOp object {right}")

            return BinaryExpression(left=OperandExpression(left), operator=comp_op, right=OperandExpression(right))

        elif isinstance(node, ast.BoolOp):
            # Handle boolean operations (e.g., x and y, x or y)
            values = [Parser._parse_lambda_body(val, args, src_table, new_table, implicit_aliases) for val in node.values]

            # Combine the values with the appropriate operator
            comp_op = AndBinaryOperator() if isinstance(node.op, ast.And) else OrBinaryOperator()

            # Ensure all values are Expression objects, not lists or tuples
            processed_values = []
            for val in values:
                if isinstance(val, list) or isinstance(val, tuple):
                    raise ValueError(f"Unsupported BoolOp object {val}")
                else:
                    processed_values.append(val)

            # Start with the first two values
            result = BinaryExpression(left=OperandExpression(processed_values[0]), operator=comp_op, right=OperandExpression(processed_values[1]))

            # Add the remaining values
            for value in processed_values[2:]:
                result = BinaryExpression(left=OperandExpression(result), operator=comp_op, right=OperandExpression(value))

            return result

        elif isinstance(node, ast.Attribute):
            # Handle column references (e.g. x.column_name)
            if isinstance(node.value, ast.Name):
                # validate the column name
                if not src_table.validate_column(node.attr):
                    raise ValueError(f"Column '{node.attr}' not found in table '{src_table}'")

                return ColumnAliasExpression(alias=node.value.id, reference=ColumnReferenceExpression(name=node.attr))

        elif isinstance(node, ast.Name):
            if node.id == "True":
                from .model.metamodel import LiteralExpression
                return LiteralExpression(BooleanLiteral(True))
            elif node.id == "False":
                from .model.metamodel import LiteralExpression
                return LiteralExpression(BooleanLiteral(False))
            else:
                alias = implicit_aliases.get(node.id, None) if implicit_aliases else None
                if alias:
                    return ColumnAliasExpression(alias, ColumnReferenceExpression(node.id))
                return ColumnReferenceExpression(node.id)

        elif isinstance(node, ast.Constant):
            # Handle literal values (e.g., 5, 'value', True)
            from .model.metamodel import LiteralExpression
            if isinstance(node.value, int):
                return LiteralExpression(IntegerLiteral(node.value))
            if isinstance(node.value, bool):
                return LiteralExpression(BooleanLiteral(node.value))
            if isinstance(node.value, str):
                return LiteralExpression(StringLiteral(node.value))

            raise ValueError(f"Cannot convert literal type {type(node.value)}")

        elif isinstance(node, ast.UnaryOp):
            # Handle unary operations (e.g., not x)
            operand = Parser._parse_lambda_body(node.operand, args, src_table, new_table, implicit_aliases)

            # Ensure operand is an Expression object, not a list or tuple
            if isinstance(operand, list) or isinstance(operand, tuple):
                # Use a fallback for list/tuple values in unary operations
                raise ValueError(f"Unsupported expression to UnaryOp: {operand}")

            if isinstance(node.op, ast.Not):
                return UnaryExpression(operator=NotUnaryOperator(), expression=OperandExpression(operand))
            else:
                # Other unary operations (e.g., +, -)
                # In a real implementation, we would handle this more robustly
                return Parser._parse_lambda_body(node.operand, args, src_table, new_table, implicit_aliases)

        elif isinstance(node, ast.IfExp):
            # Handle conditional expressions (e.g., x if y else z)
            # In a real implementation, we would handle this more robustly
            test = Parser._parse_lambda_body(node.test, args, src_table, new_table, implicit_aliases)
            body = Parser._parse_lambda_body(node.body, args, src_table, new_table, implicit_aliases)
            orelse = Parser._parse_lambda_body(node.orelse, args, src_table, new_table, implicit_aliases)

            # Ensure all values are Expression objects, not lists or tuples
            if isinstance(test, list) or isinstance(test, tuple):
                raise ValueError(f"Unsupported IfExp: {test}")
            if isinstance(body, list) or isinstance(body, tuple):
                raise ValueError(f"Unsupported IfExp: {body}")
            if isinstance(orelse, list) or isinstance(orelse, tuple):
                raise ValueError(f"Unsupported IfExp: {orelse}")

            # Create a CASE WHEN expression
            return IfExpression(test=test, body=body, orelse=orelse)

        elif isinstance(node, ast.List):
            # Handle tuples and lists (e.g., (1, 2, 3), [1, 2, 3])
            # This is used for array returns in lambdas like lambda x: [x.name, x.age]
            elements = []
            for elt in node.elts:
                elements.append(Parser._parse_lambda_body(elt, args, src_table, new_table, implicit_aliases))
            return elements

        elif isinstance(node, ast.Tuple):
            # Handle Join with rename ( x.col1 == y.col1, [ (x_col1 := x.col1 ), (y_col1 := y.col1 ) ]
            elements = []
            for elt in node.elts:
                elements.append(Parser._parse_lambda_body(elt, args, src_table, new_table, implicit_aliases))
            return elements

        elif isinstance(node, ast.Call):
            # Handle function calls (e.g., sum(x.col1 - x.col2))
            args_list = []
            # kwargs = {}

            if isinstance(node.func, ast.Name):
                # Parse the arguments to the function

                for arg in node.args:
                    parsed_arg = Parser._parse_lambda_body(arg, args, src_table, new_table, implicit_aliases)
                    args_list.append(parsed_arg)

                # Handle keyword arguments
                # kwargs = {}
                for kw in node.keywords:
                    parsed_kw = Parser._parse_lambda_body(kw.value, args, src_table, new_table, implicit_aliases)
                    # kwargs[kw.arg] = parsed_kw
                    args_list.append(parsed_kw)

            else:
                ValueError(f"Unsupported function type: {node.func}")

            if node.func.id == "date":
                from .model.metamodel import LiteralExpression
                from datetime import date
                # random use of date to prevent auto-format refactorings deleting the import
                date(1999, 1, 1)
                compiled = compile(ast.fix_missing_locations(ast.Expression(body=node)), '', 'eval')
                val = eval(compiled, None, None)
                return LiteralExpression(literal=DateLiteral(val))

            #if node.func.id not in known_functions:
            #    ValueError(f"Unknown function name: {node.func.id}")

            # very brittle, lots more checks needed here
            module = importlib.import_module("legendql.model.functions")
            class_ = getattr(module, f"{node.func.id.title()}Function")
            instance = class_()
            return FunctionExpression(instance, parameters=args_list)

        elif isinstance(node, ast.JoinedStr):
            # Handle fstring (e.g. f"hello{blah}")
            # In a real implementation, we would handle this more robustly
            expr = []
            for value in node.values:
                if isinstance(value, ast.Constant):
                    expr.append(Parser._parse_lambda_body(value, args, src_table, new_table, implicit_aliases))
                elif isinstance(value, ast.FormattedValue):
                    if value.format_spec is not None:
                        raise ValueError(f"Format Spec Not Supported: {value.format_spec}")
                    else:
                        expr.append(Parser._parse_lambda_body(value.value, args, src_table, new_table, implicit_aliases))

            return FunctionExpression(StringConcatFunction(), expr)

        elif isinstance(node, ast.Subscript):
            # Handle subscript operations (e.g., x[0], x['key'])
            # In a real implementation, we would handle this more robustly
            raise ValueError(f"Unsupported expression: {node}")

        elif isinstance(node, ast.Dict):
            # Handle dictionaries (e.g., {'a': 1, 'b': 2})
            # In a real implementation, we would handle this more robustly
            raise ValueError(f"Unsupported expression: {node}")

        elif isinstance(node, ast.Set):
            # Handle sets (e.g., {1, 2, 3})
            # In a real implementation, we would handle this more robustly
            raise ValueError(f"Unsupported expression: {node}")

        elif isinstance(node, ast.ListComp) or isinstance(node, ast.SetComp) or isinstance(node, ast.DictComp) or isinstance(node, ast.GeneratorExp):
            # Handle comprehensions (e.g., [x for x in y], {x: y for x in z})
            # In a real implementation, we would handle this more robustly
            raise ValueError(f"Unsupported expression: {node}")

        else:
            # Throw
            raise ValueError(f"Unsupported expression: {ast.dump(node)}")

        return ColumnReferenceExpression("PLACEHOLDER")

    @staticmethod
    def _get_comparison_operator(op: ast.cmpop) -> BinaryOperator:
        """
        Convert an AST comparison operator to a SQL operator.

        Args:
            op: The AST comparison operator

        Returns:
            The equivalent SQL operator
        """
        if isinstance(op, ast.Eq):
            return EqualsBinaryOperator()
        elif isinstance(op, ast.NotEq):
            return NotEqualsBinaryOperator()
        elif isinstance(op, ast.Lt):
            return LessThanBinaryOperator()
        elif isinstance(op, ast.LtE):
            return LessThanEqualsBinaryOperator()
        elif isinstance(op, ast.Gt):
            return GreaterThanBinaryOperator()
        elif isinstance(op, ast.GtE):
            return GreaterThanEqualsBinaryOperator()
        elif isinstance(op, ast.In):
            return InBinaryOperator()
        elif isinstance(op, ast.NotIn):
            return NotInBinaryOperator()
        elif isinstance(op, ast.Is):
            return IsBinaryOperator()
        elif isinstance(op, ast.IsNot):
            return IsNotBinaryOperator()
        else:
            raise ValueError(f"Unsupported comparison operator {op}")

    @staticmethod
    def _get_binary_operator(op: operator) -> BinaryOperator:
        # Map Python operators to Binary operators
        if isinstance(op, ast.Add):
            return AddBinaryOperator()
        elif isinstance(op, ast.Sub):
            return SubtractBinaryOperator()
        elif isinstance(op, ast.Mult):
            return MultiplyBinaryOperator()
        elif isinstance(op, ast.Div):
            return DivideBinaryOperator()
        elif isinstance(op, ast.BitOr):
            return BitwiseOrBinaryOperator()
        elif isinstance(op, ast.BitAnd):
            return BitwiseAndBinaryOperator()
        else:
            raise ValueError(f"Unsupported binary operator {op}")

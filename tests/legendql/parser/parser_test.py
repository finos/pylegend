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

import unittest
from datetime import date

from legendql.functions import aggregate, count, over, avg, rows, unbounded
from legendql.model.functions import StringConcatFunction, SumFunction, CountFunction, OverFunction, AvgFunction, \
    UnboundedFunction, RowsFunction
from legendql.model.metamodel import ColumnReferenceExpression, ColumnAliasExpression, LambdaExpression, \
    BinaryExpression, IntegerLiteral, LiteralExpression, ExponentFunction, ComputedColumnAliasExpression, \
    ModuloFunction, GreaterThanBinaryOperator, OperandExpression, IfExpression, DescendingOrderType, OrderByExpression, \
    VariableAliasExpression, MapReduceExpression, AddBinaryOperator, GroupByExpression, StringLiteral, \
    AscendingOrderType, OrBinaryOperator, AndBinaryOperator, LessThanBinaryOperator, DateLiteral, EqualsBinaryOperator, \
    FunctionExpression, JoinExpression
from legendql.model.schema import Table, Database
from legendql.parser import Parser, ParseType
import legendql as lq
import pyarrow as pa
import ast


class ParserTest(unittest.TestCase):

    def setUp(self):
        pass

    def test_select(self):
        table = Table("employee", [pa.field("dept_id", pa.int32()), pa.field("name", pa.utf8())])
        database = Database("employee", [table])
        q = lq.using_db(database, table.table)
        select = lambda e: [e.dept_id, e.name]
        p = Parser.parse(select, [q._query._table], ParseType.select)[0]
        self.assertEqual([ColumnReferenceExpression(name="dept_id"), ColumnReferenceExpression("name")], p)

    def test_rename(self):
        table = Table("employee", [pa.field("dept_id", pa.int32()), pa.field("name", pa.utf8())])
        database = Database("employee", [table])
        q = lq.using_db(database, table.table)
        rename = lambda e: [department_id := e.dept_id, full_name := e.name]
        p = Parser.parse(rename, [q._query._table], ParseType.rename)[0]
        self.assertEqual([ColumnAliasExpression("department_id", ColumnReferenceExpression("dept_id")), ColumnAliasExpression("full_name", ColumnReferenceExpression("name"))], p)

    def test_join(self):
        emp = Table("employee", [pa.field("dept_id", pa.int32()), pa.field("name", pa.utf8())])
        dep = Table("department", [pa.field("id", pa.int32())])
        database = Database("employee", [emp, dep])
        q = lq.using_db(database, emp.table)
        jq = lq.using_db(database, dep.table)
        join = lambda e, d: e.dept_id == d.id
        p = Parser.parse(join, [q._query._table, jq._query._table], ParseType.join)[0]

        self.assertEqual(JoinExpression(LambdaExpression(["e", "d"], BinaryExpression(
            left=OperandExpression(ColumnAliasExpression("e", ColumnReferenceExpression(name='dept_id'))),
            right=OperandExpression(ColumnAliasExpression("d", ColumnReferenceExpression(name='id'))),
            operator=EqualsBinaryOperator()))), p)


    def test_filter(self):
        table = Table("employee", [pa.field("start_date", pa.utf8())])
        database = Database("employee", [table])
        q = lq.using_db(database, table.table)
        filter = lambda e: e.start_date > date(2021, 1, 1)
        p = Parser.parse(filter, [q._query._table], ParseType.filter)[0]

        self.assertEqual(LambdaExpression(["e"], BinaryExpression(
            left=OperandExpression(ColumnAliasExpression("e", ColumnReferenceExpression(name='start_date'))),
            right=OperandExpression(LiteralExpression(DateLiteral(date(2021, 1, 1)))),
            operator=GreaterThanBinaryOperator())), p)  # add assertion here


    def test_filter_with_ast_lambda_date_gt(self):
        table = Table("employee", [pa.field("start_date", pa.utf8())])
        database = Database("employee", [table])
        q = lq.using_db(database, table.table)
        filter_lambda = ast.Lambda(
            args=ast.arguments(posonlyargs=[], args=[ast.arg(arg='e')], kwonlyargs=[], kw_defaults=[], defaults=[]),
            body=ast.Compare(
                left=ast.Attribute(value=ast.Name(id='e', ctx=ast.Load()), attr='start_date', ctx=ast.Load()),
                ops=[ast.Gt()],
                comparators=[ast.Call(func=ast.Name(id='date', ctx=ast.Load()), args=[
                    ast.Constant(value=2021), ast.Constant(value=1), ast.Constant(value=1)
                ], keywords=[])]))
        p = Parser.parse(filter_lambda, [q._query._table], ParseType.filter)[0]

        self.assertEqual(LambdaExpression(["e"], BinaryExpression(
            left=OperandExpression(ColumnAliasExpression("e", ColumnReferenceExpression(name='start_date'))),
            right=OperandExpression(LiteralExpression(DateLiteral(date(2021, 1, 1)))),
            operator=GreaterThanBinaryOperator())), p)


    def test_filter_with_ast_lambda_string_eq(self):
        table = Table("employee", [pa.field("name", pa.utf8())])
        database = Database("employee", [table])
        q = lq.using_db(database, table.table)
        filter_lambda = ast.Lambda(
            args=ast.arguments(posonlyargs=[], args=[ast.arg(arg='e')], kwonlyargs=[], kw_defaults=[], defaults=[]),
            body=ast.Compare(
                left=ast.Attribute(value=ast.Name(id='e', ctx=ast.Load()), attr='name', ctx=ast.Load()),
                ops=[ast.Gt()],
                comparators=[ast.Constant(value='Yossarian')]))
        p = Parser.parse(filter_lambda, [q._query._table], ParseType.filter)[0]

        self.assertEqual(LambdaExpression(["e"], BinaryExpression(
            left=OperandExpression(ColumnAliasExpression("e", ColumnReferenceExpression(name='name'))),
            right=OperandExpression(expression=LiteralExpression(literal=StringLiteral(val='Yossarian'))),
            operator=GreaterThanBinaryOperator())), p)

    def test_filter_with_ast_lambda_or_condition(self):
        table = Table("employee", [pa.field("name", pa.utf8())])
        database = Database("employee", [table])
        q = lq.using_db(database, table.table)
        filter_lambda = ast.Lambda(args=ast.arguments(posonlyargs=[], args=[ast.arg(arg='e')], kwonlyargs=[], kw_defaults=[], defaults=[]),
                                   body=ast.BoolOp(op=ast.Or(), values=[
                                       ast.Compare(left=ast.Attribute(value=ast.Name(id='e', ctx=ast.Load()), attr='name', ctx=ast.Load()), ops=[ast.Eq()], comparators=[ast.Constant(value='Yossarian')]),
                                       ast.Compare(left=ast.Attribute(value=ast.Name(id='e', ctx=ast.Load()), attr='name', ctx=ast.Load()), ops=[ast.Eq()], comparators=[ast.Constant(value='Orr')])]))
        p = Parser.parse(filter_lambda, [q._query._table], ParseType.filter)[0]

        self.assertEqual(LambdaExpression(parameters=['e'],
            expression=BinaryExpression(
                left=OperandExpression(
                    expression=BinaryExpression(
                        left=OperandExpression(
                            expression=ColumnAliasExpression(
                                alias='e', reference=ColumnReferenceExpression(name='name')
                            )
                        ),
                        right=OperandExpression(
                            expression=LiteralExpression(
                                literal=StringLiteral(val='Yossarian')
                            )
                        ),
                        operator=EqualsBinaryOperator()
                    )
                ),
                right=OperandExpression(
                    expression=BinaryExpression(
                        left=OperandExpression(
                            expression=ColumnAliasExpression(
                                alias='e', reference=ColumnReferenceExpression(name='name')
                            )
                        ),
                        right=OperandExpression(
                            expression=LiteralExpression(
                                literal=StringLiteral(val='Orr')
                            )
                        ),
                        operator=EqualsBinaryOperator()
                    )
                ),
                operator=OrBinaryOperator())), p)


    def test_nested_filter(self):
        table = Table("employee", [pa.field("start_date", pa.utf8()), pa.field("salary", pa.utf8())])
        database = Database("employee", [table])
        q = lq.using_db(database, table.table)
        filter = lambda e: (e.start_date > date(2021, 1, 1)) or (e.start_date < date(2000, 2, 2)) and (e.salary < 1_000_000)
        p = Parser.parse(filter, [q._query._table], ParseType.filter)[0]

        self.assertEqual(LambdaExpression(["e"], BinaryExpression(
            left=OperandExpression(
                BinaryExpression(
                    left=OperandExpression(
                        ColumnAliasExpression("e", ColumnReferenceExpression(name='start_date'))),
                    right=OperandExpression(
                        LiteralExpression(DateLiteral(date(2021, 1, 1)))),
                    operator=GreaterThanBinaryOperator())),
            right=OperandExpression(
                BinaryExpression(
                    left=OperandExpression(
                        BinaryExpression(
                            left=OperandExpression(
                                ColumnAliasExpression("e", ColumnReferenceExpression(name='start_date'))),
                            right=OperandExpression(
                                LiteralExpression(DateLiteral(date(2000, 2, 2)))),
                            operator=LessThanBinaryOperator())),
                    right=OperandExpression(
                        BinaryExpression(
                            left=OperandExpression(
                                ColumnAliasExpression("e", ColumnReferenceExpression(name='salary'))),
                            right=OperandExpression(
                                LiteralExpression(IntegerLiteral(1000000))),
                            operator=LessThanBinaryOperator())),
                    operator=AndBinaryOperator())),
            operator=OrBinaryOperator())), p)


    def test_extend(self):
        table = Table("employee", [pa.field("salary", pa.float32()), pa.field("benefits", pa.float32())])
        database = Database("employee", [table])
        q = lq.using_db(database, table.table)
        extend = lambda e: [
            (gross_salary := e.salary + 10),
            (gross_cost := gross_salary + e.benefits)]

        p = Parser.parse(extend, [q._query._table], ParseType.extend)[0]

        self.assertEqual([
            ComputedColumnAliasExpression(
                alias='gross_salary',
                expression=LambdaExpression(
                    ["e"],
                    BinaryExpression(
                        left=OperandExpression(ColumnAliasExpression("e", ColumnReferenceExpression('salary'))),
                        right=OperandExpression(LiteralExpression(IntegerLiteral(10))),
                        operator=AddBinaryOperator()))),
            ComputedColumnAliasExpression(
                alias="gross_cost",
                expression=LambdaExpression(
                    ["e"],
                    BinaryExpression(
                        left=OperandExpression(ColumnAliasExpression("e", ColumnReferenceExpression("gross_salary"))),
                        right=OperandExpression(ColumnAliasExpression("e", ColumnReferenceExpression("benefits"))),
                        operator=AddBinaryOperator())))], p)


    def test_sort(self):
        table = Table("employee", [pa.field("sum_gross_cost", pa.float32()), pa.field("country", pa.utf8())])
        database = Database("employee", [table])
        q = lq.using_db(database, table.table)
        sort = lambda e: [+e.sum_gross_cost, -e.country]
        p = Parser.parse(sort, [q._query._table], ParseType.order_by)[0]

        self.assertEqual( [
            OrderByExpression(direction=AscendingOrderType(), expression=ColumnReferenceExpression(name='sum_gross_cost')),
            OrderByExpression(direction=DescendingOrderType(), expression=ColumnReferenceExpression(name='country'))
        ], p)


    def test_fstring(self):
        table = Table("employee", [pa.field("title", pa.utf8()), pa.field("country", pa.utf8())])
        database = Database("employee", [table])
        q = lq.using_db(database, table.table)
        fstring = lambda e: (new_id := f"{e.title}_{e.country}")
        p = Parser.parse(fstring, [q._query._table], ParseType.extend)[0]

        self.assertEqual([ComputedColumnAliasExpression(
            alias='new_id',
            expression=LambdaExpression(["e"], FunctionExpression(
                function=StringConcatFunction(),
                parameters=[
                    ColumnAliasExpression("e", ColumnReferenceExpression(name='title')),
                    LiteralExpression(StringLiteral("_")),
                    ColumnAliasExpression("e", ColumnReferenceExpression(name='country'))])))], p)


    @unittest.skip
    def test_aggregate(self):
        table = Table("employee", [pa.field("id", pa.int32()), pa.field("name", pa.utf8()), pa.field("salary", pa.float32()), pa.field("department_name", pa.utf8())])
        database = Database("employee", [table])
        q = lq.using_db(database, table.table)
        group = lambda r: aggregate(
            [r.id, r.name],
            [sum_salary := sum(r.salary + 1), count_dept := count(r.department_name)],
            having=sum_salary > 100_000)

        #->groupBy(~[id, name], sum_salary: r | $r.salary + 1 : s | ($s)->sum()
        p = Parser.parse(group, [q._query._table], ParseType.group_by)[0]
        f = GroupByExpression(
            selections=[ColumnReferenceExpression(name="id"), ColumnReferenceExpression(name='name')],
            expressions=[ComputedColumnAliasExpression(
                alias='sum_salary',
                expression=MapReduceExpression(
                    map_expression=LambdaExpression(
                        parameters=["r"],
                        expression=BinaryExpression(
                                left=OperandExpression(
                                    ColumnAliasExpression(
                                        alias="r",
                                        reference=ColumnReferenceExpression("salary"))),
                                right=OperandExpression(
                                    LiteralExpression(
                                        literal=IntegerLiteral(1))),
                                operator=AddBinaryOperator())),
                    reduce_expression=LambdaExpression(
                        parameters=["r"],
                        expression=FunctionExpression(
                            function=SumFunction(),
                            parameters=[VariableAliasExpression("r")]))
                )),
                ComputedColumnAliasExpression(
                    alias='count_dept',
                    expression=MapReduceExpression(
                        map_expression=LambdaExpression(
                            parameters=["r"],
                            expression=ColumnAliasExpression("r", ColumnReferenceExpression(name='department_name'))),
                        reduce_expression=LambdaExpression(
                            parameters=["r"],
                            expression=FunctionExpression(
                                function=CountFunction(),
                                parameters=[VariableAliasExpression("r")])))
                )]
            )

        self.assertEqual(str(f), str(p))

    @unittest.skip("need to support window")
    def test_window(self):
        table = Table("employee", [pa.field("location", pa.utf8()), pa.field("salary", pa.float32()), pa.field("emp_name", pa.utf8())])
        database = Database("employee", [table])
        q = lq.using_db(database, table.table)
        window = lambda r: (avg_val :=
                            over(r.location, avg(r.salary), sort=[r.emp_name, -r.location], frame=rows(0, unbounded())))

        p = Parser.parse(window, [q._query._table], ParseType.over)[0]

        f = ComputedColumnAliasExpression(
            alias='avg_val',
            expression=LambdaExpression(["r"], FunctionExpression(
                function=OverFunction(),
                parameters=[
                    ColumnAliasExpression("r", ColumnReferenceExpression(name='location')),
                    FunctionExpression(
                        function=AvgFunction(),
                        parameters=[ColumnAliasExpression("r", ColumnReferenceExpression(name='salary'))]),
                    [ColumnAliasExpression("r", ColumnReferenceExpression(name='emp_name')),
                     OrderByExpression(
                         direction=DescendingOrderType(),
                         expression=ColumnAliasExpression("r", ColumnReferenceExpression(name='location')))],
                    FunctionExpression(
                        function=RowsFunction(),
                        parameters=[LiteralExpression(IntegerLiteral(0)),
                                    FunctionExpression(function=UnboundedFunction(), parameters=[])])])))

        self.assertEqual(f, p)

    def test_if(self):
        table = Table("employee", [pa.field("salary", pa.float32()), pa.field("min_salary", pa.float32())])
        database = Database("employee", [table])
        q = lq.using_db(database, table.table)
        extend = lambda e: (gross_salary := e.salary if e.salary > 10 else e.min_salary)
        p = Parser.parse(extend, [q._query._table], ParseType.extend)[0]

        self.assertEqual([ComputedColumnAliasExpression(alias='gross_salary',
                              expression=LambdaExpression(["e"], IfExpression(test=BinaryExpression(left=OperandExpression(expression=ColumnAliasExpression("e", ColumnReferenceExpression(name='salary'))),
                                                                            right=OperandExpression(expression=LiteralExpression(literal=IntegerLiteral(val=10))),
                                                                            operator=GreaterThanBinaryOperator()),
                                                      body=ColumnAliasExpression("e", ColumnReferenceExpression(name='salary')),
                                                      orelse=ColumnAliasExpression("e", ColumnReferenceExpression(name='min_salary')))))], p);

    def test_modulo(self):
        table = Table("employee", [pa.field("salary", pa.int32())])
        database = Database("employee", [table])
        q = lq.using_db(database, table.table)
        extend = lambda e: (mod_salary := e.salary % 2 )
        p = Parser.parse(extend, [q._query._table], ParseType.extend)[0]

        self.assertEqual([ComputedColumnAliasExpression(alias='mod_salary',
                               expression=LambdaExpression(parameters=['e'],
                                                           expression=FunctionExpression(function=ModuloFunction(),
                                                                                         parameters=[ColumnAliasExpression(alias='e',
                                                                                                                           reference=ColumnReferenceExpression(name='salary')),
                                                                                                     LiteralExpression(literal=IntegerLiteral(val=2))])))], p);

    def test_exponent(self):
        table = Table("employee", [pa.field("salary", pa.int32())])
        database = Database("employee", [table])
        q = lq.using_db(database, table.table)
        extend = lambda e: (exp_salary := e.salary ** 2 )
        p = Parser.parse(extend, [q._query._table], ParseType.extend)[0]

        self.assertEqual([ComputedColumnAliasExpression(alias='exp_salary',
                               expression=LambdaExpression(parameters=['e'],
                                                           expression=FunctionExpression(function=ExponentFunction(),
                                                                                         parameters=[ColumnAliasExpression(alias='e',
                                                                                                                           reference=ColumnReferenceExpression(name='salary')),
                                                                                                     LiteralExpression(literal=IntegerLiteral(val=2))])))], p);

# if __name__ == '__main__':
#     unittest.main()


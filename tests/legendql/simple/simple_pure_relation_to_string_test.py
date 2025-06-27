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

from _datetime import datetime

from legendql.model.metamodel import IntegerLiteral, InnerJoinType, BinaryExpression, ColumnAliasExpression, LiteralExpression, \
    EqualsBinaryOperator, OperandExpression, FunctionExpression, \
    CountFunction, AddBinaryOperator, SubtractBinaryOperator, MultiplyBinaryOperator, DivideBinaryOperator, \
    ColumnReferenceExpression, ComputedColumnAliasExpression, MapReduceExpression, LambdaExpression, \
    VariableAliasExpression, \
    AverageFunction, OrderByExpression, AscendingOrderType, DescendingOrderType, IfExpression, \
    GreaterThanBinaryOperator, DateLiteral, ModuloFunction, ExponentFunction
from legendql.model.schema import Database, Table
import legendql as lq
import pyarrow as pa


class TestClauseToPureRelationDialect(unittest.TestCase):

    def setUp(self):
        pass

    def test_simple_select(self):
        table = Table("table", [pa.field("id", pa.int32()), pa.field("departmentId", pa.int32()), pa.field("first", pa.utf8()), pa.field("last", pa.utf8())])
        database = Database("local::DuckDuckDatabase", [table])
        query = (lq.using_db(database, table.table)._query
                 .select("column"))
        pure_relation = query.to_string()
        self.assertEqual("#>{local::DuckDuckDatabase.table}#->select(~[column])->from(legendql::Runtime)", pure_relation)

    def test_simple_select_with_filter(self):
        table = Table("table", [pa.field("id", pa.int32()), pa.field("departmentId", pa.int32()), pa.field("first", pa.utf8()), pa.field("last", pa.utf8())])
        database = Database("local::DuckDuckDatabase", [table])
        query = (lq.using_db(database, table.table)._query
                 .select("column")
                 .filter(LambdaExpression(["a"], BinaryExpression(OperandExpression(ColumnAliasExpression("a", ColumnReferenceExpression("column"))), OperandExpression(LiteralExpression(IntegerLiteral(1))), EqualsBinaryOperator()))))
        pure_relation = query.to_string()
        self.assertEqual("#>{local::DuckDuckDatabase.table}#->select(~[column])->filter(a | $a.column==1)->from(legendql::Runtime)", pure_relation)

    def test_simple_select_with_extend(self):
        table = Table("table", [pa.field("id", pa.int32()), pa.field("departmentId", pa.int32()), pa.field("first", pa.utf8()), pa.field("last", pa.utf8())])
        database = Database("local::DuckDuckDatabase", [table])
        query = (lq.using_db(database, table.table)._query
                 .select("column")
                 .extend([ComputedColumnAliasExpression("a", LambdaExpression(["a"], ColumnAliasExpression("a", ColumnReferenceExpression("column"))))]))
        pure_relation = query.to_string()
        self.assertEqual("#>{local::DuckDuckDatabase.table}#->select(~[column])->extend(~[a:a | $a.column])->from(legendql::Runtime)", pure_relation)

    def test_simple_select_with_groupBy(self):
        table = Table("table", [pa.field("id", pa.int32()), pa.field("departmentId", pa.int32()), pa.field("first", pa.utf8()), pa.field("last", pa.utf8())])
        database = Database("local::DuckDuckDatabase", [table])
        query = (lq.using_db(database, table.table)._query
                 .select("column", "column2")
                 .group_by([ColumnReferenceExpression("column"), ColumnReferenceExpression("column2")],
              [ComputedColumnAliasExpression("count",
                                             MapReduceExpression(
                                                 LambdaExpression(["a"], BinaryExpression(OperandExpression(ColumnAliasExpression("a", ColumnReferenceExpression("column"))), OperandExpression(ColumnAliasExpression("a", ColumnReferenceExpression("column2"))), AddBinaryOperator())),
                                                 LambdaExpression(["a"], FunctionExpression(CountFunction(), [VariableAliasExpression("a")])))),
                        ComputedColumnAliasExpression("avg",
                                             MapReduceExpression(
                                                 LambdaExpression(["a"], ColumnAliasExpression("a", ColumnReferenceExpression("column"))),
                                                 LambdaExpression(["a"], FunctionExpression(AverageFunction(), [VariableAliasExpression("a")]))))]))
        pure_relation = query.to_string()
        self.assertEqual(
            "#>{local::DuckDuckDatabase.table}#->select(~[column, column2])->groupBy(~[column, column2], ~[count:a | $a.column+$a.column2 : a | $a->count(), avg:a | $a.column : a | $a->avg()])->from(legendql::Runtime)",
            pure_relation)

    def test_simple_select_with_groupBy_with_having(self):
        table = Table("table", [pa.field("id", pa.int32()), pa.field("departmentId", pa.int32()), pa.field("first", pa.utf8()), pa.field("last", pa.utf8())])
        database = Database("local::DuckDuckDatabase", [table])
        query = (lq.using_db(database, table.table)._query
                 .select("column", "column2")
                 .group_by([ColumnReferenceExpression("column"), ColumnReferenceExpression("column2")],
              [ComputedColumnAliasExpression("count",
                                             MapReduceExpression(
                                                 LambdaExpression(["a"], BinaryExpression(OperandExpression(ColumnAliasExpression("a", ColumnReferenceExpression("column"))), OperandExpression(ColumnAliasExpression("a", ColumnReferenceExpression("column2"))), AddBinaryOperator())),
                                                 LambdaExpression(["a"], FunctionExpression(CountFunction(), [VariableAliasExpression("a")])))),
                        ComputedColumnAliasExpression("avg",
                                             MapReduceExpression(
                                                 LambdaExpression(["a"], ColumnAliasExpression("a", ColumnReferenceExpression("column"))),
                                                 LambdaExpression(["a"], FunctionExpression(AverageFunction(), [VariableAliasExpression("a")]))))],
              LambdaExpression(["a"], BinaryExpression(OperandExpression(ColumnAliasExpression("a", ColumnReferenceExpression("column"))), OperandExpression(LiteralExpression(IntegerLiteral(1))), EqualsBinaryOperator()))))
        pure_relation = query.to_string()
        self.assertEqual(
            "#>{local::DuckDuckDatabase.table}#->select(~[column, column2])->groupBy(~[column, column2], ~[count:a | $a.column+$a.column2 : a | $a->count(), avg:a | $a.column : a | $a->avg()])->filter(a | $a.column==1)->from(legendql::Runtime)",
            pure_relation)

    def test_simple_select_with_limit(self):
        table = Table("table", [pa.field("id", pa.int32()), pa.field("departmentId", pa.int32()), pa.field("first", pa.utf8()), pa.field("last", pa.utf8())])
        database = Database("local::DuckDuckDatabase", [table])
        query = (lq.using_db(database, table.table)._query
                 .select("column")
                 .limit(10))
        pure_relation = query.to_string()
        self.assertEqual("#>{local::DuckDuckDatabase.table}#->select(~[column])->limit(10)->from(legendql::Runtime)", pure_relation)

    def test_simple_select_with_join(self):
        table = Table("table", [pa.field("id", pa.int32()), pa.field("departmentId", pa.int32()), pa.field("first", pa.utf8()), pa.field("last", pa.utf8())])
        database = Database("local::DuckDuckDatabase", [table])
        query = (lq.using_db(database, table.table)._query
                 .select("column")
                 .join("local::DuckDuckDatabase", None, "table2", InnerJoinType(), LambdaExpression(["a", "b"], BinaryExpression(OperandExpression(ColumnAliasExpression("a", ColumnReferenceExpression("column"))), OperandExpression(ColumnAliasExpression("b", ColumnReferenceExpression("column"))), EqualsBinaryOperator())))
                 .select("column2"))
        pure_relation = query.to_string()
        self.assertEqual("#>{local::DuckDuckDatabase.table}#->select(~[column])->join(#>{local::DuckDuckDatabase.table2}#, JoinKind.INNER, {a, b | $a.column==$b.column})->select(~[column2])->from(legendql::Runtime)", pure_relation)

    def test_multiple_extends(self):
        table = Table("table", [pa.field("id", pa.int32()), pa.field("departmentId", pa.int32()), pa.field("first", pa.utf8()), pa.field("last", pa.utf8())])
        database = Database("local::DuckDuckDatabase", [table])
        query = (lq.using_db(database, table.table)._query
                 .extend([ComputedColumnAliasExpression("a", LambdaExpression(["a"], ColumnAliasExpression("a", ColumnReferenceExpression("column")))), ComputedColumnAliasExpression("b", LambdaExpression(["b"], ColumnAliasExpression("b", ColumnReferenceExpression("column"))))]))
        pure_relation = query.to_string()
        self.assertEqual(
            "#>{local::DuckDuckDatabase.table}#->extend(~[a:a | $a.column, b:b | $b.column])->from(legendql::Runtime)",
            pure_relation)

    def test_math_binary_operators(self):
        table = Table("table", [pa.field("id", pa.int32()), pa.field("departmentId", pa.int32()), pa.field("first", pa.utf8()), pa.field("last", pa.utf8())])
        database = Database("local::DuckDuckDatabase", [table])
        query = (lq.using_db(database, table.table)._query
                 .extend([
                        ComputedColumnAliasExpression("add", LambdaExpression(["a"], BinaryExpression(left=OperandExpression(ColumnAliasExpression("a", ColumnReferenceExpression("column"))), right=OperandExpression(ColumnAliasExpression("a", ColumnReferenceExpression("column"))), operator=AddBinaryOperator()))),
                        ComputedColumnAliasExpression("subtract", LambdaExpression(["a"], BinaryExpression(left=OperandExpression(ColumnAliasExpression("a", ColumnReferenceExpression("column"))), right=OperandExpression(ColumnAliasExpression("a", ColumnReferenceExpression("column"))), operator=SubtractBinaryOperator()))),
                        ComputedColumnAliasExpression("multiply", LambdaExpression(["a"], BinaryExpression(left=OperandExpression(ColumnAliasExpression("a", ColumnReferenceExpression("column"))), right=OperandExpression(ColumnAliasExpression("a", ColumnReferenceExpression("column"))), operator=MultiplyBinaryOperator()))),
                        ComputedColumnAliasExpression("divide", LambdaExpression(["a"], BinaryExpression(left=OperandExpression(ColumnAliasExpression("a", ColumnReferenceExpression("column"))), right=OperandExpression(ColumnAliasExpression("a", ColumnReferenceExpression("column"))), operator=DivideBinaryOperator()))),
                    ]))
        pure_relation = query.to_string()
        self.assertEqual(
            "#>{local::DuckDuckDatabase.table}#->extend(~[add:a | $a.column+$a.column, subtract:a | $a.column-$a.column, multiply:a | $a.column*$a.column, divide:a | $a.column/$a.column])->from(legendql::Runtime)",
            pure_relation)

    def test_single_rename(self):
        table = Table("table", [pa.field("id", pa.int32()), pa.field("departmentId", pa.int32()), pa.field("first", pa.utf8()), pa.field("last", pa.utf8())])
        database = Database("local::DuckDuckDatabase", [table])
        query = (lq.using_db(database, table.table)._query
                 .rename(('column', 'newColumn')))
        pure_relation = query.to_string()
        self.assertEqual(
            "#>{local::DuckDuckDatabase.table}#->rename(~column, ~newColumn)->from(legendql::Runtime)",
            pure_relation)

    def test_multiple_renames(self):
        table = Table("table", [pa.field("id", pa.int32()), pa.field("departmentId", pa.int32()), pa.field("first", pa.utf8()), pa.field("last", pa.utf8())])
        database = Database("local::DuckDuckDatabase", [table])
        query = (lq.using_db(database, table.table)._query
                 .rename(('columnA', 'newColumnA'), ('columnB', 'newColumnB')))
        pure_relation = query.to_string()
        self.assertEqual(
            "#>{local::DuckDuckDatabase.table}#->rename(~columnA, ~newColumnA)->rename(~columnB, ~newColumnB)->from(legendql::Runtime)",
            pure_relation)

    def test_offset(self):
        table = Table("table", [pa.field("id", pa.int32()), pa.field("departmentId", pa.int32()), pa.field("first", pa.utf8()), pa.field("last", pa.utf8())])
        database = Database("local::DuckDuckDatabase", [table])
        query = (lq.using_db(database, table.table)._query
                 .offset(5))
        pure_relation = query.to_string()
        self.assertEqual(
            "#>{local::DuckDuckDatabase.table}#->drop(5)->from(legendql::Runtime)",
            pure_relation)

    def test_order_by(self):
        table = Table("table", [pa.field("id", pa.int32()), pa.field("departmentId", pa.int32()), pa.field("first", pa.utf8()), pa.field("last", pa.utf8())])
        database = Database("local::DuckDuckDatabase", [table])
        query = (lq.using_db(database, table.table)._query
                 .order_by(
                    OrderByExpression(direction=AscendingOrderType(), expression=ColumnReferenceExpression(name="columnA")),
                    OrderByExpression(direction=DescendingOrderType(), expression=ColumnReferenceExpression(name="columnB"))))
        pure_relation = query.to_string()
        self.assertEqual(
            "#>{local::DuckDuckDatabase.table}#->sort([~columnA->ascending(), ~columnB->descending()])->from(legendql::Runtime)",
            pure_relation)

    def test_conditional(self):
        table = Table("table", [pa.field("id", pa.int32()), pa.field("columnA", pa.int32()), pa.field("columnB", pa.int32())])
        database = Database("local::DuckDuckDatabase", [table])
        query = (lq.using_db(database, table.table)._query
                 .extend([ComputedColumnAliasExpression("conditional", LambdaExpression(["a"], IfExpression(test=BinaryExpression(left=OperandExpression(ColumnAliasExpression("a", ColumnReferenceExpression("columnA"))), right=OperandExpression(ColumnAliasExpression("a", ColumnReferenceExpression("columnB"))), operator=GreaterThanBinaryOperator()), body=ColumnAliasExpression("a", ColumnReferenceExpression("columnA")), orelse=ColumnAliasExpression("a", ColumnReferenceExpression("columnB")))))]))
        pure_relation = query.to_string()
        self.assertEqual(
            "#>{local::DuckDuckDatabase.table}#->extend(~[conditional:a | if($a.columnA>$a.columnB, | $a.columnA, | $a.columnB)])->from(legendql::Runtime)",
            pure_relation)

    def test_date(self):
        table = Table("table", [pa.field("id", pa.int32()), pa.field("columnA", pa.int32()), pa.field("columnB", pa.int32())])
        database = Database("local::DuckDuckDatabase", [table])
        query = (lq.using_db(database, table.table)._query
            .extend([
                ComputedColumnAliasExpression("dateGreater", LambdaExpression(parameters=["a"], expression=BinaryExpression(left=OperandExpression(LiteralExpression(literal=DateLiteral(datetime(2025, 4, 11)))), right=OperandExpression(LiteralExpression(literal=DateLiteral(datetime(2025, 4, 12)))), operator=GreaterThanBinaryOperator()))),
                ComputedColumnAliasExpression("dateTimeGreater", LambdaExpression(parameters=["a"], expression=BinaryExpression(left=OperandExpression(LiteralExpression(literal=DateLiteral(datetime(2025, 4, 11, 10, 0, 0)))), right=OperandExpression(LiteralExpression(literal=DateLiteral(datetime(2025, 4, 12, 10, 0, 0)))), operator=GreaterThanBinaryOperator()))),]))
        pure_relation = query.to_string()
        self.assertEqual(
            "#>{local::DuckDuckDatabase.table}#->extend(~[dateGreater:a | %2025-04-11T00:00:00>%2025-04-12T00:00:00, dateTimeGreater:a | %2025-04-11T10:00:00>%2025-04-12T10:00:00])->from(legendql::Runtime)",
            pure_relation)

    def test_modulo_and_exponent(self):
        table = Table("table", [pa.field("id", pa.int32()), pa.field("columnA", pa.int32()), pa.field("columnB", pa.int32())])
        database = Database("local::DuckDuckDatabase", [table])
        query = (lq.using_db(database, table.table)._query
                      .extend([
                        ComputedColumnAliasExpression("modulo", LambdaExpression(["a"], FunctionExpression(parameters=[ColumnAliasExpression("a", ColumnReferenceExpression("column")), LiteralExpression(literal=IntegerLiteral(2))], function=ModuloFunction()))),
                        ComputedColumnAliasExpression("exponent", LambdaExpression(["a"], FunctionExpression(parameters=[ColumnAliasExpression("a", ColumnReferenceExpression("column")), LiteralExpression(literal=IntegerLiteral(2))], function=ExponentFunction())))
                      ]))
        pure_relation = query.to_string()
        self.assertEqual(
            "#>{local::DuckDuckDatabase.table}#->extend(~[modulo:a | $a.column->mod(2), exponent:a | $a.column->pow(2)])->from(legendql::Runtime)",
            pure_relation)

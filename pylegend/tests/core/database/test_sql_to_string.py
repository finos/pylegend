# Copyright 2023 Goldman Sachs
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

import pytest
from textwrap import dedent
from pylegend.core.databse.sql_to_string import (
    SqlToStringGenerator,
    SqlToStringConfig,
    SqlToStringDbExtension,
    SqlToStringFormat
)
from pylegend.core.sql.metamodel import (
    AllColumns,
    IntegerLiteral,
    LongLiteral,
    StringLiteral,
    DoubleLiteral,
    NullLiteral,
    BooleanLiteral,
    QuerySpecification,
    Select,
    SingleColumn,
    ComparisonExpression,
    ComparisonOperator,
    LogicalBinaryType,
    LogicalBinaryExpression,
    NotExpression,
    ArithmeticType,
    ArithmeticExpression,
    NegativeExpression,
    SearchedCaseExpression,
    WhenClause,
    ColumnType,
    Cast,
    InListExpression,
    InPredicate,
    QualifiedName,
    Table,
    AliasedRelation,
    TableSubquery,
    Query,
    SubqueryExpression,
    JoinOn,
    Join,
    JoinType,
    SortItem,
    SortItemOrdering,
    SortItemNullOrdering,
    QualifiedNameReference,
    IsNullPredicate,
    IsNotNullPredicate,
    CurrentTime,
    CurrentTimeType,
    Extract,
    ExtractField,
    NamedArgumentExpression,
    FunctionCall,
    Window,
    TableFunction,
    Union,
)
from pylegend.core.sql.metamodel_extension import (
    StringLengthExpression,
    StringLikeExpression,
)


class TestSqlToStringDbExtension(SqlToStringDbExtension):
    @classmethod
    def process_query_specification(cls, query: QuerySpecification,
                                    config: SqlToStringConfig, nested_subquery: bool = False) -> str:
        return ""


class TestSqlToStringGenerator(SqlToStringGenerator):
    @classmethod
    def database_type(cls) -> str:
        return "TestDB"

    @classmethod
    def create_sql_generator(cls) -> "SqlToStringGenerator":
        return TestSqlToStringGenerator()

    def get_db_extension(self) -> SqlToStringDbExtension:
        return TestSqlToStringDbExtension()


class TestSqlToString:
    def test_find_sql_to_string_generator(self) -> None:
        generator = SqlToStringGenerator.find_sql_to_string_generator_for_db_type("TestDB")
        assert isinstance(generator, TestSqlToStringGenerator)
        query = QuerySpecification(
            select=Select(selectItems=[], distinct=False), from_=[], where=None, groupBy=[],
            having=None, orderBy=[], limit=None, offset=None
        )
        assert generator.generate_sql_string(query, SqlToStringConfig(SqlToStringFormat(pretty=False))) == ""

    def test_find_sql_to_string_generator_error(self) -> None:
        with pytest.raises(
            RuntimeError,
            match="Found no \\(or multiple\\) sql to string generators for database type 'UnknownDB'. " +
                  "Found generators: \\[\\]"
        ):
            SqlToStringGenerator.find_sql_to_string_generator_for_db_type("UnknownDB")


class TestSqlToStringDbExtensionProcessing:
    def test_process_identifier(self) -> None:
        plain_extension = SqlToStringDbExtension()
        config = SqlToStringConfig(SqlToStringFormat(pretty=False))
        assert plain_extension.process_identifier("A", config) == "A"
        assert plain_extension.process_identifier("date", config) == '"date"'

    def test_process_all_columns(self) -> None:
        extension = SqlToStringDbExtension()
        config = SqlToStringConfig(SqlToStringFormat(pretty=False))
        assert extension.process_all_columns(AllColumns(prefix=None), config) == "*"
        assert extension.process_all_columns(AllColumns(prefix='"root"'), config) == '"root".*'

    def test_process_single_column(self) -> None:
        extension = SqlToStringDbExtension()
        config = SqlToStringConfig(SqlToStringFormat(pretty=False))
        single_column_without_alias = SingleColumn(expression=IntegerLiteral(101), alias=None)
        single_column_with_alias = SingleColumn(expression=IntegerLiteral(101), alias='"a"')
        assert extension.process_single_column(single_column_without_alias, config) == "101"
        assert extension.process_single_column(single_column_with_alias, config) == '101 AS "a"'

    def test_process_select_item(self) -> None:
        extension = SqlToStringDbExtension()
        config = SqlToStringConfig(SqlToStringFormat(pretty=False))
        single_column = SingleColumn(expression=IntegerLiteral(101), alias='"a"')
        all_columns = AllColumns(prefix='"root"')
        assert extension.process_select_item(single_column, config) == '101 AS "a"'
        assert extension.process_select_item(all_columns, config) == '"root".*'

    def test_process_select(self) -> None:
        extension = SqlToStringDbExtension()
        config = SqlToStringConfig(SqlToStringFormat(pretty=False))
        select_with_distinct = Select(
            selectItems=[
                SingleColumn(expression=IntegerLiteral(101), alias='"a"'),
                SingleColumn(expression=IntegerLiteral(202), alias=None),
            ],
            distinct=True
        )
        assert "SELECT" + extension.process_select(select_with_distinct, config) == 'SELECT DISTINCT 101 AS "a", 202'

        select_without_distinct = Select(
            selectItems=[
                AllColumns(prefix='"alias"'),
            ],
            distinct=False
        )
        assert "SELECT" + extension.process_select(select_without_distinct, config) == 'SELECT "alias".*'

    def test_process_select_pretty_format(self) -> None:
        extension = SqlToStringDbExtension()
        config = SqlToStringConfig(SqlToStringFormat(pretty=True))
        select = Select(
            selectItems=[
                SingleColumn(expression=IntegerLiteral(101), alias='a'),
                SingleColumn(expression=IntegerLiteral(202), alias='b'),
                SingleColumn(expression=IntegerLiteral(303), alias='c'),
                SingleColumn(expression=IntegerLiteral(404), alias='d'),
            ],
            distinct=True
        )
        expected = """\
        SELECT DISTINCT
            101 AS a,
            202 AS b,
            303 AS c,
            404 AS d"""
        assert "SELECT" + extension.process_select(select, config) == dedent(expected)

        select.distinct = False
        expected = """\
        SELECT
            101 AS a,
            202 AS b,
            303 AS c,
            404 AS d"""
        assert "SELECT" + extension.process_select(select, config) == dedent(expected)

    def test_process_literal(self) -> None:
        extension = SqlToStringDbExtension()
        config = SqlToStringConfig(SqlToStringFormat(pretty=False))
        assert extension.process_literal(IntegerLiteral(101), config) == '101'
        assert extension.process_literal(LongLiteral(202), config) == '202'
        assert extension.process_literal(DoubleLiteral(303.3), config) == "303.3"
        assert extension.process_literal(StringLiteral("a'b", quoted=False), config) == "'a''b'"
        assert extension.process_literal(BooleanLiteral(True), config) == "true"
        assert extension.process_literal(BooleanLiteral(False), config) == "false"
        assert extension.process_literal(NullLiteral(), config) == "null"

    def test_process_comparison_expression(self) -> None:
        extension = SqlToStringDbExtension()
        config = SqlToStringConfig(SqlToStringFormat(pretty=False))

        comparison = ComparisonExpression(IntegerLiteral(101), IntegerLiteral(202), ComparisonOperator.EQUAL)
        assert extension.process_expression(comparison, config) == "(101 = 202)"

        comparison.operator = ComparisonOperator.NOT_EQUAL
        assert extension.process_expression(comparison, config) == "(101 <> 202)"

        comparison.operator = ComparisonOperator.GREATER_THAN
        assert extension.process_expression(comparison, config) == "(101 > 202)"

        comparison.operator = ComparisonOperator.LESS_THAN
        assert extension.process_expression(comparison, config) == "(101 < 202)"

        comparison.operator = ComparisonOperator.GREATER_THAN_OR_EQUAL
        assert extension.process_expression(comparison, config) == "(101 >= 202)"

        comparison.operator = ComparisonOperator.LESS_THAN_OR_EQUAL
        assert extension.process_expression(comparison, config) == "(101 <= 202)"

    def test_process_logical_binary_expression(self) -> None:
        extension = SqlToStringDbExtension()
        config = SqlToStringConfig(SqlToStringFormat(pretty=False))

        c1 = ComparisonExpression(IntegerLiteral(101), IntegerLiteral(202), ComparisonOperator.LESS_THAN)
        c2 = ComparisonExpression(IntegerLiteral(303), IntegerLiteral(202), ComparisonOperator.GREATER_THAN)

        binary_expression = LogicalBinaryExpression(LogicalBinaryType.AND, c1, c2)
        assert extension.process_expression(binary_expression, config) == "((101 < 202) AND (303 > 202))"

        binary_expression = LogicalBinaryExpression(LogicalBinaryType.OR, c1, c2)
        assert extension.process_expression(binary_expression, config) == "((101 < 202) OR (303 > 202))"

    def test_process_not_expression(self) -> None:
        extension = SqlToStringDbExtension()
        config = SqlToStringConfig(SqlToStringFormat(pretty=False))

        c1 = ComparisonExpression(IntegerLiteral(101), IntegerLiteral(202), ComparisonOperator.LESS_THAN)
        not_expression = NotExpression(c1)
        assert extension.process_expression(not_expression, config) == "NOT(101 < 202)"

        c2 = BooleanLiteral(True)
        not_expression = NotExpression(c2)
        assert extension.process_expression(not_expression, config) == "NOT(true)"

    def test_process_arithmetic_expression(self) -> None:
        extension = SqlToStringDbExtension()
        config = SqlToStringConfig(SqlToStringFormat(pretty=False))

        add_expression = ArithmeticExpression(ArithmeticType.ADD, IntegerLiteral(202), IntegerLiteral(101))
        assert extension.process_expression(add_expression, config) == "(202 + 101)"

        sub_expression = ArithmeticExpression(ArithmeticType.SUBTRACT, IntegerLiteral(202), IntegerLiteral(101))
        assert extension.process_expression(sub_expression, config) == "(202 - 101)"

        mul_expression = ArithmeticExpression(ArithmeticType.MULTIPLY, IntegerLiteral(202), IntegerLiteral(101))
        assert extension.process_expression(mul_expression, config) == "(202 * 101)"

        div_expression = ArithmeticExpression(ArithmeticType.DIVIDE, IntegerLiteral(202), IntegerLiteral(101))
        assert extension.process_expression(div_expression, config) == "((1.0 * 202) / 101)"

        mod_expression = ArithmeticExpression(ArithmeticType.MODULUS, IntegerLiteral(202), IntegerLiteral(101))
        assert extension.process_expression(mod_expression, config) == "mod(202, 101)"

    def test_process_negative_expression(self) -> None:
        extension = SqlToStringDbExtension()
        config = SqlToStringConfig(SqlToStringFormat(pretty=False))

        negative_expression = NegativeExpression(IntegerLiteral(101))
        assert extension.process_expression(negative_expression, config) == "-101"

        negative_expression = NegativeExpression(
            ArithmeticExpression(ArithmeticType.ADD, IntegerLiteral(101), IntegerLiteral(202))
        )
        assert extension.process_expression(negative_expression, config) == "(0 - (101 + 202))"

    def test_searched_case_expression_processor(self) -> None:
        extension = SqlToStringDbExtension()
        config = SqlToStringConfig(SqlToStringFormat(pretty=False))

        case_expression_with_default = SearchedCaseExpression(
            whenClauses=[
                WhenClause(operand=BooleanLiteral(False), result=IntegerLiteral(101)),
                WhenClause(operand=BooleanLiteral(True), result=IntegerLiteral(202))
            ],
            defaultValue=IntegerLiteral(303)
        )
        assert extension.process_expression(case_expression_with_default, config) == \
               "CASE WHEN false THEN 101 WHEN true THEN 202 ELSE 303 END"

        case_expression_without_default = SearchedCaseExpression(
            whenClauses=[
                WhenClause(operand=BooleanLiteral(False), result=IntegerLiteral(101)),
                WhenClause(operand=BooleanLiteral(True), result=IntegerLiteral(202))
            ],
            defaultValue=None
        )
        assert extension.process_expression(case_expression_without_default, config) == \
               "CASE WHEN false THEN 101 WHEN true THEN 202 END"

    def test_searched_case_expression_processor_pretty_format(self) -> None:
        extension = SqlToStringDbExtension()
        config = SqlToStringConfig(SqlToStringFormat(pretty=True, indent_count=0))

        case_expression_with_default = SearchedCaseExpression(
            whenClauses=[
                WhenClause(operand=BooleanLiteral(False), result=IntegerLiteral(101)),
                WhenClause(operand=BooleanLiteral(True), result=IntegerLiteral(202))
            ],
            defaultValue=IntegerLiteral(303)
        )
        expected = """\
        CASE
            WHEN
                false
            THEN
                101
            WHEN
                true
            THEN
                202
            ELSE
                303
        END"""
        assert extension.process_expression(case_expression_with_default, config) == dedent(expected)

        case_expression_without_default = SearchedCaseExpression(
            whenClauses=[
                WhenClause(operand=BooleanLiteral(False), result=IntegerLiteral(101)),
                WhenClause(operand=BooleanLiteral(True), result=IntegerLiteral(202))
            ],
            defaultValue=None
        )
        expected = """\
        CASE
            WHEN
                false
            THEN
                101
            WHEN
                true
            THEN
                202
        END"""
        assert extension.process_expression(case_expression_without_default, config) == dedent(expected)

    def test_select_with_case_pretty_format(self) -> None:
        extension = SqlToStringDbExtension()
        config = SqlToStringConfig(SqlToStringFormat(pretty=True, indent_count=0))

        case_expression_with_default = SearchedCaseExpression(
            whenClauses=[
                WhenClause(operand=BooleanLiteral(False), result=IntegerLiteral(101)),
                WhenClause(operand=BooleanLiteral(True), result=IntegerLiteral(202))
            ],
            defaultValue=IntegerLiteral(303)
        )
        select_with_case = Select(False, [SingleColumn(None, case_expression_with_default)])
        expected = """\
                SELECT
                    CASE
                        WHEN
                            false
                        THEN
                            101
                        WHEN
                            true
                        THEN
                            202
                        ELSE
                            303
                    END"""
        assert "SELECT" + extension.process_select(select_with_case, config) == dedent(expected)

    def test_nested_case_pretty_format(self) -> None:
        extension = SqlToStringDbExtension()
        config = SqlToStringConfig(SqlToStringFormat(pretty=True, indent_count=0))

        case_expression_with_default = SearchedCaseExpression(
            whenClauses=[
                WhenClause(
                    operand=BooleanLiteral(False),
                    result=SearchedCaseExpression(
                        whenClauses=[
                            WhenClause(operand=BooleanLiteral(False), result=IntegerLiteral(101)),
                            WhenClause(operand=BooleanLiteral(True), result=IntegerLiteral(202))
                        ],
                        defaultValue=IntegerLiteral(303)
                    )
                ),
                WhenClause(operand=BooleanLiteral(True), result=IntegerLiteral(202))
            ],
            defaultValue=IntegerLiteral(303)
        )
        select_with_case = Select(True, [SingleColumn(None, case_expression_with_default)])
        expected = """\
                SELECT DISTINCT
                    CASE
                        WHEN
                            false
                        THEN
                            CASE
                                WHEN
                                    false
                                THEN
                                    101
                                WHEN
                                    true
                                THEN
                                    202
                                ELSE
                                    303
                            END
                        WHEN
                            true
                        THEN
                            202
                        ELSE
                            303
                    END"""
        assert "SELECT" + extension.process_select(select_with_case, config) == dedent(expected)

    def test_process_column_type(self) -> None:
        extension = SqlToStringDbExtension()
        config = SqlToStringConfig(SqlToStringFormat(pretty=False))

        column_type = ColumnType("BIGINT", parameters=[])
        assert extension.process_expression(column_type, config) == "BIGINT"

        column_type = ColumnType("VARCHAR", parameters=[100])
        assert extension.process_expression(column_type, config) == "VARCHAR(100)"

        column_type = ColumnType("DECIMAL", parameters=[20, 10])
        assert extension.process_expression(column_type, config) == "DECIMAL(20, 10)"

    def test_process_cast_expression(self) -> None:
        extension = SqlToStringDbExtension()
        config = SqlToStringConfig(SqlToStringFormat(pretty=False))

        column_type = ColumnType("DECIMAL", parameters=[20, 10])
        cast = Cast(IntegerLiteral(101), column_type)
        assert extension.process_expression(cast, config) == "CAST(101 AS DECIMAL(20, 10))"

    def test_process_in_predicate(self) -> None:
        extension = SqlToStringDbExtension()
        config = SqlToStringConfig(SqlToStringFormat(pretty=False))

        in_list = InListExpression([IntegerLiteral(101), IntegerLiteral(202)])
        in_predicate = InPredicate(IntegerLiteral(101), in_list)
        assert extension.process_expression(in_predicate, config) == "101 IN (101, 202)"

    def test_process_qualified_name(self) -> None:
        extension = SqlToStringDbExtension()
        config = SqlToStringConfig(SqlToStringFormat(pretty=False))

        qualified_name = QualifiedName(["test_db", "test_schema", "test_table"])
        assert extension.process_qualified_name(qualified_name, config) == "test_db.test_schema.test_table"

        qualified_name = QualifiedName(["test_db", "test_schema", "kerberos"])
        assert extension.process_qualified_name(qualified_name, config) == 'test_db.test_schema."kerberos"'

    def test_process_qualified_name_reference(self) -> None:
        extension = SqlToStringDbExtension()
        config = SqlToStringConfig(SqlToStringFormat(pretty=False))

        ref = QualifiedNameReference(QualifiedName(["test_db", "test_schema", "test_table"]))
        assert extension.process_expression(ref, config) == "test_db.test_schema.test_table"

    def test_process_is_null_predicate(self) -> None:
        extension = SqlToStringDbExtension()
        config = SqlToStringConfig(SqlToStringFormat(pretty=False))

        column_ref = QualifiedNameReference(QualifiedName(["test_db", "test_schema", "test_table", "test_col"]))
        check = IsNullPredicate(column_ref)
        assert extension.process_expression(check, config) == "(test_db.test_schema.test_table.test_col IS NULL)"

    def test_process_is_not_null_predicate(self) -> None:
        extension = SqlToStringDbExtension()
        config = SqlToStringConfig(SqlToStringFormat(pretty=False))

        column_ref = QualifiedNameReference(QualifiedName(["test_db", "test_schema", "test_table", "test_col"]))
        check = IsNotNullPredicate(column_ref)
        assert extension.process_expression(check, config) == "(test_db.test_schema.test_table.test_col IS NOT NULL)"

    def test_process_current_time(self) -> None:
        extension = SqlToStringDbExtension()
        config = SqlToStringConfig(SqlToStringFormat(pretty=False))

        assert extension.process_expression(CurrentTime(CurrentTimeType.TIME, 2), config) == "CURRENT_TIME(2)"
        assert extension.process_expression(CurrentTime(CurrentTimeType.TIME, None), config) == "CURRENT_TIME()"
        assert extension.process_expression(CurrentTime(CurrentTimeType.TIMESTAMP, 8), config) == "CURRENT_TIMESTAMP(8)"
        assert extension.process_expression(CurrentTime(CurrentTimeType.TIMESTAMP, None), config) == \
               "CURRENT_TIMESTAMP()"
        assert extension.process_expression(CurrentTime(CurrentTimeType.DATE, None), config) == "CURRENT_DATE"

    def test_process_extract(self) -> None:
        extension = SqlToStringDbExtension()
        config = SqlToStringConfig(SqlToStringFormat(pretty=False))

        ref = QualifiedNameReference(QualifiedName(["test_db", "test_schema", "test_table", "test_col"]))
        assert extension.process_expression(Extract(ref, ExtractField.DAY), config) == \
               "EXTRACT(DAY FROM test_db.test_schema.test_table.test_col)"

    def test_process_named_argument_expression(self) -> None:
        extension = SqlToStringDbExtension()
        config = SqlToStringConfig(SqlToStringFormat(pretty=False))

        ref = QualifiedNameReference(QualifiedName(["test_db", "test_schema", "test_table", "test_col"]))
        assert extension.process_expression(NamedArgumentExpression("param1", ref), config) == \
               "param1 => test_db.test_schema.test_table.test_col"

    def test_process_function_call(self) -> None:
        extension = SqlToStringDbExtension()
        config = SqlToStringConfig(SqlToStringFormat(pretty=False))

        ref = QualifiedNameReference(QualifiedName(["test_db", "test_schema", "test_table", "test_col"]))
        func_call = FunctionCall(
            name=QualifiedName(["test", "func"]),
            distinct=False,
            arguments=[],
            filter_=None,
            window=None
        )
        assert extension.process_expression(func_call, config) == "test.func()"

        func_call.arguments = [
            ref
        ]
        assert extension.process_expression(func_call, config) == "test.func( test_db.test_schema.test_table.test_col )"

        func_call.arguments = [
            ref,
            NamedArgumentExpression("param1", ref)
        ]
        assert extension.process_expression(func_call, config) == \
               "test.func( test_db.test_schema.test_table.test_col, param1 => test_db.test_schema.test_table.test_col )"

        func_call = FunctionCall(
            name=QualifiedName(["rowNumber"]),
            distinct=False,
            filter_=None,
            arguments=[],
            window=Window(
                windowRef=None,
                partitions=[
                    QualifiedNameReference(QualifiedName(["partition_col1"])),
                    QualifiedNameReference(QualifiedName(["partition_col2"]))
                ],
                orderBy=[
                    SortItem(
                        QualifiedNameReference(QualifiedName(["sort_col1"])),
                        SortItemOrdering.DESCENDING,
                        SortItemNullOrdering.UNDEFINED
                    ),
                    SortItem(
                        QualifiedNameReference(QualifiedName(["sort_col2"])),
                        SortItemOrdering.ASCENDING,
                        SortItemNullOrdering.UNDEFINED
                    )
                ],
                windowFrame=None
            )
        )

        assert extension.process_expression(func_call, config) == \
               "rowNumber() OVER PARTITION BY partition_col1, partition_col2 ORDER BY sort_col1 DESC, sort_col2"

    def test_process_function_call_pretty_format(self) -> None:
        extension = SqlToStringDbExtension()
        config = SqlToStringConfig(SqlToStringFormat(pretty=True))

        ref = QualifiedNameReference(QualifiedName(["test_db", "test_schema", "test_table", "test_col"]))
        func_call = FunctionCall(
            name=QualifiedName(["test", "func"]),
            distinct=False,
            arguments=[],
            filter_=None,
            window=None
        )
        assert extension.process_expression(func_call, config) == "test.func()"

        func_call.arguments = [
            ref
        ]
        expected = """\
            test.func(
                test_db.test_schema.test_table.test_col
            )"""
        assert extension.process_expression(func_call, config) == dedent(expected)

        func_call.arguments = [
            ref,
            NamedArgumentExpression("param1", ref)
        ]
        expected = """\
            test.func(
                test_db.test_schema.test_table.test_col,
                param1 => test_db.test_schema.test_table.test_col
            )"""
        assert extension.process_expression(func_call, config) == dedent(expected)

        func_call = FunctionCall(
            name=QualifiedName(["rowNumber"]),
            distinct=False,
            filter_=None,
            arguments=[],
            window=Window(
                windowRef=None,
                partitions=[
                    QualifiedNameReference(QualifiedName(["partition_col1"])),
                    QualifiedNameReference(QualifiedName(["partition_col2"]))
                ],
                orderBy=[
                    SortItem(
                        QualifiedNameReference(QualifiedName(["sort_col1"])),
                        SortItemOrdering.DESCENDING,
                        SortItemNullOrdering.UNDEFINED
                    ),
                    SortItem(
                        QualifiedNameReference(QualifiedName(["sort_col2"])),
                        SortItemOrdering.ASCENDING,
                        SortItemNullOrdering.UNDEFINED
                    )
                ],
                windowFrame=None
            )
        )

        assert extension.process_expression(func_call, config) == \
               "rowNumber() OVER PARTITION BY partition_col1, partition_col2 ORDER BY sort_col1 DESC, sort_col2"

    def test_process_table(self) -> None:
        extension = SqlToStringDbExtension()
        config = SqlToStringConfig(SqlToStringFormat(pretty=False))

        table = Table(QualifiedName(["test_db", "test_schema", "test_table"]))
        assert extension.process_relation(table, config) == "test_db.test_schema.test_table"

    def test_process_table_function(self) -> None:
        extension = SqlToStringDbExtension()
        config = SqlToStringConfig(SqlToStringFormat(pretty=False))

        table_func = TableFunction(functionCall=FunctionCall(
            name=QualifiedName(["test", "func"]),
            distinct=False,
            filter_=None,
            arguments=[
                NamedArgumentExpression("param1", QualifiedNameReference(QualifiedName(["test_table", "test_col"])))
            ],
            window=None
        ))
        assert extension.process_relation(table_func, config) == "test.func( param1 => test_table.test_col )"

    def test_process_aliased_relation(self) -> None:
        extension = SqlToStringDbExtension()
        config = SqlToStringConfig(SqlToStringFormat(pretty=False))

        table = Table(QualifiedName(["test_db", "test_schema", "test_table"]))
        aliased = AliasedRelation(relation=table, alias='"root"', columnNames=[])
        assert extension.process_aliased_relation(aliased, config) == 'test_db.test_schema.test_table AS "root"'

    def test_process_table_subquery(self) -> None:
        extension = SqlToStringDbExtension()
        config = SqlToStringConfig(SqlToStringFormat(pretty=False))

        table = Table(QualifiedName(["test_db", "test_schema", "test_table"]))
        subquery = TableSubquery(query=Query(queryBody=table, limit=None, offset=None, orderBy=[]))
        assert extension.process_relation(subquery, config) == '( SELECT * FROM test_db.test_schema.test_table )'

    def test_process_table_subquery_pretty_format(self) -> None:
        extension = SqlToStringDbExtension()
        config = SqlToStringConfig(SqlToStringFormat(pretty=True))

        table = Table(QualifiedName(["test_db", "test_schema", "test_table"]))
        subquery = TableSubquery(query=Query(queryBody=table, limit=None, offset=None, orderBy=[]))
        expected = """\
                (
                    SELECT
                        *
                    FROM
                        test_db.test_schema.test_table
                )"""
        assert extension.process_relation(subquery, config) == dedent(expected)

    def test_process_subquery_expression(self) -> None:
        extension = SqlToStringDbExtension()
        config = SqlToStringConfig(SqlToStringFormat(pretty=False))

        table = Table(QualifiedName(["test_db", "test_schema", "test_table"]))
        sub = SubqueryExpression(query=Query(queryBody=table, limit=None, offset=None, orderBy=[]))
        assert extension.process_subquery_expression(sub, config) == '( SELECT * FROM test_db.test_schema.test_table )'

    def test_process_subquery_expression_pretty_format(self) -> None:
        extension = SqlToStringDbExtension()
        config = SqlToStringConfig(SqlToStringFormat(pretty=True))

        table = Table(QualifiedName(["test_db", "test_schema", "test_table"]))
        sub = SubqueryExpression(query=Query(queryBody=table, limit=None, offset=None, orderBy=[]))
        expected = """\
                (
                    SELECT
                        *
                    FROM
                        test_db.test_schema.test_table
                )"""
        assert extension.process_subquery_expression(sub, config) == dedent(expected)

    def test_process_join_on(self) -> None:
        extension = SqlToStringDbExtension()
        config = SqlToStringConfig(SqlToStringFormat(pretty=False))

        join_on = JoinOn(ComparisonExpression(IntegerLiteral(101), IntegerLiteral(202), ComparisonOperator.LESS_THAN))
        assert extension.process_join_criteria(join_on, config) == "101 < 202"

    def test_process_join(self) -> None:
        extension = SqlToStringDbExtension()
        config = SqlToStringConfig(SqlToStringFormat(pretty=False))

        table_a = Table(QualifiedName(["test_db", "test_schema", "test_table_a"]))
        table_b = Table(QualifiedName(["test_db", "test_schema", "test_table_b"]))
        criteria = JoinOn(ComparisonExpression(IntegerLiteral(101), IntegerLiteral(202), ComparisonOperator.LESS_THAN))

        join = Join(JoinType.CROSS, table_a, table_b, criteria)
        assert extension.process_relation(join, config) == \
               "test_db.test_schema.test_table_a CROSS JOIN test_db.test_schema.test_table_b ON (101 < 202)"

        join = Join(JoinType.INNER, table_a, table_b, criteria)
        assert extension.process_relation(join, config) == \
               "test_db.test_schema.test_table_a INNER JOIN test_db.test_schema.test_table_b ON (101 < 202)"

        join = Join(JoinType.LEFT, table_a, table_b, criteria)
        assert extension.process_relation(join, config) == \
               "test_db.test_schema.test_table_a LEFT OUTER JOIN test_db.test_schema.test_table_b ON (101 < 202)"

        join = Join(JoinType.RIGHT, table_a, table_b, criteria)
        assert extension.process_relation(join, config) == \
               "test_db.test_schema.test_table_a RIGHT OUTER JOIN test_db.test_schema.test_table_b ON (101 < 202)"

        join = Join(JoinType.FULL, table_a, table_b, criteria)
        assert extension.process_relation(join, config) == \
               "test_db.test_schema.test_table_a FULL OUTER JOIN test_db.test_schema.test_table_b ON (101 < 202)"

    def test_process_join_pretty_format(self) -> None:
        extension = SqlToStringDbExtension()
        config = SqlToStringConfig(SqlToStringFormat(pretty=True))

        table_a = Table(QualifiedName(["test_db", "test_schema", "test_table_a"]))
        table_b = Table(QualifiedName(["test_db", "test_schema", "test_table_b"]))
        criteria = JoinOn(ComparisonExpression(IntegerLiteral(101), IntegerLiteral(202), ComparisonOperator.LESS_THAN))

        join = Join(JoinType.CROSS, table_a, table_b, criteria)
        expected = """\
                test_db.test_schema.test_table_a
                CROSS JOIN
                    test_db.test_schema.test_table_b
                    ON (101 < 202)"""
        assert extension.process_relation(join, config) == dedent(expected)

    def test_process_top(self) -> None:
        extension = SqlToStringDbExtension()
        config = SqlToStringConfig(SqlToStringFormat(pretty=False))

        query = QuerySpecification(
            select=Select(selectItems=[], distinct=False), from_=[], where=None, groupBy=[],
            having=None, orderBy=[], limit=None, offset=None
        )
        assert extension.process_top(query, config) == ""

        query.limit = IntegerLiteral(101)
        assert extension.process_top(query, config) == " TOP 101"

        query.offset = IntegerLiteral(202)
        assert extension.process_top(query, config) == ""

    def test_process_limit(self) -> None:
        extension = SqlToStringDbExtension()
        config = SqlToStringConfig(SqlToStringFormat(pretty=False))

        query = QuerySpecification(
            select=Select(selectItems=[], distinct=False), from_=[], where=None, groupBy=[],
            having=None, orderBy=[], limit=None, offset=None
        )
        assert extension.process_limit(query, config) == ""

        query.limit = IntegerLiteral(101)
        assert extension.process_limit(query, config) == ""

        query.offset = IntegerLiteral(202)
        assert extension.process_limit(query, config) == " LIMIT 202, 101"

    def test_process_group_by(self) -> None:
        extension = SqlToStringDbExtension()
        config = SqlToStringConfig(SqlToStringFormat(pretty=False))

        query = QuerySpecification(
            select=Select(selectItems=[], distinct=False), from_=[], where=None, groupBy=[],
            having=None, orderBy=[], limit=None, offset=None
        )
        assert extension.process_group_by(query, config) == ""

        query.groupBy = [
            QualifiedNameReference(QualifiedName(["test_db", "test_schema", "test_table", "test_col1"]))
        ]
        assert extension.process_group_by(query, config) == " GROUP BY test_db.test_schema.test_table.test_col1"

        query.groupBy = [
            QualifiedNameReference(QualifiedName(["test_db", "test_schema", "test_table", "test_col1"])),
            QualifiedNameReference(QualifiedName(["test_db", "test_schema", "test_table", "test_col2"]))
        ]
        assert extension.process_group_by(query, config) == \
               " GROUP BY test_db.test_schema.test_table.test_col1, test_db.test_schema.test_table.test_col2"

    def test_process_group_by_pretty_format(self) -> None:
        extension = SqlToStringDbExtension()
        config = SqlToStringConfig(SqlToStringFormat(pretty=True))

        query = QuerySpecification(
            select=Select(selectItems=[], distinct=False), from_=[], where=None, groupBy=[],
            having=None, orderBy=[], limit=None, offset=None
        )
        assert extension.process_group_by(query, config) == ""

        query.groupBy = [
            QualifiedNameReference(QualifiedName(["test_db", "test_schema", "test_table", "test_col1"]))
        ]
        expected = """\
                GROUP BY
                    test_db.test_schema.test_table.test_col1"""
        assert extension.process_group_by(query, config) == "\n" + dedent(expected)

        query.groupBy = [
            QualifiedNameReference(QualifiedName(["test_db", "test_schema", "test_table", "test_col1"])),
            QualifiedNameReference(QualifiedName(["test_db", "test_schema", "test_table", "test_col2"]))
        ]
        expected = """\
                GROUP BY
                    test_db.test_schema.test_table.test_col1,
                    test_db.test_schema.test_table.test_col2"""
        assert extension.process_group_by(query, config) == "\n" + dedent(expected)

    def test_process_order_by(self) -> None:
        extension = SqlToStringDbExtension()
        config = SqlToStringConfig(SqlToStringFormat(pretty=False))

        query = QuerySpecification(
            select=Select(selectItems=[], distinct=False), from_=[], where=None, groupBy=[],
            having=None, orderBy=[], limit=None, offset=None
        )
        assert extension.process_order_by(query, config) == ""

        query.orderBy = [
            SortItem(
                QualifiedNameReference(QualifiedName(["test_db", "test_schema", "test_table", "test_col1"])),
                SortItemOrdering.ASCENDING,
                SortItemNullOrdering.UNDEFINED
            )
        ]
        assert extension.process_order_by(query, config) == " ORDER BY test_db.test_schema.test_table.test_col1"

        query.orderBy = [
            SortItem(
                QualifiedNameReference(QualifiedName(["test_db", "test_schema", "test_table", "test_col1"])),
                SortItemOrdering.ASCENDING,
                SortItemNullOrdering.UNDEFINED
            ),
            SortItem(
                QualifiedNameReference(QualifiedName(["test_db", "test_schema", "test_table", "test_col2"])),
                SortItemOrdering.DESCENDING,
                SortItemNullOrdering.UNDEFINED
            )
        ]
        assert extension.process_order_by(query, config) == \
               " ORDER BY test_db.test_schema.test_table.test_col1, test_db.test_schema.test_table.test_col2 DESC"

    def test_process_order_by_pretty_format(self) -> None:
        extension = SqlToStringDbExtension()
        config = SqlToStringConfig(SqlToStringFormat(pretty=True))

        query = QuerySpecification(
            select=Select(selectItems=[], distinct=False), from_=[], where=None, groupBy=[],
            having=None, orderBy=[], limit=None, offset=None
        )
        assert extension.process_order_by(query, config) == ""

        query.orderBy = [
            SortItem(
                QualifiedNameReference(QualifiedName(["test_db", "test_schema", "test_table", "test_col1"])),
                SortItemOrdering.ASCENDING,
                SortItemNullOrdering.UNDEFINED
            )
        ]
        expected = """\
                ORDER BY
                    test_db.test_schema.test_table.test_col1"""
        assert extension.process_order_by(query, config) == "\n" + dedent(expected)

        query.orderBy = [
            SortItem(
                QualifiedNameReference(QualifiedName(["test_db", "test_schema", "test_table", "test_col1"])),
                SortItemOrdering.ASCENDING,
                SortItemNullOrdering.UNDEFINED
            ),
            SortItem(
                QualifiedNameReference(QualifiedName(["test_db", "test_schema", "test_table", "test_col2"])),
                SortItemOrdering.DESCENDING,
                SortItemNullOrdering.UNDEFINED
            )
        ]
        expected = """\
                ORDER BY
                    test_db.test_schema.test_table.test_col1,
                    test_db.test_schema.test_table.test_col2 DESC"""
        assert extension.process_order_by(query, config) == "\n" + dedent(expected)

    def test_process_simple_query_specification(self) -> None:
        extension = SqlToStringDbExtension()
        config = SqlToStringConfig(SqlToStringFormat(pretty=False))

        query = QuerySpecification(
            select=Select(selectItems=[AllColumns(prefix='"root"')], distinct=True),
            from_=[AliasedRelation(Table(QualifiedName(["test_db", "test_schema", "test_table"])), '"root"', [])],
            where=ComparisonExpression(IntegerLiteral(101), IntegerLiteral(202), ComparisonOperator.LESS_THAN),
            groupBy=[],
            having=None,
            orderBy=[],
            limit=IntegerLiteral(101),
            offset=IntegerLiteral(202)
        )
        assert extension.process_query_specification(query, config) == \
               'SELECT DISTINCT "root".* FROM test_db.test_schema.test_table AS "root" ' \
               'WHERE (101 < 202) LIMIT 202, 101'

        assert extension.process_query_specification(query, config, True) == \
               '( SELECT DISTINCT "root".* FROM test_db.test_schema.test_table AS "root" ' \
               'WHERE (101 < 202) LIMIT 202, 101 )'

    def test_process_simple_query_specification_pretty_format(self) -> None:
        extension = SqlToStringDbExtension()
        config = SqlToStringConfig(SqlToStringFormat(pretty=True))

        query = QuerySpecification(
            select=Select(selectItems=[AllColumns(prefix='"root"')], distinct=True),
            from_=[AliasedRelation(Table(QualifiedName(["test_db", "test_schema", "test_table"])), '"root"', [])],
            where=ComparisonExpression(IntegerLiteral(101), IntegerLiteral(202), ComparisonOperator.LESS_THAN),
            groupBy=[],
            having=None,
            orderBy=[],
            limit=IntegerLiteral(101),
            offset=IntegerLiteral(202)
        )
        expected = """\
            SELECT DISTINCT
                "root".*
            FROM
                test_db.test_schema.test_table AS "root"
            WHERE
                (101 < 202)
            LIMIT 202, 101"""
        assert extension.process_query_specification(query, config) == dedent(expected)

        expected = """\
            (
                SELECT DISTINCT
                    "root".*
                FROM
                    test_db.test_schema.test_table AS "root"
                WHERE
                    (101 < 202)
                LIMIT 202, 101
            )"""
        assert extension.process_query_specification(query, config, True) == dedent(expected)

    def test_process_query_spec_no_from(self) -> None:
        extension = SqlToStringDbExtension()
        config = SqlToStringConfig(SqlToStringFormat(pretty=False))

        query = QuerySpecification(
            select=Select(selectItems=[
                SingleColumn("a", ArithmeticExpression(ArithmeticType.ADD, IntegerLiteral(101), IntegerLiteral(202)))
            ], distinct=False),
            from_=[],
            where=None,
            groupBy=[],
            having=None,
            orderBy=[],
            limit=None,
            offset=None
        )
        assert extension.process_query_specification(query, config) == \
               'SELECT (101 + 202) AS a'

    def test_process_query(self) -> None:
        extension = SqlToStringDbExtension()
        config = SqlToStringConfig(SqlToStringFormat(pretty=False))

        query_spec = QuerySpecification(
            select=Select(selectItems=[AllColumns(prefix='"root"')], distinct=True),
            from_=[AliasedRelation(Table(QualifiedName(["test_db", "test_schema", "test_table"])), '"root"', [])],
            where=ComparisonExpression(IntegerLiteral(101), IntegerLiteral(202), ComparisonOperator.LESS_THAN),
            groupBy=[],
            having=None,
            orderBy=[],
            limit=IntegerLiteral(101),
            offset=IntegerLiteral(202)
        )
        query = Query(query_spec, None, [], None)
        assert extension.process_query(query, config) == \
               'SELECT DISTINCT "root".* FROM test_db.test_schema.test_table AS "root" ' \
               'WHERE (101 < 202) LIMIT 202, 101'

    def test_process_query_pretty_format(self) -> None:
        extension = SqlToStringDbExtension()
        config = SqlToStringConfig(SqlToStringFormat(pretty=True))

        query_spec = QuerySpecification(
            select=Select(selectItems=[AllColumns(prefix='"root"')], distinct=True),
            from_=[AliasedRelation(Table(QualifiedName(["test_db", "test_schema", "test_table"])), '"root"', [])],
            where=ComparisonExpression(IntegerLiteral(101), IntegerLiteral(202), ComparisonOperator.LESS_THAN),
            groupBy=[],
            having=None,
            orderBy=[],
            limit=IntegerLiteral(101),
            offset=IntegerLiteral(202)
        )
        query = Query(query_spec, None, [], None)
        expected = """\
            SELECT DISTINCT
                "root".*
            FROM
                test_db.test_schema.test_table AS "root"
            WHERE
                (101 < 202)
            LIMIT 202, 101"""
        assert extension.process_query(query, config) == dedent(expected)

    def test_process_union(self) -> None:
        extension = SqlToStringDbExtension()
        config = SqlToStringConfig(SqlToStringFormat(pretty=False))

        rel1 = QuerySpecification(
            select=Select(selectItems=[AllColumns(prefix='"root"')], distinct=True),
            from_=[AliasedRelation(Table(QualifiedName(["test_db", "test_schema", "test_table"])), '"root"', [])],
            where=ComparisonExpression(IntegerLiteral(101), IntegerLiteral(202), ComparisonOperator.LESS_THAN),
            groupBy=[],
            having=None,
            orderBy=[],
            limit=IntegerLiteral(101),
            offset=IntegerLiteral(202)
        )

        rel2 = QuerySpecification(
            select=Select(selectItems=[AllColumns(prefix='"root"')], distinct=True),
            from_=[AliasedRelation(Table(QualifiedName(["test_db", "test_schema", "test_table"])), '"root"', [])],
            where=ComparisonExpression(IntegerLiteral(101), IntegerLiteral(202), ComparisonOperator.LESS_THAN),
            groupBy=[],
            having=None,
            orderBy=[],
            limit=IntegerLiteral(101),
            offset=IntegerLiteral(303)
        )

        union1 = Union(rel1, rel2, True)
        assert extension.process_relation(union1, config) == \
               'SELECT DISTINCT "root".* FROM test_db.test_schema.test_table AS "root" ' \
               'WHERE (101 < 202) LIMIT 202, 101 ' \
               'UNION ' \
               'SELECT DISTINCT "root".* FROM test_db.test_schema.test_table AS "root" ' \
               'WHERE (101 < 202) LIMIT 303, 101'

        union2 = Union(rel1, rel2, False)
        assert extension.process_relation(union2, config) == \
               'SELECT DISTINCT "root".* FROM test_db.test_schema.test_table AS "root" ' \
               'WHERE (101 < 202) LIMIT 202, 101 ' \
               'UNION ALL ' \
               'SELECT DISTINCT "root".* FROM test_db.test_schema.test_table AS "root" ' \
               'WHERE (101 < 202) LIMIT 303, 101'

    def test_process_union_pretty_format(self) -> None:
        extension = SqlToStringDbExtension()
        config = SqlToStringConfig(SqlToStringFormat(pretty=True))

        rel1 = QuerySpecification(
            select=Select(selectItems=[AllColumns(prefix='"root"')], distinct=True),
            from_=[AliasedRelation(Table(QualifiedName(["test_db", "test_schema", "test_table"])), '"root"', [])],
            where=ComparisonExpression(IntegerLiteral(101), IntegerLiteral(202), ComparisonOperator.LESS_THAN),
            groupBy=[],
            having=None,
            orderBy=[],
            limit=IntegerLiteral(101),
            offset=IntegerLiteral(202)
        )

        rel2 = QuerySpecification(
            select=Select(selectItems=[AllColumns(prefix='"root"')], distinct=True),
            from_=[AliasedRelation(Table(QualifiedName(["test_db", "test_schema", "test_table"])), '"root"', [])],
            where=ComparisonExpression(IntegerLiteral(101), IntegerLiteral(202), ComparisonOperator.LESS_THAN),
            groupBy=[],
            having=None,
            orderBy=[],
            limit=IntegerLiteral(101),
            offset=IntegerLiteral(303)
        )

        union = Union(rel1, rel2, False)
        expected = """\
            SELECT DISTINCT
                "root".*
            FROM
                test_db.test_schema.test_table AS "root"
            WHERE
                (101 < 202)
            LIMIT 202, 101
            UNION ALL
            SELECT DISTINCT
                "root".*
            FROM
                test_db.test_schema.test_table AS "root"
            WHERE
                (101 < 202)
            LIMIT 303, 101"""
        assert extension.process_relation(union, config) == dedent(expected)

        expected = """\
            (
                SELECT DISTINCT
                    "root".*
                FROM
                    test_db.test_schema.test_table AS "root"
                WHERE
                    (101 < 202)
                LIMIT 202, 101
                UNION ALL
                SELECT DISTINCT
                    "root".*
                FROM
                    test_db.test_schema.test_table AS "root"
                WHERE
                    (101 < 202)
                LIMIT 303, 101
            )"""
        assert extension.process_relation(union, config, True) == dedent(expected)

    def test_process_string_length_expression(self) -> None:
        extension = SqlToStringDbExtension()
        config = SqlToStringConfig(SqlToStringFormat(pretty=False))

        expr = StringLengthExpression(StringLiteral("Hello", quoted=False))
        assert extension.process_expression(expr, config) == "CHAR_LENGTH('Hello')"

    def test_process_string_like_expression(self) -> None:
        extension = SqlToStringDbExtension()
        config = SqlToStringConfig(SqlToStringFormat(pretty=False))

        expr = StringLikeExpression(StringLiteral("Hello", quoted=False), StringLiteral('He%', quoted=False))
        assert extension.process_expression(expr, config) == "('Hello' LIKE 'He%')"

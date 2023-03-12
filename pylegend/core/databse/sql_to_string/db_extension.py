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


from pylegend._typing import (
    PyLegendCallable,
    PyLegendList,
    PyLegendSequence
)
from pylegend.core.databse.sql_to_string.config import SqlToStringConfig
from pylegend.core.sql.metamodel import (
    QuerySpecification,
    Select,
    SelectItem,
    AllColumns,
    SingleColumn,
    Literal,
    IntegerLiteral,
    LongLiteral,
    BooleanLiteral,
    DoubleLiteral,
    StringLiteral,
    NullLiteral,
    Expression,
    ComparisonExpression,
    ComparisonOperator,
    ArithmeticExpression,
    ArithmeticType,
    NegativeExpression,
    LogicalBinaryExpression,
    LogicalBinaryType,
    NotExpression,
    ColumnType,
    Cast,
    InPredicate,
    InListExpression,
    WhenClause,
    SearchedCaseExpression,
    QualifiedName,
    Relation,
    Table,
    AliasedRelation,
    Query,
    TableSubquery,
    SubqueryExpression,
    Join,
    JoinType,
    JoinCriteria,
    JoinOn,
    JoinUsing,
    SortItemOrdering,
    QualifiedNameReference,
    IsNullPredicate,
    IsNotNullPredicate,
    CurrentTime,
    CurrentTimeType,
)


__all__: PyLegendSequence[str] = [
    "SqlToStringDbExtension"
]


def query_specification_processor(
    query: QuerySpecification,
    extension: "SqlToStringDbExtension",
    config: SqlToStringConfig,
    nested_subquery: bool
) -> str:
    sep0 = config.format.separator(0)
    sep1 = config.format.separator(1)

    if nested_subquery:
        return "({sep1}{sub_query}{sep0})".format(
            sep0=sep0,
            sep1=sep1,
            sub_query=extension.process_query_specification(query, config.push_indent(), False)
        )

    top = extension.process_top(query, config)
    group_by = extension.process_group_by(query, config)
    order_by = extension.process_order_by(query, config)
    limit = extension.process_limit(query, config)
    columns = extension.process_select(query.select, config)

    _from = "{sep0}from{sep1}{relations}".format(
        sep0=sep0,
        sep1=sep1,
        relations=("," + sep1).join([
            extension.process_relation(f, config.push_indent()) for f in query.from_
        ])
    ) if query.from_ else ""

    where_clause = "{sep0}where{sep1}{expr}".format(
        sep0=sep0,
        sep1=sep1,
        expr=extension.process_expression(query.where, config.push_indent())
    ) if query.where else ""

    having_clause = "{sep0}having{sep1}{expr}".format(
        sep0=sep0,
        sep1=sep1,
        expr=extension.process_expression(query.having, config.push_indent())
    ) if query.having else ""

    return "select{top}{columns}{_from}{where}{group_by}{having}{order_by}{limit}".format(
        top=top,
        columns=columns,
        _from=_from,
        where=where_clause,
        group_by=group_by,
        having=having_clause,
        order_by=order_by,
        limit=limit
    )


def top_processor(
    query: QuerySpecification,
    extension: "SqlToStringDbExtension",
    config: SqlToStringConfig
) -> str:
    if query.offset is None and query.limit is not None:
        return " top " + extension.process_expression(query.limit, config)
    else:
        return ""


def limit_processor(
    query: QuerySpecification,
    extension: "SqlToStringDbExtension",
    config: SqlToStringConfig
) -> str:
    if query.offset is not None and query.limit is not None:
        return "{sep0}limit {offset}, {limit}".format(
            sep0=config.format.separator(0),
            offset=extension.process_expression(query.offset, config),
            limit=extension.process_expression(query.limit, config)
        )
    else:
        return ""


def group_by_processor(
    query: QuerySpecification,
    extension: "SqlToStringDbExtension",
    config: SqlToStringConfig
) -> str:
    if query.groupBy:
        return "{sep0}group by{sep1}{group_by_args}".format(
            sep0=config.format.separator(0),
            sep1=config.format.separator(1),
            group_by_args=("," + config.format.separator(1)).join(
                [extension.process_expression(g, config.push_indent()) for g in query.groupBy]
            )
        )
    else:
        return ""


def order_by_processor(
    query: QuerySpecification,
    extension: "SqlToStringDbExtension",
    config: SqlToStringConfig
) -> str:
    if query.orderBy:
        return "{sep0}order by{sep1}{order_by_args}".format(
            sep0=config.format.separator(0),
            sep1=config.format.separator(1),
            order_by_args=("," + config.format.separator(1)).join(
                [
                    extension.process_expression(o.sortKey, config.push_indent()) +
                    (" desc" if o.ordering == SortItemOrdering.DESCENDING else "") for o in query.orderBy
                ]
            )
        )
    else:
        return ""


def select_processor(
    select: Select,
    extension: "SqlToStringDbExtension",
    config: SqlToStringConfig
) -> str:
    distinct_flag = " distinct" if select.distinct else ""
    items = [extension.process_select_item(item, config.push_indent()) for item in select.selectItems]
    return "{distinct}{sep1}{select_items}".format(
        distinct=distinct_flag,
        sep1=config.format.separator(1),
        select_items=("," + config.format.separator(1)).join(items)
    )


def select_item_processor(
    select_item: SelectItem,
    extension: "SqlToStringDbExtension",
    config: SqlToStringConfig
) -> str:
    if isinstance(select_item, AllColumns):
        return extension.process_all_columns(select_item, config)
    elif isinstance(select_item, SingleColumn):
        return extension.process_single_column(select_item, config)
    else:
        raise ValueError("Unsupported select item type: " + str(type(select_item)))  # pragma: no cover


def all_columns_processor(
    all_columns: AllColumns,
    extension: "SqlToStringDbExtension",
    config: SqlToStringConfig
) -> str:
    if all_columns.prefix:
        return extension.process_identifier(all_columns.prefix, config, False) + '.*'
    else:
        return '*'


def single_column_processor(
    single_column: SingleColumn,
    extension: "SqlToStringDbExtension",
    config: SqlToStringConfig
) -> str:
    processed_expression = extension.process_expression(single_column.expression, config)
    if single_column.alias:
        return "{expr} as {alias}".format(
            expr=processed_expression,
            alias=extension.process_identifier(single_column.alias, config, False)
        )
    else:
        return processed_expression


def identifier_processor(
    identifier: str,
    extension: "SqlToStringDbExtension",
    config: SqlToStringConfig,
    should_quote: bool,
    quote_character: str
) -> str:
    if should_quote or identifier in extension.reserved_keywords():
        return "{q}{i}{q}".format(q=quote_character, i=identifier)
    else:
        return identifier


def expression_processor(
    expression: Expression,
    extension: "SqlToStringDbExtension",
    config: SqlToStringConfig
) -> str:
    if isinstance(expression, Literal):
        return extension.process_literal(expression, config)
    elif isinstance(expression, ComparisonExpression):
        return extension.process_comparison_expression(expression, config)
    elif isinstance(expression, LogicalBinaryExpression):
        return extension.process_logical_binary_expression(expression, config)
    elif isinstance(expression, NotExpression):
        return extension.process_not_expression(expression, config)
    elif isinstance(expression, ArithmeticExpression):
        return extension.process_arithmetic_expression(expression, config)
    elif isinstance(expression, NegativeExpression):
        return extension.process_negative_expression(expression, config)
    elif isinstance(expression, WhenClause):
        return extension.process_when_clause(expression, config)
    elif isinstance(expression, SearchedCaseExpression):
        return extension.process_searched_case_expression(expression, config)
    elif isinstance(expression, ColumnType):
        return extension.process_column_type(expression, config)
    elif isinstance(expression, Cast):
        return extension.process_cast_expression(expression, config)
    elif isinstance(expression, InListExpression):
        return extension.process_in_list_expression(expression, config)
    elif isinstance(expression, InPredicate):
        return extension.process_in_predicate(expression, config)
    elif isinstance(expression, QualifiedNameReference):
        return extension.process_qualified_name_reference(expression, config)
    elif isinstance(expression, IsNullPredicate):
        return extension.process_is_null_predicate(expression, config)
    elif isinstance(expression, IsNotNullPredicate):
        return extension.process_is_not_null_predicate(expression, config)
    elif isinstance(expression, CurrentTime):
        return extension.process_current_time(expression, config)
    else:
        raise ValueError("Unsupported expression type: " + str(type(expression)))  # pragma: no cover


def literal_processor(
    literal: Literal,
    extension: "SqlToStringDbExtension",
    config: SqlToStringConfig
) -> str:
    return extension.literal_processor()(literal, config)


def comparison_expression_processor(
    comparison: ComparisonExpression,
    extension: "SqlToStringDbExtension",
    config: SqlToStringConfig
) -> str:
    if comparison.operator == ComparisonOperator.EQUAL:
        cmp = "="
    elif comparison.operator == ComparisonOperator.NOT_EQUAL:
        cmp = "<>"
    elif comparison.operator == ComparisonOperator.GREATER_THAN:
        cmp = ">"
    elif comparison.operator == ComparisonOperator.GREATER_THAN_OR_EQUAL:
        cmp = ">="
    elif comparison.operator == ComparisonOperator.LESS_THAN:
        cmp = "<"
    elif comparison.operator == ComparisonOperator.LESS_THAN_OR_EQUAL:
        cmp = "<="
    else:
        raise ValueError("Unknown comparison operator type: " + str(comparison.operator))  # pragma: no cover

    return "({left} {op} {right})".format(
        left=extension.process_expression(comparison.left, config),
        op=cmp,
        right=extension.process_expression(comparison.right, config)
    )


def logical_binary_expression_processor(
        logical: LogicalBinaryExpression,
        extension: "SqlToStringDbExtension",
        config: SqlToStringConfig
) -> str:
    op_type = logical.type_
    if op_type == LogicalBinaryType.AND:
        op = "and"
    elif op_type == LogicalBinaryType.OR:
        op = "or"
    else:
        raise ValueError("Unknown logical binary operator type: " + str(op_type))  # pragma: no cover

    return "({left} {op} {right})".format(
        left=extension.process_expression(logical.left, config),
        op=op,
        right=extension.process_expression(logical.right, config)
    )


def not_expression_processor(
        not_expression: NotExpression,
        extension: "SqlToStringDbExtension",
        config: SqlToStringConfig
) -> str:
    expr = extension.process_expression(not_expression.value, config)
    if isinstance(not_expression.value, (LogicalBinaryExpression, ComparisonExpression)):
        return "not{expr}".format(expr=expr)
    else:
        return "not({expr})".format(expr=expr)


def arithmetic_expression_processor(
    arithmetic: ArithmeticExpression,
    extension: "SqlToStringDbExtension",
    config: SqlToStringConfig
) -> str:
    op_type = arithmetic.type_
    if op_type == ArithmeticType.ADD:
        to_format = "({left} + {right})"
    elif op_type == ArithmeticType.SUBTRACT:
        to_format = "({left} - {right})"
    elif op_type == ArithmeticType.MULTIPLY:
        to_format = "({left} * {right})"
    elif op_type == ArithmeticType.DIVIDE:
        to_format = "((1.0 * {left}) / {right})"
    elif op_type == ArithmeticType.MODULUS:
        to_format = "mod({left}, {right})"
    else:
        raise ValueError("Unknown arithmetic operator type: " + str(op_type))  # pragma: no cover

    return to_format.format(
        left=extension.process_expression(arithmetic.left, config),
        right=extension.process_expression(arithmetic.right, config)
    )


def negative_expression_processor(
        negative: NegativeExpression,
        extension: "SqlToStringDbExtension",
        config: SqlToStringConfig
) -> str:
    expr = extension.process_expression(negative.value, config)
    if isinstance(negative.value, Literal):
        return "-{expr}".format(expr=expr)
    else:
        return "(0 - {expr})".format(expr=expr)


def when_clause_processor(
        when: WhenClause,
        extension: "SqlToStringDbExtension",
        config: SqlToStringConfig
) -> str:
    return "when{sep1}{when}{sep0}then{sep1}{then}".format(
        when=extension.process_expression(when.operand, config.push_indent()),
        then=extension.process_expression(when.result, config.push_indent()),
        sep1=config.format.separator(1),
        sep0=config.format.separator(0)
    )


def searched_case_expression_processor(
        case: SearchedCaseExpression,
        extension: "SqlToStringDbExtension",
        config: SqlToStringConfig
) -> str:
    if case.defaultValue:
        else_clause = "{sep1}else{sep2}{e}".format(
            sep1=config.format.separator(1),
            e=extension.process_expression(case.defaultValue, config.push_indent().push_indent()),
            sep2=config.format.separator(2)
        )
    else:
        else_clause = ""
    return "case{sep1}{when_clauses}{default}{sep0}end".format(
        sep1=config.format.separator(1),
        when_clauses=config.format.separator(1).join(
            [extension.process_expression(clause, config.push_indent()) for clause in case.whenClauses]
        ),
        default=else_clause,
        sep0=config.format.separator(0)
    )


def column_type_processor(
        column_type: ColumnType,
        extension: "SqlToStringDbExtension",
        config: SqlToStringConfig
) -> str:
    if column_type.parameters:
        return "{data_type}({params})".format(
            data_type=column_type.name,
            params=", ".join([str(p) for p in column_type.parameters])
        )
    else:
        return column_type.name


def cast_expression_processor(
        cast: Cast,
        extension: "SqlToStringDbExtension",
        config: SqlToStringConfig
) -> str:
    return "cast({value} as {data_type})".format(
        value=extension.process_expression(cast.expression, config),
        data_type=extension.process_expression(cast.type_, config)
    )


def in_list_expression_processor(
        in_list: InListExpression,
        extension: "SqlToStringDbExtension",
        config: SqlToStringConfig
) -> str:
    return "({values})".format(
        values=", ".join([extension.process_expression(value, config) for value in in_list.values])
    )


def in_predicate_processor(
        in_predicate: InPredicate,
        extension: "SqlToStringDbExtension",
        config: SqlToStringConfig
) -> str:
    return "{value} in {value_list}".format(
        value=extension.process_expression(in_predicate.value, config),
        value_list=extension.process_expression(in_predicate.valueList, config)
    )


def is_null_predicate_processor(
        is_null_predicate: IsNullPredicate,
        extension: "SqlToStringDbExtension",
        config: SqlToStringConfig
) -> str:
    return "({value} is NULL)".format(value=extension.process_expression(is_null_predicate.value, config))


def is_not_null_predicate_processor(
        is_not_null_predicate: IsNotNullPredicate,
        extension: "SqlToStringDbExtension",
        config: SqlToStringConfig
) -> str:
    return "({value} is not NULL)".format(value=extension.process_expression(is_not_null_predicate.value, config))


def current_time_processor(
        current_time: CurrentTime,
        extension: "SqlToStringDbExtension",
        config: SqlToStringConfig
) -> str:
    if current_time.type_ == CurrentTimeType.DATE:
        return "CURRENT_DATE"
    elif current_time.type_ == CurrentTimeType.TIMESTAMP:
        return "CURRENT_TIMESTAMP(" + (str(current_time.precision) if current_time.precision else "") + ")"
    elif current_time.type_ == CurrentTimeType.TIME:
        return "CURRENT_TIME(" + (str(current_time.precision) if current_time.precision else "") + ")"
    else:
        raise ValueError("Unknown current time type: " + str(current_time.type_))  # pragma: no cover


def qualified_name_processor(
        qualified_name: QualifiedName,
        extension: "SqlToStringDbExtension",
        config: SqlToStringConfig
) -> str:
    return ".".join([extension.process_identifier(p, config) for p in qualified_name.parts])


def qualified_name_reference_processor(
        reference: QualifiedNameReference,
        extension: "SqlToStringDbExtension",
        config: SqlToStringConfig
) -> str:
    return extension.process_qualified_name(reference.name, config)


def relation_processor(
        relation: Relation,
        extension: "SqlToStringDbExtension",
        config: SqlToStringConfig,
        nested_subquery: bool
) -> str:
    if isinstance(relation, Table):
        return extension.process_table(relation, config)
    elif isinstance(relation, AliasedRelation):
        return extension.process_aliased_relation(relation, config, nested_subquery)
    elif isinstance(relation, QuerySpecification):
        return extension.process_query_specification(relation, config, nested_subquery)
    elif isinstance(relation, TableSubquery):
        return extension.process_table_subquery(relation, config)
    elif isinstance(relation, Join):
        return extension.process_join(relation, config)
    raise ValueError("Unknown relation type: " + str(type(relation)))  # pragma: no cover


def table_processor(
        table: Table,
        extension: "SqlToStringDbExtension",
        config: SqlToStringConfig
) -> str:
    return extension.process_qualified_name(table.name, config)


def aliased_relation_processor(
        aliased_relation: AliasedRelation,
        extension: "SqlToStringDbExtension",
        config: SqlToStringConfig,
        nested_subquery: bool
) -> str:
    return "{relation} as {alias}".format(
        relation=extension.process_relation(aliased_relation.relation, config, nested_subquery),
        alias=extension.process_identifier(aliased_relation.alias, config)
    )


def query_processor(
        query: Query,
        extension: "SqlToStringDbExtension",
        config: SqlToStringConfig
) -> str:
    # TODO: Use limit, orderBy, offset at query level
    return "({sep1}select{sep2}*{sep1}from{sep2}{relation}{sep0})".format(
        sep0=config.format.separator(0),
        sep1=config.format.separator(1),
        sep2=config.format.separator(2),
        relation=extension.process_relation(query.queryBody, config.push_indent().push_indent(), True)
    )


def join_criteria_processor(
        join_criteria: JoinCriteria,
        extension: "SqlToStringDbExtension",
        config: SqlToStringConfig
) -> str:
    if isinstance(join_criteria, JoinOn):
        return extension.process_join_on(join_criteria, config)
    elif isinstance(join_criteria, JoinUsing):
        return extension.process_join_using(join_criteria, config)
    raise ValueError("Unknown join criteria type: " + str(type(join_criteria)))  # pragma: no cover


def join_on_processor(
        join_on: JoinOn,
        extension: "SqlToStringDbExtension",
        config: SqlToStringConfig
) -> str:
    expr = extension.process_expression(join_on.expression, config)
    if expr[0] == "(" and expr[-1] == ")":
        return expr[1:-1]
    else:
        return expr


def join_using_processor(
        join_using: JoinUsing,
        extension: "SqlToStringDbExtension",
        config: SqlToStringConfig
) -> str:
    # TODO: code this
    raise RuntimeError("Not supported yet!")


def join_processor(
        join: Join,
        extension: "SqlToStringDbExtension",
        config: SqlToStringConfig
) -> str:
    left = extension.process_relation(join.left, config)
    right = extension.process_relation(join.right, config)
    join_type = join.type_
    condition = "on ({op})".format(op=extension.process_join_criteria(join.criteria, config)) if join.criteria else ""
    if join_type == JoinType.CROSS:
        join_type_str = 'cross join'
    elif join_type == JoinType.INNER:
        join_type_str = 'inner join'
    elif join_type == JoinType.LEFT:
        join_type_str = 'left outer join'
    elif join_type == JoinType.RIGHT:
        join_type_str = 'right outer join'
    elif join_type == JoinType.FULL:
        join_type_str = 'full outer join'
    else:
        raise ValueError("Unknown join type: " + str(join_type))  # pragma: no cover

    return "{left}{sep0}{join}{sep1}{right}{sep1}{on}".format(
        sep0=config.format.separator(0),
        sep1=config.format.separator(1),
        left=left,
        join=join_type_str,
        right=right,
        on=condition
    )


class SqlToStringDbExtension:
    @classmethod
    def reserved_keywords(cls) -> PyLegendList[str]:
        return [
            "kerberos",
            "date",
            "first"
        ]

    @classmethod
    def quote_character(cls) -> str:
        return '"'

    @classmethod
    def quote_identifier(cls, identifier: str) -> str:
        return "{quote}{identifier}{quote}".format(quote=cls.quote_character(), identifier=identifier)

    @classmethod
    def literal_processor(cls) -> PyLegendCallable[[Literal, SqlToStringConfig], str]:
        def literal_process_function(literal: Literal, config: SqlToStringConfig) -> str:
            if isinstance(literal, (IntegerLiteral, LongLiteral, DoubleLiteral)):
                return str(literal.value)
            if isinstance(literal, BooleanLiteral):
                return "true" if literal.value else "false"
            if isinstance(literal, NullLiteral):
                return "null"
            if isinstance(literal, StringLiteral):
                # TODO: check quoted flag
                return "'" + literal.value.replace("'", "''") + "'"
            raise RuntimeError("Unsupported literal type: " + str(type(literal)))
        return literal_process_function

    def process_query_specification(self, query: QuerySpecification,
                                    config: SqlToStringConfig, nested_subquery: bool = False) -> str:
        return query_specification_processor(query, self, config, nested_subquery)

    def process_select(self, select: Select, config: SqlToStringConfig) -> str:
        return select_processor(select, self, config)

    def process_select_item(self, select_item: SelectItem, config: SqlToStringConfig) -> str:
        return select_item_processor(select_item, self, config)

    def process_all_columns(self, all_columns: AllColumns, config: SqlToStringConfig) -> str:
        return all_columns_processor(all_columns, self, config)

    def process_single_column(self, single_column: SingleColumn, config: SqlToStringConfig) -> str:
        return single_column_processor(single_column, self, config)

    def process_identifier(self, identifier: str, config: SqlToStringConfig, should_quote: bool = False) -> str:
        return identifier_processor(identifier, self, config, should_quote, self.quote_character())

    def process_expression(self, expression: Expression, config: SqlToStringConfig) -> str:
        return expression_processor(expression, self, config)

    def process_literal(self, literal: Literal, config: SqlToStringConfig) -> str:
        return literal_processor(literal, self, config)

    def process_comparison_expression(self, comparison: ComparisonExpression, config: SqlToStringConfig) -> str:
        return comparison_expression_processor(comparison, self, config)

    def process_logical_binary_expression(self, logical: LogicalBinaryExpression, config: SqlToStringConfig) -> str:
        return logical_binary_expression_processor(logical, self, config)

    def process_not_expression(self, not_expression: NotExpression, config: SqlToStringConfig) -> str:
        return not_expression_processor(not_expression, self, config)

    def process_arithmetic_expression(self, arithmetic: ArithmeticExpression, config: SqlToStringConfig) -> str:
        return arithmetic_expression_processor(arithmetic, self, config)

    def process_negative_expression(self, negative: NegativeExpression, config: SqlToStringConfig) -> str:
        return negative_expression_processor(negative, self, config)

    def process_when_clause(self, when: WhenClause, config: SqlToStringConfig) -> str:
        return when_clause_processor(when, self, config)

    def process_searched_case_expression(self, case: SearchedCaseExpression, config: SqlToStringConfig) -> str:
        return searched_case_expression_processor(case, self, config)

    def process_column_type(self, column_type: ColumnType, config: SqlToStringConfig) -> str:
        return column_type_processor(column_type, self, config)

    def process_cast_expression(self, cast: Cast, config: SqlToStringConfig) -> str:
        return cast_expression_processor(cast, self, config)

    def process_in_list_expression(self, in_list: InListExpression, config: SqlToStringConfig) -> str:
        return in_list_expression_processor(in_list, self, config)

    def process_in_predicate(self, in_predicate: InPredicate, config: SqlToStringConfig) -> str:
        return in_predicate_processor(in_predicate, self, config)

    def process_is_null_predicate(self, is_null_predicate: IsNullPredicate, config: SqlToStringConfig) -> str:
        return is_null_predicate_processor(is_null_predicate, self, config)

    def process_is_not_null_predicate(self, is_not_null_predicate: IsNotNullPredicate, config: SqlToStringConfig) -> str:
        return is_not_null_predicate_processor(is_not_null_predicate, self, config)

    def process_current_time(self, current_time: CurrentTime, config: SqlToStringConfig) -> str:
        return current_time_processor(current_time, self, config)

    def process_qualified_name(self, qualified_name: QualifiedName, config: SqlToStringConfig) -> str:
        return qualified_name_processor(qualified_name, self, config)

    def process_qualified_name_reference(self, reference: QualifiedNameReference, config: SqlToStringConfig) -> str:
        return qualified_name_reference_processor(reference, self, config)

    def process_relation(self, relation: Relation, config: SqlToStringConfig, nested_subquery: bool = False) -> str:
        return relation_processor(relation, self, config, nested_subquery)

    def process_table(self, table: Table, config: SqlToStringConfig) -> str:
        return table_processor(table, self, config)

    def process_aliased_relation(self, aliased_relation: AliasedRelation,
                                 config: SqlToStringConfig, nested_subquery: bool = False) -> str:
        return aliased_relation_processor(aliased_relation, self, config, nested_subquery)

    def process_query(self, query: Query, config: SqlToStringConfig) -> str:
        return query_processor(query, self, config)

    def process_table_subquery(self, table_subquery: TableSubquery, config: SqlToStringConfig) -> str:
        return query_processor(table_subquery.query, self, config)

    def process_subquery_expression(self, subquery_expression: SubqueryExpression, config: SqlToStringConfig) -> str:
        return query_processor(subquery_expression.query, self, config)

    def process_join_criteria(self, join_criteria: JoinCriteria, config: SqlToStringConfig) -> str:
        return join_criteria_processor(join_criteria, self, config)

    def process_join_on(self, join_on: JoinOn, config: SqlToStringConfig) -> str:
        return join_on_processor(join_on, self, config)

    def process_join_using(self, join_using: JoinUsing, config: SqlToStringConfig) -> str:
        return join_using_processor(join_using, self, config)

    def process_join(self, join: Join, config: SqlToStringConfig) -> str:
        return join_processor(join, self, config)

    def process_top(self, query: QuerySpecification, config: SqlToStringConfig) -> str:
        return top_processor(query, self, config)

    def process_limit(self, query: QuerySpecification, config: SqlToStringConfig) -> str:
        return limit_processor(query, self, config)

    def process_group_by(self, query: QuerySpecification, config: SqlToStringConfig) -> str:
        return group_by_processor(query, self, config)

    def process_order_by(self, query: QuerySpecification, config: SqlToStringConfig) -> str:
        return order_by_processor(query, self, config)

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
from pylegend.core.database.sql_to_string.config import SqlToStringConfig
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
    SortItem,
    SortItemOrdering,
    QualifiedNameReference,
    IsNullPredicate,
    IsNotNullPredicate,
    CurrentTime,
    CurrentTimeType,
    Extract,
    FunctionCall,
    NamedArgumentExpression,
    Window,
    TableFunction,
    Union,
)
from pylegend.core.sql.metamodel_extension import (
    StringLengthExpression,
    StringLikeExpression,
    StringUpperExpression,
    StringLowerExpression,
    TrimType,
    StringTrimExpression,
    StringPosExpression,
    StringConcatExpression,
    AbsoluteExpression,
    PowerExpression,
    CeilExpression,
    FloorExpression,
    SqrtExpression,
    CbrtExpression,
    ExpExpression,
    LogExpression,
    RemainderExpression,
    RoundExpression,
    SineExpression,
    ArcSineExpression,
    CosineExpression,
    ArcCosineExpression,
    TanExpression,
    ArcTanExpression,
    ArcTan2Expression,
    CotExpression,
    CountExpression,
    DistinctCountExpression,
    AverageExpression,
    MaxExpression,
    MinExpression,
    SumExpression,
    StdDevSampleExpression,
    StdDevPopulationExpression,
    VarianceSampleExpression,
    VariancePopulationExpression,
    JoinStringsExpression,
    FirstDayOfYearExpression,
    FirstDayOfQuarterExpression,
    FirstDayOfMonthExpression,
    FirstDayOfWeekExpression,
    FirstHourOfDayExpression,
    FirstMinuteOfHourExpression,
    FirstSecondOfMinuteExpression,
    FirstMillisecondOfSecondExpression,
    YearExpression,
    QuarterExpression,
    MonthExpression,
    WeekOfYearExpression,
    DayOfYearExpression,
    DayOfMonthExpression,
    DayOfWeekExpression,
    HourExpression,
    MinuteExpression,
    SecondExpression,
    EpochExpression,
    WindowExpression,
    ConstantExpression,
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
        return f"({sep1}{extension.process_query_specification(query, config.push_indent(), False)}{sep0})"

    top = extension.process_top(query, config)
    group_by = extension.process_group_by(query, config)
    order_by = extension.process_order_by(query, config)
    limit = extension.process_limit(query, config)
    columns = extension.process_select(query.select, config)

    relations = ("," + sep1).join([
        extension.process_relation(f, config.push_indent()) for f in query.from_
    ])
    _from = f"{sep0}FROM{sep1}{relations}" if query.from_ else ""

    where_clause = f"{sep0}WHERE{sep1}{extension.process_expression(query.where, config.push_indent())}" \
        if query.where else ""

    having_clause = f"{sep0}HAVING{sep1}{extension.process_expression(query.having, config.push_indent())}" \
        if query.having else ""

    return f"SELECT{top}{columns}{_from}{where_clause}{group_by}{having_clause}{order_by}{limit}"


def top_processor(
    query: QuerySpecification,
    extension: "SqlToStringDbExtension",
    config: SqlToStringConfig
) -> str:
    return ""


def limit_processor(
    query: QuerySpecification,
    extension: "SqlToStringDbExtension",
    config: SqlToStringConfig
) -> str:
    sep0 = config.format.separator(0)
    limit = f"{sep0}LIMIT {extension.process_expression(query.limit, config)}" \
        if query.limit else ""
    offset = f"{sep0}OFFSET {extension.process_expression(query.offset, config)}" \
        if query.offset else ""
    return f"{limit}{offset}"


def group_by_processor(
    query: QuerySpecification,
    extension: "SqlToStringDbExtension",
    config: SqlToStringConfig
) -> str:
    if query.groupBy:
        sep0 = config.format.separator(0)
        sep1 = config.format.separator(1)
        group_by_args = ("," + config.format.separator(1)).join(
            [extension.process_expression(g, config.push_indent()) for g in query.groupBy]
        )
        return f"{sep0}GROUP BY{sep1}{group_by_args}"
    else:
        return ""


def sort_item_processor(
    sort_item: SortItem,
    extension: "SqlToStringDbExtension",
    config: SqlToStringConfig
) -> str:
    return extension.process_expression(sort_item.sortKey, config.push_indent()) + \
        (" DESC" if sort_item.ordering == SortItemOrdering.DESCENDING else "")


def order_by_processor(
    query: QuerySpecification,
    extension: "SqlToStringDbExtension",
    config: SqlToStringConfig
) -> str:
    if query.orderBy:
        sep0 = config.format.separator(0)
        sep1 = config.format.separator(1)
        order_by_args = ("," + config.format.separator(1)).join(
            [extension.process_sort_item(o, config) for o in query.orderBy]
        )
        return f"{sep0}ORDER BY{sep1}{order_by_args}"
    else:
        return ""


def select_processor(
    select: Select,
    extension: "SqlToStringDbExtension",
    config: SqlToStringConfig
) -> str:
    distinct_flag = " DISTINCT" if select.distinct else ""
    items = [extension.process_select_item(item, config.push_indent()) for item in select.selectItems]
    sep1 = config.format.separator(1)
    select_items = ("," + config.format.separator(1)).join(items)
    return f"{distinct_flag}{sep1}{select_items}"


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
        expr = processed_expression
        alias = extension.process_identifier(single_column.alias, config, False)
        return f"{expr} AS {alias}"
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
        return f"{quote_character}{identifier}{quote_character}"
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
    elif isinstance(expression, Extract):
        return extension.process_extract(expression, config)
    elif isinstance(expression, FunctionCall):
        return extension.process_function_call(expression, config)
    elif isinstance(expression, NamedArgumentExpression):
        return extension.process_named_argument_expression(expression, config)
    elif isinstance(expression, StringLengthExpression):
        return extension.process_string_length_expression(expression, config)
    elif isinstance(expression, StringLikeExpression):
        return extension.process_string_like_expression(expression, config)
    elif isinstance(expression, StringUpperExpression):
        return extension.process_string_upper_expression(expression, config)
    elif isinstance(expression, StringLowerExpression):
        return extension.process_string_lower_expression(expression, config)
    elif isinstance(expression, StringTrimExpression):
        return extension.process_string_trim_expression(expression, config)
    elif isinstance(expression, StringPosExpression):
        return extension.process_string_pos_expression(expression, config)
    elif isinstance(expression, StringConcatExpression):
        return extension.process_string_concat_expression(expression, config)
    elif isinstance(expression, AbsoluteExpression):
        return extension.process_absolute_expression(expression, config)
    elif isinstance(expression, PowerExpression):
        return extension.process_power_expression(expression, config)
    elif isinstance(expression, CeilExpression):
        return extension.process_ceil_expression(expression, config)
    elif isinstance(expression, FloorExpression):
        return extension.process_floor_expression(expression, config)
    elif isinstance(expression, SqrtExpression):
        return extension.process_sqrt_expression(expression, config)
    elif isinstance(expression, CbrtExpression):
        return extension.process_cbrt_expression(expression, config)
    elif isinstance(expression, ExpExpression):
        return extension.process_exp_expression(expression, config)
    elif isinstance(expression, LogExpression):
        return extension.process_log_expression(expression, config)
    elif isinstance(expression, RemainderExpression):
        return extension.process_remainder_expression(expression, config)
    elif isinstance(expression, RoundExpression):
        return extension.process_round_expression(expression, config)
    elif isinstance(expression, SineExpression):
        return extension.process_sine_expression(expression, config)
    elif isinstance(expression, ArcSineExpression):
        return extension.process_arc_sine_expression(expression, config)
    elif isinstance(expression, CosineExpression):
        return extension.process_cosine_expression(expression, config)
    elif isinstance(expression, ArcCosineExpression):
        return extension.process_arc_cosine_expression(expression, config)
    elif isinstance(expression, TanExpression):
        return extension.process_tan_expression(expression, config)
    elif isinstance(expression, ArcTanExpression):
        return extension.process_arc_tan_expression(expression, config)
    elif isinstance(expression, ArcTan2Expression):
        return extension.process_arc_tan2_expression(expression, config)
    elif isinstance(expression, CotExpression):
        return extension.process_cot_expression(expression, config)
    elif isinstance(expression, CountExpression):
        return extension.process_count_expression(expression, config)
    elif isinstance(expression, DistinctCountExpression):
        return extension.process_distinct_count_expression(expression, config)
    elif isinstance(expression, AverageExpression):
        return extension.process_average_expression(expression, config)
    elif isinstance(expression, MaxExpression):
        return extension.process_max_expression(expression, config)
    elif isinstance(expression, MinExpression):
        return extension.process_min_expression(expression, config)
    elif isinstance(expression, SumExpression):
        return extension.process_sum_expression(expression, config)
    elif isinstance(expression, StdDevSampleExpression):
        return extension.process_std_dev_sample_expression(expression, config)
    elif isinstance(expression, StdDevPopulationExpression):
        return extension.process_std_dev_population_expression(expression, config)
    elif isinstance(expression, VarianceSampleExpression):
        return extension.process_variance_sample_expression(expression, config)
    elif isinstance(expression, VariancePopulationExpression):
        return extension.process_variance_population_expression(expression, config)
    elif isinstance(expression, JoinStringsExpression):
        return extension.process_join_strings_expression(expression, config)
    elif isinstance(expression, FirstDayOfYearExpression):
        return extension.process_first_day_of_year_expression(expression, config)
    elif isinstance(expression, FirstDayOfQuarterExpression):
        return extension.process_first_day_of_quarter_expression(expression, config)
    elif isinstance(expression, FirstDayOfMonthExpression):
        return extension.process_first_day_of_month_expression(expression, config)
    elif isinstance(expression, FirstDayOfWeekExpression):
        return extension.process_first_day_of_week_expression(expression, config)
    elif isinstance(expression, FirstHourOfDayExpression):
        return extension.process_first_hour_of_day_expression(expression, config)
    elif isinstance(expression, FirstMinuteOfHourExpression):
        return extension.process_first_minute_of_hour_expression(expression, config)
    elif isinstance(expression, FirstSecondOfMinuteExpression):
        return extension.process_first_second_of_minute_expression(expression, config)
    elif isinstance(expression, FirstMillisecondOfSecondExpression):
        return extension.process_first_millisecond_of_second_expression(expression, config)
    elif isinstance(expression, YearExpression):
        return extension.process_year_expression(expression, config)
    elif isinstance(expression, QuarterExpression):
        return extension.process_quarter_expression(expression, config)
    elif isinstance(expression, MonthExpression):
        return extension.process_month_expression(expression, config)
    elif isinstance(expression, WeekOfYearExpression):
        return extension.process_week_of_year_expression(expression, config)
    elif isinstance(expression, DayOfYearExpression):
        return extension.process_day_of_year_expression(expression, config)
    elif isinstance(expression, DayOfMonthExpression):
        return extension.process_day_of_month_expression(expression, config)
    elif isinstance(expression, DayOfWeekExpression):
        return extension.process_day_of_week_expression(expression, config)
    elif isinstance(expression, HourExpression):
        return extension.process_hour_expression(expression, config)
    elif isinstance(expression, MinuteExpression):
        return extension.process_minute_expression(expression, config)
    elif isinstance(expression, SecondExpression):
        return extension.process_second_expression(expression, config)
    elif isinstance(expression, EpochExpression):
        return extension.process_epoch_expression(expression, config)
    elif isinstance(expression, WindowExpression):
        return extension.process_window_expression(expression, config)
    elif isinstance(expression, ConstantExpression):
        return expression.name

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

    left = extension.process_expression(comparison.left, config)
    right = extension.process_expression(comparison.right, config)
    return f"({left} {cmp} {right})"


def logical_binary_expression_processor(
        logical: LogicalBinaryExpression,
        extension: "SqlToStringDbExtension",
        config: SqlToStringConfig
) -> str:
    op_type = logical.type_
    if op_type == LogicalBinaryType.AND:
        op = "AND"
    elif op_type == LogicalBinaryType.OR:
        op = "OR"
    else:
        raise ValueError("Unknown logical binary operator type: " + str(op_type))  # pragma: no cover

    left = extension.process_expression(logical.left, config)
    right = extension.process_expression(logical.right, config)
    return f"({left} {op} {right})"


def not_expression_processor(
        not_expression: NotExpression,
        extension: "SqlToStringDbExtension",
        config: SqlToStringConfig
) -> str:
    expr = extension.process_expression(not_expression.value, config)
    if isinstance(not_expression.value, (LogicalBinaryExpression, ComparisonExpression)):
        return f"NOT{expr}"
    else:
        return f"NOT({expr})"


def arithmetic_expression_processor(
    arithmetic: ArithmeticExpression,
    extension: "SqlToStringDbExtension",
    config: SqlToStringConfig
) -> str:
    left = extension.process_expression(arithmetic.left, config)
    right = extension.process_expression(arithmetic.right, config)
    op_type = arithmetic.type_
    if op_type == ArithmeticType.ADD:
        return f"({left} + {right})"
    elif op_type == ArithmeticType.SUBTRACT:
        return f"({left} - {right})"
    elif op_type == ArithmeticType.MULTIPLY:
        return f"({left} * {right})"
    elif op_type == ArithmeticType.DIVIDE:
        return f"((1.0 * {left}) / {right})"
    elif op_type == ArithmeticType.MODULUS:
        return f"MOD({left}, {right})"
    else:
        raise ValueError("Unknown arithmetic operator type: " + str(op_type))  # pragma: no cover


def negative_expression_processor(
        negative: NegativeExpression,
        extension: "SqlToStringDbExtension",
        config: SqlToStringConfig
) -> str:
    expr = extension.process_expression(negative.value, config)
    if isinstance(negative.value, Literal):
        return f"-{expr}"
    else:
        return f"(0 - {expr})"


def when_clause_processor(
        when: WhenClause,
        extension: "SqlToStringDbExtension",
        config: SqlToStringConfig
) -> str:
    when_str = extension.process_expression(when.operand, config.push_indent())
    then_str = extension.process_expression(when.result, config.push_indent())
    sep1 = config.format.separator(1)
    sep0 = config.format.separator(0)
    return f"WHEN{sep1}{when_str}{sep0}THEN{sep1}{then_str}"


def searched_case_expression_processor(
        case: SearchedCaseExpression,
        extension: "SqlToStringDbExtension",
        config: SqlToStringConfig
) -> str:
    if case.defaultValue:
        sep1 = config.format.separator(1)
        expr = extension.process_expression(case.defaultValue, config.push_indent().push_indent())
        sep2 = config.format.separator(2)
        else_clause = f"{sep1}ELSE{sep2}{expr}"
    else:
        else_clause = ""

    sep1 = config.format.separator(1)
    when_clauses = config.format.separator(1).join(
        [extension.process_expression(clause, config.push_indent()) for clause in case.whenClauses]
    )
    sep0 = config.format.separator(0)
    return f"CASE{sep1}{when_clauses}{else_clause}{sep0}END"


def column_type_processor(
        column_type: ColumnType,
        extension: "SqlToStringDbExtension",
        config: SqlToStringConfig
) -> str:
    if column_type.parameters:
        return f"{column_type.name}({', '.join([str(p) for p in column_type.parameters])})"
    else:
        return column_type.name


def cast_expression_processor(
        cast: Cast,
        extension: "SqlToStringDbExtension",
        config: SqlToStringConfig
) -> str:
    value = extension.process_expression(cast.expression, config)
    data_type = extension.process_expression(cast.type_, config)
    return f"CAST({value} AS {data_type})"


def in_list_expression_processor(
        in_list: InListExpression,
        extension: "SqlToStringDbExtension",
        config: SqlToStringConfig
) -> str:
    return f"({', '.join([extension.process_expression(value, config) for value in in_list.values])})"


def in_predicate_processor(
        in_predicate: InPredicate,
        extension: "SqlToStringDbExtension",
        config: SqlToStringConfig
) -> str:
    value = extension.process_expression(in_predicate.value, config)
    value_list = extension.process_expression(in_predicate.valueList, config)
    return f"{value} IN {value_list}"


def is_null_predicate_processor(
        is_null_predicate: IsNullPredicate,
        extension: "SqlToStringDbExtension",
        config: SqlToStringConfig
) -> str:
    return f"({extension.process_expression(is_null_predicate.value, config)} IS NULL)"


def is_not_null_predicate_processor(
        is_not_null_predicate: IsNotNullPredicate,
        extension: "SqlToStringDbExtension",
        config: SqlToStringConfig
) -> str:
    return f"({extension.process_expression(is_not_null_predicate.value, config)} IS NOT NULL)"


def current_time_processor(
        current_time: CurrentTime,
        extension: "SqlToStringDbExtension",
        config: SqlToStringConfig
) -> str:
    if current_time.type_ == CurrentTimeType.DATE:
        return "CURRENT_DATE"
    elif current_time.type_ == CurrentTimeType.TIMESTAMP:
        return "CURRENT_TIMESTAMP" + (("(" + str(current_time.precision) + ")") if current_time.precision else "")
    elif current_time.type_ == CurrentTimeType.TIME:
        return "CURRENT_TIME" + (("(" + str(current_time.precision) + ")") if current_time.precision else "")
    else:
        raise ValueError("Unknown current time type: " + str(current_time.type_))  # pragma: no cover


def extract_processor(
        extract: Extract,
        extension: "SqlToStringDbExtension",
        config: SqlToStringConfig
) -> str:
    return f"EXTRACT({extract.field.name} FROM {extension.process_expression(extract.expression, config)})"


def function_call_processor(
        function_call: FunctionCall,
        extension: "SqlToStringDbExtension",
        config: SqlToStringConfig
) -> str:
    # TODO: Handle distinct and filter

    arguments = ("," + config.format.separator(1)).join(
        [extension.process_expression(arg, config.push_indent()) for arg in function_call.arguments]
    )

    window = ""
    if function_call.window:
        window = " " + extension.process_window(function_call.window, config)

    first_sep = config.format.separator(1) if function_call.arguments else ""
    sep0 = config.format.separator(0) if function_call.arguments else ""
    name = extension.process_qualified_name(function_call.name, config)
    return f"{name}({first_sep}{arguments}{sep0}){window}"


def named_argument_processor(
        named_arg: NamedArgumentExpression,
        extension: "SqlToStringDbExtension",
        config: SqlToStringConfig
) -> str:
    name = named_arg.name
    expr = extension.process_expression(named_arg.expression, config)
    return f"{name} => {expr}"


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
    elif isinstance(relation, TableFunction):
        return extension.process_table_function(relation, config)
    elif isinstance(relation, Union):
        return extension.process_union(relation, config, nested_subquery)
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
    relation = extension.process_relation(aliased_relation.relation, config, nested_subquery)
    alias = extension.process_identifier(aliased_relation.alias, config)
    return f"{relation} AS {alias}"


def query_processor(
        query: Query,
        extension: "SqlToStringDbExtension",
        config: SqlToStringConfig,
        nested_subquery: bool = False
) -> str:
    if isinstance(query.queryBody, (QuerySpecification, Union)) and \
            (query.limit is None) and \
            (query.offset is None) and \
            (query.orderBy is None or len(query.orderBy) == 0):
        return extension.process_relation(query.queryBody, config, nested_subquery)
    else:
        # TODO: Use limit, orderBy, offset at query level
        sep0 = config.format.separator(0)
        sep1 = config.format.separator(1)
        sep2 = config.format.separator(2)
        relation = extension.process_relation(query.queryBody, config.push_indent().push_indent(), True)
        return f"({sep1}SELECT{sep2}*{sep1}FROM{sep2}{relation}{sep0})"


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
    right = extension.process_relation(join.right, config.push_indent())
    join_type = join.type_
    condition = f"ON ({extension.process_join_criteria(join.criteria, config)})" if join.criteria else ""
    if join_type == JoinType.CROSS:
        join_type_str = 'CROSS JOIN'
    elif join_type == JoinType.INNER:
        join_type_str = 'INNER JOIN'
    elif join_type == JoinType.LEFT:
        join_type_str = 'LEFT OUTER JOIN'
    elif join_type == JoinType.RIGHT:
        join_type_str = 'RIGHT OUTER JOIN'
    elif join_type == JoinType.FULL:
        join_type_str = 'FULL OUTER JOIN'
    else:
        raise ValueError("Unknown join type: " + str(join_type))  # pragma: no cover

    sep0 = config.format.separator(0)
    sep1 = config.format.separator(1)
    return f"{left}{sep0}{join_type_str}{sep1}{right}{sep1}{condition}"


def window_processor(
        window: Window,
        extension: "SqlToStringDbExtension",
        config: SqlToStringConfig
) -> str:
    if window.windowRef:
        return window.windowRef

    partitions = "PARTITION BY " + (", ".join([extension.process_expression(e, config) for e in window.partitions])) \
        if window.partitions else ""

    order_by = "ORDER BY " + (", ".join([extension.process_sort_item(o, config) for o in window.orderBy])) \
        if window.orderBy else ""

    # TODO: Handle window frame
    return f"OVER ({partitions}{' ' if (partitions != '') and (order_by != '') else ''}{order_by}){''}"


def table_function_processor(
        table_func: TableFunction,
        extension: "SqlToStringDbExtension",
        config: SqlToStringConfig
) -> str:
    return extension.process_function_call(table_func.functionCall, config)


def union_processor(
        union: Union,
        extension: "SqlToStringDbExtension",
        config: SqlToStringConfig,
        nested_subquery: bool
) -> str:
    if nested_subquery:
        sep0 = config.format.separator(0)
        sep1 = config.format.separator(1)
        sub_query = extension.process_union(union, config.push_indent(), False)
        return f"({sep1}{sub_query}{sep0})"

    sep0 = config.format.separator(0)
    left = extension.process_relation(union.left, config)
    union_str = "UNION" if union.distinct else "UNION ALL"
    right = extension.process_relation(union.right, config)
    return f"{left}{sep0}{union_str}{sep0}{right}"


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
        return f"{cls.quote_character()}{identifier}{cls.quote_character()}"

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

    def process_extract(self, extract: Extract, config: SqlToStringConfig) -> str:
        return extract_processor(extract, self, config)

    def process_function_call(self, function_call: FunctionCall, config: SqlToStringConfig) -> str:
        return function_call_processor(function_call, self, config)

    def process_named_argument_expression(self, named_arg: NamedArgumentExpression, config: SqlToStringConfig) -> str:
        return named_argument_processor(named_arg, self, config)

    def process_string_length_expression(self, expr: StringLengthExpression, config: SqlToStringConfig) -> str:
        return f"CHAR_LENGTH({self.process_expression(expr.value, config)})"

    def process_string_like_expression(self, expr: StringLikeExpression, config: SqlToStringConfig) -> str:
        return f"({self.process_expression(expr.value, config)} LIKE {self.process_expression(expr.other, config)})"

    def process_string_upper_expression(self, expr: StringUpperExpression, config: SqlToStringConfig) -> str:
        return f"UPPER({self.process_expression(expr.value, config)})"

    def process_string_lower_expression(self, expr: StringLowerExpression, config: SqlToStringConfig) -> str:
        return f"LOWER({self.process_expression(expr.value, config)})"

    def process_string_trim_expression(self, expr: StringTrimExpression, config: SqlToStringConfig) -> str:
        op = f"({self.process_expression(expr.value, config)})"
        return ("LTRIM" if (expr.trim_type == TrimType.Left) else
                ("RTRIM" if (expr.trim_type == TrimType.Right) else "BTRIM")) + op

    def process_string_pos_expression(self, expr: StringPosExpression, config: SqlToStringConfig) -> str:
        return f"STRPOS({self.process_expression(expr.value, config)}, {self.process_expression(expr.other, config)})"

    def process_string_concat_expression(self, expr: StringConcatExpression, config: SqlToStringConfig) -> str:
        return f"CONCAT({self.process_expression(expr.first, config)}, {self.process_expression(expr.second, config)})"

    def process_absolute_expression(self, expr: AbsoluteExpression, config: SqlToStringConfig) -> str:
        return f"ABS({self.process_expression(expr.value, config)})"

    def process_power_expression(self, expr: PowerExpression, config: SqlToStringConfig) -> str:
        return f"POWER({self.process_expression(expr.first, config)}, {self.process_expression(expr.second, config)})"

    def process_ceil_expression(self, expr: CeilExpression, config: SqlToStringConfig) -> str:
        return f"CEIL({self.process_expression(expr.value, config)})"

    def process_floor_expression(self, expr: FloorExpression, config: SqlToStringConfig) -> str:
        return f"FLOOR({self.process_expression(expr.value, config)})"

    def process_sqrt_expression(self, expr: SqrtExpression, config: SqlToStringConfig) -> str:
        return f"SQRT({self.process_expression(expr.value, config)})"

    def process_cbrt_expression(self, expr: CbrtExpression, config: SqlToStringConfig) -> str:
        return f"CBRT({self.process_expression(expr.value, config)})"

    def process_exp_expression(self, expr: ExpExpression, config: SqlToStringConfig) -> str:
        return f"EXP({self.process_expression(expr.value, config)})"

    def process_log_expression(self, expr: LogExpression, config: SqlToStringConfig) -> str:
        return f"LN({self.process_expression(expr.value, config)})"

    def process_remainder_expression(self, expr: RemainderExpression, config: SqlToStringConfig) -> str:
        return f"MOD({self.process_expression(expr.first, config)}, {self.process_expression(expr.second, config)})"

    def process_round_expression(self, expr: RoundExpression, config: SqlToStringConfig) -> str:
        if expr.second is None:
            return f"ROUND({self.process_expression(expr.first, config)})"
        if not isinstance(expr.second, (IntegerLiteral, LongLiteral)):
            raise TypeError("Unexpected round argument type - " + str(type(expr.second)))

        if expr.second.value == 0:
            return f"ROUND({self.process_expression(expr.first, config)})"
        else:
            return f"ROUND({self.process_expression(expr.first, config)}, {self.process_expression(expr.second, config)})"

    def process_sine_expression(self, expr: SineExpression, config: SqlToStringConfig) -> str:
        return f"SIN({self.process_expression(expr.value, config)})"

    def process_arc_sine_expression(self, expr: ArcSineExpression, config: SqlToStringConfig) -> str:
        return f"ASIN({self.process_expression(expr.value, config)})"

    def process_cosine_expression(self, expr: CosineExpression, config: SqlToStringConfig) -> str:
        return f"COS({self.process_expression(expr.value, config)})"

    def process_arc_cosine_expression(self, expr: ArcCosineExpression, config: SqlToStringConfig) -> str:
        return f"ACOS({self.process_expression(expr.value, config)})"

    def process_tan_expression(self, expr: TanExpression, config: SqlToStringConfig) -> str:
        return f"TAN({self.process_expression(expr.value, config)})"

    def process_arc_tan_expression(self, expr: ArcTanExpression, config: SqlToStringConfig) -> str:
        return f"ATAN({self.process_expression(expr.value, config)})"

    def process_arc_tan2_expression(self, expr: ArcTan2Expression, config: SqlToStringConfig) -> str:
        return f"ATAN2({self.process_expression(expr.first, config)}, {self.process_expression(expr.second, config)})"

    def process_cot_expression(self, expr: CotExpression, config: SqlToStringConfig) -> str:
        return f"COT({self.process_expression(expr.value, config)})"

    def process_count_expression(self, expr: CountExpression, config: SqlToStringConfig) -> str:
        return f"COUNT({self.process_expression(expr.value, config)})"

    def process_distinct_count_expression(self, expr: DistinctCountExpression, config: SqlToStringConfig) -> str:
        return f"COUNT(DISTINCT {self.process_expression(expr.value, config)})"

    def process_average_expression(self, expr: AverageExpression, config: SqlToStringConfig) -> str:
        return f"AVG({self.process_expression(expr.value, config)})"

    def process_max_expression(self, expr: MaxExpression, config: SqlToStringConfig) -> str:
        return f"MAX({self.process_expression(expr.value, config)})"

    def process_min_expression(self, expr: MinExpression, config: SqlToStringConfig) -> str:
        return f"MIN({self.process_expression(expr.value, config)})"

    def process_sum_expression(self, expr: SumExpression, config: SqlToStringConfig) -> str:
        return f"SUM({self.process_expression(expr.value, config)})"

    def process_std_dev_sample_expression(self, expr: StdDevSampleExpression, config: SqlToStringConfig) -> str:
        return f"STDDEV_SAMP({self.process_expression(expr.value, config)})"

    def process_std_dev_population_expression(self, expr: StdDevPopulationExpression, config: SqlToStringConfig) -> str:
        return f"STDDEV_POP({self.process_expression(expr.value, config)})"

    def process_variance_sample_expression(self, expr: VarianceSampleExpression, config: SqlToStringConfig) -> str:
        return f"VAR_SAMP({self.process_expression(expr.value, config)})"

    def process_variance_population_expression(self, expr: VariancePopulationExpression, config: SqlToStringConfig) -> str:
        return f"VAR_POP({self.process_expression(expr.value, config)})"

    def process_join_strings_expression(self, expr: JoinStringsExpression, config: SqlToStringConfig) -> str:
        return f"STRING_AGG({self.process_expression(expr.value, config)}, {self.process_expression(expr.other, config)})"

    def process_first_day_of_year_expression(self, expr: FirstDayOfYearExpression, config: SqlToStringConfig) -> str:
        return f"DATE_TRUNC('year', {self.process_expression(expr.value, config)})"

    def process_first_day_of_quarter_expression(self, expr: FirstDayOfQuarterExpression, config: SqlToStringConfig) -> str:
        return f"DATE_TRUNC('quarter', {self.process_expression(expr.value, config)})"

    def process_first_day_of_month_expression(self, expr: FirstDayOfMonthExpression, config: SqlToStringConfig) -> str:
        return f"DATE_TRUNC('month', {self.process_expression(expr.value, config)})"

    def process_first_day_of_week_expression(self, expr: FirstDayOfWeekExpression, config: SqlToStringConfig) -> str:
        return f"DATE_TRUNC('week', {self.process_expression(expr.value, config)})"

    def process_first_hour_of_day_expression(self, expr: FirstHourOfDayExpression, config: SqlToStringConfig) -> str:
        return f"DATE_TRUNC('day', {self.process_expression(expr.value, config)})"

    def process_first_minute_of_hour_expression(self, expr: FirstMinuteOfHourExpression, config: SqlToStringConfig) \
            -> str:
        return f"DATE_TRUNC('hour', {self.process_expression(expr.value, config)})"

    def process_first_second_of_minute_expression(self, expr: FirstSecondOfMinuteExpression, config: SqlToStringConfig)\
            -> str:
        return f"DATE_TRUNC('minute', {self.process_expression(expr.value, config)})"

    def process_first_millisecond_of_second_expression(
            self, expr: FirstMillisecondOfSecondExpression, config: SqlToStringConfig
    ) -> str:
        return f"DATE_TRUNC('second', {self.process_expression(expr.value, config)})"

    def process_year_expression(self, expr: YearExpression, config: SqlToStringConfig) -> str:
        return f"DATE_PART('year', {self.process_expression(expr.value, config)})"

    def process_quarter_expression(self, expr: QuarterExpression, config: SqlToStringConfig) -> str:
        return f"DATE_PART('quarter', {self.process_expression(expr.value, config)})"

    def process_month_expression(self, expr: MonthExpression, config: SqlToStringConfig) -> str:
        return f"DATE_PART('month', {self.process_expression(expr.value, config)})"

    def process_week_of_year_expression(self, expr: WeekOfYearExpression, config: SqlToStringConfig) -> str:
        return f"DATE_PART('week', {self.process_expression(expr.value, config)})"

    def process_day_of_year_expression(self, expr: DayOfYearExpression, config: SqlToStringConfig) -> str:
        return f"DATE_PART('doy', {self.process_expression(expr.value, config)})"

    def process_day_of_month_expression(self, expr: DayOfMonthExpression, config: SqlToStringConfig) -> str:
        return f"DATE_PART('day', {self.process_expression(expr.value, config)})"

    def process_day_of_week_expression(self, expr: DayOfWeekExpression, config: SqlToStringConfig) -> str:
        return f"DATE_PART('dow', {self.process_expression(expr.value, config)})"

    def process_hour_expression(self, expr: HourExpression, config: SqlToStringConfig) -> str:
        return f"DATE_PART('hour', {self.process_expression(expr.value, config)})"

    def process_minute_expression(self, expr: MinuteExpression, config: SqlToStringConfig) -> str:
        return f"DATE_PART('minute', {self.process_expression(expr.value, config)})"

    def process_second_expression(self, expr: SecondExpression, config: SqlToStringConfig) -> str:
        return f"DATE_PART('second', {self.process_expression(expr.value, config)})"

    def process_epoch_expression(self, expr: EpochExpression, config: SqlToStringConfig) -> str:
        return f"DATE_PART('epoch', {self.process_expression(expr.value, config)})"

    def process_window_expression(self, expr: WindowExpression, config: SqlToStringConfig) -> str:
        return f"{self.process_expression(expr.nested, config)} {self.process_window(expr.window, config)}"

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
        return query_processor(query, self, config, False)

    def process_table_subquery(self, table_subquery: TableSubquery, config: SqlToStringConfig) -> str:
        return query_processor(table_subquery.query, self, config, True)

    def process_subquery_expression(self, subquery_expression: SubqueryExpression, config: SqlToStringConfig) -> str:
        return query_processor(subquery_expression.query, self, config, True)

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

    def process_sort_item(self, sort_item: SortItem, config: SqlToStringConfig) -> str:
        return sort_item_processor(sort_item, self, config)

    def process_order_by(self, query: QuerySpecification, config: SqlToStringConfig) -> str:
        return order_by_processor(query, self, config)

    def process_window(self, window: Window, config: SqlToStringConfig) -> str:
        return window_processor(window, self, config)

    def process_table_function(self, table_func: TableFunction, config: SqlToStringConfig) -> str:
        return table_function_processor(table_func, self, config)

    def process_union(self, union: Union, config: SqlToStringConfig, nested_subquery: bool = False) -> str:
        return union_processor(union, self, config, nested_subquery)

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
    Expression
)


__all__: PyLegendSequence[str] = [
    "SqlToStringDbExtension"
]


def query_specification_processor(
    query: QuerySpecification,
    extension: "SqlToStringDbExtension",
    config: SqlToStringConfig
) -> str:
    # TODO: code this
    raise RuntimeError("Not supported yet")


def select_processor(
    select: Select,
    extension: "SqlToStringDbExtension",
    config: SqlToStringConfig
) -> str:
    distinct_flag = "distinct " if select.distinct else ""
    items = [extension.process_select_item(item, config) for item in select.selectItems]
    return distinct_flag + ", ".join(items)


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
        raise ValueError("Unsupported select item type: " + str(type(select_item)))


def all_columns_processor(
    all_columns: AllColumns,
    extension: "SqlToStringDbExtension",
    config: SqlToStringConfig
) -> str:
    if all_columns.prefix:
        return extension.process_identifier(all_columns.prefix, config, True) + '.*'
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
            alias=extension.process_identifier(single_column.alias, config, True)
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
    if should_quote or config.quoted_identifiers or identifier in extension.reserved_keywords():
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
    else:
        raise ValueError("Unsupported expression type: " + str(type(expression)))


def literal_processor(
    literal: Literal,
    extension: "SqlToStringDbExtension",
    config: SqlToStringConfig
) -> str:
    return extension.literal_processor()(literal, config)


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

    def process_query_specification(self, query: QuerySpecification, config: SqlToStringConfig) -> str:
        return query_specification_processor(query, self, config)

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

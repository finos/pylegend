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


from enum import Enum
from pylegend._typing import (
    PyLegendList,
    PyLegendSequence,
    PyLegendOptional
)


__all__: PyLegendSequence[str] = [
    'TrimMode',
    'JoinType',
    'LogicalBinaryType',
    'ArithmeticType',
    'SortItemOrdering',
    'SortItemNullOrdering',
    'ComparisonOperator',
    'Node',
    'Statement',
    'Query',
    'Relation',
    'QueryBody',
    'TableSubquery',
    'QuerySpecification',
    'SetOperation',
    'AliasedRelation',
    'Union',
    'Select',
    'SelectItem',
    'AllColumns',
    'SingleColumn',
    'Table',
    'Expression',
    'SubqueryExpression',
    'Literal',
    'LongLiteral',
    'BooleanLiteral',
    'DoubleLiteral',
    'IntegerLiteral',
    'StringLiteral',
    'ArrayLiteral',
    'NullLiteral',
    'SortItem',
    'ComparisonExpression',
    'LogicalBinaryExpression',
    'NotExpression',
    'ArithmeticExpression',
    'NegativeExpression',
    'FunctionCall',
    'SimpleCaseExpression',
    'SearchedCaseExpression',
    'WhenClause',
    'Join',
    'JoinCriteria',
    'JoinOn',
    'JoinUsing',
    'Cast',
    'ColumnType',
    'QualifiedName',
    'QualifiedNameReference',
    'InListExpression',
    'InPredicate'
]


class TrimMode(Enum):
    LEADING = 1,
    TRAILING = 2,
    BOTH = 3


class JoinType(Enum):
    CROSS = 1,
    INNER = 2,
    LEFT = 3,
    RIGHT = 4,
    FULL = 5


class LogicalBinaryType(Enum):
    AND = 1,
    OR = 2


class ArithmeticType(Enum):
    ADD = 1,
    SUBTRACT = 2,
    MULTIPLY = 3,
    DIVIDE = 4,
    MODULUS = 5


class SortItemOrdering(Enum):
    ASCENDING = 1,
    DESCENDING = 2


class SortItemNullOrdering(Enum):
    FIRST = 1,
    LAST = 2,
    UNDEFINED = 3


class ComparisonOperator(Enum):
    EQUAL = 1,
    NOT_EQUAL = 2,
    LESS_THAN = 3,
    LESS_THAN_OR_EQUAL = 4,
    GREATER_THAN = 5,
    GREATER_THAN_OR_EQUAL = 6


class Node:
    _type: str

    def __init__(
        self,
        _type: str
    ) -> None:
        self._type = _type


class Statement(Node):

    def __init__(
        self,
        _type: str = "statement"
    ) -> None:
        super().__init__(_type=_type)


class Query(Statement):
    queryBody: "QueryBody"
    limit: "PyLegendOptional[Expression]"
    orderBy: "PyLegendList[SortItem]"
    offset: "PyLegendOptional[Expression]"

    def __init__(
        self,
        queryBody: "QueryBody",
        limit: "PyLegendOptional[Expression]",
        orderBy: "PyLegendList[SortItem]",
        offset: "PyLegendOptional[Expression]"
    ) -> None:
        super().__init__(_type="query")
        self.queryBody = queryBody
        self.limit = limit
        self.orderBy = orderBy
        self.offset = offset


class Relation(Node):

    def __init__(
        self,
        _type: str = "relation"
    ) -> None:
        super().__init__(_type=_type)


class QueryBody(Relation):

    def __init__(
        self,
        _type: str = "queryBody"
    ) -> None:
        super().__init__(_type=_type)


class TableSubquery(QueryBody):
    query: "Query"

    def __init__(
        self,
        query: "Query"
    ) -> None:
        super().__init__(_type="tableSubquery")
        self.query = query


class QuerySpecification(QueryBody):
    select: "Select"
    from_: "PyLegendList[Relation]"
    where: "PyLegendOptional[Expression]"
    groupBy: "PyLegendList[Expression]"
    having: "PyLegendOptional[Expression]"
    orderBy: "PyLegendList[SortItem]"
    limit: "PyLegendOptional[Expression]"
    offset: "PyLegendOptional[Expression]"

    def __init__(
        self,
        select: "Select",
        from_: "PyLegendList[Relation]",
        where: "PyLegendOptional[Expression]",
        groupBy: "PyLegendList[Expression]",
        having: "PyLegendOptional[Expression]",
        orderBy: "PyLegendList[SortItem]",
        limit: "PyLegendOptional[Expression]",
        offset: "PyLegendOptional[Expression]"
    ) -> None:
        super().__init__(_type="querySpecification")
        self.select = select
        self.from_ = from_
        self.where = where
        self.groupBy = groupBy
        self.having = having
        self.orderBy = orderBy
        self.limit = limit
        self.offset = offset


class SetOperation(QueryBody):

    def __init__(
        self,
        _type: str = "setOperation"
    ) -> None:
        super().__init__(_type=_type)


class AliasedRelation(Relation):
    relation: "Relation"
    alias: "str"
    columnNames: "PyLegendList[str]"

    def __init__(
        self,
        relation: "Relation",
        alias: "str",
        columnNames: "PyLegendList[str]"
    ) -> None:
        super().__init__(_type="aliasedRelation")
        self.relation = relation
        self.alias = alias
        self.columnNames = columnNames


class Union(SetOperation):
    left: "Relation"
    right: "Relation"
    distinct: "bool"

    def __init__(
        self,
        left: "Relation",
        right: "Relation",
        distinct: "bool"
    ) -> None:
        super().__init__(_type="union")
        self.left = left
        self.right = right
        self.distinct = distinct


class Select(Node):
    distinct: "bool"
    selectItems: "PyLegendList[SelectItem]"

    def __init__(
        self,
        distinct: "bool",
        selectItems: "PyLegendList[SelectItem]"
    ) -> None:
        super().__init__(_type="select")
        self.distinct = distinct
        self.selectItems = selectItems


class SelectItem(Node):

    def __init__(
        self,
        _type: str = "selectItem"
    ) -> None:
        super().__init__(_type=_type)


class AllColumns(SelectItem):
    prefix: "PyLegendOptional[str]"

    def __init__(
        self,
        prefix: "PyLegendOptional[str]"
    ) -> None:
        super().__init__(_type="allColumns")
        self.prefix = prefix


class SingleColumn(SelectItem):
    alias: "PyLegendOptional[str]"
    expression: "Expression"

    def __init__(
        self,
        alias: "PyLegendOptional[str]",
        expression: "Expression"
    ) -> None:
        super().__init__(_type="singleColumn")
        self.alias = alias
        self.expression = expression


class Table(QueryBody):
    name: "QualifiedName"

    def __init__(
        self,
        name: "QualifiedName"
    ) -> None:
        super().__init__(_type="table")
        self.name = name


class Expression(Node):

    def __init__(
        self,
        _type: str = "expression"
    ) -> None:
        super().__init__(_type=_type)


class SubqueryExpression(Expression):
    query: "Query"

    def __init__(
        self,
        query: "Query"
    ) -> None:
        super().__init__(_type="subqueryExpression")
        self.query = query


class Literal(Expression):

    def __init__(
        self,
        _type: str = "literal"
    ) -> None:
        super().__init__(_type=_type)


class LongLiteral(Literal):
    value: "int"

    def __init__(
        self,
        value: "int"
    ) -> None:
        super().__init__(_type="longLiteral")
        self.value = value


class BooleanLiteral(Literal):
    value: "bool"

    def __init__(
        self,
        value: "bool"
    ) -> None:
        super().__init__(_type="booleanLiteral")
        self.value = value


class DoubleLiteral(Literal):
    value: "float"

    def __init__(
        self,
        value: "float"
    ) -> None:
        super().__init__(_type="doubleLiteral")
        self.value = value


class IntegerLiteral(Literal):
    value: "int"

    def __init__(
        self,
        value: "int"
    ) -> None:
        super().__init__(_type="integerLiteral")
        self.value = value


class StringLiteral(Literal):
    value: "str"
    quoted: "bool"

    def __init__(
        self,
        value: "str",
        quoted: "bool"
    ) -> None:
        super().__init__(_type="stringLiteral")
        self.value = value
        self.quoted = quoted


class ArrayLiteral(Literal):
    values: "PyLegendList[Expression]"

    def __init__(
        self,
        values: "PyLegendList[Expression]"
    ) -> None:
        super().__init__(_type="arrayLiteral")
        self.values = values


class NullLiteral(Literal):

    def __init__(
        self
    ) -> None:
        super().__init__(_type="nullLiteral")


class SortItem(Node):
    sortKey: "Expression"
    ordering: "SortItemOrdering"
    nullOrdering: "SortItemNullOrdering"

    def __init__(
        self,
        sortKey: "Expression",
        ordering: "SortItemOrdering",
        nullOrdering: "SortItemNullOrdering"
    ) -> None:
        super().__init__(_type="sortItem")
        self.sortKey = sortKey
        self.ordering = ordering
        self.nullOrdering = nullOrdering


class ComparisonExpression(Expression):
    left: "Expression"
    right: "Expression"
    operator: "ComparisonOperator"

    def __init__(
        self,
        left: "Expression",
        right: "Expression",
        operator: "ComparisonOperator"
    ) -> None:
        super().__init__(_type="comparisonExpression")
        self.left = left
        self.right = right
        self.operator = operator


class LogicalBinaryExpression(Expression):
    type_: "LogicalBinaryType"
    left: "Expression"
    right: "Expression"

    def __init__(
        self,
        type_: "LogicalBinaryType",
        left: "Expression",
        right: "Expression"
    ) -> None:
        super().__init__(_type="logicalBinaryExpression")
        self.type_ = type_
        self.left = left
        self.right = right


class NotExpression(Expression):
    value: "Expression"

    def __init__(
        self,
        value: "Expression"
    ) -> None:
        super().__init__(_type="notExpression")
        self.value = value


class ArithmeticExpression(Expression):
    type_: "ArithmeticType"
    left: "Expression"
    right: "Expression"

    def __init__(
        self,
        type_: "ArithmeticType",
        left: "Expression",
        right: "Expression"
    ) -> None:
        super().__init__(_type="arithmeticExpression")
        self.type_ = type_
        self.left = left
        self.right = right


class NegativeExpression(Expression):
    value: "Expression"

    def __init__(
        self,
        value: "Expression"
    ) -> None:
        super().__init__(_type="negativeExpression")
        self.value = value


class FunctionCall(Expression):
    name: "QualifiedName"
    distinct: "bool"
    arguments: "PyLegendList[Expression]"
    filter_: "PyLegendOptional[Expression]"

    def __init__(
        self,
        name: "QualifiedName",
        distinct: "bool",
        arguments: "PyLegendList[Expression]",
        filter_: "PyLegendOptional[Expression]"
    ) -> None:
        super().__init__(_type="functionCall")
        self.name = name
        self.distinct = distinct
        self.arguments = arguments
        self.filter_ = filter_


class SimpleCaseExpression(Expression):
    operand: "Expression"
    whenClauses: "PyLegendList[WhenClause]"
    defaultValue: "PyLegendOptional[Expression]"

    def __init__(
        self,
        operand: "Expression",
        whenClauses: "PyLegendList[WhenClause]",
        defaultValue: "PyLegendOptional[Expression]"
    ) -> None:
        super().__init__(_type="simpleCaseExpression")
        self.operand = operand
        self.whenClauses = whenClauses
        self.defaultValue = defaultValue


class SearchedCaseExpression(Expression):
    whenClauses: "PyLegendList[WhenClause]"
    defaultValue: "PyLegendOptional[Expression]"

    def __init__(
        self,
        whenClauses: "PyLegendList[WhenClause]",
        defaultValue: "PyLegendOptional[Expression]"
    ) -> None:
        super().__init__(_type="searchedCaseExpression")
        self.whenClauses = whenClauses
        self.defaultValue = defaultValue


class WhenClause(Expression):
    operand: "Expression"
    result: "Expression"

    def __init__(
        self,
        operand: "Expression",
        result: "Expression"
    ) -> None:
        super().__init__(_type="whenClause")
        self.operand = operand
        self.result = result


class Join(Relation):
    type_: "JoinType"
    left: "Relation"
    right: "Relation"
    criteria: "PyLegendOptional[JoinCriteria]"

    def __init__(
        self,
        type_: "JoinType",
        left: "Relation",
        right: "Relation",
        criteria: "PyLegendOptional[JoinCriteria]"
    ) -> None:
        super().__init__(_type="join")
        self.type_ = type_
        self.left = left
        self.right = right
        self.criteria = criteria


class JoinCriteria:
    _type: str

    def __init__(
        self,
        _type: str
    ) -> None:
        self._type = _type


class JoinOn(JoinCriteria):
    expression: "Expression"

    def __init__(
        self,
        expression: "Expression"
    ) -> None:
        super().__init__(_type="joinOn")
        self.expression = expression


class JoinUsing(JoinCriteria):
    columns: "PyLegendList[str]"

    def __init__(
        self,
        columns: "PyLegendList[str]"
    ) -> None:
        super().__init__(_type="joinUsing")
        self.columns = columns


class Cast(Expression):
    expression: "Expression"
    type_: "ColumnType"

    def __init__(
        self,
        expression: "Expression",
        type_: "ColumnType"
    ) -> None:
        super().__init__(_type="cast")
        self.expression = expression
        self.type_ = type_


class ColumnType(Expression):
    name: "str"
    parameters: "PyLegendList[int]"

    def __init__(
        self,
        name: "str",
        parameters: "PyLegendList[int]"
    ) -> None:
        super().__init__(_type="columnType")
        self.name = name
        self.parameters = parameters


class QualifiedName:
    parts: "PyLegendList[str]"

    def __init__(
        self,
        parts: "PyLegendList[str]"
    ) -> None:
        self.parts = parts


class QualifiedNameReference(Expression):
    name: "QualifiedName"

    def __init__(
        self,
        name: "QualifiedName"
    ) -> None:
        super().__init__(_type="qualifiedNameReference")
        self.name = name


class InListExpression(Expression):
    values: "PyLegendList[Expression]"

    def __init__(
        self,
        values: "PyLegendList[Expression]"
    ) -> None:
        super().__init__(_type="inListExpression")
        self.values = values


class InPredicate(Expression):
    value: "Expression"
    valueList: "Expression"

    def __init__(
        self,
        value: "Expression",
        valueList: "Expression"
    ) -> None:
        super().__init__(_type="inPredicate")
        self.value = value
        self.valueList = valueList

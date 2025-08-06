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

from abc import ABCMeta
from enum import Enum
from pylegend.core.language import (
    PyLegendPrimitive,
    PyLegendBoolean,
    PyLegendString,
    PyLegendNumber,
    PyLegendInteger,
    PyLegendFloat,
    PyLegendDate,
    PyLegendDateTime,
    PyLegendStrictDate,
    PyLegendColumnExpression,
)
from pylegend._typing import (
    PyLegendSequence,
)

__all__: PyLegendSequence[str] = [
    "LegendQLApiPrimitive",
    "LegendQLApiBoolean",
    "LegendQLApiString",
    "LegendQLApiNumber",
    "LegendQLApiInteger",
    "LegendQLApiFloat",
    "LegendQLApiDate",
    "LegendQLApiDateTime",
    "LegendQLApiStrictDate",
    "LegendQLApiSortInfo",
    "LegendQLApiSortDirection",
]

from pylegend.core.language.shared.helpers import escape_column_name

from pylegend.core.sql.metamodel import QuerySpecification, Expression, SingleColumn, SortItem, SortItemOrdering, \
    SortItemNullOrdering
from pylegend.core.tds.tds_frame import FrameToSqlConfig, FrameToPureConfig


class LegendQLApiPrimitive(PyLegendPrimitive, metaclass=ABCMeta):
    def ascending(self) -> "LegendQLApiSortInfo":
        val = self.value()
        if isinstance(val, PyLegendColumnExpression):
            return LegendQLApiSortInfo(column_expr=val, direction=LegendQLApiSortDirection.ASC)
        else:
            raise RuntimeError("'ascending' function can only be called on column expressions. "
                               "E.g. - r.col1.ascending() / r['col1'].ascending()\n."
                               f"Found expression type - {type(val)}")

    def descending(self) -> "LegendQLApiSortInfo":
        val = self.value()
        if isinstance(val, PyLegendColumnExpression):
            return LegendQLApiSortInfo(column_expr=val, direction=LegendQLApiSortDirection.DESC)
        else:
            raise RuntimeError("'descending' function can only be called on column expressions. "
                               "E.g. - r.col1.descending() / r['col1'].descending()\n."
                               f"Found expression type - {type(val)}")


class LegendQLApiBoolean(LegendQLApiPrimitive, PyLegendBoolean):
    def __init__(self, expr: PyLegendBoolean):
        PyLegendBoolean.__init__(self, expr.value())


class LegendQLApiString(LegendQLApiPrimitive, PyLegendString):
    def __init__(self, expr: PyLegendString):
        PyLegendString.__init__(self, expr.value())


class LegendQLApiNumber(LegendQLApiPrimitive, PyLegendNumber):
    def __init__(self, expr: PyLegendNumber):
        PyLegendNumber.__init__(self, expr.value())


class LegendQLApiInteger(LegendQLApiPrimitive, PyLegendInteger):
    def __init__(self, expr: PyLegendInteger):
        PyLegendInteger.__init__(self, expr.value())


class LegendQLApiFloat(LegendQLApiPrimitive, PyLegendFloat):
    def __init__(self, expr: PyLegendFloat):
        PyLegendFloat.__init__(self, expr.value())


class LegendQLApiDate(LegendQLApiPrimitive, PyLegendDate):
    def __init__(self, expr: PyLegendDate):
        PyLegendDate.__init__(self, expr.value())


class LegendQLApiDateTime(LegendQLApiPrimitive, PyLegendDateTime):
    def __init__(self, expr: PyLegendDateTime):
        PyLegendDateTime.__init__(self, expr.value())


class LegendQLApiStrictDate(LegendQLApiPrimitive, PyLegendStrictDate):
    def __init__(self, expr: PyLegendStrictDate):
        PyLegendStrictDate.__init__(self, expr.value())


class LegendQLApiSortDirection(Enum):
    ASC = 1,
    DESC = 2


class LegendQLApiSortInfo:
    __column: str
    __direction: LegendQLApiSortDirection

    def __init__(self, column_expr: PyLegendColumnExpression, direction: LegendQLApiSortDirection) -> None:
        self.__column = column_expr.get_column()
        self.__direction = direction

    def to_sql_node(
            self,
            query: QuerySpecification,
            config: FrameToSqlConfig
    ) -> SortItem:
        return SortItem(
            sortKey=self.__find_column_expression(query, config),
            ordering=(SortItemOrdering.ASCENDING if self.__direction == LegendQLApiSortDirection.ASC
                      else SortItemOrdering.DESCENDING),
            nullOrdering=SortItemNullOrdering.UNDEFINED
        )

    def __find_column_expression(self, query: QuerySpecification, config: FrameToSqlConfig) -> Expression:
        db_extension = config.sql_to_string_generator().get_db_extension()
        filtered = [
            s for s in query.select.selectItems
            if (isinstance(s, SingleColumn) and
                s.alias == db_extension.quote_identifier(self.__column))
        ]
        if len(filtered) == 0:
            raise RuntimeError("Cannot find column: " + self.__column)  # pragma: no cover
        return filtered[0].expression

    def to_pure_expression(self, config: FrameToPureConfig) -> str:
        func = 'ascending' if self.__direction == LegendQLApiSortDirection.ASC else 'descending'
        return f"{func}(~{escape_column_name(self.__column)})"

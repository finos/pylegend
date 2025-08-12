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
from pylegend._typing import (
    PyLegendSequence,
    PyLegendDict,
)
from pylegend.core.sql.metamodel import (
    QuerySpecification,
    Expression,
    SingleColumn,
)
from pylegend.core.tds.tds_frame import (
    PyLegendTdsFrame,
    FrameToPureConfig,
    FrameToSqlConfig,
)
from pylegend.core.tds.tds_column import TdsColumn, PrimitiveTdsColumn
from pylegend.core.language import (
    PyLegendColumnExpression,
    PyLegendBooleanColumnExpression,
    PyLegendStringColumnExpression,
    PyLegendNumberColumnExpression,
    PyLegendIntegerColumnExpression,
    PyLegendFloatColumnExpression,
    PyLegendDateColumnExpression,
    PyLegendDateTimeColumnExpression,
    PyLegendStrictDateColumnExpression,
    PyLegendPrimitive,
    PyLegendBoolean,
    PyLegendString,
    PyLegendNumber,
    PyLegendInteger,
    PyLegendFloat,
    PyLegendDate,
    PyLegendDateTime,
    PyLegendStrictDate,
)

__all__: PyLegendSequence[str] = [
    "AbstractTdsRow",
]


class AbstractTdsRow(metaclass=ABCMeta):
    __frame_name: str
    __columns: PyLegendSequence[TdsColumn]

    def __init__(self, frame_name: str, frame: PyLegendTdsFrame) -> None:
        self.__frame_name = frame_name
        self.__columns = frame.columns()

    def get_boolean(self, column: str) -> PyLegendBoolean:
        col_expr = self.__get_col(column)
        if not isinstance(col_expr, PyLegendBooleanColumnExpression):
            raise RuntimeError(
                f"Column expression for '{column}' is of type '{type(col_expr)}'. "
                "get_boolean method is not valid on this column."
            )
        return PyLegendBoolean(col_expr)

    def get_string(self, column: str) -> PyLegendString:
        col_expr = self.__get_col(column)
        if not isinstance(col_expr, PyLegendStringColumnExpression):
            raise RuntimeError(
                f"Column expression for '{column}' is of type '{type(col_expr)}'. "
                "get_string method is not valid on this column."
            )
        return PyLegendString(col_expr)

    def get_number(self, column: str) -> PyLegendNumber:
        col_expr = self.__get_col(column)
        allowed_types = (PyLegendNumberColumnExpression, PyLegendIntegerColumnExpression, PyLegendFloatColumnExpression)
        if not isinstance(col_expr, allowed_types):
            raise RuntimeError(
                f"Column expression for '{column}' is of type '{type(col_expr)}'. "
                "get_number method is not valid on this column."
            )
        return PyLegendNumber(col_expr)

    def get_integer(self, column: str) -> PyLegendInteger:
        col_expr = self.__get_col(column)
        if not isinstance(col_expr, PyLegendIntegerColumnExpression):
            raise RuntimeError(
                f"Column expression for '{column}' is of type '{type(col_expr)}'. "
                "get_integer method is not valid on this column."
            )
        return PyLegendInteger(col_expr)

    def get_float(self, column: str) -> PyLegendFloat:
        col_expr = self.__get_col(column)
        if not isinstance(col_expr, PyLegendFloatColumnExpression):
            raise RuntimeError(
                f"Column expression for '{column}' is of type '{type(col_expr)}'. "
                "get_float method is not valid on this column."
            )
        return PyLegendFloat(col_expr)

    def get_date(self, column: str) -> PyLegendDate:
        col_expr = self.__get_col(column)
        allowed_types = (
            PyLegendDateColumnExpression, PyLegendDateTimeColumnExpression, PyLegendStrictDateColumnExpression
        )
        if not isinstance(col_expr, allowed_types):
            raise RuntimeError(
                f"Column expression for '{column}' is of type '{type(col_expr)}'. "
                "get_date method is not valid on this column."
            )
        return PyLegendDate(col_expr)

    def get_datetime(self, column: str) -> PyLegendDateTime:
        col_expr = self.__get_col(column)
        if not isinstance(col_expr, PyLegendDateTimeColumnExpression):
            raise RuntimeError(
                f"Column expression for '{column}' is of type '{type(col_expr)}'. "
                "get_datetime method is not valid on this column."
            )
        return PyLegendDateTime(col_expr)

    def get_strictdate(self, column: str) -> PyLegendStrictDate:
        col_expr = self.__get_col(column)
        if not isinstance(col_expr, PyLegendStrictDateColumnExpression):
            raise RuntimeError(
                f"Column expression for '{column}' is of type '{type(col_expr)}'. "
                "get_strictdate method is not valid on this column."
            )
        return PyLegendStrictDate(col_expr)

    def __getitem__(self, item: str) -> PyLegendPrimitive:
        if not isinstance(item, str):
            raise TypeError("Column indexing on a TDSRow should be with column name (string). Got - " + str(type(item)))

        col_expr = self.__get_col(item)

        if isinstance(col_expr, PyLegendBooleanColumnExpression):
            return PyLegendBoolean(col_expr)
        if isinstance(col_expr, PyLegendStringColumnExpression):
            return PyLegendString(col_expr)
        if isinstance(col_expr, PyLegendIntegerColumnExpression):
            return PyLegendInteger(col_expr)
        if isinstance(col_expr, PyLegendFloatColumnExpression):
            return PyLegendFloat(col_expr)
        if isinstance(col_expr, PyLegendNumberColumnExpression):
            return PyLegendNumber(col_expr)
        if isinstance(col_expr, PyLegendDateTimeColumnExpression):
            return PyLegendDateTime(col_expr)
        if isinstance(col_expr, PyLegendStrictDateColumnExpression):
            return PyLegendStrictDate(col_expr)
        if isinstance(col_expr, PyLegendDateColumnExpression):
            return PyLegendDate(col_expr)

        raise RuntimeError(f"Column expression for '{item}' of type {type(col_expr)} not supported yet")

    def __get_col(self, column: str) -> PyLegendColumnExpression:
        for base_col in self.__columns:
            if base_col.get_name() == column:
                if isinstance(base_col, PrimitiveTdsColumn):
                    if base_col.get_type() == "Boolean":
                        return PyLegendBooleanColumnExpression(self, column)
                    if base_col.get_type() == "String":
                        return PyLegendStringColumnExpression(self, column)
                    if base_col.get_type() == "Number":
                        return PyLegendNumberColumnExpression(self, column)
                    if base_col.get_type() == "Integer":
                        return PyLegendIntegerColumnExpression(self, column)
                    if base_col.get_type() == "Float":
                        return PyLegendFloatColumnExpression(self, column)
                    if base_col.get_type() == "Date":
                        return PyLegendDateColumnExpression(self, column)
                    if base_col.get_type() == "DateTime":
                        return PyLegendDateTimeColumnExpression(self, column)
                    if base_col.get_type() == "StrictDate":
                        return PyLegendStrictDateColumnExpression(self, column)

                raise RuntimeError(f"Column '{column}' of type {base_col.get_type()} not supported yet")

        raise ValueError(
            f"Column - '{column}' doesn't exist in the current frame. "
            f"Current frame columns: {[x.get_name() for x in self.__columns]}"
        )

    def get_frame_name(self) -> str:
        return self.__frame_name

    def to_pure_expression(self, config: FrameToPureConfig) -> str:
        return f"${self.__frame_name}"

    def column_sql_expression(
            self,
            column: str,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        query = frame_name_to_base_query_map[self.__frame_name]
        db_extension = config.sql_to_string_generator().get_db_extension()
        filtered = [
            s for s in query.select.selectItems
            if (isinstance(s, SingleColumn) and
                s.alias == db_extension.quote_identifier(column))
        ]
        if len(filtered) == 0:
            raise RuntimeError("Cannot find column: " + column)  # pragma: no cover
        return filtered[0].expression

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
    PyLegendSequence,
)
from pylegend.core.tds.tds_frame import PyLegendTdsFrame
from pylegend.core.tds.tds_column import TdsColumn, PrimitiveTdsColumn
from pylegend.core.language import (
    LegendApiColumnExpression,
    LegendApiBooleanColumnExpression,
    LegendApiStringColumnExpression,
    LegendApiNumberColumnExpression,
    LegendApiIntegerColumnExpression,
    LegendApiFloatColumnExpression,
    LegendApiDateColumnExpression,
    LegendApiDateTimeColumnExpression,
    LegendApiStrictDateColumnExpression,
    LegendApiPrimitive,
    LegendApiBoolean,
    LegendApiString,
    LegendApiNumber,
    LegendApiInteger,
    LegendApiFloat,
    LegendApiDate,
    LegendApiDateTime,
    LegendApiStrictDate,
)

__all__: PyLegendSequence[str] = [
    "LegendApiTdsRow",
]


class LegendApiTdsRow:
    __frame_name: str
    __columns: PyLegendSequence[TdsColumn]

    def __init__(self, frame_name: str, frame: PyLegendTdsFrame) -> None:
        self.__frame_name = frame_name
        self.__columns = frame.columns()

    @staticmethod
    def from_tds_frame(frame_name: str, frame: PyLegendTdsFrame) -> "LegendApiTdsRow":
        return LegendApiTdsRow(frame_name=frame_name, frame=frame)

    def get_boolean(self, column: str) -> LegendApiBoolean:
        col_expr = self.__get_col(column)
        if not isinstance(col_expr, LegendApiBooleanColumnExpression):
            raise RuntimeError(
                f"Column expression for '{column}' is of type '{type(col_expr)}'. "
                "get_boolean method is not valid on this column."
            )
        return LegendApiBoolean(col_expr)

    def get_string(self, column: str) -> LegendApiString:
        col_expr = self.__get_col(column)
        if not isinstance(col_expr, LegendApiStringColumnExpression):
            raise RuntimeError(
                f"Column expression for '{column}' is of type '{type(col_expr)}'. "
                "get_string method is not valid on this column."
            )
        return LegendApiString(col_expr)

    def get_number(self, column: str) -> LegendApiNumber:
        col_expr = self.__get_col(column)
        allowed_types = (LegendApiNumberColumnExpression, LegendApiIntegerColumnExpression, LegendApiFloatColumnExpression)
        if not isinstance(col_expr, allowed_types):
            raise RuntimeError(
                f"Column expression for '{column}' is of type '{type(col_expr)}'. "
                "get_number method is not valid on this column."
            )
        return LegendApiNumber(col_expr)

    def get_integer(self, column: str) -> LegendApiInteger:
        col_expr = self.__get_col(column)
        if not isinstance(col_expr, LegendApiIntegerColumnExpression):
            raise RuntimeError(
                f"Column expression for '{column}' is of type '{type(col_expr)}'. "
                "get_integer method is not valid on this column."
            )
        return LegendApiInteger(col_expr)

    def get_float(self, column: str) -> LegendApiFloat:
        col_expr = self.__get_col(column)
        if not isinstance(col_expr, LegendApiFloatColumnExpression):
            raise RuntimeError(
                f"Column expression for '{column}' is of type '{type(col_expr)}'. "
                "get_float method is not valid on this column."
            )
        return LegendApiFloat(col_expr)

    def get_date(self, column: str) -> LegendApiDate:
        col_expr = self.__get_col(column)
        allowed_types = (
            LegendApiDateColumnExpression, LegendApiDateTimeColumnExpression, LegendApiStrictDateColumnExpression
        )
        if not isinstance(col_expr, allowed_types):
            raise RuntimeError(
                f"Column expression for '{column}' is of type '{type(col_expr)}'. "
                "get_date method is not valid on this column."
            )
        return LegendApiDate(col_expr)

    def get_datetime(self, column: str) -> LegendApiDateTime:
        col_expr = self.__get_col(column)
        if not isinstance(col_expr, LegendApiDateTimeColumnExpression):
            raise RuntimeError(
                f"Column expression for '{column}' is of type '{type(col_expr)}'. "
                "get_datetime method is not valid on this column."
            )
        return LegendApiDateTime(col_expr)

    def get_strictdate(self, column: str) -> LegendApiStrictDate:
        col_expr = self.__get_col(column)
        if not isinstance(col_expr, LegendApiStrictDateColumnExpression):
            raise RuntimeError(
                f"Column expression for '{column}' is of type '{type(col_expr)}'. "
                "get_strictdate method is not valid on this column."
            )
        return LegendApiStrictDate(col_expr)

    def __getitem__(self, item: str) -> LegendApiPrimitive:
        if not isinstance(item, str):
            raise TypeError("Column indexing on a TDSRow should be with column name (string). Got - " + str(type(item)))

        col_expr = self.__get_col(item)

        if isinstance(col_expr, LegendApiBooleanColumnExpression):
            return LegendApiBoolean(col_expr)
        if isinstance(col_expr, LegendApiStringColumnExpression):
            return LegendApiString(col_expr)
        if isinstance(col_expr, LegendApiIntegerColumnExpression):
            return LegendApiInteger(col_expr)
        if isinstance(col_expr, LegendApiFloatColumnExpression):
            return LegendApiFloat(col_expr)
        if isinstance(col_expr, LegendApiNumberColumnExpression):
            return LegendApiNumber(col_expr)
        if isinstance(col_expr, LegendApiDateTimeColumnExpression):
            return LegendApiDateTime(col_expr)
        if isinstance(col_expr, LegendApiStrictDateColumnExpression):
            return LegendApiStrictDate(col_expr)
        if isinstance(col_expr, LegendApiDateColumnExpression):
            return LegendApiDate(col_expr)

        raise RuntimeError(f"Column expression for '{item}' of type {type(col_expr)} not supported yet")

    def __get_col(self, column: str) -> LegendApiColumnExpression:
        for base_col in self.__columns:
            if base_col.get_name() == column:
                if isinstance(base_col, PrimitiveTdsColumn):
                    if base_col.get_type() == "Boolean":
                        return LegendApiBooleanColumnExpression(self.__frame_name, column)
                    if base_col.get_type() == "String":
                        return LegendApiStringColumnExpression(self.__frame_name, column)
                    if base_col.get_type() == "Number":
                        return LegendApiNumberColumnExpression(self.__frame_name, column)
                    if base_col.get_type() == "Integer":
                        return LegendApiIntegerColumnExpression(self.__frame_name, column)
                    if base_col.get_type() == "Float":
                        return LegendApiFloatColumnExpression(self.__frame_name, column)
                    if base_col.get_type() == "Date":
                        return LegendApiDateColumnExpression(self.__frame_name, column)
                    if base_col.get_type() == "DateTime":
                        return LegendApiDateTimeColumnExpression(self.__frame_name, column)
                    if base_col.get_type() == "StrictDate":
                        return LegendApiStrictDateColumnExpression(self.__frame_name, column)

                raise RuntimeError(f"Column '{column}' of type {base_col.get_type()} not supported yet")

        raise ValueError(
            f"Column - '{column}' doesn't exist in the current frame. "
            f"Current frame columns: {[x.get_name() for x in self.__columns]}"
        )

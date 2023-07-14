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
    PyLegendColumnExpression,
    PyLegendBooleanColumnExpression,
    PyLegendStringColumnExpression,
    PyLegendNumberColumnExpression,
    PyLegendIntegerColumnExpression,
    PyLegendBoolean,
    PyLegendString,
    PyLegendNumber,
    PyLegendInteger,
)

__all__: PyLegendSequence[str] = [
    "TdsRow",
]


class TdsRow:
    __frame_name: str
    __columns: PyLegendSequence[TdsColumn]

    def __init__(self, frame_name: str, frame: PyLegendTdsFrame) -> None:
        self.__frame_name = frame_name
        self.__columns = frame.columns()

    @staticmethod
    def from_tds_frame(frame_name: str, frame: PyLegendTdsFrame) -> "TdsRow":
        return TdsRow(frame_name=frame_name, frame=frame)

    def get_boolean(self, column: str) -> PyLegendBoolean:
        col_expr = self.__get_col(column)
        if not isinstance(col_expr, PyLegendBooleanColumnExpression):
            raise RuntimeError(
                "Column expression for '{name}' is of type '{type}'. "
                "get_boolean method is not valid on this column.".format(
                    name=column,
                    type=type(col_expr)
                )
            )
        return PyLegendBoolean(col_expr)

    def get_string(self, column: str) -> PyLegendString:
        col_expr = self.__get_col(column)
        if not isinstance(col_expr, PyLegendStringColumnExpression):
            raise RuntimeError(
                "Column expression for '{name}' is of type '{type}'. "
                "get_string method is not valid on this column.".format(
                    name=column,
                    type=type(col_expr)
                )
            )
        return PyLegendString(col_expr)

    def get_number(self, column: str) -> PyLegendNumber:
        col_expr = self.__get_col(column)
        if not isinstance(col_expr, (PyLegendNumberColumnExpression, PyLegendIntegerColumnExpression)):
            raise RuntimeError(
                "Column expression for '{name}' is of type '{type}'. "
                "get_number method is not valid on this column.".format(
                    name=column,
                    type=type(col_expr)
                )
            )
        return PyLegendNumber(col_expr)

    def get_integer(self, column: str) -> PyLegendInteger:
        col_expr = self.__get_col(column)
        if not isinstance(col_expr, PyLegendIntegerColumnExpression):
            raise RuntimeError(
                "Column expression for '{name}' is of type '{type}'. "
                "get_integer method is not valid on this column.".format(
                    name=column,
                    type=type(col_expr)
                )
            )
        return PyLegendInteger(col_expr)

    def __get_col(self, column: str) -> PyLegendColumnExpression:
        for base_col in self.__columns:
            if base_col.get_name() == column:
                if isinstance(base_col, PrimitiveTdsColumn):
                    if base_col.get_type() == "Boolean":
                        return PyLegendBooleanColumnExpression(self.__frame_name, column)
                    if base_col.get_type() == "String":
                        return PyLegendStringColumnExpression(self.__frame_name, column)
                    if base_col.get_type() == "Number":
                        return PyLegendNumberColumnExpression(self.__frame_name, column)
                    if base_col.get_type() == "Integer":
                        return PyLegendIntegerColumnExpression(self.__frame_name, column)

                raise RuntimeError(
                    "Column '{col}' of type {type} not supported yet".format(
                        col=column,
                        type=base_col.get_type()
                    )
                )

        raise ValueError(
            "Column - '{col}' doesn't exist in the current frame. "
            "Current frame columns: {cols}".format(
                col=column,
                cols=[x.get_name() for x in self.__columns]
            )
        )

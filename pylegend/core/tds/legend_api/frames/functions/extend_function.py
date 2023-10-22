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

from datetime import date, datetime
from pylegend._typing import (
    PyLegendList,
    PyLegendSequence,
    PyLegendCallable,
)
from pylegend.core.tds.legend_api.frames.legend_api_applied_function_tds_frame import LegendApiAppliedFunction
from pylegend.core.tds.sql_query_helpers import copy_query, create_sub_query
from pylegend.core.sql.metamodel import (
    QuerySpecification,
    SingleColumn,
)
from pylegend.core.tds.tds_column import TdsColumn
from pylegend.core.tds.tds_frame import FrameToSqlConfig
from pylegend.core.tds.legend_api.frames.legend_api_base_tds_frame import LegendApiBaseTdsFrame
from pylegend.core.language import (
    TdsRow,
    PyLegendPrimitive,
    PyLegendPrimitiveOrPythonPrimitive,
    convert_literal_to_literal_expression,
)
from pylegend.core.tds.legend_api.frames.functions.function_helpers import tds_column_for_primitive

__all__: PyLegendSequence[str] = [
    "ExtendFunction"
]


class ExtendFunction(LegendApiAppliedFunction):
    __base_frame: LegendApiBaseTdsFrame
    __functions_list: PyLegendList[PyLegendCallable[[TdsRow], PyLegendPrimitiveOrPythonPrimitive]]
    __column_names_list: PyLegendList[str]

    @classmethod
    def name(cls) -> str:
        return "extend"

    def __init__(
            self,
            base_frame: LegendApiBaseTdsFrame,
            functions_list: PyLegendList[PyLegendCallable[[TdsRow], PyLegendPrimitiveOrPythonPrimitive]],
            column_names_list: PyLegendList[str]
    ) -> None:
        self.__base_frame = base_frame
        self.__functions_list = functions_list
        self.__column_names_list = column_names_list

    def to_sql(self, config: FrameToSqlConfig) -> QuerySpecification:
        base_query = self.__base_frame.to_sql_query_object(config)
        should_create_sub_query = len(base_query.groupBy) > 0
        db_extension = config.sql_to_string_generator().get_db_extension()

        new_query = (
            create_sub_query(base_query, config, "root") if should_create_sub_query else
            copy_query(base_query)
        )

        tds_row = TdsRow.from_tds_frame("frame", self.__base_frame)
        for (func, name) in zip(self.__functions_list, self.__column_names_list):
            col_expr = func(tds_row)

            if isinstance(col_expr, (bool, int, float, str, date, datetime)):
                col_sql_expr = convert_literal_to_literal_expression(col_expr).to_sql_expression(
                    {"frame": new_query},
                    config
                )
            else:
                col_sql_expr = col_expr.to_sql_expression({"frame": new_query}, config)

            new_query.select.selectItems.append(
                SingleColumn(alias=db_extension.quote_identifier(name), expression=col_sql_expr)
            )

        return new_query

    def base_frame(self) -> LegendApiBaseTdsFrame:
        return self.__base_frame

    def tds_frame_parameters(self) -> PyLegendList["LegendApiBaseTdsFrame"]:
        return []

    def calculate_columns(self) -> PyLegendSequence["TdsColumn"]:
        new_columns = [c.copy() for c in self.__base_frame.columns()]

        tds_row = TdsRow.from_tds_frame("frame", self.__base_frame)
        for (func, name) in zip(self.__functions_list, self.__column_names_list):
            result = func(tds_row)
            new_columns.append(tds_column_for_primitive(name, result))

        return new_columns

    def validate(self) -> bool:
        if len(self.__functions_list) != len(self.__column_names_list):
            raise ValueError(
                "For extend function, function list and column names list arguments should be of same size. "
                f"Passed param sizes -  Functions: {len(self.__functions_list)}, "
                f"Column names: {len(self.__column_names_list)}"
            )

        tds_row = TdsRow.from_tds_frame("frame", self.__base_frame)
        index = 0
        for (func, name) in zip(self.__functions_list, self.__column_names_list):
            copy = func  # For MyPy
            if not isinstance(copy, type(lambda x: 0)) or (copy.__code__.co_argcount != 1):
                raise TypeError(
                    f"Error at extend function at index {index} (0-indexed). "
                    "Each extend function should be a lambda which takes one argument (TDSRow)"
                )
            if not isinstance(name, str):
                raise TypeError(
                    f"Error at extend column name at index {index} (0-indexed). "
                    "Column name should be a string"
                )

            try:
                result = func(tds_row)
            except Exception as e:
                raise RuntimeError(
                    f"Extend function at index {index} (0-indexed) incompatible. "
                    f"Error occurred while evaluating. Message: {str(e)}"
                ) from e

            if not isinstance(result, (int, float, bool, str, PyLegendPrimitive)):
                raise ValueError(
                    f"Extend function at index {index} (0-indexed) incompatible. "
                    f"Returns non-primitive - {str(type(result))}"
                )

            index += 1

        if len(self.__column_names_list) != len(set(self.__column_names_list)):
            raise ValueError(f"Extend column names list has duplicates: {self.__column_names_list}")

        base_cols = [c.get_name() for c in self.__base_frame.columns()]
        for c in self.__column_names_list:
            if c in base_cols:
                raise ValueError(f"Extend column name - '{c}' already exists in base frame")

        return True

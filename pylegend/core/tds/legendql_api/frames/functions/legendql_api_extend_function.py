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

from datetime import date, datetime
from pylegend._typing import (
    PyLegendList,
    PyLegendSequence,
    PyLegendCallable,
    PyLegendUnion,
    PyLegendTuple,
)
from pylegend.core.language.legendql_api.legendql_api_tds_row import LegendQLApiTdsRow
from pylegend.core.tds.legendql_api.frames.legendql_api_applied_function_tds_frame import LegendQLApiAppliedFunction
from pylegend.core.tds.sql_query_helpers import copy_query, create_sub_query
from pylegend.core.sql.metamodel import (
    QuerySpecification,
    SingleColumn,
)
from pylegend.core.tds.tds_column import TdsColumn
from pylegend.core.tds.tds_frame import FrameToSqlConfig
from pylegend.core.tds.tds_frame import FrameToPureConfig
from pylegend.core.tds.legendql_api.frames.legendql_api_base_tds_frame import LegendQLApiBaseTdsFrame
from pylegend.core.language import (
    PyLegendPrimitive,
    PyLegendPrimitiveOrPythonPrimitive,
    convert_literal_to_literal_expression,
)
from pylegend.core.language.shared.helpers import generate_pure_lambda, escape_column_name
from pylegend.core.tds.abstract.function_helpers import tds_column_for_primitive

__all__: PyLegendSequence[str] = [
    "LegendQLApiExtendFunction"
]


class LegendQLApiExtendFunction(LegendQLApiAppliedFunction):
    __base_frame: LegendQLApiBaseTdsFrame
    __new_column_expression: PyLegendList[PyLegendTuple[str, PyLegendPrimitiveOrPythonPrimitive]]

    @classmethod
    def name(cls) -> str:
        return "extend"

    def __init__(
            self,
            base_frame: LegendQLApiBaseTdsFrame,
            extend_columns: PyLegendUnion[
                PyLegendTuple[str, PyLegendCallable[[LegendQLApiTdsRow], PyLegendPrimitiveOrPythonPrimitive]],
                PyLegendList[
                    PyLegendTuple[str, PyLegendCallable[[LegendQLApiTdsRow], PyLegendPrimitiveOrPythonPrimitive]]
                ]
            ]
    ) -> None:
        self.__base_frame = base_frame
        col_expressions: PyLegendList[PyLegendTuple[str, PyLegendPrimitiveOrPythonPrimitive]] = []
        tds_row = LegendQLApiTdsRow.from_tds_frame("r", self.__base_frame)
        for (i, extend_column) in enumerate(extend_columns if isinstance(extend_columns, list) else [extend_columns]):
            if isinstance(extend_column, tuple) and len(extend_column) == 2:
                if not isinstance(extend_column[0], str):
                    raise TypeError(
                        "'extend' function extend_columns argument incompatible. "
                        "First element in an extend tuple should be a string (new column name). "
                        "E.g - ('new col', lambda r: r.c1 + 1). "
                        f"Element at index {i} (0-indexed) is incompatible"
                    )
                if not isinstance(extend_column[1], type(lambda x: 0)) or (extend_column[1].__code__.co_argcount != 1):
                    raise TypeError(
                        "'extend' function extend_columns argument incompatible. "
                        "Second element in an extend tuple should be a lambda function which takes one argument "
                        "(LegendQLApiTdsRow) E.g - ('new col', lambda r: r.c1 + 1)."
                        f"Element at index {i} (0-indexed) is incompatible"
                    )
                try:
                    result = extend_column[1](tds_row)
                except Exception as e:
                    raise RuntimeError(
                        "'extend' function extend_columns argument incompatible. "
                        f"Error occurred while evaluating extend lambda at index {i} (0-indexed). Message: " + str(e)
                    ) from e

                if not isinstance(result, (int, float, bool, str, date, datetime, PyLegendPrimitive)):
                    raise TypeError(
                        "'extend' function extend_columns argument incompatible. "
                        f"Extend lambda at index {i} (0-indexed) returns non-primitive - {str(type(result))}"
                    )
                col_expressions.append((extend_column[0], result))
            else:
                raise TypeError(
                    "'extend' function extend_columns argument should be a list of tuples with two elements - "
                    "first element being a string (new column name) and second element being a lambda "
                    "function which takes one argument (LegendQLApiTdsRow). "
                    "E.g - [('new col1', lambda r: r.c1 + 1), ('new col2', lambda r: r.c2 + 2)]"
                )
        self.__new_column_expression = col_expressions

    def to_sql(self, config: FrameToSqlConfig) -> QuerySpecification:
        base_query = self.__base_frame.to_sql_query_object(config)
        should_create_sub_query = len(base_query.groupBy) > 0
        db_extension = config.sql_to_string_generator().get_db_extension()

        new_query = (
            create_sub_query(base_query, config, "root") if should_create_sub_query else
            copy_query(base_query)
        )

        for (name, expr) in self.__new_column_expression:
            if isinstance(expr, (bool, int, float, str, date, datetime)):
                col_sql_expr = convert_literal_to_literal_expression(expr).to_sql_expression(
                    {"r": new_query},
                    config
                )
            else:
                col_sql_expr = expr.to_sql_expression({"r": new_query}, config)

            new_query.select.selectItems.append(
                SingleColumn(alias=db_extension.quote_identifier(name), expression=col_sql_expr)
            )

        return new_query

    def to_pure(self, config: FrameToPureConfig) -> str:
        rendered_columns = []
        for (name, expr) in self.__new_column_expression:
            escaped_col_name = escape_column_name(name)
            pure_expr = (expr.to_pure_expression(config) if isinstance(expr, PyLegendPrimitive) else
                         convert_literal_to_literal_expression(expr).to_pure_expression(config))
            rendered_columns.append(f"{escaped_col_name}:{generate_pure_lambda('r', pure_expr)}")

        return (f"{self.__base_frame.to_pure(config)}{config.separator(1)}" +
                (
                    (f"->extend(~{rendered_columns[0]})") if len(rendered_columns) == 1 else
                    (f"->extend(~[{config.separator(2)}"
                     f"{(',' + config.separator(2, True)).join(rendered_columns)}"
                     f"{config.separator(1)}])")
                ))

    def base_frame(self) -> LegendQLApiBaseTdsFrame:
        return self.__base_frame

    def tds_frame_parameters(self) -> PyLegendList["LegendQLApiBaseTdsFrame"]:
        return []

    def calculate_columns(self) -> PyLegendSequence["TdsColumn"]:
        new_columns = [c.copy() for c in self.__base_frame.columns()]
        for (name, expr) in self.__new_column_expression:
            new_columns.append(tds_column_for_primitive(name, expr))
        return new_columns

    def validate(self) -> bool:
        col_names = [x[0] for x in self.__new_column_expression]

        if len(col_names) != len(set(col_names)):
            raise ValueError(f"Extend column names list has duplicates: {col_names}")

        for c in col_names:
            if c in [c.get_name() for c in self.__base_frame.columns()]:
                raise ValueError(f"Extend column name - '{c}' already exists in base frame")

        return True

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
from pylegend.core.tds.sql_query_helpers import create_sub_query
from pylegend.core.sql.metamodel import (
    QuerySpecification,
    SingleColumn,
    SelectItem,
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
    "LegendQLApiProjectFunction"
]


class LegendQLApiProjectFunction(LegendQLApiAppliedFunction):
    __base_frame: LegendQLApiBaseTdsFrame
    __new_column_expressions: PyLegendList[PyLegendTuple[str, PyLegendPrimitiveOrPythonPrimitive]]

    @classmethod
    def name(cls) -> str:
        return "project"

    def __init__(
            self,
            base_frame: LegendQLApiBaseTdsFrame,
            project_columns: PyLegendUnion[
                PyLegendTuple[
                    str,
                    PyLegendCallable[[LegendQLApiTdsRow], PyLegendPrimitiveOrPythonPrimitive]
                ],
                PyLegendList[
                    PyLegendTuple[
                        str,
                        PyLegendCallable[[LegendQLApiTdsRow], PyLegendPrimitiveOrPythonPrimitive]
                    ]
                ]
            ]
    ) -> None:
        self.__base_frame = base_frame
        col_expressions: PyLegendList[PyLegendTuple[str, PyLegendPrimitiveOrPythonPrimitive]] = []
        tds_row = LegendQLApiTdsRow.from_tds_frame("r", self.__base_frame)
        for (i, project_col) in enumerate(project_columns if isinstance(project_columns, list) else [project_columns]):
            if isinstance(project_col, tuple) and (len(project_col) == 2):
                if not isinstance(project_col[0], str):
                    raise TypeError(
                        "'project' function project_columns argument incompatible. "
                        "First element in an project tuple should be a string (new column name). "
                        "E.g - ('new col', lambda r: r.c1 + 1). "
                        f"Element at index {i} (0-indexed) is incompatible"
                    )
                if not isinstance(project_col[1], type(lambda x: 0)) or (project_col[1].__code__.co_argcount != 1):
                    raise TypeError(
                        "'project' function project_columns argument incompatible. "
                        "Second element in an project tuple should be a lambda function which takes one argument "
                        "(LegendQLApiTdsRow) E.g - ('new col', lambda r: r.c1 + 1)."
                        f"Element at index {i} (0-indexed) is incompatible"
                    )
                try:
                    result = project_col[1](tds_row)
                except Exception as e:
                    raise RuntimeError(
                        "'project' function project_columns argument incompatible. "
                        f"Error occurred while evaluating project lambda at index {i} (0-indexed). Message: " + str(e)
                    ) from e

                if not isinstance(result, (int, float, bool, str, date, datetime, PyLegendPrimitive)):
                    raise TypeError(
                        "'project' function project_columns argument incompatible. "
                        f"Project lambda at index {i} (0-indexed) returns non-primitive - {str(type(result))}"
                    )
                col_expressions.append((project_col[0], result))
            else:
                raise TypeError(
                    "'project' function project_columns argument should be a list of tuples with two elements - "
                    "first element being a string (new column name), second element being a lambda "
                    "function which takes one argument (LegendQLApiTdsRow) "
                    "E.g - [('new col1', lambda r: r.c1 + 1), ('new col2', lambda r: r.c2)]. "
                    f"Element at index {i} (0-indexed) is incompatible"
                )
        self.__new_column_expressions = col_expressions

    def to_sql(self, config: FrameToSqlConfig) -> QuerySpecification:
        base_query = self.__base_frame.to_sql_query_object(config)
        db_extension = config.sql_to_string_generator().get_db_extension()
        new_query = create_sub_query(base_query, config, "root")
        new_select_items: PyLegendList[SelectItem] = []
        for c in self.__new_column_expressions:
            if isinstance(c[1], (bool, int, float, str, date, datetime)):
                col_sql_expr = convert_literal_to_literal_expression(c[1]).to_sql_expression(
                    {"r": new_query},
                    config
                )
            else:
                col_sql_expr = c[1].to_sql_expression({"r": new_query}, config)
            new_select_items.append(
                SingleColumn(alias=db_extension.quote_identifier(c[0]), expression=col_sql_expr)
            )
        new_query.select.selectItems = new_select_items
        return new_query

    def to_pure(self, config: FrameToPureConfig) -> str:
        def render_single_column_expression(
                c: PyLegendUnion[
                    PyLegendTuple[str, PyLegendPrimitiveOrPythonPrimitive],
                    PyLegendTuple[str, PyLegendPrimitiveOrPythonPrimitive, PyLegendPrimitive]
                ]
        ) -> str:
            escaped_col_name = escape_column_name(c[0])
            expr_str = (c[1].to_pure_expression(config) if isinstance(c[1], PyLegendPrimitive) else
                        convert_literal_to_literal_expression(c[1]).to_pure_expression(config))
            return f"{escaped_col_name}:{generate_pure_lambda('r', expr_str)}"

        project_str = (f"->project(~[{config.separator(2)}" +
                       ("," + config.separator(2, True)).join(
                          [render_single_column_expression(x) for x in self.__new_column_expressions]
                       ) +
                       f"{config.separator(1)}])")
        return f"{self.__base_frame.to_pure(config)}{config.separator(1)}" + project_str

    def base_frame(self) -> LegendQLApiBaseTdsFrame:
        return self.__base_frame

    def tds_frame_parameters(self) -> PyLegendList["LegendQLApiBaseTdsFrame"]:
        return []

    def calculate_columns(self) -> PyLegendSequence["TdsColumn"]:
        new_columns = []
        for c in self.__new_column_expressions:
            new_columns.append(tds_column_for_primitive(c[0], c[1] if len(c) == 2 else c[2]))
        return new_columns

    def validate(self) -> bool:
        col_names = [x[0] for x in self.__new_column_expressions]
        if len(col_names) != len(set(col_names)):
            raise ValueError(f"Project column names list has duplicates: {col_names}")
        return True

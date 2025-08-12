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
from pylegend.core.language.legendql_api.legendql_api_custom_expressions import (
    LegendQLApiWindow,
    LegendQLApiPartialFrame,
    LegendQLApiWindowReference
)
from pylegend.core.language.legendql_api.legendql_api_tds_row import LegendQLApiTdsRow
from pylegend.core.sql.metamodel_extension import WindowExpression
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
    convert_literal_to_literal_expression, PyLegendPrimitiveCollection, create_primitive_collection,
)
from pylegend.core.language.shared.helpers import generate_pure_lambda, escape_column_name
from pylegend.core.tds.abstract.function_helpers import tds_column_for_primitive

__all__: PyLegendSequence[str] = [
    "LegendQLApiWindowExtendFunction"
]


class LegendQLApiWindowExtendFunction(LegendQLApiAppliedFunction):
    __base_frame: LegendQLApiBaseTdsFrame
    __window: LegendQLApiWindow
    __new_column_expressions: PyLegendList[
        PyLegendUnion[
            PyLegendTuple[str, PyLegendPrimitiveOrPythonPrimitive],
            PyLegendTuple[str, PyLegendPrimitiveOrPythonPrimitive, PyLegendPrimitive]
        ]
    ]

    @classmethod
    def name(cls) -> str:
        return "windowExtend"

    def __init__(
            self,
            base_frame: LegendQLApiBaseTdsFrame,
            window: LegendQLApiWindow,
            extend_columns: PyLegendUnion[
                PyLegendTuple[
                    str,
                    PyLegendCallable[
                        [LegendQLApiPartialFrame, LegendQLApiWindowReference, LegendQLApiTdsRow],
                        PyLegendPrimitiveOrPythonPrimitive
                    ]
                ],
                PyLegendTuple[
                    str,
                    PyLegendCallable[
                        [LegendQLApiPartialFrame, LegendQLApiWindowReference, LegendQLApiTdsRow],
                        PyLegendPrimitiveOrPythonPrimitive
                    ],
                    PyLegendCallable[[PyLegendPrimitiveCollection], PyLegendPrimitive]
                ],
                PyLegendList[
                    PyLegendUnion[
                        PyLegendTuple[
                            str,
                            PyLegendCallable[
                                [LegendQLApiPartialFrame, LegendQLApiWindowReference, LegendQLApiTdsRow],
                                PyLegendPrimitiveOrPythonPrimitive
                            ]
                        ],
                        PyLegendTuple[
                            str,
                            PyLegendCallable[
                                [LegendQLApiPartialFrame, LegendQLApiWindowReference, LegendQLApiTdsRow],
                                PyLegendPrimitiveOrPythonPrimitive
                            ],
                            PyLegendCallable[[PyLegendPrimitiveCollection], PyLegendPrimitive]
                        ]
                    ]
                ]
            ]
    ) -> None:
        self.__base_frame = base_frame
        self.__window = window
        col_expressions: PyLegendList[
            PyLegendUnion[
                PyLegendTuple[str, PyLegendPrimitiveOrPythonPrimitive],
                PyLegendTuple[str, PyLegendPrimitiveOrPythonPrimitive, PyLegendPrimitive]
            ]
        ] = []
        tds_row = LegendQLApiTdsRow.from_tds_frame("r", self.__base_frame)
        partial_frame = LegendQLApiPartialFrame(base_frame=self.__base_frame, var_name="p")
        window_ref = LegendQLApiWindowReference(window=self.__window, var_name="w")
        for (i, extend_column) in enumerate(extend_columns if isinstance(extend_columns, list) else [extend_columns]):
            if isinstance(extend_column, tuple) and len(extend_column) in [2, 3]:
                if not isinstance(extend_column[0], str):
                    raise TypeError(
                        "'window_extend' function extend_columns argument incompatible. "
                        "First element in an window_extend tuple should be a string (new column name). "
                        "E.g - ('new col', lambda p,w,r: r.c1 + 1). "
                        f"Element at index {i} (0-indexed) is incompatible"
                    )
                if not isinstance(extend_column[1], type(lambda x: 0)) or (extend_column[1].__code__.co_argcount != 3):
                    raise TypeError(
                        "'window_extend' function extend_columns argument incompatible. "
                        "Second element in an window_extend tuple should be a lambda function which takes three "
                        "arguments (LegendQLApiPartialFrame, LegendQLApiWindowReference, LegendQLApiTdsRow) "
                        "E.g - ('new col', lambda p,w,r: r.c1 + 1). Element at index {i} (0-indexed) is incompatible"
                    )
                try:
                    result = extend_column[1](partial_frame, window_ref, tds_row)
                except Exception as e:
                    raise RuntimeError(
                        "'window_extend' function extend_columns argument incompatible. "
                        f"Error occurred while evaluating window_extend lambda at index {i} (0-indexed). "
                        f"Message: " + str(e)
                    ) from e

                if not isinstance(result, (int, float, bool, str, date, datetime, PyLegendPrimitive)):
                    raise TypeError(
                        "'window_extend' function extend_columns argument incompatible. "
                        f"window_extend lambda at index {i} (0-indexed) returns non-primitive - {str(type(result))}"
                    )
                if len(extend_column) == 2:
                    col_expressions.append((extend_column[0], result))
                else:
                    if (not isinstance(extend_column[2], type(lambda x: 0)) or
                            (extend_column[2].__code__.co_argcount != 1)):
                        raise TypeError(
                            "'window_extend' function extend_columns argument incompatible. "
                            "Third element in an window_extend tuple should be a lambda function which takes one argument "
                            "(collection) E.g - ('new col', lambda p,w,r: r.c1, lambda c: c.sum()). "
                            f"Element at index {i} (0-indexed) is incompatible"
                        )
                    collection = create_primitive_collection(result)
                    try:
                        agg_result = extend_column[2](collection)
                    except Exception as e:
                        raise RuntimeError(
                            "'window_extend' function extend_columns argument incompatible. "
                            f"Error occurred while evaluating aggregation lambda at index {i} (0-indexed). "
                            "Message: " + str(e)
                        ) from e
                    if not isinstance(agg_result, PyLegendPrimitive):
                        raise TypeError(
                            "'window_extend' function extend_columns argument incompatible. "
                            f"Aggregation lambda at index {i} (0-indexed) "
                            f"returns non-primitive - {str(type(agg_result))}"
                        )
                    col_expressions.append((extend_column[0], result, agg_result))
            else:
                raise TypeError(
                    "'window_extend' function extend_columns argument should be a list of tuples with two/three elements"
                    " - first element being a string (new column name), second element being a lambda function which "
                    "takes three arguments (LegendQLApiPartialFrame, LegendQLApiWindowReference, LegendQLApiTdsRow) and "
                    "third element being an optional aggregation lambda function which takes one argument "
                    "E.g - [('new col1', lambda p,w,r: r.c1 + 1), ('new col2', lambda p,w,r: r.c2, lambda c: c.sum())]. "
                    f"Element at index {i} (0-indexed) is incompatible"
                )
        self.__new_column_expressions = col_expressions

    def to_sql(self, config: FrameToSqlConfig) -> QuerySpecification:
        base_query = self.__base_frame.to_sql_query_object(config)
        should_create_sub_query = len(base_query.groupBy) > 0
        db_extension = config.sql_to_string_generator().get_db_extension()

        new_query = (
            create_sub_query(base_query, config, "root") if should_create_sub_query else
            copy_query(base_query)
        )

        for c in self.__new_column_expressions:
            if len(c) == 2:
                if isinstance(c[1], (bool, int, float, str, date, datetime)):
                    col_sql_expr = convert_literal_to_literal_expression(c[1]).to_sql_expression(
                        {"r": new_query},
                        config
                    )
                else:
                    col_sql_expr = c[1].to_sql_expression({"r": new_query}, config)
                window_expr = WindowExpression(
                    nested=col_sql_expr,
                    window=self.__window.to_sql_node(new_query, config),
                )
                new_query.select.selectItems.append(
                    SingleColumn(alias=db_extension.quote_identifier(c[0]), expression=window_expr)
                )
            else:
                agg_sql_expr = c[2].to_sql_expression({"r": new_query}, config)
                window_expr = WindowExpression(
                    nested=agg_sql_expr,
                    window=self.__window.to_sql_node(new_query, config),
                )
                new_query.select.selectItems.append(
                    SingleColumn(alias=db_extension.quote_identifier(c[0]), expression=window_expr)
                )
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
            if len(c) == 2:
                return f"{escaped_col_name}:{generate_pure_lambda('p,w,r', expr_str)}"
            else:
                agg_expr_str = c[2].to_pure_expression(config).replace(expr_str, "$c")
                return (f"{escaped_col_name}:"
                        f"{generate_pure_lambda('p,w,r', expr_str)}:"
                        f"{generate_pure_lambda('c', agg_expr_str)}")

        window = self.__window.to_pure_expression(config)

        if all([len(t) == 2 for t in self.__new_column_expressions]) or all(
                [len(t) == 3 for t in self.__new_column_expressions]):
            if len(self.__new_column_expressions) == 1:
                extend_str = f"->extend({window}, ~{render_single_column_expression(self.__new_column_expressions[0])})"
            else:
                extend_str = (f"->extend({window}, ~[{config.separator(2)}" +
                              ("," + config.separator(2, True)).join(
                                  [render_single_column_expression(x) for x in self.__new_column_expressions]
                              ) +
                              f"{config.separator(1)}])")
            return f"{self.__base_frame.to_pure(config)}{config.separator(1)}" + extend_str
        else:
            extend_str = self.__base_frame.to_pure(config)
            for c in self.__new_column_expressions:
                extend_str += f"{config.separator(1)}->extend({window}, ~{render_single_column_expression(c)})"
            return extend_str

    def base_frame(self) -> LegendQLApiBaseTdsFrame:
        return self.__base_frame

    def tds_frame_parameters(self) -> PyLegendList["LegendQLApiBaseTdsFrame"]:
        return []

    def calculate_columns(self) -> PyLegendSequence["TdsColumn"]:
        new_columns = [c.copy() for c in self.__base_frame.columns()]
        for c in self.__new_column_expressions:
            new_columns.append(tds_column_for_primitive(c[0], c[1] if len(c) == 2 else c[2]))
        return new_columns

    def validate(self) -> bool:
        col_names = [x[0] for x in self.__new_column_expressions]

        if len(col_names) != len(set(col_names)):
            raise ValueError(f"Extend column names list has duplicates: {col_names}")

        for c in col_names:
            if c in [c.get_name() for c in self.__base_frame.columns()]:
                raise ValueError(f"Extend column name - '{c}' already exists in base frame")

        return True

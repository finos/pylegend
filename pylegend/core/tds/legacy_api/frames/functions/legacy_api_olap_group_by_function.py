# Copyright 2026 Goldman Sachs
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
    PyLegendList,
    PyLegendSequence,
    PyLegendOptional,
    PyLegendTuple,
    PyLegendUnion,
)
from pylegend.core.language.legacy_api.legacy_api_custom_expressions import (
    LegacyApiOLAPGroupByOperation,
    LegacyApiOLAPAggregation,
    LegacyApiOLAPRank,
    LegacyApiSortInfo,
    LegacyApiWindow,
    LegacyApiPartialFrame,
    LegacyApiRankExpression,
)
from pylegend.core.sql.metamodel_extension import WindowExpression
from pylegend.core.tds.legacy_api.frames.legacy_api_applied_function_tds_frame import LegacyApiAppliedFunction
from pylegend.core.tds.sql_query_helpers import copy_query, create_sub_query
from pylegend.core.sql.metamodel import (
    QuerySpecification,
    SingleColumn,
)
from pylegend.core.tds.tds_column import TdsColumn
from pylegend.core.tds.tds_frame import FrameToSqlConfig
from pylegend.core.tds.tds_frame import FrameToPureConfig
from pylegend.core.tds.legacy_api.frames.legacy_api_base_tds_frame import LegacyApiBaseTdsFrame
from pylegend.core.language import (
    LegacyApiTdsRow,
    PyLegendPrimitive,
    PyLegendPrimitiveOrPythonPrimitive,
    convert_literal_to_literal_expression,
    create_primitive_collection,
)
from pylegend.core.language.shared.helpers import generate_pure_lambda, escape_column_name
from pylegend.core.tds.abstract.function_helpers import tds_column_for_primitive

__all__: PyLegendSequence[str] = [
    "LegacyApiOlapGroupByFunction"
]


def _infer_agg_suffix(agg_result: PyLegendPrimitive) -> str:
    import re
    class_name = type(agg_result.value()).__name__
    m = re.match(
        r"^PyLegend(?:Integer|Float|Number|String|StrictDate|Date|DateTime)?(.+?)Expression$",
        class_name
    )
    return m.group(1) if m is not None else ""


def _infer_rank_column_name(result: PyLegendPrimitive) -> str:
    expr = result.value()
    if isinstance(expr, LegacyApiRankExpression):
        return "Rank"
    else:
        return "DenseRank"


class LegacyApiOlapGroupByFunction(LegacyApiAppliedFunction):
    __base_frame: LegacyApiBaseTdsFrame
    __window: LegacyApiWindow
    __operations: PyLegendList[LegacyApiOLAPGroupByOperation]
    __new_column_expressions: PyLegendList[
        PyLegendUnion[
            PyLegendTuple[str, PyLegendPrimitiveOrPythonPrimitive],
            PyLegendTuple[str, PyLegendPrimitiveOrPythonPrimitive, PyLegendPrimitive]
        ]
    ]

    @classmethod
    def name(cls) -> str:
        return "olap_group_by"

    def __init__(
            self,
            base_frame: LegacyApiBaseTdsFrame,
            column_name_list: PyLegendOptional[PyLegendList[str]],
            sort_column_list: PyLegendOptional[PyLegendList[str]],
            sort_direction_list: PyLegendOptional[PyLegendList[str]],
            operations_list: PyLegendList[LegacyApiOLAPGroupByOperation],
    ) -> None:
        self.__base_frame = base_frame
        self.__operations = operations_list

        # Build sort info list
        order_by: PyLegendOptional[PyLegendList[LegacyApiSortInfo]] = None
        if sort_column_list is not None and len(sort_column_list) > 0:
            directions = sort_direction_list if sort_direction_list else ["ASC"] * len(sort_column_list)
            order_by = [
                LegacyApiSortInfo(column=col, direction=d)
                for col, d in zip(sort_column_list, directions)
            ]

        self.__window = LegacyApiWindow(
            partition_by=column_name_list if column_name_list and len(column_name_list) > 0 else None,
            order_by=order_by,
        )

        # Evaluate each operation to produce column expressions
        col_expressions: PyLegendList[
            PyLegendUnion[
                PyLegendTuple[str, PyLegendPrimitiveOrPythonPrimitive],
                PyLegendTuple[str, PyLegendPrimitiveOrPythonPrimitive, PyLegendPrimitive]
            ]
        ] = []
        tds_row = LegacyApiTdsRow.from_tds_frame("r", self.__base_frame)
        partial_frame = LegacyApiPartialFrame(base_frame=self.__base_frame, var_name="p")

        for (i, op) in enumerate(operations_list):
            if isinstance(op, LegacyApiOLAPRank):
                try:
                    result = op.rank(partial_frame)
                except Exception as e:
                    raise RuntimeError(
                        "'olap_group_by' function operations_list argument incompatible. "
                        f"Error occurred while evaluating rank lambda at index {i} (0-indexed). "
                        f"Message: " + str(e)
                    ) from e

                if not isinstance(result, PyLegendPrimitive):
                    raise TypeError(
                        "'olap_group_by' function operations_list argument incompatible. "
                        f"Rank lambda at index {i} (0-indexed) returns non-primitive - {str(type(result))}"
                    )

                # Derive column name from the rank function type
                col_name = op.name if op.name else _infer_rank_column_name(result)
                col_expressions.append((col_name, result))

            elif isinstance(op, LegacyApiOLAPAggregation):
                # The column_name on the aggregation identifies which column to map from the row
                try:
                    map_result = tds_row[op.column_name]
                except Exception as e:
                    raise RuntimeError(
                        "'olap_group_by' function operations_list argument incompatible. "
                        f"Error occurred while accessing column '{op.column_name}' "
                        f"at index {i} (0-indexed). "
                        f"Message: " + str(e)
                    ) from e

                collection = create_primitive_collection(map_result)
                try:
                    agg_result = op.function(collection)
                except Exception as e:
                    raise RuntimeError(
                        "'olap_group_by' function operations_list argument incompatible. "
                        f"Error occurred while evaluating aggregation function at index {i} (0-indexed). "
                        f"Message: " + str(e)
                    ) from e

                if not isinstance(agg_result, PyLegendPrimitive):
                    raise TypeError(
                        "'olap_group_by' function operations_list argument incompatible. "
                        f"Aggregation function at index {i} (0-indexed) "
                        f"returns non-primitive - {str(type(agg_result))}"
                    )

                col_name = f"{op.column_name} {_infer_agg_suffix(agg_result)}"
                col_expressions.append((col_name, map_result, agg_result))
            else:
                raise TypeError(
                    "'olap_group_by' function operations_list argument incompatible. "
                    f"Operation at index {i} (0-indexed) is not a recognized "
                    "LegacyApiOLAPAggregation or LegacyApiOLAPRank"
                )

        self.__new_column_expressions = col_expressions

    def to_sql(self, config: FrameToSqlConfig) -> QuerySpecification:
        base_query = self.__base_frame.to_sql_query_object(config)
        should_create_sub_query = True
        db_extension = config.sql_to_string_generator().get_db_extension()

        new_query = (
            create_sub_query(base_query, config, "root") if should_create_sub_query else
            copy_query(base_query)
        )

        for c in self.__new_column_expressions:
            if len(c) == 2:
                rank_expr = c[1]
                assert isinstance(rank_expr, PyLegendPrimitive)
                col_sql_expr = rank_expr.to_sql_expression({"r": new_query}, config)
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
            if len(c) == 2:
                rank_val = c[1]
                assert isinstance(rank_val, PyLegendPrimitive)
                expr_str = rank_val.to_pure_expression(config)
                return f"{escaped_col_name}:{generate_pure_lambda('p', expr_str)}"
            else:
                expr_str = (c[1].to_pure_expression(config) if isinstance(c[1], PyLegendPrimitive) else
                            convert_literal_to_literal_expression(c[1]).to_pure_expression(config))
                agg_expr_str = c[2].to_pure_expression(config).replace(expr_str, "$c")
                return (f"{escaped_col_name}:"
                        f"{generate_pure_lambda('r', expr_str)}:"
                        f"{generate_pure_lambda('c', agg_expr_str)}")

        window_str = self.__window.to_pure_expression(config)

        if all([len(t) == 2 for t in self.__new_column_expressions]) or all(
                [len(t) == 3 for t in self.__new_column_expressions]):
            if len(self.__new_column_expressions) == 1:
                extend_str = f"->extend({window_str}, ~{render_single_column_expression(self.__new_column_expressions[0])})"
            else:
                extend_str = (f"->extend({window_str}, ~[{config.separator(2)}" +
                              ("," + config.separator(2, True)).join(
                                  [render_single_column_expression(x) for x in self.__new_column_expressions]
                              ) +
                              f"{config.separator(1)}])")
            return f"{self.__base_frame.to_pure(config)}{config.separator(1)}" + extend_str
        else:
            extend_str = self.__base_frame.to_pure(config)
            for c in self.__new_column_expressions:
                extend_str += f"{config.separator(1)}->extend({window_str}, ~{render_single_column_expression(c)})"
            return extend_str

    def base_frame(self) -> LegacyApiBaseTdsFrame:
        return self.__base_frame

    def tds_frame_parameters(self) -> PyLegendList["LegacyApiBaseTdsFrame"]:
        return []

    def calculate_columns(self) -> PyLegendSequence["TdsColumn"]:
        new_columns = [c.copy() for c in self.__base_frame.columns()]
        for c in self.__new_column_expressions:
            new_columns.append(tds_column_for_primitive(c[0], c[1] if len(c) == 2 else c[2]))
        return new_columns

    def validate(self) -> bool:
        if len(self.__operations) == 0:
            raise ValueError("At least one operation must be provided for olap_group_by")

        col_names = [x[0] for x in self.__new_column_expressions]

        if len(col_names) != len(set(col_names)):
            raise ValueError(f"OLAP group by column names list has duplicates: {col_names}")

        base_col_names = [c.get_name() for c in self.__base_frame.columns()]
        for c in col_names:
            if c in base_col_names:
                raise ValueError(f"OLAP group by column name - '{c}' already exists in base frame")

        # Validate partition columns exist in base frame
        if self.__window.get_partition_by() is not None:
            for c in self.__window.get_partition_by():  # type: ignore
                if c not in base_col_names:
                    raise ValueError(
                        f"Column - '{c}' in partition columns list doesn't exist in the current frame. "
                        f"Current frame columns: {base_col_names}"
                    )

        # Validate sort columns exist in base frame and sort directions are valid
        if self.__window.get_order_by() is not None:
            for sort_info in self.__window.get_order_by():  # type: ignore
                if sort_info.get_column() not in base_col_names:
                    raise ValueError(
                        f"Column - '{sort_info.get_column()}' in sort columns list doesn't exist "
                        f"in the current frame. Current frame columns: {base_col_names}"
                    )

        return True

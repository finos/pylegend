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
    PyLegendTuple,
    PyLegendCallable,
    PyLegendUnion,
)
from pylegend.core.language import PyLegendColumnExpression, PyLegendPrimitiveOrPythonPrimitive, \
    PyLegendPrimitiveCollection, PyLegendPrimitive, create_primitive_collection, convert_literal_to_literal_expression
from pylegend.core.language.legendql_api.legendql_api_custom_expressions import LegendQLApiPrimitive
from pylegend.core.language.legendql_api.legendql_api_tds_row import LegendQLApiTdsRow
from pylegend.core.tds.abstract.function_helpers import tds_column_for_primitive
from pylegend.core.tds.legendql_api.frames.legendql_api_applied_function_tds_frame import LegendQLApiAppliedFunction
from pylegend.core.tds.sql_query_helpers import copy_query, create_sub_query
from pylegend.core.sql.metamodel import (
    QuerySpecification,
    SingleColumn,
    SelectItem
)
from pylegend.core.tds.tds_column import TdsColumn
from pylegend.core.tds.tds_frame import FrameToSqlConfig
from pylegend.core.tds.tds_frame import FrameToPureConfig
from pylegend.core.tds.legendql_api.frames.legendql_api_base_tds_frame import LegendQLApiBaseTdsFrame
from pylegend.core.language.shared.helpers import escape_column_name, generate_pure_lambda

__all__: PyLegendSequence[str] = [
    "LegendQLApiGroupByFunction"
]


class LegendQLApiGroupByFunction(LegendQLApiAppliedFunction):
    __base_frame: LegendQLApiBaseTdsFrame
    __grouping_column_name_list: PyLegendList[str]
    __aggregates_list: PyLegendList[
        PyLegendTuple[
            str,
            PyLegendPrimitiveOrPythonPrimitive,
            PyLegendPrimitive
        ]
    ]

    @classmethod
    def name(cls) -> str:
        return "groupBy"

    def __init__(
            self,
            base_frame: LegendQLApiBaseTdsFrame,
            grouping_columns_function: PyLegendCallable[[LegendQLApiTdsRow], PyLegendUnion[
                LegendQLApiPrimitive,
                str,
                PyLegendList[PyLegendUnion[str, LegendQLApiPrimitive]]
            ]],
            aggregate_specifications: PyLegendUnion[
                PyLegendTuple[
                    str,
                    PyLegendCallable[[LegendQLApiTdsRow], PyLegendPrimitiveOrPythonPrimitive],
                    PyLegendCallable[[PyLegendPrimitiveCollection], PyLegendPrimitive]
                ],
                PyLegendList[
                    PyLegendTuple[
                        str,
                        PyLegendCallable[[LegendQLApiTdsRow], PyLegendPrimitiveOrPythonPrimitive],
                        PyLegendCallable[[PyLegendPrimitiveCollection], PyLegendPrimitive]
                    ]
                ]
            ]
    ) -> None:
        self.__base_frame = base_frame
        tds_row = LegendQLApiTdsRow.from_tds_frame("r", self.__base_frame)

        if (not isinstance(grouping_columns_function, type(lambda x: 0)) or
                (grouping_columns_function.__code__.co_argcount != 1)):
            raise TypeError("GroupBy grouping_columns_function should be a lambda which takes one argument (TDSRow)")

        try:
            result = grouping_columns_function(tds_row)
        except Exception as e:
            raise RuntimeError(
                "GroupBy columns lambda incompatible. Error occurred while evaluating. Message: " + str(e)
            ) from e

        list_result: PyLegendList[PyLegendUnion[str, LegendQLApiPrimitive]]
        if isinstance(result, list):
            list_result = result
        elif isinstance(result, tuple):
            list_result = list(result)
        else:
            list_result = [result]

        columns_list = []
        for (i, r) in enumerate(list_result):
            if isinstance(r, str):
                columns_list.append(r)
            elif isinstance(r, LegendQLApiPrimitive) and isinstance(r.value(), PyLegendColumnExpression):
                col_expr: PyLegendColumnExpression = r.value()
                columns_list.append(col_expr.get_column())
            else:
                raise RuntimeError(
                    "GroupBy columns lambda incompatible. Columns can either be strings (lambda r: ['c1', 'c2']) or "
                    "simple column expressions (lambda r: [r.c1, r.c2]). "
                    f"Element at index {i} in the list is incompatible."
                )
        self.__grouping_column_name_list = columns_list

        list_result_2: PyLegendList[
            PyLegendTuple[
                str,
                PyLegendCallable[[LegendQLApiTdsRow], PyLegendPrimitiveOrPythonPrimitive],
                PyLegendCallable[[PyLegendPrimitiveCollection], PyLegendPrimitive]
            ]
        ]
        if isinstance(aggregate_specifications, list):
            list_result_2 = aggregate_specifications
        else:
            list_result_2 = [aggregate_specifications]

        aggregates_list = []
        for (i, r) in enumerate(list_result_2):
            if isinstance(r, tuple) and isinstance(r[0], str):
                if not isinstance(r[1], type(lambda x: 0)) or (r[1].__code__.co_argcount != 1):
                    raise RuntimeError(
                        "GroupBy aggregate incompatible. Each element in aggregates list should be a triplet with "
                        "second element being a lambda (mapper function) which takes one argument (TDSRow). "
                        f"Element at index {i} in the list is incompatible."
                    )

                try:
                    map_result = r[1](tds_row)
                except Exception as e:
                    raise RuntimeError(
                        "GroupBy aggregate incompatible. "
                        f"Error occurred while evaluating map lambda at index {i}. Message: " + str(e)
                    ) from e

                if not isinstance(map_result, (int, float, bool, str, date, datetime, PyLegendPrimitive)):
                    raise RuntimeError(
                        "GroupBy aggregate incompatible. "
                        f"Map lambda at index {i} returns non-primitive - {str(type(map_result))}"
                    )

                collection = create_primitive_collection(map_result)

                if not isinstance(r[2], type(lambda x: 0)) or (r[2].__code__.co_argcount != 1):
                    raise RuntimeError(
                        "GroupBy aggregate incompatible. Each element in aggregates list should be a triplet with "
                        "third element being a lambda (aggregation function) which takes one argument (TDSRow). "
                        f"Element at index {i} in the list is incompatible."
                    )

                try:
                    agg_result = r[2](collection)
                except Exception as e:
                    raise RuntimeError(
                        "GroupBy aggregate incompatible. "
                        f"Error occurred while evaluating aggregation lambda at index {i}. Message: " + str(e)
                    ) from e

                if not isinstance(agg_result, PyLegendPrimitive):
                    raise RuntimeError(
                        "GroupBy aggregate incompatible. "
                        f"Aggregation lambda at index {i} returns non-primitive - {str(type(agg_result))}"
                    )

                aggregates_list.append((r[0], map_result, agg_result))

            else:
                raise RuntimeError(
                    "GroupBy aggregate incompatible. Each element in aggregates list should be a triplet with "
                    "first element being a string (aggregate column name)"
                    f"Element at index {i} in the list is incompatible."
                )
        self.__aggregates_list = aggregates_list

    def to_sql(self, config: FrameToSqlConfig) -> QuerySpecification:
        db_extension = config.sql_to_string_generator().get_db_extension()
        base_query = self.__base_frame.to_sql_query_object(config)

        should_create_sub_query = (len(base_query.groupBy) > 0) or base_query.select.distinct or \
                                  (base_query.offset is not None) or (base_query.limit is not None)

        columns_to_retain = [db_extension.quote_identifier(x) for x in self.__grouping_column_name_list]
        if should_create_sub_query:
            new_query = create_sub_query(base_query, config, "root")
        else:
            new_query = copy_query(base_query)

        new_cols_with_index: PyLegendList[PyLegendTuple[int, 'SelectItem']] = []
        for col in new_query.select.selectItems:
            if not isinstance(col, SingleColumn):
                raise ValueError("Group By operation not supported for queries "
                                 "with columns other than SingleColumn")  # pragma: no cover
            if col.alias is None:
                raise ValueError("Group By operation not supported for queries "
                                 "with SingleColumns with missing alias")  # pragma: no cover
            if col.alias in columns_to_retain:
                new_cols_with_index.append((columns_to_retain.index(col.alias), col))

        new_select_items = [y[1] for y in sorted(new_cols_with_index, key=lambda x: x[0])]

        tds_row = LegendQLApiTdsRow.from_tds_frame("r", self.__base_frame)
        for agg in self.__aggregates_list:
            agg_sql_expr = agg[2].to_sql_expression({"r": new_query}, config)

            new_select_items.append(
                SingleColumn(alias=db_extension.quote_identifier(agg[0]), expression=agg_sql_expr)
            )

        new_query.select.selectItems = new_select_items
        new_query.groupBy = [
            (lambda x: x[c])(tds_row).to_sql_expression({"r": new_query}, config)
            for c in self.__grouping_column_name_list
        ]
        return new_query

    def to_pure(self, config: FrameToPureConfig) -> str:
        group_strings = []
        for col_name in self.__grouping_column_name_list:
            group_strings.append(escape_column_name(col_name))

        agg_strings = []
        for agg in self.__aggregates_list:
            map_expr_string = (agg[1].to_pure_expression(config) if isinstance(agg[1], PyLegendPrimitive)
                               else convert_literal_to_literal_expression(agg[1]).to_pure_expression(config))
            agg_expr_string = agg[2].to_pure_expression(config).replace(map_expr_string, "$c")
            agg_strings.append(f"{escape_column_name(agg[0])}:{generate_pure_lambda('r', map_expr_string)}:"
                               f"{generate_pure_lambda('c', agg_expr_string)}")

        return (f"{self.__base_frame.to_pure(config)}{config.separator(1)}" +
                f"->groupBy({config.separator(2)}"
                f"~[{', '.join(group_strings)}],{config.separator(2, True)}"
                f"~[{', '.join(agg_strings)}]{config.separator(1)}"
                f")")

    def base_frame(self) -> LegendQLApiBaseTdsFrame:
        return self.__base_frame

    def tds_frame_parameters(self) -> PyLegendList["LegendQLApiBaseTdsFrame"]:
        return []

    def calculate_columns(self) -> PyLegendSequence["TdsColumn"]:
        base_columns = self.__base_frame.columns()
        new_columns = []
        for c in self.__grouping_column_name_list:
            for base_col in base_columns:
                if base_col.get_name() == c:
                    new_columns.append(base_col.copy())
                    break
        for agg in self.__aggregates_list:
            new_columns.append(tds_column_for_primitive(agg[0], agg[2]))
        return new_columns

    def validate(self) -> bool:
        base_columns = self.__base_frame.columns()
        for c in self.__grouping_column_name_list:
            found_col = False
            for base_col in base_columns:
                if base_col.get_name() == c:
                    found_col = True
                    break
            if not found_col:
                raise ValueError(
                    f"Column - '{c}' in group_by columns list doesn't exist in the current frame. "
                    f"Current frame columns: {[x.get_name() for x in base_columns]}"
                )

        agg_cols = [c[0] for c in self.__aggregates_list]
        new_cols = self.__grouping_column_name_list + agg_cols
        if len(new_cols) == 0:
            raise ValueError("At-least one grouping column or aggregate must be provided "
                             "when using group_by function")
        if len(new_cols) != len(set(new_cols)):
            raise ValueError("Found duplicate column names in grouping columns and aggregation columns. "
                             f"Grouping columns - {self.__grouping_column_name_list}, Aggregation columns - {agg_cols}")
        return True

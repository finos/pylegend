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

from datetime import date, datetime
from pylegend._typing import (
    PyLegendList,
    PyLegendSequence,
    PyLegendTuple,
    PyLegendCallable,
    PyLegendUnion,
)
from pylegend.core.language import (
    PyLegendPrimitiveOrPythonPrimitive,
    PyLegendPrimitiveCollection,
    PyLegendPrimitive,
    create_primitive_collection,
    convert_literal_to_literal_expression
)
from pylegend.core.language.legendql_api.legendql_api_tds_row import LegendQLApiTdsRow
from pylegend.core.tds.abstract.function_helpers import tds_column_for_primitive
from pylegend.core.tds.legendql_api.frames.legendql_api_applied_function_tds_frame import LegendQLApiAppliedFunction
from pylegend.core.tds.sql_query_helpers import copy_query, create_sub_query
from pylegend.core.sql.metamodel import (
    QuerySpecification,
    SingleColumn,
    SelectItem,
)
from pylegend.core.tds.tds_column import TdsColumn
from pylegend.core.tds.tds_frame import FrameToSqlConfig
from pylegend.core.tds.tds_frame import FrameToPureConfig
from pylegend.core.tds.legendql_api.frames.legendql_api_base_tds_frame import LegendQLApiBaseTdsFrame
from pylegend.core.language.shared.helpers import escape_column_name, generate_pure_lambda

__all__: PyLegendSequence[str] = [
    "LegendQLApiAggregateFunction"
]


class LegendQLApiAggregateFunction(LegendQLApiAppliedFunction):
    __base_frame: LegendQLApiBaseTdsFrame
    __aggregates_list: PyLegendList[PyLegendTuple[str, PyLegendPrimitiveOrPythonPrimitive, PyLegendPrimitive]]

    @classmethod
    def name(cls) -> str:
        return "aggregate"

    def __init__(
            self,
            base_frame: LegendQLApiBaseTdsFrame,
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
        list_result = (
            aggregate_specifications if isinstance(aggregate_specifications, list) else [aggregate_specifications]
        )
        aggregates_list: PyLegendList[PyLegendTuple[str, PyLegendPrimitiveOrPythonPrimitive, PyLegendPrimitive]] = []
        for (i, agg_spec) in enumerate(list_result):
            error = (
                "'aggregate' function aggregate specifications incompatible. "
                "Each aggregate specification should be a triplet with first element being the aggregation column "
                "name, second element being a mapper function (single argument lambda) and third element being the "
                "aggregation function (single argument lambda). "
                "E.g - ('count_col', lambda r: r['col1'], lambda c: c.count()). "
                f"Element at index {i} (0-indexed) is incompatible"
            )

            if isinstance(agg_spec, tuple) and isinstance(agg_spec[0], str):

                if not isinstance(agg_spec[1], type(lambda x: 0)) or (agg_spec[1].__code__.co_argcount != 1):
                    raise TypeError(error)

                try:
                    map_result = agg_spec[1](tds_row)
                except Exception as e:
                    raise RuntimeError(
                        "'aggregate' function aggregate specifications incompatible. "
                        f"Error occurred while evaluating mapper lambda in the aggregate specification at "
                        f"index {i} (0-indexed). Message: " + str(e)
                    ) from e

                if not isinstance(map_result, (int, float, bool, str, date, datetime, PyLegendPrimitive)):
                    raise TypeError(
                        "'aggregate' function aggregate specifications incompatible. "
                        f"Mapper lambda in the aggregate specification at index {i} (0-indexed) "
                        f"returns non-primitive - {str(type(map_result))}"
                    )

                collection = create_primitive_collection(map_result)

                if not isinstance(agg_spec[2], type(lambda x: 0)) or (agg_spec[2].__code__.co_argcount != 1):
                    raise TypeError(error)

                try:
                    agg_result = agg_spec[2](collection)
                except Exception as e:
                    raise RuntimeError(
                        "'aggregate' function aggregate specifications incompatible. "
                        f"Error occurred while evaluating aggregation lambda in the aggregate specification at "
                        f"index {i} (0-indexed). Message: " + str(e)
                    ) from e

                if not isinstance(agg_result, PyLegendPrimitive):
                    raise TypeError(
                        "'aggregate' function aggregate specifications incompatible. "
                        f"Aggregation lambda in the aggregate specification at index {i} (0-indexed) "
                        f"returns non-primitive - {str(type(agg_result))}"
                    )

                aggregates_list.append((agg_spec[0], map_result, agg_result))

            else:
                raise TypeError(error)

        self.__aggregates_list = aggregates_list

    def to_sql(self, config: FrameToSqlConfig) -> QuerySpecification:
        db_extension = config.sql_to_string_generator().get_db_extension()
        base_query = self.__base_frame.to_sql_query_object(config)

        should_create_sub_query = (len(base_query.groupBy) > 0) or base_query.select.distinct or \
                                  (base_query.offset is not None) or (base_query.limit is not None)

        if should_create_sub_query:
            new_query = create_sub_query(base_query, config, "root")
        else:
            new_query = copy_query(base_query)

        new_select_items: PyLegendList[SelectItem] = []
        for agg in self.__aggregates_list:
            agg_sql_expr = agg[2].to_sql_expression({"r": new_query}, config)
            new_select_items.append(
                SingleColumn(alias=db_extension.quote_identifier(agg[0]), expression=agg_sql_expr)
            )

        new_query.select.selectItems = new_select_items
        return new_query

    def to_pure(self, config: FrameToPureConfig) -> str:
        agg_strings = []
        for agg in self.__aggregates_list:
            map_expr_string = (agg[1].to_pure_expression(config) if isinstance(agg[1], PyLegendPrimitive)
                               else convert_literal_to_literal_expression(agg[1]).to_pure_expression(config))
            agg_expr_string = agg[2].to_pure_expression(config).replace(map_expr_string, "$c")
            agg_strings.append(f"{escape_column_name(agg[0])}:{generate_pure_lambda('r', map_expr_string)}:"
                               f"{generate_pure_lambda('c', agg_expr_string)}")

        return (f"{self.__base_frame.to_pure(config)}{config.separator(1)}"
                f"->aggregate({config.separator(2)}"
                f"~[{', '.join(agg_strings)}]{config.separator(1)}"
                f")")

    def base_frame(self) -> LegendQLApiBaseTdsFrame:
        return self.__base_frame

    def tds_frame_parameters(self) -> PyLegendList["LegendQLApiBaseTdsFrame"]:
        return []

    def calculate_columns(self) -> PyLegendSequence["TdsColumn"]:
        new_columns = []
        for agg in self.__aggregates_list:
            new_columns.append(tds_column_for_primitive(agg[0], agg[2]))
        return new_columns

    def validate(self) -> bool:
        agg_cols = [c[0] for c in self.__aggregates_list]
        if len(agg_cols) == 0:
            raise ValueError("At-least one aggregate specification must be provided "
                             "when using aggregate function")
        if len(agg_cols) != len(set(agg_cols)):
            raise ValueError("Found duplicate column names in aggregation columns. "
                             f"Aggregation columns - {agg_cols}")
        return True

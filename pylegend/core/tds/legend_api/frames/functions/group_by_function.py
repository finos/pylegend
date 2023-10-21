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
    PyLegendList,
    PyLegendSequence,
    PyLegendTuple,
)
from pylegend.core.tds.legend_api.frames.legend_api_applied_function_tds_frame import LegendApiAppliedFunction
from pylegend.core.tds.sql_query_helpers import copy_query, create_sub_query
from pylegend.core.sql.metamodel import (
    QuerySpecification,
    SelectItem,
    SingleColumn,
)
from pylegend.core.tds.tds_column import TdsColumn
from pylegend.core.tds.tds_frame import FrameToSqlConfig
from pylegend.core.tds.legend_api.frames.legend_api_base_tds_frame import LegendApiBaseTdsFrame
from pylegend.core.language import (
    TdsRow,
    AggregateSpecification,
    PyLegendPrimitive,
    create_primitive_collection,
    convert_literal_to_literal_expression,
)
from pylegend.core.tds.legend_api.frames.functions.function_helpers import tds_column_for_primitive


__all__: PyLegendSequence[str] = [
    "GroupByFunction"
]


class GroupByFunction(LegendApiAppliedFunction):
    __base_frame: LegendApiBaseTdsFrame
    __grouping_columns: PyLegendList[str]
    __aggregations: PyLegendList[AggregateSpecification]

    @classmethod
    def name(cls) -> str:
        return "group_by"

    def __init__(
            self,
            base_frame: LegendApiBaseTdsFrame,
            grouping_columns: PyLegendList[str],
            aggregations: PyLegendList[AggregateSpecification],
    ) -> None:
        self.__base_frame = base_frame
        self.__grouping_columns = grouping_columns
        self.__aggregations = aggregations

    def to_sql(self, config: FrameToSqlConfig) -> QuerySpecification:
        db_extension = config.sql_to_string_generator().get_db_extension()
        base_query = self.__base_frame.to_sql_query_object(config)

        should_create_sub_query = (len(base_query.groupBy) > 0) or base_query.select.distinct or \
                                  (base_query.offset is not None) or (base_query.limit is not None)

        columns_to_retain = [db_extension.quote_identifier(x) for x in self.__grouping_columns]
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

        tds_row = TdsRow.from_tds_frame("frame", self.__base_frame)
        for agg in self.__aggregations:
            map_result = agg.get_map_fn()(tds_row)
            collection = create_primitive_collection(map_result)
            agg_result = agg.get_aggregate_fn()(collection)

            if isinstance(agg_result, (bool, int, float, str)):
                agg_sql_expr = convert_literal_to_literal_expression(agg_result).to_sql_expression(
                    {"frame": new_query},
                    config
                )
            else:
                agg_sql_expr = agg_result.to_sql_expression({"frame": new_query}, config)

            new_select_items.append(
                SingleColumn(alias=db_extension.quote_identifier(agg.get_name()), expression=agg_sql_expr)
            )

        new_query.select.selectItems = new_select_items
        new_query.groupBy = [
            (lambda x: x[c])(tds_row).to_sql_expression({"frame": new_query}, config) for c in self.__grouping_columns
        ]
        return new_query

    def base_frame(self) -> LegendApiBaseTdsFrame:
        return self.__base_frame

    def tds_frame_parameters(self) -> PyLegendList["LegendApiBaseTdsFrame"]:
        return []

    def calculate_columns(self) -> PyLegendSequence["TdsColumn"]:
        new_columns = []
        base_columns = self.__base_frame.columns()
        for c in self.__grouping_columns:
            for base_col in base_columns:
                if base_col.get_name() == c:
                    new_columns.append(base_col.copy())

        tds_row = TdsRow.from_tds_frame("frame", self.__base_frame)
        for agg in self.__aggregations:
            map_result = agg.get_map_fn()(tds_row)
            collection = create_primitive_collection(map_result)
            agg_result = agg.get_aggregate_fn()(collection)
            new_columns.append(tds_column_for_primitive(agg.get_name(), agg_result))

        return new_columns

    def validate(self) -> bool:
        base_columns = self.__base_frame.columns()
        for c in self.__grouping_columns:
            found_col = False
            for base_col in base_columns:
                if base_col.get_name() == c:
                    found_col = True
                    break
            if not found_col:
                raise ValueError(
                    f"Column - '{c}' in group by columns list doesn't exist in the current frame. "
                    f"Current frame columns: {[x.get_name() for x in base_columns]}"
                )

        agg_cols = [c.get_name() for c in self.__aggregations]
        new_cols = self.__grouping_columns + agg_cols

        if len(new_cols) == 0:
            raise ValueError("At-least one grouping column or aggregate specification must be provided "
                             "when using group_by function")

        if len(new_cols) != len(set(new_cols)):
            raise ValueError("Found duplicate column names in grouping columns and aggregation columns. "
                             f"Grouping columns - {self.__grouping_columns}, Aggregation columns - {agg_cols}")

        tds_row = TdsRow.from_tds_frame("frame", self.__base_frame)
        index = 0
        for agg in self.__aggregations:
            map_fn_copy = agg.get_map_fn()  # For MyPy
            if not isinstance(map_fn_copy, type(lambda x: 0)) or (map_fn_copy.__code__.co_argcount != 1):
                raise TypeError(
                    f"AggregateSpecification at index {index} (0-indexed) incompatible. "
                    "Map function should be a lambda which takes one argument (TDSRow)"
                )
            try:
                map_result = agg.get_map_fn()(tds_row)
            except Exception as e:
                raise RuntimeError(
                    f"AggregateSpecification at index {index} (0-indexed) incompatible. "
                    f"Error occurred while evaluating map function. Message: {str(e)}"
                ) from e

            if not isinstance(map_result, (int, float, bool, str, PyLegendPrimitive)):
                raise ValueError(
                    f"AggregateSpecification at index {index} (0-indexed) incompatible. "
                    f"Map function returns non-primitive - {str(type(map_result))}"
                )

            collection = create_primitive_collection(map_result)

            agg_fn_copy = agg.get_aggregate_fn()  # For MyPy
            if not isinstance(agg_fn_copy, type(lambda x: 0)) or (agg_fn_copy.__code__.co_argcount != 1):
                raise TypeError(
                    f"AggregateSpecification at index {index} (0-indexed) incompatible. "
                    "Aggregate function should be a lambda which takes one argument (primitive collection)"
                )
            try:
                agg_result = agg.get_aggregate_fn()(collection)
            except Exception as e:
                raise RuntimeError(
                    f"AggregateSpecification at index {index} (0-indexed) incompatible. "
                    f"Error occurred while evaluating aggregate function. Message: {str(e)}"
                ) from e

            if not isinstance(agg_result, PyLegendPrimitive):
                raise ValueError(
                    f"AggregateSpecification at index {index} (0-indexed) incompatible. "
                    f"Aggregate function returns non-primitive - {str(type(agg_result))}"
                )

            index += 1

        return True

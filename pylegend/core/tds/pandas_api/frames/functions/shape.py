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

from pylegend.core.language.shared.literal_expressions import convert_literal_to_literal_expression

from pylegend.core.language.shared.primitive_collection import create_primitive_collection

from pylegend._typing import (
    PyLegendList,
    PyLegendSequence,
    PyLegendDict,
    PyLegendCallable,
    PyLegendUnion,
)
from pylegend.core.language.shared.aggregation import AggregationSpecification
from pylegend.core.tds.pandas_api.frames.pandas_api_applied_function_tds_frame import PandasApiAppliedFunction
from pylegend.core.tds.sql_query_helpers import copy_query, create_sub_query
from pylegend.core.sql.metamodel import (
    QuerySpecification,
    SingleColumn, Select,
)
from pylegend.core.tds.pandas_api.frames.pandas_api_base_tds_frame import PandasApiBaseTdsFrame
from pylegend.core.tds.tds_column import TdsColumn, PrimitiveTdsColumn
from pylegend.core.tds.tds_frame import FrameToSqlConfig, FrameToPureConfig
from pylegend.core.language import (
    LegacyApiTdsRow,
    PyLegendPrimitive,
    PyLegendInteger,
    PyLegendFloat,
    PyLegendNumber,
    PyLegendBoolean,
    PyLegendString,
)


__all__: PyLegendSequence[str] = [
    "PandasApiShapeFunction"
]

class PandasApiShapeFunction(PandasApiAppliedFunction):
    __base_frame: PandasApiBaseTdsFrame

    @classmethod
    def name(cls) -> str:
        return "shape"

    def __init__(
            self,
            base_frame: PandasApiBaseTdsFrame,
    ) -> None:
        self.__base_frame = base_frame

    def to_sql(self, config: FrameToSqlConfig) -> QuerySpecification:
        db_extension = config.sql_to_string_generator().get_db_extension()
        base_query = self.__base_frame.to_sql_query_object(config)
        should_create_sub_query = (len(base_query.groupBy) > 0) or base_query.select.distinct or base_query.where or base_query.offset or base_query.limit or (len(base_query.orderBy) > 0)


        new_query = (
            create_sub_query(base_query, config, "root") if should_create_sub_query else
            copy_query(base_query)
        )

        tds_row = LegacyApiTdsRow.from_tds_frame("frame", self.__base_frame)
        agg = AggregationSpecification(
            lambda x: 1,
            lambda y: y.count(),
            "Count"
        )
        tds_row = LegacyApiTdsRow.from_tds_frame("frame", self.__base_frame)
        map_result = agg.get_map_fn()(tds_row)
        collection = create_primitive_collection(map_result)
        agg_result = agg.get_aggregate_fn()(collection)

        if isinstance(agg_result, int):
            row_count_expr = convert_literal_to_literal_expression(agg_result).to_sql_expression(
                {"frame": base_query},
                config
            )
        else:
            row_count_expr = agg_result.to_sql_expression({"frame": base_query}, config)

        # column_count_expr = convert_literal_to_literal_expression(len(self.__base_frame.columns())).to_sql_expression(
        #     {"frame": base_query},
        #     config
        # )

        new_query.select = Select(
            selectItems=[
                SingleColumn(alias=db_extension.quote_identifier("row_count"), expression=row_count_expr),
            ],
            distinct=False
        )
        return new_query

    def to_pure(self, config: FrameToPureConfig) -> str:
        return (f"{self.__base_frame.to_pure(config)}{config.separator(1)}" + "->size()")

    def base_frame(self) -> PandasApiBaseTdsFrame:
        return self.__base_frame

    def tds_frame_parameters(self) -> PyLegendList["PandasApiBaseTdsFrame"]:
        return []

    def calculate_columns(self) -> PyLegendSequence["TdsColumn"]:
        return [c.copy() for c in self.__base_frame.columns()]

    def validate(self) -> bool:
        return True


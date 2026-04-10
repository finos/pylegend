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

from pylegend._typing import (
    PyLegendSequence,
    PyLegendTuple,
    PyLegendUnion,
    PyLegendList,
    PyLegendMapping,
)
from pylegend.core.language.pandas_api.pandas_api_aggregate_specification import (
    PyLegendAggInput,
)
from pylegend.core.language.pandas_api.pandas_api_tds_row import PandasApiTdsRow
from pylegend.core.language.shared.helpers import escape_column_name, generate_pure_lambda
from pylegend.core.language.shared.literal_expressions import convert_literal_to_literal_expression
from pylegend.core.language.shared.primitives.primitive import PyLegendPrimitive, PyLegendPrimitiveOrPythonPrimitive
from pylegend.core.sql.metamodel import (
    QuerySpecification,
    SelectItem,
    SingleColumn,
)
from pylegend.core.tds.pandas_api.frames.helpers.aggregate_helper import (
    AggregateEntry,
    build_aggregates_list,
    infer_column_from_primitive,
)
from pylegend.core.tds.pandas_api.frames.pandas_api_applied_function_tds_frame import PandasApiAppliedFunction
from pylegend.core.tds.pandas_api.frames.pandas_api_base_tds_frame import PandasApiBaseTdsFrame
from pylegend.core.tds.pandas_api.frames.pandas_api_groupby_tds_frame import PandasApiGroupbyTdsFrame
from pylegend.core.tds.sql_query_helpers import copy_query, create_sub_query
from pylegend.core.tds.tds_column import TdsColumn
from pylegend.core.tds.tds_frame import FrameToPureConfig, FrameToSqlConfig


class AggregateFunction(PandasApiAppliedFunction):
    __base_frame: PyLegendUnion[PandasApiBaseTdsFrame, PandasApiGroupbyTdsFrame]
    __func: PyLegendAggInput
    __axis: PyLegendUnion[int, str]
    __args: PyLegendSequence[PyLegendPrimitiveOrPythonPrimitive]
    __kwargs: PyLegendMapping[str, PyLegendPrimitiveOrPythonPrimitive]

    @classmethod
    def name(cls) -> str:
        return "aggregate"  # pragma: no cover

    def __init__(
        self,
        base_frame: PyLegendUnion[PandasApiBaseTdsFrame, PandasApiGroupbyTdsFrame],
        func: PyLegendAggInput,
        axis: PyLegendUnion[int, str],
        *args: PyLegendPrimitiveOrPythonPrimitive,
        **kwargs: PyLegendPrimitiveOrPythonPrimitive,
    ) -> None:
        self.__base_frame = base_frame
        self.__func = func
        self.__axis = axis
        self.__args = args
        self.__kwargs = kwargs

    @property
    def func(self) -> PyLegendAggInput:
        return self.__func

    def get_aggregates(self, frame_name: str = "r") -> PyLegendList[AggregateEntry]:
        group_col_names: PyLegendList[str] = []
        validation_columns: PyLegendList[str]
        default_broadcast_columns: PyLegendList[str]

        all_cols = [col.get_name() for col in self.base_frame().columns()]

        if isinstance(self.__base_frame, PandasApiGroupbyTdsFrame):
            group_col_names = [col.get_name() for col in self.__base_frame.get_grouping_columns()]
            group_cols_set = set(group_col_names)

            selected_cols = self.__base_frame.get_selected_columns()
            if selected_cols is not None:
                validation_columns = [col.get_name() for col in selected_cols]
                default_broadcast_columns = [col.get_name() for col in selected_cols]
            else:
                validation_columns = all_cols
                default_broadcast_columns = [c for c in all_cols if c not in group_cols_set]
        else:
            validation_columns = all_cols
            default_broadcast_columns = all_cols

        return build_aggregates_list(
            frame_name=frame_name,
            base_frame=self.base_frame(),
            func=self.__func,
            axis=self.__axis,
            args=self.__args,
            kwargs=self.__kwargs,
            group_col_names=group_col_names,
            validation_columns=validation_columns,
            default_broadcast_columns=default_broadcast_columns,
        )

    def to_sql(self, config: FrameToSqlConfig) -> QuerySpecification:
        db_extension = config.sql_to_string_generator().get_db_extension()

        base_query: QuerySpecification = self.base_frame().to_sql_query_object(config)

        should_create_sub_query = (
            len(base_query.groupBy) > 0
            or base_query.select.distinct
            or base_query.offset is not None
            or base_query.limit is not None
        )

        new_query: QuerySpecification
        if should_create_sub_query:
            new_query = create_sub_query(base_query, config, "root")
        else:
            new_query = copy_query(base_query)

        new_select_items: PyLegendList[SelectItem] = []

        if isinstance(self.__base_frame, PandasApiGroupbyTdsFrame):
            columns_to_retain: PyLegendList[str] = [
                db_extension.quote_identifier(x.get_name()) for x in self.__base_frame.get_grouping_columns()
            ]
            new_cols_with_index: PyLegendList[PyLegendTuple[int, "SelectItem"]] = []
            for col in new_query.select.selectItems:
                if not isinstance(col, SingleColumn):
                    raise ValueError(
                        "Group By operation not supported for queries " "with columns other than SingleColumn"
                    )  # pragma: no cover
                if col.alias is None:
                    raise ValueError(
                        "Group By operation not supported for queries " "with SingleColumns with missing alias"
                    )  # pragma: no cover
                if col.alias in columns_to_retain:
                    new_cols_with_index.append((columns_to_retain.index(col.alias), col))

            new_select_items = [y[1] for y in sorted(new_cols_with_index, key=lambda x: x[0])]

        aggregates_list = self.get_aggregates()
        for agg in aggregates_list:
            agg_sql_expr = agg[2].to_sql_expression({"r": new_query}, config)

            new_select_items.append(SingleColumn(alias=db_extension.quote_identifier(agg[0]), expression=agg_sql_expr))

        new_query.select.selectItems = new_select_items

        if isinstance(self.__base_frame, PandasApiGroupbyTdsFrame):
            tds_row = PandasApiTdsRow.from_tds_frame("r", self.base_frame())
            new_query.groupBy = [
                (lambda x: x[c.get_name()])(tds_row).to_sql_expression({"r": new_query}, config)
                for c in self.__base_frame.get_grouping_columns()
            ]

        return new_query

    def to_pure(self, config: FrameToPureConfig) -> str:
        aggregates_list = self.get_aggregates()
        agg_strings = []
        for agg in aggregates_list:
            map_expr_string = (
                agg[1].to_pure_expression(config)
                if isinstance(agg[1], PyLegendPrimitive)
                else convert_literal_to_literal_expression(agg[1]).to_pure_expression(config)
            )
            agg_expr_string = agg[2].to_pure_expression(config).replace(map_expr_string, "$c")
            agg_strings.append(
                f"{escape_column_name(agg[0])}:{generate_pure_lambda('r', map_expr_string)}:"
                f"{generate_pure_lambda('c', agg_expr_string)}"
            )

        if isinstance(self.__base_frame, PandasApiGroupbyTdsFrame):
            group_strings = []
            for col in self.__base_frame.get_grouping_columns():
                group_strings.append(escape_column_name(col.get_name()))

            pure_expression = (
                f"{self.base_frame().to_pure(config)}{config.separator(1)}" + f"->groupBy({config.separator(2)}"
                f"~[{', '.join(group_strings)}],{config.separator(2, True)}"
                f"~[{', '.join(agg_strings)}]{config.separator(1)}"
                f")"
            )

            return pure_expression
        else:
            return (
                f"{self.__base_frame.to_pure(config)}{config.separator(1)}"
                f"->aggregate({config.separator(2)}"
                f"~[{', '.join(agg_strings)}]{config.separator(1)}"
                f")"
            )

    def base_frame(self) -> PandasApiBaseTdsFrame:
        if isinstance(self.__base_frame, PandasApiGroupbyTdsFrame):
            return self.__base_frame.base_frame()
        else:
            return self.__base_frame

    def tds_frame_parameters(self) -> PyLegendList["PandasApiBaseTdsFrame"]:
        return []

    def calculate_columns(self) -> PyLegendSequence["TdsColumn"]:
        new_columns = []

        if isinstance(self.__base_frame, PandasApiGroupbyTdsFrame):
            base_cols_map = {c.get_name(): c for c in self.base_frame().columns()}
            for group_col in self.__base_frame.get_grouping_columns():
                group_col_name = group_col.get_name()
                if group_col_name in base_cols_map:
                    new_columns.append(base_cols_map[group_col_name].copy())

        aggregates_list = self.get_aggregates()
        for alias, _, agg_expr in aggregates_list:
            new_columns.append(infer_column_from_primitive(alias, agg_expr))

        return new_columns

    def validate(self) -> bool:
        self.get_aggregates()
        return True

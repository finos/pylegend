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
    PyLegendDict,
    PyLegendList,
    PyLegendMapping,
    PyLegendOptional,
    PyLegendSequence,
    PyLegendUnion,
    TYPE_CHECKING,
)
from pylegend.core.language.pandas_api.pandas_api_aggregate_specification import (
    PyLegendAggInput,
)
from pylegend.core.language.shared.helpers import escape_column_name, generate_pure_lambda
from pylegend.core.language.shared.literal_expressions import convert_literal_to_literal_expression
from pylegend.core.language.shared.primitives.primitive import PyLegendPrimitive, PyLegendPrimitiveOrPythonPrimitive
from pylegend.core.sql.metamodel import (
    Expression,
    IntegerLiteral,
    QualifiedName,
    QualifiedNameReference,
    QuerySpecification,
    SelectItem,
    SingleColumn,
)
from pylegend.core.sql.metamodel_extension import WindowExpression
from pylegend.core.tds.pandas_api.frames.helpers.aggregate_helper import (
    AggregateEntry,
    build_aggregates_list,
    infer_column_from_primitive,
)
from pylegend.core.tds.pandas_api.frames.pandas_api_applied_function_tds_frame import PandasApiAppliedFunction
from pylegend.core.tds.pandas_api.frames.pandas_api_base_tds_frame import PandasApiBaseTdsFrame
from pylegend.core.tds.pandas_api.frames.pandas_api_window_tds_frame import PandasApiWindowTdsFrame, ZERO_COLUMN_NAME
from pylegend.core.tds.sql_query_helpers import create_sub_query
from pylegend.core.tds.tds_column import TdsColumn
from pylegend.core.tds.tds_frame import FrameToPureConfig, FrameToSqlConfig

if TYPE_CHECKING:
    from pylegend.core.language.pandas_api.pandas_api_custom_expressions import PandasApiWindow


class WindowAggregateFunction(PandasApiAppliedFunction):
    __base_frame: PandasApiWindowTdsFrame
    __func: PyLegendAggInput
    __axis: PyLegendUnion[int, str]
    __args: PyLegendSequence[PyLegendPrimitiveOrPythonPrimitive]
    __kwargs: PyLegendMapping[str, PyLegendPrimitiveOrPythonPrimitive]

    @classmethod
    def name(cls) -> str:
        return "window_aggregate"  # pragma: no cover

    def __init__(
            self,
            base_frame: PandasApiWindowTdsFrame,
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

    # ──────────────────────────────────────────────────────────────────────
    # Helpers
    # ──────────────────────────────────────────────────────────────────────

    def _build_aggregates(self, frame_name: str = "r") -> PyLegendList[AggregateEntry]:
        base = self.base_frame()
        all_cols = [c.get_name() for c in base.columns()]
        partition_cols = self.__base_frame.get_partition_columns()
        partition_cols_set = set(partition_cols)
        default_broadcast_columns = [c for c in all_cols if c not in partition_cols_set]
        return build_aggregates_list(
            frame_name=frame_name,
            base_frame=base,
            func=self.__func,
            axis=self.__axis,
            args=self.__args,
            kwargs=self.__kwargs,
            group_col_names=partition_cols,
            validation_columns=all_cols,
            default_broadcast_columns=default_broadcast_columns,
        )

    def _resolve_order_by(self, fallback_column: PyLegendOptional[str] = None) -> PyLegendList[str]:
        """
        Resolve the ORDER BY columns for the window.
        If the window frame has an explicit order_by, use that.
        Otherwise fall back to ``fallback_column`` if provided,
        or the first column of the base frame.
        """
        if self.__base_frame._order_by is not None:
            return list(self.__base_frame._order_by)

        if fallback_column is not None:
            return [fallback_column]

        columns = [c.get_name() for c in self.base_frame().columns()]
        assert len(columns) > 0, (
            "Cannot determine ORDER BY for window aggregate: "
            "the base frame has no columns and no explicit order_by was provided."
        )
        return [columns[0]]

    def _resolved_window(
        self,
        fallback_column: PyLegendOptional[str] = None,
        include_zero_column: bool = True,
    ) -> "PandasApiWindow":
        """
        Build a PandasApiWindow with the resolved order_by baked in.
        """
        if self._is_partition_only():
            return self.__base_frame.construct_window(include_zero_column=False)
        resolved_cols = self._resolve_order_by(fallback_column)
        # Preserve the user's ascending directions when using explicit order_by;
        # fall back to all-ascending when order_by was auto-resolved.
        if self.__base_frame._order_by is not None:
            ascending = self.__base_frame._ascending
        else:
            ascending = [True] * len(resolved_cols)
        return self.__base_frame.with_order_by(
            resolved_cols, ascending
        ).construct_window(include_zero_column=include_zero_column)

    @staticmethod
    def _get_source_column_name(agg: AggregateEntry) -> str:
        """Extract the source column name from an aggregate entry's map expression."""
        from pylegend.core.language.shared.column_expressions import PyLegendColumnExpression
        map_expr = agg[1]
        if isinstance(map_expr, PyLegendColumnExpression):
            return map_expr.get_column()  # pragma: no cover
        # Fallback: use the alias (output name) which is derived from the source column
        return agg[0]

    @staticmethod
    def _render_single_column_expression(
        agg: AggregateEntry,
        temp_column_name_suffix: str,
        config: FrameToPureConfig,
    ) -> str:
        escaped_col_name = escape_column_name(agg[0] + temp_column_name_suffix)
        map_expr = (
            agg[1].to_pure_expression(config)
            if isinstance(agg[1], PyLegendPrimitive)
            else convert_literal_to_literal_expression(agg[1]).to_pure_expression(config)
        )
        agg_expr = agg[2].to_pure_expression(config).replace(map_expr, "$c")
        return (
            f"{escaped_col_name}:"
            f"{generate_pure_lambda('p,w,r', map_expr)}:"
            f"{generate_pure_lambda('c', agg_expr)}"
        )

    def _is_partition_only(self) -> bool:
        return self.__base_frame._partition_only

    def to_sql(self, config: FrameToSqlConfig) -> QuerySpecification:
        temp_column_name_suffix = "__pylegend_olap_column__"

        base_query = self.base_frame().to_sql_query_object(config)
        db_extension = config.sql_to_string_generator().get_db_extension()

        if not self._is_partition_only():
            # Add the zero column to the base query
            base_query.select.selectItems.append(
                SingleColumn(
                    alias=db_extension.quote_identifier(ZERO_COLUMN_NAME),
                    expression=IntegerLiteral(0),
                )
            )

        window = self._resolved_window()

        new_query: QuerySpecification = create_sub_query(base_query, config, "root")
        new_select_items: PyLegendList[SelectItem] = []

        aggregates_list = self._build_aggregates()
        for agg in aggregates_list:
            agg_sql_expr = agg[2].to_sql_expression({"r": new_query}, config)
            window_expr = WindowExpression(
                nested=agg_sql_expr,
                window=window.to_sql_node(new_query, config),
            )
            new_select_items.append(
                SingleColumn(
                    alias=db_extension.quote_identifier(agg[0] + temp_column_name_suffix),
                    expression=window_expr,
                )
            )

        new_query.select.selectItems = new_select_items

        # Wrap in an outer query that renames from suffix alias to final alias
        new_query = create_sub_query(new_query, config, "root")
        final_select_items: PyLegendList[SelectItem] = []
        for agg in aggregates_list:
            col_expr = QualifiedNameReference(QualifiedName([
                db_extension.quote_identifier("root"),
                db_extension.quote_identifier(agg[0] + temp_column_name_suffix),
            ]))
            final_select_items.append(
                SingleColumn(
                    alias=db_extension.quote_identifier(agg[0]),
                    expression=col_expr,
                )
            )
        new_query.select.selectItems = final_select_items

        return new_query

    def to_sql_expression(
        self,
        frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
        config: FrameToSqlConfig,
    ) -> Expression:
        aggregates_list = self._build_aggregates(frame_name="c")

        assert len(aggregates_list) == 1, (
            "to_sql_expression is only supported for single-column window aggregates"
        )

        agg = aggregates_list[0]
        source_col_name = self._get_source_column_name(agg)

        # Auto-detect: include zero column in partition only if it exists in the base query
        base_query = frame_name_to_base_query_map["c"]
        db_ext = config.sql_to_string_generator().get_db_extension()
        zero_col_alias = db_ext.quote_identifier(ZERO_COLUMN_NAME)
        has_zero_col = any(
            isinstance(si, SingleColumn) and si.alias == zero_col_alias
            for si in base_query.select.selectItems
        )
        window = self._resolved_window(fallback_column=source_col_name, include_zero_column=has_zero_col)

        agg_sql_expr = agg[2].to_sql_expression(frame_name_to_base_query_map, config)
        window_node = window.to_sql_node(frame_name_to_base_query_map["c"], config)

        return WindowExpression(nested=agg_sql_expr, window=window_node)

    def to_pure(self, config: FrameToPureConfig) -> str:
        temp_column_name_suffix = "__pylegend_olap_column__"

        window_expression = self._resolved_window().to_pure_expression(config)

        aggregates_list = self._build_aggregates()

        extend_col_expressions: PyLegendList[str] = [
            self._render_single_column_expression(agg, temp_column_name_suffix, config)
            for agg in aggregates_list
        ]
        extend_str = (
            f"->extend({window_expression}, ~[{config.separator(2)}"
            + ("," + config.separator(2, True)).join(extend_col_expressions)
            + f"{config.separator(1)}])"
        )

        project_col_expressions = [
            f"{escape_column_name(agg[0])}:p|$p.{escape_column_name(agg[0] + temp_column_name_suffix)}"
            for agg in aggregates_list
        ]
        project_str = (
            f"->project(~[{config.separator(2)}"
            + ("," + config.separator(2, True)).join(project_col_expressions)
            + f"{config.separator(1)}])"
        )

        return (
            self.base_frame().to_pure(config)
            + (config.separator(1) + f"->extend(~{escape_column_name(ZERO_COLUMN_NAME)}:{{r|0}})"
               if not self._is_partition_only() else "")
            + config.separator(1) + extend_str
            + config.separator(1) + project_str
        )

    def to_pure_expression(self, config: FrameToPureConfig) -> str:
        temp_column_name_suffix = "__pylegend_olap_column__"
        aggregates_list = self._build_aggregates()

        assert len(aggregates_list) == 1, (
            "to_pure_expression is only supported for single-column window aggregates"
        )

        agg = aggregates_list[0]
        return f"$c.{escape_column_name(agg[0] + temp_column_name_suffix)}"

    def build_pure_extend_strs(self, temp_column_name_suffix: str, config: FrameToPureConfig) -> PyLegendList[str]:
        """Build the Pure extend expression(s) for this window function.
        Shared interface with TwoColumnWindowFunction so callers can treat them uniformly."""
        result: PyLegendList[str] = []
        agg = self._build_aggregates()[0]
        source_col = self._get_source_column_name(agg)
        window_expr = self._resolved_window(fallback_column=source_col).to_pure_expression(config)
        render = self._render_single_column_expression(agg, temp_column_name_suffix, config)
        if not self._is_partition_only():
            result.append(f"->extend(~{escape_column_name(ZERO_COLUMN_NAME)}:{{r|0}})")
        result.append(f"->extend({window_expr}, ~{render})")
        return result

    def base_frame(self) -> PandasApiBaseTdsFrame:
        return self.__base_frame.base_frame()

    def tds_frame_parameters(self) -> PyLegendList["PandasApiBaseTdsFrame"]:
        return []

    def calculate_columns(self) -> PyLegendSequence["TdsColumn"]:
        aggregates_list = self._build_aggregates()
        return [
            infer_column_from_primitive(alias, agg_expr)
            for alias, _, agg_expr in aggregates_list
        ]

    def validate(self) -> bool:
        if self.__axis not in [0, "index"]:
            raise NotImplementedError(
                f"The 'axis' parameter of the aggregate function must be 0 or 'index', but got: {self.__axis}"
            )

        if len(self.__args) > 0 or len(self.__kwargs) > 0:
            raise NotImplementedError(
                "WindowAggregateFunction currently does not support additional positional "
                "or keyword arguments. Please remove extra *args/**kwargs."
            )

        # Trigger aggregate list construction to validate func input
        self._build_aggregates()
        return True

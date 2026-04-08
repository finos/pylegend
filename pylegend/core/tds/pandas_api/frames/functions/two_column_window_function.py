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
    PyLegendSequence,
    PyLegendOptional,
    TYPE_CHECKING,
)
from pylegend.core.language.pandas_api.pandas_api_custom_expressions import (
    PandasApiWindow,
)
from pylegend.core.language.shared.primitives.primitive import PyLegendPrimitive
from pylegend.core.language.shared.operations.collection_operation_expressions import (
    PyLegendCorrExpression,
    PyLegendCovarPopulationExpression,
    PyLegendCovarSampleExpression,
)
from pylegend.core.sql.metamodel import (
    Expression,
    QuerySpecification,
    SelectItem,
    SingleColumn,
    QualifiedNameReference,
    QualifiedName,
)
from pylegend.core.sql.metamodel_extension import WindowExpression
from pylegend.core.tds.pandas_api.frames.pandas_api_applied_function_tds_frame import (
    PandasApiAppliedFunction,
)
from pylegend.core.tds.pandas_api.frames.pandas_api_base_tds_frame import (
    PandasApiBaseTdsFrame,
)
from pylegend.core.language.pandas_api.pandas_api_tds_row import PandasApiTdsRow
from pylegend.core.tds.tds_frame import FrameToSqlConfig, FrameToPureConfig
from pylegend.core.tds.tds_column import TdsColumn, PrimitiveTdsColumn
from pylegend.core.tds.sql_query_helpers import create_sub_query
from pylegend.core.language.shared.helpers import generate_pure_lambda, escape_column_name

if TYPE_CHECKING:
    from pylegend.core.tds.pandas_api.frames.pandas_api_groupby_tds_frame import PandasApiGroupbyTdsFrame


_FUNC_TYPE_CONFIG = {
    "corr": {
        "expression_class": PyLegendCorrExpression,
        "pure_func": "meta::pure::functions::math::corr",
    },
    "covar_population": {
        "expression_class": PyLegendCovarPopulationExpression,
        "pure_func": "meta::pure::functions::math::covarPopulation",
    },
    "covar_sample": {
        "expression_class": PyLegendCovarSampleExpression,
        "pure_func": "meta::pure::functions::math::covarSample",
    },
}


class TwoColumnWindowFunction(PandasApiAppliedFunction):
    """Window function for two-column aggregations: corr, covarPopulation, covarSample.

    Generates SQL like ``CORR(col_a, col_b) OVER (PARTITION BY ...)`` and the
    corresponding Pure ``extend(over(...), ~col:{p,w,r | rowMapper(...)}:y | $y->corr())``.
    """

    __base_frame: "PandasApiGroupbyTdsFrame"
    __col_name_a: str
    __col_name_b: str
    __result_col_name: str
    __func_type: str
    __window: PandasApiWindow
    __expr: PyLegendPrimitive

    @classmethod
    def name(cls) -> str:
        return "two_column_window"

    def __init__(
            self,
            base_frame: "PandasApiGroupbyTdsFrame",
            col_name_a: str,
            col_name_b: str,
            result_col_name: str,
            func_type: str = "corr",
    ) -> None:
        self.__base_frame = base_frame
        self.__col_name_a = col_name_a
        self.__col_name_b = col_name_b
        self.__result_col_name = result_col_name
        self.__func_type = func_type

        if func_type not in _FUNC_TYPE_CONFIG:
            raise ValueError(
                f"Unsupported func_type '{func_type}'. "
                f"Supported types: {sorted(_FUNC_TYPE_CONFIG.keys())}"
            )

        base_columns = {c.get_name() for c in self.__base_frame.base_frame().columns()}
        if self.__col_name_a not in base_columns:
            raise ValueError(
                f"Column '{self.__col_name_a}' does not exist in the current frame. "
                f"Available columns: {sorted(base_columns)}"
            )
        if self.__col_name_b not in base_columns:
            raise ValueError(
                f"Column '{self.__col_name_b}' does not exist in the current frame. "
                f"Available columns: {sorted(base_columns)}"
            )

        partition_by: PyLegendOptional[PyLegendList[str]] = [
            col.get_name() for col in self.__base_frame.get_grouping_columns()
        ]
        self.__window = PandasApiWindow(partition_by, [], frame=None)

        self.__expr = self._build_expr("r")

    # ──────────────────────────────────────────────────────────────────────
    # Internal helpers
    # ───────────────���──────────────────────────────────────────────────────

    def _build_expr(self, frame_name: str) -> PyLegendPrimitive:
        tds_row = PandasApiTdsRow.from_tds_frame(frame_name, self.__base_frame.base_frame())
        expr_a = tds_row[self.__col_name_a]
        expr_b = tds_row[self.__col_name_b]
        from pylegend.core.language import PyLegendFloat
        expr_class = _FUNC_TYPE_CONFIG[self.__func_type]["expression_class"]
        return PyLegendFloat(expr_class(expr_a.value(), expr_b.value()))  # type: ignore

    def get_window(self) -> PandasApiWindow:
        return self.__window

    def get_expr(self) -> PyLegendPrimitive:
        return self.__expr

    def get_mapper_pure_expr(self, config: FrameToPureConfig) -> str:
        """Returns fully qualified rowMapper Pure expression for the mapper part of the 3-part lambda."""
        tds_row = PandasApiTdsRow.from_tds_frame("r", self.__base_frame.base_frame())
        expr_a_str = tds_row[self.__col_name_a].to_pure_expression(config)
        expr_b_str = tds_row[self.__col_name_b].to_pure_expression(config)
        return f"meta::pure::functions::math::mathUtility::rowMapper({expr_a_str}, {expr_b_str})"

    def get_agg_pure_expr(self) -> str:
        """Returns fully qualified aggregation Pure expression with cast to Float."""
        pure_func = _FUNC_TYPE_CONFIG[self.__func_type]["pure_func"]
        return f"$y->{pure_func}()->cast(@Float)"

    # ──────────────────────────────────────────────────────────────────────
    # Uniform interface (shared with WindowAggregateFunction)
    # ──────────────────────────────────────────────────────────────────────

    def build_pure_extend_strs(self, temp_column_name_suffix: str, config: FrameToPureConfig) -> PyLegendList[str]:
        """Build the Pure extend expression(s) for this window function.
        Shared interface with WindowAggregateFunction so callers can treat them uniformly."""
        window_expr = self.__window.to_pure_expression(config)
        mapper_pure = self.get_mapper_pure_expr(config)
        agg_pure = self.get_agg_pure_expr()
        target_col_name = self.__result_col_name + temp_column_name_suffix
        extend = (
            f"->extend({window_expr}, "
            f"~{target_col_name}:{generate_pure_lambda('p,w,r', mapper_pure)}:"
            f"{generate_pure_lambda('y', agg_pure, wrap_in_braces=False)})"
        )
        return [extend]

    # ──────────────────────────────────────────────────────────────────────
    # PandasApiAppliedFunction interface
    # ──────────────────────────────────────────────────────────────────────

    def to_sql(self, config: FrameToSqlConfig) -> QuerySpecification:
        temp_column_name_suffix = "__pylegend_olap_column__"
        base_query = self.base_frame().to_sql_query_object(config)
        db_extension = config.sql_to_string_generator().get_db_extension()

        new_query: QuerySpecification = create_sub_query(base_query, config, "root")

        col_sql_expr: Expression = self.__expr.to_sql_expression({"r": new_query}, config)
        window_expr = WindowExpression(
            nested=col_sql_expr,
            window=self.__window.to_sql_node(new_query, config),
        )
        new_select_items: list[SelectItem] = [
            SingleColumn(
                alias=db_extension.quote_identifier(self.__result_col_name + temp_column_name_suffix),
                expression=window_expr
            )
        ]
        new_query.select.selectItems = new_select_items

        new_query = create_sub_query(new_query, config, "root")
        final_select_items: list[SelectItem] = [
            SingleColumn(
                alias=db_extension.quote_identifier(self.__result_col_name),
                expression=QualifiedNameReference(QualifiedName([
                    db_extension.quote_identifier("root"),
                    db_extension.quote_identifier(self.__result_col_name + temp_column_name_suffix)
                ]))
            )
        ]
        new_query.select.selectItems = final_select_items
        return new_query

    def to_sql_expression(
            self,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        frame_name = list(frame_name_to_base_query_map.keys())[0]
        expr = self._build_expr(frame_name)
        col_sql_expr: Expression = expr.to_sql_expression(frame_name_to_base_query_map, config)
        window_expr = WindowExpression(
            nested=col_sql_expr,
            window=self.__window.to_sql_node(frame_name_to_base_query_map[frame_name], config),
        )
        return window_expr

    def to_pure(self, config: FrameToPureConfig) -> str:
        temp_column_name_suffix = "__pylegend_olap_column__"
        extend_strs = self.build_pure_extend_strs(temp_column_name_suffix, config)
        extend = config.separator(1).join(extend_strs)

        project_col = (
            f"{escape_column_name(self.__result_col_name)}:"
            f"p|$p.{escape_column_name(self.__result_col_name + temp_column_name_suffix)}"
        )
        project_str = f"->project(~[{project_col}])"

        return (
            f"{self.base_frame().to_pure(config)}{config.separator(1)}"
            f"{extend}{config.separator(1)}"
            f"{project_str}"
        )

    def to_pure_expression(self, config: FrameToPureConfig) -> str:
        temp_column_name_suffix = "__pylegend_olap_column__"
        return f"$c.{self.__result_col_name + temp_column_name_suffix}"

    def base_frame(self) -> PandasApiBaseTdsFrame:
        return self.__base_frame.base_frame()

    def tds_frame_parameters(self) -> PyLegendList["PandasApiBaseTdsFrame"]:
        return []

    def calculate_columns(self) -> PyLegendSequence["TdsColumn"]:
        return [PrimitiveTdsColumn.float_column(self.__result_col_name)]

    def validate(self) -> bool:
        return True

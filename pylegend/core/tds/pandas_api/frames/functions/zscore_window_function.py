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
    PyLegendOptional,
    PyLegendSequence,
    TYPE_CHECKING,
)
from pylegend.core.language.pandas_api.pandas_api_custom_expressions import (
    PandasApiWindow,
)
from pylegend.core.language.pandas_api.pandas_api_tds_row import PandasApiTdsRow
from pylegend.core.language.shared.helpers import escape_column_name
from pylegend.core.language.shared.operations.collection_operation_expressions import (
    PyLegendAverageExpression,
    PyLegendStdDevPopulationExpression,
)
from pylegend.core.sql.metamodel import (
    ArithmeticExpression,
    ArithmeticType,
    Expression,
    QualifiedName,
    QualifiedNameReference,
    QuerySpecification,
    SelectItem,
    SingleColumn,
)
from pylegend.core.sql.metamodel_extension import WindowExpression
from pylegend.core.tds.pandas_api.frames.pandas_api_applied_function_tds_frame import (
    PandasApiAppliedFunction,
)
from pylegend.core.tds.pandas_api.frames.pandas_api_base_tds_frame import (
    PandasApiBaseTdsFrame,
)
from pylegend.core.tds.sql_query_helpers import create_sub_query
from pylegend.core.tds.tds_column import TdsColumn, PrimitiveTdsColumn
from pylegend.core.tds.tds_frame import FrameToPureConfig, FrameToSqlConfig

if TYPE_CHECKING:
    from pylegend.core.language import PyLegendFloat, PyLegendNumber
    from pylegend.core.tds.pandas_api.frames.pandas_api_groupby_tds_frame import PandasApiGroupbyTdsFrame


class ZScoreWindowFunction(PandasApiAppliedFunction):
    """Window function for zScore: (col - AVG(col) OVER ...) / STDDEV_POP(col) OVER ...

    Generates SQL like::

        (col - AVG(col) OVER (PARTITION BY ...)) / STDDEV_POP(col) OVER (PARTITION BY ...)

    and the corresponding Pure::

        extend(over(~[grp], []), ~zScore:{p,w,r | zScore($p, $w, $r, ~col)})
    """

    __base_frame: "PandasApiGroupbyTdsFrame"
    __col_name: str
    __result_col_name: str
    __window: PandasApiWindow

    @classmethod
    def name(cls) -> str:
        return "zscore_window"

    def __init__(
            self,
            base_frame: "PandasApiGroupbyTdsFrame",
            col_name: str,
            result_col_name: str,
    ) -> None:
        self.__base_frame = base_frame
        self.__col_name = col_name
        self.__result_col_name = result_col_name

        base_columns = {c.get_name() for c in self.__base_frame.base_frame().columns()}
        if self.__col_name not in base_columns:
            raise ValueError(
                f"Column '{self.__col_name}' does not exist in the current frame. "
                f"Available columns: {sorted(base_columns)}"
            )

        partition_by: PyLegendOptional[PyLegendList[str]] = [
            col.get_name() for col in self.__base_frame.get_grouping_columns()
        ]
        self.__window = PandasApiWindow(partition_by, [], frame=None)

    # ──────────────────────────────────────────────────────────────────────
    # Internal helpers
    # ──────────────────────────────────────────────────────────────────────

    def _build_avg_expr(self, frame_name: str) -> "PyLegendFloat":
        """Build the AVG(col) expression (pre-window)."""
        tds_row = PandasApiTdsRow.from_tds_frame(frame_name, self.__base_frame.base_frame())
        col_primitive = tds_row[self.__col_name]
        from pylegend.core.language import PyLegendFloat
        avg = PyLegendFloat(PyLegendAverageExpression(col_primitive.value()))  # type: ignore
        return avg

    def _build_stddev_pop_expr(self, frame_name: str) -> "PyLegendNumber":
        """Build the STDDEV_POP(col) expression (pre-window)."""
        tds_row = PandasApiTdsRow.from_tds_frame(frame_name, self.__base_frame.base_frame())
        col_primitive = tds_row[self.__col_name]
        from pylegend.core.language import PyLegendNumber
        stddev = PyLegendNumber(PyLegendStdDevPopulationExpression(col_primitive.value()))  # type: ignore
        return stddev

    def _build_sql_zscore(
            self,
            frame_name: str,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig,
    ) -> Expression:
        """Build the full SQL expression: (col - AVG(col) OVER(...)) / STDDEV_POP(col) OVER(...)."""
        tds_row = PandasApiTdsRow.from_tds_frame(frame_name, self.__base_frame.base_frame())
        col_primitive = tds_row[self.__col_name]
        col_sql = col_primitive.to_sql_expression(frame_name_to_base_query_map, config)

        avg_primitive = self._build_avg_expr(frame_name)
        avg_sql = avg_primitive.to_sql_expression(frame_name_to_base_query_map, config)
        query = frame_name_to_base_query_map[frame_name]
        avg_window = WindowExpression(
            nested=avg_sql,
            window=self.__window.to_sql_node(query, config),
        )

        stddev_primitive = self._build_stddev_pop_expr(frame_name)
        stddev_sql = stddev_primitive.to_sql_expression(frame_name_to_base_query_map, config)
        stddev_window = WindowExpression(
            nested=stddev_sql,
            window=self.__window.to_sql_node(query, config),
        )

        # (col - AVG(col) OVER (...))
        diff = ArithmeticExpression(
            type_=ArithmeticType.SUBTRACT,
            left=col_sql,
            right=avg_window,
        )
        # (col - AVG(col) OVER (...)) / STDDEV_POP(col) OVER (...)
        zscore = ArithmeticExpression(
            type_=ArithmeticType.DIVIDE,
            left=diff,
            right=stddev_window,
        )
        return zscore

    # ──────────────────────────────────────────────────────────────────────
    # Uniform interface (shared with WindowAggregateFunction / TwoColumnWindowFunction)
    # ──────────────────────────────────────────────────────────────────────

    def build_pure_extend_strs(self, temp_column_name_suffix: str, config: FrameToPureConfig) -> PyLegendList[str]:
        """Build the Pure extend expression for zScore."""
        window_expr = self.__window.to_pure_expression(config)
        target_col_name = escape_column_name(self.__result_col_name + temp_column_name_suffix)
        col_spec = escape_column_name(self.__col_name)
        extend = (
            f"->extend({window_expr}, "
            f"~{target_col_name}:{{p,w,r | "
            f"meta::pure::functions::math::zScore($p, $w, $r, ~{col_spec})}})"
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

        zscore_sql = self._build_sql_zscore("r", {"r": new_query}, config)

        new_select_items: list[SelectItem] = [
            SingleColumn(
                alias=db_extension.quote_identifier(
                    self.__result_col_name + temp_column_name_suffix
                ),
                expression=zscore_sql,
            )
        ]
        new_query.select.selectItems = new_select_items

        # Outer query to rename
        new_query = create_sub_query(new_query, config, "root")
        final_select_items: list[SelectItem] = [
            SingleColumn(
                alias=db_extension.quote_identifier(self.__result_col_name),
                expression=QualifiedNameReference(QualifiedName([
                    db_extension.quote_identifier("root"),
                    db_extension.quote_identifier(
                        self.__result_col_name + temp_column_name_suffix
                    ),
                ])),
            )
        ]
        new_query.select.selectItems = final_select_items
        return new_query

    def to_sql_expression(
            self,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig,
    ) -> Expression:
        frame_name = list(frame_name_to_base_query_map.keys())[0]
        return self._build_sql_zscore(frame_name, frame_name_to_base_query_map, config)

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
        return f"$c.{escape_column_name(self.__result_col_name + temp_column_name_suffix)}"

    def base_frame(self) -> PandasApiBaseTdsFrame:
        return self.__base_frame.base_frame()

    def tds_frame_parameters(self) -> PyLegendList["PandasApiBaseTdsFrame"]:
        return []

    def calculate_columns(self) -> PyLegendSequence["TdsColumn"]:
        return [PrimitiveTdsColumn.float_column(self.__result_col_name)]

    def validate(self) -> bool:
        return True

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
    PyLegendUnion,
    PyLegendList,
    PyLegendSequence,
    PyLegendTuple,
    PyLegendOptional,
    PyLegendCallable,
    TYPE_CHECKING,
)
from pylegend.core.language.pandas_api.pandas_api_custom_expressions import (
    PandasApiPartialFrame,
    PandasApiSortDirection,
    PandasApiSortInfo,
    PandasApiWindow,
    PandasApiWindowReference
)
from pylegend.core.language.shared.primitives.primitive import PyLegendPrimitive
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


class RankFunction(PandasApiAppliedFunction):
    __base_frame: PyLegendUnion[PandasApiBaseTdsFrame, "PandasApiGroupbyTdsFrame"]
    __axis: PyLegendUnion[str, int]
    __method: str
    __numeric_only: bool
    __na_option: str
    __ascending: bool
    __pct: bool

    __column_expression_and_window_tuples: PyLegendList[
        PyLegendTuple[
            PyLegendTuple[str, PyLegendPrimitive],
            PandasApiWindow
        ]
    ]

    @classmethod
    def name(cls) -> str:
        return "rank"  # pragma: no cover

    def __init__(
            self,
            base_frame: PyLegendUnion[PandasApiBaseTdsFrame, "PandasApiGroupbyTdsFrame"],
            axis: PyLegendUnion[str, int],
            method: str,
            numeric_only: bool,
            na_option: str,
            ascending: bool,
            pct: bool
    ) -> None:
        self.__base_frame = base_frame
        self.__axis = axis
        self.__method = method
        self.__numeric_only = numeric_only
        self.__na_option = na_option
        self.__ascending = ascending
        self.__pct = pct

    def to_sql(self, config: FrameToSqlConfig) -> QuerySpecification:
        temp_column_name_suffix = "__INTERNAL_PYLEGEND_COLUMN__"

        base_query = self.base_frame().to_sql_query_object(config)
        db_extension = config.sql_to_string_generator().get_db_extension()

        new_query: QuerySpecification = create_sub_query(base_query, config, "root")
        new_select_items: list[SelectItem] = []

        for c, window in self.__column_expression_and_window_tuples:
            col_sql_expr: Expression = c[1].to_sql_expression({"r": new_query}, config)

            window_expr = WindowExpression(
                nested=col_sql_expr,
                window=window.to_sql_node(new_query, config),
            )
            new_select_items.append(
                SingleColumn(alias=db_extension.quote_identifier(c[0] + temp_column_name_suffix), expression=window_expr)
            )

        new_query.select.selectItems = new_select_items

        new_query = create_sub_query(new_query, config, "root")

        final_select_items: list[SelectItem] = []
        for col in self.calculate_columns():
            col_name = col.get_name()
            col_expr = QualifiedNameReference(QualifiedName([
                db_extension.quote_identifier("root"), db_extension.quote_identifier(col_name + temp_column_name_suffix)
            ]))
            final_select_items.append(
                SingleColumn(
                    alias=db_extension.quote_identifier(col_name),
                    expression=col_expr
                )
            )

        new_query.select.selectItems = final_select_items

        return new_query

    def to_sql_expression(
            self,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        self._assert_single_column_in_base_frame()

        c, window = self.__column_expression_and_window_tuples[0]
        col_sql_expr: Expression = c[1].to_sql_expression(frame_name_to_base_query_map, config)
        window_expr = WindowExpression(
            nested=col_sql_expr,
            window=window.to_sql_node(frame_name_to_base_query_map['c'], config),
        )

        return window_expr

    @staticmethod
    def _render_single_column_expression(
            c: PyLegendUnion[PyLegendTuple[str, PyLegendPrimitive]], col_name: str, config: FrameToPureConfig
    ) -> str:
        escaped_col_name: str = escape_column_name(col_name)
        expr_str: str = c[1].to_pure_expression(config)
        return f"{escaped_col_name}:{generate_pure_lambda('p,w,r', expr_str)}"

    def to_pure(self, config: FrameToPureConfig) -> str:
        temp_column_name_suffix: str = "__INTERNAL_PYLEGEND_COLUMN__"

        extend_strs: PyLegendList[str] = []
        for c, window in self.__column_expression_and_window_tuples:
            window_expression = window.to_pure_expression(config)
            col_name = c[0] + temp_column_name_suffix
            extend_strs.append(
                f"->extend({window_expression}, ~{self._render_single_column_expression(c, col_name, config)})"
            )
        extend_str = f"{config.separator(1)}".join(extend_strs)

        project_cols = [
            f"{escape_column_name(c[0])}:p|$p.{escape_column_name(c[0] + temp_column_name_suffix)}"
            for c, _ in self.__column_expression_and_window_tuples
        ]
        joined_project_cols = ("," + config.separator(2)).join(project_cols)
        project_str = (
                f"->project(~[{config.separator(2)}"
                f"{joined_project_cols}"
                f"{config.separator(1)}])"
        )

        return (
                f"{self.base_frame().to_pure(config)}{config.separator(1)}"
                f"{extend_str}{config.separator(1)}"
                f"{project_str}"
        )

    def to_pure_expression(self, config: FrameToPureConfig) -> str:
        temp_column_name_suffix = "__INTERNAL_PYLEGEND_COLUMN__"
        self._assert_single_column_in_base_frame()
        c, window = self.__column_expression_and_window_tuples[0]
        return f"$c.{c[0] + temp_column_name_suffix}"

    def base_frame(self) -> PandasApiBaseTdsFrame:
        from pylegend.core.tds.pandas_api.frames.pandas_api_groupby_tds_frame import PandasApiGroupbyTdsFrame
        if isinstance(self.__base_frame, PandasApiGroupbyTdsFrame):
            return self.__base_frame.base_frame()
        return self.__base_frame

    def tds_frame_parameters(self) -> PyLegendList["PandasApiBaseTdsFrame"]:
        return []

    def calculate_columns(self) -> PyLegendSequence["TdsColumn"]:
        from pylegend.core.tds.pandas_api.frames.pandas_api_groupby_tds_frame import PandasApiGroupbyTdsFrame

        def __validate_and_convert_column(col: TdsColumn) -> PyLegendOptional["TdsColumn"]:
            valid_column_types_for_numeric_only = ["Integer", "Float", "Number"]
            if self.__numeric_only and col.get_type() not in valid_column_types_for_numeric_only:
                return None
            if self.__pct:
                new_col = PrimitiveTdsColumn.float_column(col.get_name())
            else:
                new_col = PrimitiveTdsColumn.integer_column(col.get_name())
            return new_col

        new_columns: PyLegendList["TdsColumn"] = []
        if isinstance(self.__base_frame, PandasApiGroupbyTdsFrame):
            grouping_column_names = set([col.get_name() for col in self.__base_frame.get_grouping_columns()])
            selected_columns: PyLegendOptional[PyLegendList[TdsColumn]] = self.__base_frame.get_selected_columns()
            if selected_columns is None:
                for col in self.base_frame().columns():
                    if col.get_name() in grouping_column_names:
                        continue
                    validated_col = __validate_and_convert_column(col)
                    if validated_col is not None:
                        new_columns.append(validated_col)
            else:
                for col in selected_columns:
                    validated_col = __validate_and_convert_column(col)
                    if validated_col is not None:
                        new_columns.append(validated_col)
        else:
            for col in self.base_frame().columns():
                validated_col = __validate_and_convert_column(col)
                if validated_col is not None:
                    new_columns.append(validated_col)

        return new_columns

    def validate(self) -> bool:
        if self.__axis not in [0, "index"]:
            raise NotImplementedError(
                f"The 'axis' parameter of the rank function must be 0 or 'index', but got: axis={self.__axis!r}"
            )

        valid_methods: set[str] = {'min', 'first', 'dense'}
        if self.__method not in valid_methods:
            raise NotImplementedError(
                f"The 'method' parameter of the rank function must be one of {sorted(list(valid_methods))!r},"
                f" but got: method={self.__method!r}"
            )
        elif self.__pct is True and self.__method != 'min':
            raise NotImplementedError(
                "The 'pct=True' parameter of the rank function is only supported with method='min',"
                f" but got: method={self.__method!r}."
            )

        valid_na_options = {'bottom'}
        if self.__na_option not in valid_na_options:
            raise NotImplementedError(
                f"The 'na_option' parameter of the rank function must be one of {sorted(list(valid_na_options))!r},"
                f" but got: na_option={self.__na_option!r}"
            )

        self.__column_expression_and_window_tuples = self.construct_column_expression_and_window_tuples()

        return True

    def construct_column_expression_and_window_tuples(self) -> PyLegendList[
        PyLegendTuple[
            PyLegendTuple[str, PyLegendPrimitive],
            PandasApiWindow
        ]
    ]:
        from pylegend.core.tds.pandas_api.frames.pandas_api_groupby_tds_frame import PandasApiGroupbyTdsFrame
        column_names: list[str] = [col.get_name() for col in self.calculate_columns()]

        lambda_func: PyLegendCallable[
            [PandasApiPartialFrame, PandasApiWindowReference, PandasApiTdsRow],
            PyLegendPrimitive
        ]

        if self.__pct:
            def lambda_func(
                p: PandasApiPartialFrame,
                w: PandasApiWindowReference,
                r: PandasApiTdsRow,
            ) -> PyLegendPrimitive:
                return p.percent_rank(w, r)

        elif self.__method == 'min':
            def lambda_func(
                p: PandasApiPartialFrame,
                w: PandasApiWindowReference,
                r: PandasApiTdsRow,
            ) -> PyLegendPrimitive:
                return p.rank(w, r)

        elif self.__method == 'first':
            def lambda_func(
                p: PandasApiPartialFrame,
                w: PandasApiWindowReference,
                r: PandasApiTdsRow,
            ) -> PyLegendPrimitive:
                return p.row_number(r)

        elif self.__method == 'dense':
            def lambda_func(
                p: PandasApiPartialFrame,
                w: PandasApiWindowReference,
                r: PandasApiTdsRow,
            ) -> PyLegendPrimitive:
                return p.dense_rank(w, r)

        else:
            raise ValueError(
                f"Encountered unsupported method parameter (method={self.__method!r}) in rank function")  # pragma: no cover

        extend_columns = [(column_name, lambda_func) for column_name in column_names]

        partial_frame = PandasApiPartialFrame(base_frame=self.base_frame(), var_name="p")

        column_expression_and_window_tuples: PyLegendList[
            PyLegendTuple[
                PyLegendTuple[str, PyLegendPrimitive],
                PandasApiWindow
            ]
        ] = []

        for extend_column in extend_columns:
            current_column_name: str = extend_column[0]

            partition_by: PyLegendOptional[PyLegendList[str]] = None
            if isinstance(self.__base_frame, PandasApiGroupbyTdsFrame):
                partition_by = [col.get_name() for col in self.__base_frame.get_grouping_columns()]

            tds_row = PandasApiTdsRow.from_tds_frame("r", self.base_frame())
            sort_direction: PandasApiSortDirection
            if self.__ascending:
                sort_direction = PandasApiSortDirection.ASC
            else:
                sort_direction = PandasApiSortDirection.DESC
            order_by = PandasApiSortInfo(current_column_name, sort_direction)

            window = PandasApiWindow(partition_by, [order_by], frame=None)
            window_ref = PandasApiWindowReference(window=window, var_name="w")

            result = extend_column[1](partial_frame, window_ref, tds_row)

            column_expression: PyLegendTuple[str, PyLegendPrimitive] = (current_column_name, result)
            column_expression_and_window_tuples.append((column_expression, window))

        return column_expression_and_window_tuples

    def _assert_single_column_in_base_frame(self) -> None:
        from pylegend.core.tds.pandas_api.frames.pandas_api_groupby_tds_frame import PandasApiGroupbyTdsFrame

        if isinstance(self.__base_frame, PandasApiGroupbyTdsFrame):
            selected_columns = self.__base_frame.get_selected_columns()
            assert selected_columns is not None, "To get an SQL or a pure expression, exactly one column must be selected."
            base_frame_columns = selected_columns
        else:
            base_frame_columns = list(self.__base_frame.columns())

        assert len(base_frame_columns) == 1, (
            "To get an SQL or a pure expression, the base frame must have exactly one column, but got "
            f"{len(base_frame_columns)} columns: {[str(col) for col in base_frame_columns]}"
        )

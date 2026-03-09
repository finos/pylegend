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

import copy
from pylegend._typing import (
    PyLegendDict,
    PyLegendCallable,
    PyLegendHashable,
    PyLegendList,
    PyLegendOptional,
    PyLegendSequence,
    PyLegendTuple,
    PyLegendUnion
)
from pylegend.core.language.pandas_api.pandas_api_custom_expressions import (
    PandasApiPartialFrame,
    PandasApiSortInfo,
    PandasApiSortDirection,
    PandasApiWindow,
    PandasApiWindowReference,
)
from pylegend.core.language.pandas_api.pandas_api_tds_row import PandasApiTdsRow
from pylegend.core.language.shared.helpers import (
    escape_column_name,
    generate_pure_lambda,
)
from pylegend.core.language.shared.primitives.primitive import PyLegendPrimitive
from pylegend.core.sql.metamodel import (
    Expression,
    QuerySpecification,
    IntegerLiteral,
    SingleColumn,
    SelectItem,
)
from pylegend.core.sql.metamodel_extension import WindowExpression
from pylegend.core.tds.pandas_api.frames.pandas_api_applied_function_tds_frame import (
    PandasApiAppliedFunction,
    PandasApiAppliedFunctionTdsFrame,
)
from pylegend.core.tds.pandas_api.frames.pandas_api_base_tds_frame import PandasApiBaseTdsFrame
from pylegend.core.tds.pandas_api.frames.pandas_api_groupby_tds_frame import PandasApiGroupbyTdsFrame
from pylegend.core.tds.sql_query_helpers import create_sub_query
from pylegend.core.tds.tds_column import TdsColumn
from pylegend.core.tds.tds_frame import (
    FrameToPureConfig,
    FrameToSqlConfig,
)


class ShiftExtendFunction(PandasApiAppliedFunction):
    __base_frame: PyLegendUnion[PandasApiBaseTdsFrame, PandasApiGroupbyTdsFrame]
    __order_by: PyLegendUnion[str, PyLegendSequence[str]]
    __periods: PyLegendUnion[int, PyLegendSequence[int]]
    __freq: PyLegendOptional[PyLegendUnion[str, int]]
    __axis: PyLegendUnion[int, str]
    __fill_value: PyLegendOptional[PyLegendHashable]
    __suffix: PyLegendOptional[str]

    __column_expression_and_window_tuples: PyLegendList[
        PyLegendTuple[
            PyLegendTuple[str, PyLegendPrimitive],
            PandasApiWindow
        ]
    ]

    @classmethod
    def name(cls) -> str:
        return "shift_extend"  # pragma: no cover

    def __init__(
            self,
            base_frame: PyLegendUnion[PandasApiBaseTdsFrame, PandasApiGroupbyTdsFrame],
            order_by: PyLegendUnion[str, PyLegendSequence[str]],
            periods: PyLegendUnion[int, PyLegendSequence[int]] = 1,
            freq: PyLegendOptional[PyLegendUnion[str, int]] = None,
            axis: PyLegendUnion[int, str] = 0,
            fill_value: PyLegendOptional[PyLegendHashable] = None,
            suffix: PyLegendOptional[str] = None
    ) -> None:
        self.__base_frame = base_frame
        self.__order_by = order_by
        self.__periods = periods
        self.__freq = freq
        self.__axis = axis
        self.__fill_value = fill_value
        self.__suffix = suffix

    def to_sql(self, config: FrameToSqlConfig) -> QuerySpecification:
        temp_column_name_suffix = "__pylegend_olap_column__"
        zero_column_name = "__pylegend_zero_column__"

        base_query = self.base_frame().to_sql_query_object(config)
        db_extension = config.sql_to_string_generator().get_db_extension()

        new_select_items: list[SelectItem] = copy.copy(base_query.select.selectItems)
        if not isinstance(self.__base_frame, PandasApiGroupbyTdsFrame):
            base_query.select.selectItems.append(
                SingleColumn(alias=db_extension.quote_identifier(zero_column_name), expression=IntegerLiteral(0))
            )

        new_query = create_sub_query(base_query, config, "root")
        for c, window in self.__column_expression_and_window_tuples:
            col_sql_expr = c[1].to_sql_expression({"r": new_query}, config)
            window_expr = WindowExpression(
                nested=col_sql_expr,
                window=window.to_sql_node(new_query, config)
            )
            new_select_items.append(
                SingleColumn(
                    alias=db_extension.quote_identifier(c[0] + temp_column_name_suffix),
                    expression=window_expr
                )
            )
        new_query.select.selectItems = new_select_items
        return new_query

    @staticmethod
    def _render_single_column_expression(
            c: PyLegendTuple[str, PyLegendPrimitive], col_name: str, config: FrameToPureConfig
    ) -> str:
        escaped_col_name: str = escape_column_name(col_name)
        expr_str: str = c[1].to_pure_expression(config)
        return f"{escaped_col_name}:{generate_pure_lambda('p,w,r', expr_str)}"

    def to_pure(self, config: FrameToPureConfig) -> str:
        temp_column_name_suffix = "__pylegend_olap_column__"
        zero_column_name = "__pylegend_zero_column__"

        extend_0_column = f"->extend(~{zero_column_name}:{{r | 0}})"

        extend_exprs: PyLegendList[str] = []
        for c, window in self.__column_expression_and_window_tuples:
            window_expression: str = window.to_pure_expression(config)
            col_name = c[0] + temp_column_name_suffix
            extend_exprs.append(
                f"->extend({window_expression}, ~{self._render_single_column_expression(c, col_name, config)})"
            )
        extend_str = config.separator(1).join(extend_exprs)

        return (
            self.base_frame().to_pure(config) +
            (config.separator(1) + extend_0_column if not isinstance(self.__base_frame, PandasApiGroupbyTdsFrame) else "") +
            config.separator(1) + extend_str
        )

    def base_frame(self) -> PandasApiBaseTdsFrame:
        if isinstance(self.__base_frame, PandasApiGroupbyTdsFrame):
            return self.__base_frame.base_frame()
        return self.__base_frame

    def tds_frame_parameters(self) -> PyLegendList["PandasApiBaseTdsFrame"]:
        return []  # pragma: no cover (This frame is an intermediate step, so its intentionally bypassed)

    def calculate_columns(self) -> PyLegendSequence["TdsColumn"]:
        temp_column_name_suffix = "__pylegend_olap_column__"
        new_columns: PyLegendList["TdsColumn"] = []
        source_columns: PyLegendList["TdsColumn"]

        if isinstance(self.__base_frame, PandasApiGroupbyTdsFrame):
            grouping_column_names = set([col.get_name() for col in self.__base_frame.get_grouping_columns()])
            selected_columns: PyLegendOptional[PyLegendList[TdsColumn]] = self.__base_frame.get_selected_columns()

            if selected_columns is None:
                source_columns = []
                for col in self.base_frame().columns():
                    if col.get_name() in grouping_column_names:
                        continue
                    source_columns.append(col)
            else:
                source_columns = selected_columns
        else:
            source_columns = list(self.base_frame().columns())

        if isinstance(self.__periods, int):
            for col in source_columns:
                new_columns.append(col)
        else:
            for period in self.__periods:
                for col in source_columns:
                    suffix = self.__suffix if self.__suffix is not None else ""
                    new_name = f"{col.get_name()}{suffix}_{period}"
                    new_columns.append(col.copy_with_changed_name(new_name))

        new_columns = [col.copy_with_changed_name(col.get_name() + temp_column_name_suffix) for col in new_columns]
        return list(self.base_frame().columns()) + new_columns

    def validate(self) -> bool:
        base_frame_columns = set(column.get_name() for column in self.base_frame().columns())
        order_by_list = set(
            self.__order_by if isinstance(self.__order_by, PyLegendSequence) and not isinstance(self.__order_by, str)
            else [self.__order_by]
        )
        invalid_columns = order_by_list - base_frame_columns
        if invalid_columns:
            raise ValueError(
                f"The following columns in the 'order_by' argument are not present in the base_frame: {invalid_columns}"
            )

        valid_periods = {1, -1}
        periods_list = (
            self.__periods if isinstance(self.__periods, PyLegendSequence) and not isinstance(self.__periods, str)
            else [self.__periods]
        )
        invalid_periods = set(periods_list) - valid_periods
        if invalid_periods:
            raise NotImplementedError(
                f"The 'periods' argument of the shift function only supports these values (or a list of them): {valid_periods}"
                f"\nBut got these unsupported values: {invalid_periods}."
            )
        if len(periods_list) != len(set(periods_list)):
            raise ValueError(
                f"The 'periods' argument of the shift function cannot contain duplicate values, but got: "
                f"periods={self.__periods!r}"
            )

        if self.__freq is not None:
            raise NotImplementedError(
                f"The 'freq' argument of the shift function is not supported, but got: freq={self.__freq!r}"
            )

        if self.__axis not in [0, "index"]:
            raise NotImplementedError(
                f"The 'axis' argument of the shift function must be 0 or 'index', but got: axis={self.__axis!r}"
            )

        if self.__fill_value is not None:
            raise NotImplementedError(
                f"The 'fill_value' argument of the shift function is not supported, but got: fill_value={self.__fill_value!r}"
            )

        if self.__suffix is not None and isinstance(self.__periods, int):
            raise ValueError(
                "Cannot specify the 'suffix' argument of the shift function if the 'periods' argument is an int."
            )

        self.__column_expression_and_window_tuples = self.construct_column_expression_and_window_tuples("r")

        return True

    def construct_column_expression_and_window_tuples(self, frame_name: str) -> PyLegendList[
        PyLegendTuple[
            PyLegendTuple[str, PyLegendPrimitive],
            PandasApiWindow
        ]
    ]:
        zero_column_name = "__pylegend_zero_column__"
        column_names: list[str] = []
        if isinstance(self.__base_frame, PandasApiGroupbyTdsFrame):
            grouping_column_names = set([col.get_name() for col in self.__base_frame.get_grouping_columns()])
            selected_columns: PyLegendOptional[PyLegendList[TdsColumn]] = self.__base_frame.get_selected_columns()

            if selected_columns is None:
                for col in self.base_frame().columns():
                    if col.get_name() in grouping_column_names:
                        continue
                    column_names.append(col.get_name())
            else:
                column_names = [col.get_name() for col in selected_columns]
        else:
            column_names = [col.get_name() for col in self.base_frame().columns()]

        periods_list: PyLegendList[int] = [self.__periods] if isinstance(self.__periods, int) else list(self.__periods)

        extend_columns: PyLegendList[
            PyLegendTuple[
                str,
                PyLegendCallable[
                    [PandasApiPartialFrame, PandasApiWindowReference, PandasApiTdsRow],
                    PyLegendPrimitive
                ]
            ]
        ] = []

        for period in periods_list:
            for column_name in column_names:
                if isinstance(self.__periods, int):
                    current_col_name = column_name
                else:
                    suffix = self.__suffix if self.__suffix is not None else ""
                    current_col_name = f"{column_name}{suffix}_{period}"

                if period > 0:
                    def lambda_func(
                            p: PandasApiPartialFrame,
                            w: PandasApiWindowReference,
                            r: PandasApiTdsRow,
                            column_name: str = column_name,
                            period: int = period
                    ) -> PyLegendPrimitive:
                        return p.lag(r, period)[column_name]
                else:
                    def lambda_func(
                            p: PandasApiPartialFrame,
                            w: PandasApiWindowReference,
                            r: PandasApiTdsRow,
                            column_name: str = column_name,
                            period: int = period
                    ) -> PyLegendPrimitive:
                        return p.lead(r, -period)[column_name]

                extend_columns.append((current_col_name, lambda_func))

        tds_row = PandasApiTdsRow.from_tds_frame(frame_name, self.base_frame())
        partial_frame = PandasApiPartialFrame(base_frame=self.base_frame(), var_name="p")

        column_expression_and_window_tuples: PyLegendList[
            PyLegendTuple[
                PyLegendTuple[str, PyLegendPrimitive],
                PandasApiWindow
            ]
        ] = []

        for extend_column in extend_columns:
            current_column_name: str = extend_column[0]

            if isinstance(self.__base_frame, PandasApiGroupbyTdsFrame):
                partition_by = [col.get_name() for col in self.__base_frame.get_grouping_columns()]
            else:
                partition_by = [zero_column_name]

            order_by_list = (
                self.__order_by if isinstance(self.__order_by, PyLegendSequence) and not isinstance(self.__order_by, str)
                else [self.__order_by]
            )
            order_by = [
                PandasApiSortInfo(ordering_column, PandasApiSortDirection.ASC)
                for ordering_column in order_by_list
            ]

            window = PandasApiWindow(partition_by, order_by, frame=None)

            window_ref = PandasApiWindowReference(window=window, var_name="w")
            result: PyLegendPrimitive = extend_column[1](partial_frame, window_ref, tds_row)
            column_expression = (current_column_name, result)

            column_expression_and_window_tuples.append((column_expression, window))

        return column_expression_and_window_tuples


class ShiftFunction(PandasApiAppliedFunction):
    _shift_extended_frame: PandasApiAppliedFunctionTdsFrame

    @classmethod
    def name(cls) -> str:
        return "shift"  # pragma: no cover

    def __init__(self, shift_extended_frame: PandasApiAppliedFunctionTdsFrame) -> None:
        self._shift_extended_frame = shift_extended_frame

    def base_frame(self) -> PandasApiBaseTdsFrame:
        return self._shift_extended_frame.get_applied_function().base_frame()

    def tds_frame_parameters(self) -> PyLegendList["PandasApiBaseTdsFrame"]:
        return []

    def calculate_columns(self) -> PyLegendSequence["TdsColumn"]:
        temp_column_name_suffix = "__pylegend_olap_column__"
        suffix_removed_cols = [col.copy_with_changed_name(
                                   col.get_name().removesuffix(temp_column_name_suffix)
                               )
                               for col in self._shift_extended_frame.columns()
                               if col.get_name().endswith(temp_column_name_suffix)]
        return suffix_removed_cols

    def validate(self) -> bool:
        if not isinstance(self._shift_extended_frame.get_applied_function(), ShiftExtendFunction):  # pragma: no cover
            raise TypeError(
                "ShiftFunction can only be applied after a ShiftExtendFunction."
            )
        return True

    def _get_final_sql_expression(
            self,
            col_name: str,
            temp_column_name_suffix: str,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        sql_expr = (
            self._shift_extended_frame[col_name + temp_column_name_suffix]
            .to_sql_expression(frame_name_to_base_query_map, config)
        )
        assert isinstance(sql_expr, Expression)
        return sql_expr

    def to_sql(self, config: FrameToSqlConfig) -> QuerySpecification:
        temp_column_name_suffix = "__pylegend_olap_column__"
        base_query = self._shift_extended_frame.to_sql_query_object(config)
        final_query = create_sub_query(base_query, config, "root")

        final_select_items: PyLegendList[SelectItem] = []
        db_extension = config.sql_to_string_generator().get_db_extension()
        for col in self.calculate_columns():
            col_name = col.get_name()
            col_expr = self._get_final_sql_expression(col_name, temp_column_name_suffix, {"c": final_query}, config)
            final_select_items.append(
                SingleColumn(
                    alias=db_extension.quote_identifier(col_name),
                    expression=col_expr
                )
            )

        final_query.select.selectItems = final_select_items
        return final_query

    def _get_final_pure_expression(self, col_name: str, temp_column_name_suffix: str, config: FrameToPureConfig) -> str:
        pure_expr = (
            self._shift_extended_frame[col_name + temp_column_name_suffix]
            .to_pure_expression(config)
        )
        assert isinstance(pure_expr, str)
        return pure_expr

    def to_pure(self, config: FrameToPureConfig) -> str:
        temp_column_name_suffix = "__pylegend_olap_column__"

        project_cols: PyLegendList[str] = []
        for col in self.calculate_columns():
            col_name = col.get_name()
            col_expr = self._get_final_pure_expression(col_name, temp_column_name_suffix, config)
            project_cols.append(
                f"{escape_column_name(col_name)}:c|{col_expr}"
            )

        joined_project_cols = ("," + config.separator(2)).join(project_cols)
        project_str = (
            f"->project(~[{config.separator(2)}"
            f"{joined_project_cols}"
            f"{config.separator(1)}])"
        )

        return (
            self._shift_extended_frame.to_pure(config) +
            config.separator(1) + project_str
        )


class DiffFunction(ShiftFunction):
    @classmethod
    def name(cls) -> str:
        return "diff"  # pragma: no cover

    def __init__(self, shift_extended_frame: PandasApiAppliedFunctionTdsFrame) -> None:
        super().__init__(shift_extended_frame)

    def _get_final_sql_expression(
            self,
            col_name: str,
            temp_column_name_suffix: str,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        sql_expr = (
            (self._shift_extended_frame[col_name] - self._shift_extended_frame[col_name + temp_column_name_suffix])  # type: ignore[operator]  # noqa: E501
            .to_sql_expression(frame_name_to_base_query_map, config)
        )
        assert isinstance(sql_expr, Expression)
        return sql_expr

    def _get_final_pure_expression(self, col_name: str, temp_column_name_suffix: str, config: FrameToPureConfig) -> str:
        pure_expr = (
            (self._shift_extended_frame[col_name] - self._shift_extended_frame[col_name + temp_column_name_suffix])  # type: ignore[operator]  # noqa: E501
            .to_pure_expression(config)
        )
        assert isinstance(pure_expr, str)
        return pure_expr


class PctChangeFunction(ShiftFunction):
    @classmethod
    def name(cls) -> str:
        return "pct_change"  # pragma: no cover

    def __init__(self, shift_extended_frame: PandasApiAppliedFunctionTdsFrame) -> None:
        super().__init__(shift_extended_frame)

    def _get_final_sql_expression(
            self,
            col_name: str,
            temp_column_name_suffix: str,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        sql_expr = (
            ((self._shift_extended_frame[col_name] / self._shift_extended_frame[col_name + temp_column_name_suffix]) - 1)  # type: ignore[operator]  # noqa: E501
            .to_sql_expression(frame_name_to_base_query_map, config)
        )
        assert isinstance(sql_expr, Expression)
        return sql_expr

    def _get_final_pure_expression(self, col_name: str, temp_column_name_suffix: str, config: FrameToPureConfig) -> str:
        pure_expr = (
            ((self._shift_extended_frame[col_name] / self._shift_extended_frame[col_name + temp_column_name_suffix]) - 1)  # type: ignore[operator]  # noqa: E501
            .to_pure_expression(config)
        )
        assert isinstance(pure_expr, str)
        return pure_expr

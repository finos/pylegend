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
)
from pylegend.core.tds.pandas_api.frames.pandas_api_applied_function_tds_frame import PandasApiAppliedFunction
from pylegend.core.tds.pandas_api.frames.pandas_api_base_tds_frame import PandasApiBaseTdsFrame
from pylegend.core.tds.pandas_api.frames.pandas_api_groupby_tds_frame import PandasApiGroupbyTdsFrame
from pylegend.core.tds.tds_column import TdsColumn
from pylegend.core.tds.tds_frame import FrameToPureConfig, FrameToSqlConfig


class ShiftFunction(PandasApiAppliedFunction):
    __base_frame: PyLegendUnion[PandasApiBaseTdsFrame, PandasApiGroupbyTdsFrame]
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
        return "shift"  # pragma: no cover

    def __init__(
            self,
            base_frame: PyLegendUnion[PandasApiBaseTdsFrame, PandasApiGroupbyTdsFrame],
            periods: PyLegendUnion[int, PyLegendSequence[int]] = 1,
            freq: PyLegendOptional[PyLegendUnion[str, int]] = None,
            axis: PyLegendUnion[int, str] = 0,
            fill_value: PyLegendOptional[PyLegendHashable] = None,
            suffix: PyLegendOptional[str] = None,
    ) -> None:
        self.__base_frame = base_frame
        self.__periods = periods
        self.__freq = freq
        self.__axis = axis
        self.__fill_value = fill_value
        self.__suffix = suffix

    def to_sql(self, config: FrameToSqlConfig) -> QuerySpecification:
        raise NotImplementedError("SQL query execution is not supported for the shift function")

    def to_sql_expression(
            self,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        raise NotImplementedError("SQL query execution is not supported for the shift function")

    @staticmethod
    def _render_single_column_expression(
            c: PyLegendTuple[str, PyLegendPrimitive], col_name: str, config: FrameToPureConfig
    ) -> str:
        escaped_col_name: str = escape_column_name(col_name)
        expr_str: str = c[1].to_pure_expression(config)
        return f"{escaped_col_name}:{generate_pure_lambda('p,w,r', expr_str)}"

    def to_pure(self, config: FrameToPureConfig) -> str:
        temp_column_name_suffix = "__INTERNAL_PYLEGEND_COLUMN__"
        zero_column_name = "__INTERNAL_PYLEGEND_COLUMN__"

        extend_0_column = f"->extend(~{zero_column_name}:{{r | 0}})"

        extend_exprs: PyLegendList[str] = []
        for c, window in self.__column_expression_and_window_tuples:
            window_expression: str = window.to_pure_expression(config)
            col_name = c[0] + temp_column_name_suffix
            extend_exprs.append(
                f"->extend({window_expression}, ~{self._render_single_column_expression(c, col_name, config)})"
            )
        extend_str = config.separator(1).join(extend_exprs)

        project_cols: PyLegendList[str] = [
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
            self.base_frame().to_pure(config) +
            (config.separator(1) + extend_0_column if not isinstance(self.__base_frame, PandasApiGroupbyTdsFrame) else "") +
            config.separator(1) + extend_str +
            config.separator(1) + project_str
        )

    def to_pure_expression(self, config: FrameToPureConfig) -> str:
        temp_column_name_suffix = "__INTERNAL_PYLEGEND_COLUMN__"
        self._assert_single_column_in_base_frame()
        c, window = self.__column_expression_and_window_tuples[0]
        return f"$c.{escape_column_name(c[0] + temp_column_name_suffix)}"

    def base_frame(self) -> PandasApiBaseTdsFrame:
        if isinstance(self.__base_frame, PandasApiGroupbyTdsFrame):
            return self.__base_frame.base_frame()
        return self.__base_frame

    def tds_frame_parameters(self) -> PyLegendList["PandasApiBaseTdsFrame"]:
        return []

    def calculate_columns(self) -> PyLegendSequence["TdsColumn"]:
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

        return new_columns

    def validate(self) -> bool:
        if isinstance(self.__periods, PyLegendSequence):
            if len(self.__periods) != len(set(self.__periods)):
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
        zero_column_name = "__INTERNAL_PYLEGEND_COLUMN__"
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

            window = PandasApiWindow(partition_by, [], frame=None)

            window_ref = PandasApiWindowReference(window=window, var_name="w")
            result: PyLegendPrimitive = extend_column[1](partial_frame, window_ref, tds_row)
            column_expression = (current_column_name, result)

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

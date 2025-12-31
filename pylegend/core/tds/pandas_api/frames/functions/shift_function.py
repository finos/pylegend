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
    PyLegendUnion,
    PyLegendList,
    PyLegendSequence,
    PyLegendTuple,
    PyLegendOptional,
    PyLegendCallable,
    PyLegendHashable,
)
from pylegend.core.language.pandas_api.pandas_api_custom_expressions import (
    PandasApiDirectSortInfo,
    PandasApiPartialFrame,
    PandasApiSortDirection,
    PandasApiWindow,
    PandasApiWindowReference
)
from pylegend.core.language.shared.primitives.primitive import PyLegendPrimitive
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
from pylegend.core.tds.pandas_api.frames.pandas_api_applied_function_tds_frame import PandasApiAppliedFunction
from pylegend.core.tds.pandas_api.frames.pandas_api_base_tds_frame import PandasApiBaseTdsFrame
from pylegend.core.tds.pandas_api.frames.pandas_api_groupby_tds_frame import PandasApiGroupbyTdsFrame
from pylegend.core.language.pandas_api.pandas_api_tds_row import PandasApiTdsRow
from pylegend.core.tds.tds_frame import FrameToSqlConfig, FrameToPureConfig
from pylegend.core.tds.tds_column import TdsColumn
from pylegend.core.tds.sql_query_helpers import create_sub_query


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
    temp_column_name: str
    temp_column_name_suffix: str

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

        self.temp_column_name = "__internal_sql_column_name__"
        self.temp_column_name_suffix = "__internal_sql_column_name__"

    def to_sql(self, config: FrameToSqlConfig) -> QuerySpecification:

        base_query = self.base_frame().to_sql_query_object(config)
        db_extension = config.sql_to_string_generator().get_db_extension()

        base_query.select.selectItems.append(
            SingleColumn(alias=db_extension.quote_identifier(self.temp_column_name), expression=IntegerLiteral(0)))

        new_query: QuerySpecification = create_sub_query(base_query, config, "root")
        new_select_items: list[SelectItem] = []

        for c, window in self.__column_expression_and_window_tuples:
            col_sql_expr: Expression = c[1].to_sql_expression({"r": new_query}, config)
            window_expr = WindowExpression(
                nested=col_sql_expr,
                window=window.to_sql_node(new_query, config))

            new_select_items.append(
                SingleColumn(alias=db_extension.quote_identifier(c[0] + self.temp_column_name_suffix), expression=window_expr))

        new_query.select.selectItems = new_select_items

        new_query = create_sub_query(new_query, config, "root")

        final_select_items: list[SelectItem] = []
        for col in self.calculate_columns():
            col_name = col.get_name()
            col_expr = QualifiedNameReference(QualifiedName([
                db_extension.quote_identifier("root"),
                db_extension.quote_identifier(col_name + self.temp_column_name_suffix)]))
            final_select_items.append(
                SingleColumn(
                    alias=db_extension.quote_identifier(col_name),
                    expression=col_expr
                )
            )

        new_query.select.selectItems = final_select_items

        return new_query

    def to_pure(self, config: FrameToPureConfig) -> str:
        return f"{self.base_frame().to_pure(config)}{config.separator(1)}"

    def base_frame(self) -> PandasApiBaseTdsFrame:
        if isinstance(self.__base_frame, PandasApiGroupbyTdsFrame):
            return self.__base_frame.base_frame()
        return self.__base_frame

    def tds_frame_parameters(self) -> PyLegendList["PandasApiBaseTdsFrame"]:
        return []

    def calculate_columns(self) -> PyLegendSequence["TdsColumn"]:

        new_columns: PyLegendList["TdsColumn"] = []
        source_columns: PyLegendSequence["TdsColumn"]

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
            source_columns = self.base_frame().columns()

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

        valid_periods: PyLegendList[int] = [1, -1]
        if isinstance(self.__periods, int):
            if self.__periods not in valid_periods:
                raise NotImplementedError(
                    f"The 'periods' argument of the shift function is only supported for the values of {valid_periods}"
                    f" or a list of these, but got: periods={self.__periods!r}")
        else:
            for period in self.__periods:
                if period not in valid_periods:
                    raise NotImplementedError(
                        f"The 'periods' argument of the shift function is only supported for the values of {valid_periods}"
                        f" or a list of these, but got: periods={self.__periods!r}")
            if len(self.__periods) != len(set(self.__periods)):
                raise ValueError(
                    f"The 'periods' argument of the shift function cannot contain duplicate values, but got: "
                    f"periods={self.__periods!r}")

        if self.__freq is not None:
            raise NotImplementedError(
                f"The 'freq' argument of the shift function is not supported, but got: freq={self.__freq!r}")

        if self.__axis not in [0, "index"]:
            raise NotImplementedError(
                f"The 'axis' argument of the shift function must be 0 or 'index', but got: axis={self.__axis!r}")

        if self.__fill_value is not None:
            raise NotImplementedError(
                f"The 'fill_value' argument of the shift function is not supported, but got: fill_value={self.__fill_value!r}")

        if self.__suffix is not None and isinstance(self.__periods, int):
            raise ValueError(
                "Cannot specify the 'suffix' argument of the shift function if the 'periods' argument is an int.")

        self.__column_expression_and_window_tuples = self.__construct_column_expression_and_window_tuples()

        return True

    def __construct_column_expression_and_window_tuples(self) -> PyLegendList[
        PyLegendTuple[
            PyLegendTuple[str, PyLegendPrimitive],
            PandasApiWindow
        ]
    ]:
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

        tds_row = PandasApiTdsRow.from_tds_frame("r", self.base_frame())
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

            order_by = PandasApiDirectSortInfo(self.temp_column_name, PandasApiSortDirection.ASC)

            window = PandasApiWindow(partition_by, [order_by], frame=None)

            window_ref = PandasApiWindowReference(window=window, var_name="w")
            result: PyLegendPrimitive = extend_column[1](partial_frame, window_ref, tds_row)
            column_expression = (current_column_name, result)

            column_expression_and_window_tuples.append((column_expression, window))

        return column_expression_and_window_tuples

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
import datetime
from typing import Type
from pylegend._typing import (
    PyLegendUnion,
    PyLegendList,
    PyLegendSequence,
    PyLegendTuple,
    PyLegendOptional,
    PyLegendCallable,
    PyLegendHashable,
    PyLegendDict,
)
from pylegend.core.language import PyLegendColumnExpression
from pylegend.core.language.pandas_api.pandas_api_custom_expressions import (
    PandasApiDirectSortInfo,
    PandasApiPartialFrame,
    PandasApiSortDirection,
    PandasApiSortInfo,
    PandasApiWindow,
    PandasApiWindowReference
)
from pylegend.core.language.shared.literal_expressions import convert_literal_to_literal_expression
from pylegend.core.language.shared.primitives.primitive import PyLegendPrimitive
from pylegend.core.sql.metamodel import (
    Expression,
    IntegerLiteral,
    IsNullPredicate,
    NullLiteral,
    QualifiedName,
    QualifiedNameReference,
    QuerySpecification,
    SearchedCaseExpression,
    SelectItem,
    SingleColumn,
    WhenClause,
)
from pylegend.core.sql.metamodel_extension import WindowExpression
from pylegend.core.tds.pandas_api.frames.pandas_api_applied_function_tds_frame import PandasApiAppliedFunction
from pylegend.core.tds.pandas_api.frames.pandas_api_base_tds_frame import PandasApiBaseTdsFrame
from pylegend.core.tds.pandas_api.frames.pandas_api_groupby_tds_frame import PandasApiGroupbyTdsFrame
from pylegend.core.language.pandas_api.pandas_api_tds_row import PandasApiTdsRow
from pylegend.core.tds.tds_frame import FrameToSqlConfig, FrameToPureConfig
from pylegend.core.tds.tds_column import TdsColumn, PrimitiveTdsColumn
from pylegend.core.tds.sql_query_helpers import create_sub_query
from pylegend.core.language.shared.helpers import generate_pure_lambda, escape_column_name


class ShiftFunction(PandasApiAppliedFunction):
    __base_frame: PyLegendUnion[PandasApiBaseTdsFrame, PandasApiGroupbyTdsFrame]
    __axis: PyLegendUnion[str, int]
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

    def to_sql(self, config: FrameToSqlConfig) -> QuerySpecification:

        base_query = self.base_frame().to_sql_query_object(config)
        db_extension = config.sql_to_string_generator().get_db_extension()

        base_query.select.selectItems.append(
            SingleColumn(alias=self.temp_column_name, expression=IntegerLiteral(0)))

        new_query: QuerySpecification = create_sub_query(base_query, config, "root")

        new_select_items: list[SelectItem] = []

        for c, window in self.__column_expression_and_window_tuples:
            col_sql_expr: Expression = c[1].to_sql_expression({"r": new_query}, config)

            window_expr: Expression
            window_expr = WindowExpression(
                nested=col_sql_expr,
                window=window.to_sql_node(new_query, config))

            new_select_items.append(
                SingleColumn(alias=db_extension.quote_identifier(self.temp_column_name), expression=window_expr))

        new_query.select.selectItems = new_select_items

        return new_query

    def to_pure(self, config: FrameToPureConfig) -> str:
        temp_column_name_suffix: str = "__internal_pure_col_name__"

        def render_single_column_expression(
                c: PyLegendUnion[
                    PyLegendTuple[str, PyLegendPrimitive],
                    PyLegendTuple[str, PyLegendPrimitive, PyLegendPrimitive]
                ]
        ) -> str:
            escaped_col_name: str = escape_column_name(
                c[0] + temp_column_name_suffix)
            expr_str: str = (c[1].to_pure_expression(config) if isinstance(c[1], PyLegendPrimitive) else
                             convert_literal_to_literal_expression(c[1]).to_pure_expression(config))
            if self.__na_option == 'keep':  # pragma: no cover
                escaped_column_name: str = escape_column_name(c[0])
                expr_str = f"if($r.{escaped_column_name}->isEmpty(), | [], | {expr_str})"
            return f"{escaped_col_name}:{generate_pure_lambda('p,w,r', expr_str)}"

        extend_strs: PyLegendList[str] = []
        for c, window in self.__column_expression_and_window_tuples:
            window_expression: str = window.to_pure_expression(config)
            extend_strs.append(
                f"->extend({window_expression}, ~{render_single_column_expression(c)})")
        extend_str: str = f"{config.separator(1)}".join(extend_strs)

        project_str: str = "->project(~[" + \
            ", ".join([f"{escape_column_name(c[0])}:p|$p.{escape_column_name(c[0] + temp_column_name_suffix)}"
                       for c, _ in self.__column_expression_and_window_tuples]) + \
            "])"

        return f"{self.base_frame().to_pure(config)}{config.separator(1)}" + \
            f"{extend_str}{config.separator(1)}{project_str}"

    def base_frame(self) -> PandasApiBaseTdsFrame:
        if isinstance(self.__base_frame, PandasApiGroupbyTdsFrame):
            return self.__base_frame.base_frame()
        return self.__base_frame

    def tds_frame_parameters(self) -> PyLegendList["PandasApiBaseTdsFrame"]:
        return []

    def calculate_columns(self) -> PyLegendSequence["TdsColumn"]:

        new_columns: PyLegendList["TdsColumn"] = []
        if isinstance(self.__base_frame, PandasApiGroupbyTdsFrame):
            pass
        else:
            for col in self.base_frame().columns():
                new_columns.append(col)
        return new_columns

    def validate(self) -> bool:
        if self.__axis not in [0, "index"]:
            raise NotImplementedError(
                f"The 'axis' argument of the shift function must be 0 or 'index', but got: axis={self.__axis!r}")

        if self.__freq is not None:
            raise NotImplementedError(
                f"The 'freq' argument of the shift function is not supported, but got: freq={self.__freq!r}")

        if self.__suffix is not None and isinstance(self.__periods, int):
            raise ValueError(
                "Cannot specify the 'suffix' argument of the shift function if the 'periods' argument is an int.")

        if self.__fill_value is not None:
            valid_fill_value_mapping: PyLegendDict[str, PyLegendList[Type]] = {
                "Boolean": [bool],
                "StrictDate": [date],
                "Number": [int, float],
                "String": [str],
                "LatestDate": [date],
                "Float": [float, int],
                "DateTime": [datetime],
                "Date": [date],
                "Integer": [int],
                "Decimal": [float]
            }

            mismatched_columns = []
            fill_value_type = type(self.__fill_value)

            for tds_column in self.calculate_columns():
                col_type_name = tds_column.get_type()
                valid_types_for_col = valid_fill_value_mapping.get(col_type_name, [])
                if fill_value_type not in valid_types_for_col:
                    mismatched_columns.append(str(tds_column))

            if mismatched_columns:
                raise NotImplementedError(
                    "Invalid 'fill_value' argument for the shift function. "
                    f"fill_value argument: {self.__fill_value} (type: {fill_value_type.__name__}) "
                    f"cannot be applied to the following column(s): {mismatched_columns} "
                    "because of type mismatch."
                )

        self.__column_expression_and_window_tuples = self.construct_column_expression_and_window_tuples()

        return True

    def construct_column_expression_and_window_tuples(self) -> PyLegendList[
        PyLegendTuple[
            PyLegendTuple[str, PyLegendPrimitive],
            PandasApiWindow
        ]
    ]:
        column_names: list[str] = [col.get_name() for col in self.calculate_columns()]

        lambda_func: PyLegendCallable[
            [PandasApiPartialFrame, PandasApiWindowReference, PandasApiTdsRow],
            PyLegendPrimitive
        ]

        extend_columns: PyLegendList[
            PyLegendTuple[
                str,
                PyLegendCallable[
                    [PandasApiPartialFrame, PandasApiWindowReference, PandasApiTdsRow],
                    PyLegendPrimitive
                ]
            ]
        ] = []

        for column_name in column_names:
            if self.__periods > 0:
                def lambda_func(
                        p: PandasApiPartialFrame,
                        w: PandasApiWindowReference,
                        r: PandasApiTdsRow,
                ) -> PyLegendPrimitive:
                    return p.lag(r)[column_name]
            else:
                def lambda_func(
                        p: PandasApiPartialFrame,
                        w: PandasApiWindowReference,
                        r: PandasApiTdsRow,
                ) -> PyLegendPrimitive:
                    return p.lead(r)[column_name]

            extend_columns.append((column_name, lambda_func))

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

            order_by = PandasApiDirectSortInfo(self.temp_column_name, PandasApiSortDirection.ASC)

            window = PandasApiWindow(partition_by, [order_by], frame=None)

            window_ref = PandasApiWindowReference(window=window, var_name="w")
            result: PyLegendPrimitive = extend_column[1](partial_frame, window_ref, tds_row)
            column_expression = (current_column_name, result)

            column_expression_and_window_tuples.append((column_expression, window))

        return column_expression_and_window_tuples

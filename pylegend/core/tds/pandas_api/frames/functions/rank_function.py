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
from pylegend._typing import (
    PyLegendUnion,
    PyLegendList,
    PyLegendSequence,
    PyLegendTuple,
    PyLegendOptional,
    PyLegendCallable,
)
from pylegend.core.language import PyLegendColumnExpression
from pylegend.core.language.pandas_api.pandas_api_custom_expressions import PandasApiPartialFrame, PandasApiSortDirection, PandasApiSortInfo, PandasApiWindow, PandasApiWindowReference
from pylegend.core.language.shared.literal_expressions import convert_literal_to_literal_expression
from pylegend.core.language.shared.primitive_collection import PyLegendPrimitiveCollection, create_primitive_collection
from pylegend.core.language.shared.primitives.primitive import PyLegendPrimitive, PyLegendPrimitiveOrPythonPrimitive
from pylegend.core.sql.metamodel import (
    IsNullPredicate,
    NullLiteral,
    QuerySpecification,
    SearchedCaseExpression,
    SingleColumn,
    WhenClause,
)
from pylegend.core.sql.metamodel_extension import WindowExpression
from pylegend.core.tds.pandas_api.frames.pandas_api_applied_function_tds_frame import (
    PandasApiAppliedFunction,
)
from pylegend.core.tds.pandas_api.frames.pandas_api_base_tds_frame import (
    PandasApiBaseTdsFrame,
)
from pylegend.core.tds.pandas_api.frames.pandas_api_groupby_tds_frame import (
    PandasApiGroupbyTdsFrame,
)
from pylegend.core.language.pandas_api.pandas_api_tds_row import PandasApiTdsRow
from pylegend.core.tds.tds_frame import FrameToSqlConfig, FrameToPureConfig
from pylegend.core.tds.tds_column import TdsColumn, PrimitiveTdsColumn
from pylegend.core.tds.sql_query_helpers import copy_query, create_sub_query


class RankFunction(PandasApiAppliedFunction):
    __base_frame: PyLegendUnion[PandasApiBaseTdsFrame, PandasApiGroupbyTdsFrame]
    __axis: PyLegendUnion[str, int]
    __method: str
    __numeric_only: bool
    __na_option: str
    __ascending: bool
    __pct: bool

    __window: PandasApiWindow
    __new_column_expressions: PyLegendList[
        PyLegendUnion[
            PyLegendTuple[str, PyLegendPrimitiveOrPythonPrimitive],
            PyLegendTuple[str, PyLegendPrimitiveOrPythonPrimitive, PyLegendPrimitive]
        ]
    ]

    @classmethod
    def name(cls) -> str:
        return "rank"  # pragma: no cover

    def __init__(
            self,
            base_frame: PyLegendUnion[PandasApiBaseTdsFrame, PandasApiGroupbyTdsFrame],
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
        base_query = self.base_frame().to_sql_query_object(config)
        should_create_sub_query = True
        db_extension = config.sql_to_string_generator().get_db_extension()

        new_query = (
            create_sub_query(base_query, config, "root") if should_create_sub_query else
            copy_query(base_query)
        )

        new_select_items: list[SingleColumn] = []

        for c in self.__new_column_expressions:
            if len(c) == 2:
                if isinstance(c[1], (bool, int, float, str, date, datetime)):
                    col_sql_expr = convert_literal_to_literal_expression(c[1]).to_sql_expression(
                        {"r": new_query},
                        config
                    )
                else:
                    col_sql_expr = c[1].to_sql_expression({"r": new_query}, config)
                window_expr = WindowExpression(
                    nested=col_sql_expr,
                    window=self.__window.to_sql_node(new_query, config),
                )
                tds_row = PandasApiTdsRow.from_tds_frame("root", self.base_frame())
                print(f'self.__na_option = {self.__na_option}')
                if self.__na_option == 'keep':
                    curr_col_expr = (lambda x: x[c[0]])(tds_row).to_sql_expression({"root": new_query}, config)
                    window_expr = SearchedCaseExpression(
                        whenClauses=[
                            WhenClause(
                                operand=IsNullPredicate(curr_col_expr),
                                result=NullLiteral()
                            )
                        ],
                        defaultValue=window_expr
                    )
                new_select_items.append(
                    SingleColumn(alias=db_extension.quote_identifier(c[0]), expression=window_expr))

            else:
                agg_sql_expr = c[2].to_sql_expression({"r": new_query}, config)
                window_expr = WindowExpression(
                    nested=agg_sql_expr,
                    window=self.__window.to_sql_node(new_query, config),
                )
                new_select_items.append(
                    SingleColumn(alias=db_extension.quote_identifier(c[0]), expression=window_expr))

        new_query.select.selectItems = new_select_items
        return new_query

    def to_pure(self, config: FrameToPureConfig) -> str:
        return f"{self.__base_frame.to_pure(config)}->rank(method='{self.__method}', pct={self.__pct})"

    def base_frame(self) -> PandasApiBaseTdsFrame:
        if isinstance(self.__base_frame, PandasApiGroupbyTdsFrame):
            return self.__base_frame.base_frame()
        return self.__base_frame

    def tds_frame_parameters(self) -> PyLegendList["PandasApiBaseTdsFrame"]:
        return []

    def calculate_columns(self) -> PyLegendSequence["TdsColumn"]:

        def __validate_and_convert_column(col: TdsColumn) -> PyLegendOptional["TdsColumn"]:
            if self.__numeric_only and col.get_type() not in ["Integer", "Float", "Number"]:
                return None

            if self.__pct:
                new_col = PrimitiveTdsColumn.float_column(col.get_name())
            else:
                new_col = PrimitiveTdsColumn.integer_column(col.get_name())
            return new_col

        new_columns: PyLegendSequence["TdsColumn"] = []
        if isinstance(self.__base_frame, PandasApiGroupbyTdsFrame):
            if self.__base_frame.selected_columns() is None:
                new_columns = [__validate_and_convert_column(col) for col in self.base_frame().columns()]
            else:
                new_columns = [__validate_and_convert_column(col) for col in self.__base_frame.selected_columns()]
        else:
            new_columns = [__validate_and_convert_column(col) for col in self.base_frame().columns()]
        return [col for col in new_columns if col is not None]

    def validate(self) -> bool:
        if self.__axis not in [0, "index"]:
            raise NotImplementedError(
                f"The 'axis' parameter of the rank function must be 0 or 'index', but got: axis={self.__axis}")

        valid_methods: set[str] = {'min', 'first', 'dense'}
        if self.__method not in valid_methods:
            raise NotImplementedError(
                f"The 'method' parameter of the rank function must be one of {list(valid_methods)},"
                f" but got: method={self.__method}")
        elif self.__pct is True and self.__method != 'min':
            raise NotImplementedError(
                "The 'pct=True' parameter of the rank function is only supported with method='min',"
                f" but got: method={self.__method!r}.")

        valid_na_options = {'keep', 'top', 'bottom'}
        if self.__na_option not in valid_na_options:
            raise ValueError(
                f"The 'na_option' parameter of the rank function must be one of {valid_na_options},"
                f" but got: na_option={self.__na_option}")

        self.__window = self.__calculate_window()
        self.__new_column_expressions = self.__calculate_new_column_expressions()

        return True

    def __calculate_window(self) -> PandasApiWindow:
        partition_by: PyLegendOptional[PyLegendList[str]] = None
        if isinstance(self.__base_frame, PandasApiGroupbyTdsFrame):
            partition_by = self.__base_frame.grouping_column_name_list()

        order_by: PyLegendOptional[PyLegendList[PandasApiSortInfo]] = None
        if isinstance(self.__base_frame, PandasApiGroupbyTdsFrame):
            pass
        else:
            tds_row = PandasApiTdsRow.from_tds_frame("r", self.__base_frame)

            order_by = []
            for col in self.__base_frame.columns():
                col_expr: PyLegendColumnExpression = tds_row[col.get_name()].value()
                sort_direction: PandasApiSortDirection = PandasApiSortDirection.ASC if self.__ascending \
                                                            else PandasApiSortDirection.DESC
                order_by.append(PandasApiSortInfo(col_expr, sort_direction))

        return PandasApiWindow(partition_by, order_by, frame=None)

    def __calculate_new_column_expressions(self) -> PyLegendList[
        PyLegendUnion[
            PyLegendTuple[str, PyLegendPrimitiveOrPythonPrimitive],
            PyLegendTuple[str, PyLegendPrimitiveOrPythonPrimitive, PyLegendPrimitive]
        ]
    ]:
        tds_row = PandasApiTdsRow.from_tds_frame("r", self.__base_frame)
        partial_frame = PandasApiPartialFrame(base_frame=self.__base_frame, var_name="p")
        window_ref = PandasApiWindowReference(window=self.__window, var_name="w")

        extend_columns: PyLegendList[
            PyLegendUnion[
                PyLegendTuple[
                    str,
                    PyLegendCallable[
                        [PandasApiPartialFrame, PandasApiWindowReference, PandasApiTdsRow],
                        PyLegendPrimitiveOrPythonPrimitive
                    ]
                ],
                PyLegendTuple[
                    str,
                    PyLegendCallable[
                        [PandasApiPartialFrame, PandasApiWindowReference, PandasApiTdsRow],
                        PyLegendPrimitiveOrPythonPrimitive
                    ],
                    PyLegendCallable[[PyLegendPrimitiveCollection], PyLegendPrimitive]
                ]
            ]
        ] = self.__get_extend_columns()

        col_expressions: PyLegendList[
            PyLegendUnion[
                PyLegendTuple[str, PyLegendPrimitiveOrPythonPrimitive],
                PyLegendTuple[str, PyLegendPrimitiveOrPythonPrimitive, PyLegendPrimitive]
            ]
        ] = []

        for (i, extend_column) in enumerate(extend_columns):
            result = extend_column[1](partial_frame, window_ref, tds_row)
            if len(extend_column) == 2:
                col_expressions.append((extend_column[0], result))
            else:
                collection = create_primitive_collection(result)
                agg_result = extend_column[2](collection)
                col_expressions.append((extend_column[0], result, agg_result))

        return col_expressions

    def __get_extend_columns(self):

        column_names: list[str] = [col.get_name() for col in self.calculate_columns()]

        lambda_func: PyLegendCallable[
            [PandasApiPartialFrame, PandasApiWindowReference, PandasApiTdsRow],
            PyLegendPrimitiveOrPythonPrimitive
        ]

        if self.__method == 'min':
            lambda_func = lambda p,w,r: p.rank(w,r)
        elif self.__method == 'first':
            lambda_func = lambda p,w,r: p.row_number(r)
        elif self.__method == 'dense':
            lambda_func = lambda p,w,r: p.dense_rank(w,r)

        if self.__pct == True:
            lambda_func = lambda p,w,r: p.percent_rank(w,r)

        return [
            (column_name, lambda_func)
            for column_name in column_names
        ]

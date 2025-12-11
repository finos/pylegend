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
)
from pylegend.core.sql.metamodel import (
    QuerySpecification,
    SingleColumn,
    Window,
    SortItem,
    SortItemOrdering,
    SortItemNullOrdering,
    FunctionCall,
    QualifiedName,
    SearchedCaseExpression,
    WhenClause,
    IsNullPredicate,
    NullLiteral,
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
from pylegend.core.tds.sql_query_helpers import create_sub_query


class RankFunction(PandasApiAppliedFunction):
    __base_frame: PyLegendUnion[PandasApiBaseTdsFrame, PandasApiGroupbyTdsFrame]
    __axis: PyLegendUnion[str, int]
    __method: str
    __numeric_only: bool
    __na_option: str
    __ascending: bool
    __pct: bool

    @classmethod
    def name(cls) -> str:
        return "rank"

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
        db_extension = config.sql_to_string_generator().get_db_extension()

        base_query = self.__base_frame.to_sql_query_object(config)
        new_query = create_sub_query(base_query, config, "root")

        tds_row = PandasApiTdsRow.from_tds_frame("root", self.__base_frame)

        partition_items = []
        if isinstance(self.__base_frame, PandasApiGroupbyTdsFrame):
            for group_col in self.__base_frame.grouping_column_name_list():
                col_expr = (lambda x: x[group_col])(tds_row).to_sql_expression({"root": new_query}, config)
                partition_items.append(col_expr)

        func_name_str = "RANK"
        if self.__pct:
            func_name_str = "PERCENT_RANK"
        elif self.__method == 'min':
            func_name_str = "RANK"
        elif self.__method == 'dense':
            func_name_str = "DENSE_RANK"
        elif self.__method == 'first':
            func_name_str = "ROW_NUMBER"
        elif self.__method == 'average':
            # TODO: Implement complex logic for average ( (RANK + (COUNT + RANK - 1)) / 2 )
            raise NotImplementedError(
                "Rank method 'average' is not yet supported. Please use 'min', 'dense', or 'first'.")
        else:
            raise NotImplementedError(
                f"Rank method '{self.__method}' is not supported.")

        rank_func = FunctionCall(
            name=QualifiedName([func_name_str]),
            distinct=False,
            arguments=[],
            filter_=None,
            window=None
        )

        # 3. Null Ordering
        null_ordering = SortItemNullOrdering.UNDEFINED
        if self.__na_option == 'top':
            null_ordering = SortItemNullOrdering.FIRST
        elif self.__na_option == 'bottom':
            null_ordering = SortItemNullOrdering.LAST

        new_select_items = []

        for col in self.__base_frame.columns():
            col_name = col.get_name()

            if self.__numeric_only:
                if col.get_type() not in ["Integer", "Float", "Number"]:
                    continue

            col_sql_expr = (lambda x: x[col_name])(
                tds_row).to_sql_expression({"root": new_query}, config)

            sort_item = SortItem(
                sortKey=col_sql_expr,
                ordering=SortItemOrdering.ASCENDING if self.__ascending else SortItemOrdering.DESCENDING,
                nullOrdering=null_ordering
            )

            window_spec = Window(
                windowRef=None,
                partitions=partition_items,
                orderBy=[sort_item],
                windowFrame=None
            )

            window_expr = WindowExpression(
                nested=rank_func,
                window=window_spec
            )

            final_expr = window_expr
            if self.__na_option == 'keep':
                final_expr = SearchedCaseExpression(
                    whenClauses=[
                        WhenClause(
                            operand=IsNullPredicate(col_sql_expr),
                            result=NullLiteral()
                        )
                    ],
                    defaultValue=window_expr
                )

            new_select_items.append(
                SingleColumn(
                    alias=db_extension.quote_identifier(col_name),
                    expression=final_expr
                )
            )

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
        new_columns = []
        for col in self.__base_frame.columns():
            if self.__numeric_only:
                if col.get_type() not in ["Integer", "Float", "Number"]:
                    continue

            if self.__pct or self.__method == 'average':
                new_col = PrimitiveTdsColumn.float_column(col.get_name())
            else:
                new_col = PrimitiveTdsColumn.integer_column(col.get_name())
            new_columns.append(new_col)
        return new_columns

    def validate(self) -> bool:
        if self.__axis not in [0, "index"]:
            raise ValueError(f"No axis named {self.__axis} for object type {type(self.__base_frame).__name__}")

        valid_methods = ['average', 'min', 'max', 'first', 'dense']
        if self.__method not in valid_methods:
            raise ValueError(f"method must be one of {valid_methods}")

        valid_na_options = ['keep', 'top', 'bottom']
        if self.__na_option not in valid_na_options:
            raise ValueError(f"na_option must be one of {valid_na_options}")

        return True

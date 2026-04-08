# Copyright 2023 Goldman Sachs
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
from decimal import Decimal as PythonDecimal

from pylegend._typing import (
    PyLegendList,
    PyLegendSequence,
    PyLegendDict,
    PyLegendCallable,
    PyLegendUnion,
)
from pylegend.core.language import (
    PyLegendPrimitive,
    PyLegendInteger,
    PyLegendFloat,
    PyLegendNumber,
    PyLegendBoolean,
    PyLegendString,
    PyLegendDate,
    PyLegendDateTime,
    PyLegendDecimal,
)
from pylegend.core.language.pandas_api.pandas_api_groupby_series import GroupbySeries
from pylegend.core.language.pandas_api.pandas_api_series import Series
from pylegend.core.language.pandas_api.pandas_api_tds_row import PandasApiTdsRow
from pylegend.core.language.shared.helpers import generate_pure_lambda
from pylegend.core.language.shared.literal_expressions import convert_literal_to_literal_expression
from pylegend.core.sql.metamodel import (
    QuerySpecification,
    SingleColumn,
)
from pylegend.core.tds.pandas_api.frames.functions.rank_function import RankFunction
from pylegend.core.tds.pandas_api.frames.functions.two_column_window_function import TwoColumnWindowFunction
from pylegend.core.tds.pandas_api.frames.functions.window_aggregate_function import WindowAggregateFunction
from pylegend.core.tds.pandas_api.frames.functions.zscore_window_function import ZScoreWindowFunction
from pylegend.core.tds.pandas_api.frames.helpers.series_helper import (
    has_window_function,
    has_window_aggregate_function,
    needs_zero_column_for_window,
    has_aggregate_function,
    split_window_from_arithmetic,
    convert_aggregate_series_to_window_aggregate_series,
)
from pylegend.core.tds.pandas_api.frames.pandas_api_applied_function_tds_frame import PandasApiAppliedFunction
from pylegend.core.tds.pandas_api.frames.pandas_api_base_tds_frame import PandasApiBaseTdsFrame
from pylegend.core.tds.sql_query_helpers import copy_query, create_sub_query
from pylegend.core.tds.tds_column import TdsColumn, PrimitiveTdsColumn
from pylegend.core.tds.tds_frame import FrameToSqlConfig, FrameToPureConfig


class AssignFunction(PandasApiAppliedFunction):
    __base_frame: PandasApiBaseTdsFrame
    __col_definitions: PyLegendDict[
        str,
        PyLegendCallable[
            [PandasApiTdsRow],
            PyLegendUnion[int, float, bool, str, date, datetime, PythonDecimal, PyLegendPrimitive]
        ],
    ]

    @classmethod
    def name(cls) -> str:
        return "assign"  # pragma: no cover

    def __init__(
            self,
            base_frame: PandasApiBaseTdsFrame,
            col_definitions: PyLegendDict[
                str,
                PyLegendCallable[
                    [PandasApiTdsRow],
                    PyLegendUnion[int, float, bool, str, date, datetime, PythonDecimal, PyLegendPrimitive]
                ],
            ]
    ) -> None:
        self.__base_frame = base_frame
        self.__col_definitions = col_definitions

    def to_sql(self, config: FrameToSqlConfig) -> QuerySpecification:
        temp_column_name_suffix = "__pylegend_olap_column__"
        db_extension = config.sql_to_string_generator().get_db_extension()
        base_query = self.__base_frame.to_sql_query_object(config)

        # Check if any assigned column uses a window aggregate function.
        # If so, add the zero column to base_query so that PARTITION BY can reference it.
        from pylegend.core.sql.metamodel import IntegerLiteral
        from pylegend.core.tds.pandas_api.frames.pandas_api_window_tds_frame import ZERO_COLUMN_NAME
        tds_row_check = PandasApiTdsRow.from_tds_frame("c", self.__base_frame)
        needs_zero_column = False
        for _, func in self.__col_definitions.items():
            res = func(tds_row_check)
            if isinstance(res, (Series, GroupbySeries)) and needs_zero_column_for_window(res):
                needs_zero_column = True
                break
        if needs_zero_column:
            base_query.select.selectItems.append(
                SingleColumn(
                    alias=db_extension.quote_identifier(ZERO_COLUMN_NAME),
                    expression=IntegerLiteral(0),
                )
            )
            # Wrap in a subquery so the zero column becomes a proper column reference
            # (otherwise PARTITION BY would use the literal 0 instead of the column)
            base_query = create_sub_query(base_query, config, "root")

        should_create_sub_query = (len(base_query.groupBy) > 0) or base_query.select.distinct

        new_query = (
            create_sub_query(base_query, config, "root") if should_create_sub_query else
            copy_query(base_query)
        )

        if needs_zero_column:
            zero_col_alias = db_extension.quote_identifier(ZERO_COLUMN_NAME)
            new_query.select.selectItems = [
                si for si in new_query.select.selectItems
                if not (isinstance(si, SingleColumn) and si.alias == zero_col_alias)
            ]

        base_cols = {c.get_name() for c in self.__base_frame.columns()}
        tds_row = PandasApiTdsRow.from_tds_frame("c", self.__base_frame)
        # For window-aggregate series with arithmetic, store the make_outer factory
        # so we can apply arithmetic in the outer query instead of the middle subquery.
        outer_factories: PyLegendDict[str, PyLegendCallable[..., object]] = {}  # type: ignore[explicit-any]
        for col, func in self.__col_definitions.items():
            res = func(tds_row)
            res_expr = res if isinstance(res, PyLegendPrimitive) else convert_literal_to_literal_expression(res)
            new_col_expr = res_expr.to_sql_expression(
                {"c": base_query},
                config
            )

            # For window-aggregate-containing series with arithmetic on top,
            # put only the WindowExpression in the middle subquery.
            # (This does NOT apply to RankFunction — only WindowAggregateFunction.)
            if isinstance(res, (Series, GroupbySeries)) and has_window_aggregate_function(res):
                window_only, make_outer = split_window_from_arithmetic(new_col_expr)
                if make_outer is not None:
                    outer_factories[col] = make_outer
                    new_col_expr = window_only

            alias = db_extension.quote_identifier(col)
            if col in base_cols:
                for i, si in enumerate(new_query.select.selectItems):
                    if isinstance(si, SingleColumn) and si.alias == alias:
                        if isinstance(res, (Series, GroupbySeries)):
                            alias = (db_extension.quote_identifier(col + temp_column_name_suffix) if has_window_function(res)
                                     else alias)
                        new_query.select.selectItems[i] = SingleColumn(alias=alias, expression=new_col_expr)

            else:
                if isinstance(res, (Series, GroupbySeries)):
                    alias = (db_extension.quote_identifier(col + temp_column_name_suffix) if has_window_function(res)
                             else alias)
                new_query.select.selectItems.append(SingleColumn(alias=alias, expression=new_col_expr))

        expr_contains_window_func = False
        for col, func in self.__col_definitions.items():
            res = func(tds_row)
            if isinstance(res, (Series, GroupbySeries)):
                expr_contains_window_func |= has_window_function(res)

        if expr_contains_window_func:
            final_query = create_sub_query(new_query, config, "root")
            # Strip the zero column from the final output
            zero_col_alias = db_extension.quote_identifier(ZERO_COLUMN_NAME)
            final_query.select.selectItems = [
                si for si in final_query.select.selectItems
                if not (isinstance(si, SingleColumn) and si.alias == zero_col_alias)
            ]
            for col, func in self.__col_definitions.items():
                res = func(tds_row)
                if isinstance(res, (Series, GroupbySeries)):
                    if has_window_function(res):
                        alias = db_extension.quote_identifier(col + temp_column_name_suffix)
                        new_alias = db_extension.quote_identifier(col)
                        for i, si in enumerate(final_query.select.selectItems):
                            if isinstance(si, SingleColumn) and si.alias == alias:
                                if col in outer_factories:
                                    # Arithmetic was separated: apply it in the outer query
                                    final_query.select.selectItems[i] = SingleColumn(
                                        alias=new_alias, expression=outer_factories[col](si.expression)  # type: ignore
                                    )
                                else:
                                    final_query.select.selectItems[i] = SingleColumn(
                                        alias=new_alias, expression=si.expression
                                    )
            return final_query

        return new_query

    def to_pure(self, config: FrameToPureConfig) -> str:
        temp_column_name_suffix = "__pylegend_olap_column__"
        tds_row = PandasApiTdsRow.from_tds_frame("c", self.__base_frame)
        base_cols = [c.get_name() for c in self.__base_frame.columns()]

        extend_exprs: PyLegendList[str] = []
        assigned_exprs: PyLegendDict[str, str] = {}
        for col, func in self.__col_definitions.items():
            res = func(tds_row)
            if isinstance(res, (Series, GroupbySeries)):
                sub_expressions = res.get_leaf_expressions()
                for expr in sub_expressions:
                    if isinstance(expr, Series):
                        applied_func = expr.get_filtered_frame().get_applied_function()
                    elif isinstance(expr, GroupbySeries):
                        applied_func = expr.raise_exception_if_no_function_applied().get_applied_function()
                    else:
                        continue

                    if isinstance(applied_func, RankFunction):
                        c, window = applied_func.construct_column_expression_and_window_tuples("r")[0]
                        window_expr = window.to_pure_expression(config)
                        function_expr = c[1].to_pure_expression(config)
                        target_col_name = c[0] + temp_column_name_suffix
                        extend = f"->extend({window_expr}, ~{target_col_name}:{generate_pure_lambda('p,w,r', function_expr)})"
                        extend_exprs.append(extend)
                    elif isinstance(applied_func, (TwoColumnWindowFunction, WindowAggregateFunction, ZScoreWindowFunction)):
                        extend_exprs.extend(
                            applied_func.build_pure_extend_strs(temp_column_name_suffix, config)
                        )
            res_expr = res if isinstance(res, PyLegendPrimitive) else convert_literal_to_literal_expression(res)
            assigned_exprs[col] = res_expr.to_pure_expression(config)

        # build project clauses
        clauses: PyLegendList[str] = []

        for col in base_cols:
            if col in assigned_exprs:
                clauses.append(f"{col}:c|{assigned_exprs[col]}")
            else:
                clauses.append(f"{col}:c|$c.{col}")

        for col, pure_expr in assigned_exprs.items():
            if col not in base_cols:
                clauses.append(f"{col}:c|{pure_expr}")

        return (
            f"{self.__base_frame.to_pure(config)}{config.separator(1)}"
            f"{config.separator(1).join(extend_exprs) + config.separator(1) if len(extend_exprs) > 0 else ''}"
            f"->project(~[{', '.join(clauses)}])"
        )

    def base_frame(self) -> PandasApiBaseTdsFrame:
        return self.__base_frame

    def tds_frame_parameters(self) -> PyLegendList["PandasApiBaseTdsFrame"]:
        return []

    def calculate_columns(self) -> PyLegendSequence["TdsColumn"]:
        new_cols = [c.copy() for c in self.__base_frame.columns() if c.get_name() not in self.__col_definitions]
        tds_row = PandasApiTdsRow.from_tds_frame("frame", self.__base_frame)
        for col, func in self.__col_definitions.items():
            res = func(tds_row)
            if isinstance(res, (int, PyLegendInteger)):
                new_cols.append(PrimitiveTdsColumn.integer_column(col))
            elif isinstance(res, (float, PyLegendFloat)):
                new_cols.append(PrimitiveTdsColumn.float_column(col))
            elif isinstance(res, (PythonDecimal, PyLegendDecimal)):
                new_cols.append(PrimitiveTdsColumn.decimal_column(col))
            elif isinstance(res, PyLegendNumber):
                new_cols.append(PrimitiveTdsColumn.number_column(col))  # pragma: no cover
            elif isinstance(res, (bool, PyLegendBoolean)):
                new_cols.append(
                    PrimitiveTdsColumn.boolean_column(col)
                )  # pragma: no cover (Boolean column not supported in PURE)
            elif isinstance(res, (str, PyLegendString)):
                new_cols.append(PrimitiveTdsColumn.string_column(col))
            elif isinstance(res, (datetime, PyLegendDateTime)):
                new_cols.append(PrimitiveTdsColumn.datetime_column(col))
            elif isinstance(res, (date, PyLegendDate)):
                new_cols.append(PrimitiveTdsColumn.date_column(col))
            else:
                raise RuntimeError("Type not supported")
        return new_cols

    def _update_col_definitions(self) -> None:
        tds_row = PandasApiTdsRow.from_tds_frame("frame", self.__base_frame)
        for col, f in list(self.__col_definitions.items()):
            res = f(tds_row)
            if isinstance(res, (Series, GroupbySeries)) and has_aggregate_function(res):
                converted: PyLegendUnion[Series, GroupbySeries] = convert_aggregate_series_to_window_aggregate_series(res)
                self.__col_definitions[col] = lambda row, _value=converted: _value  # type: ignore[misc]

    def validate(self) -> bool:
        self._update_col_definitions()
        tds_row = PandasApiTdsRow.from_tds_frame("frame", self.__base_frame)
        for col, f in self.__col_definitions.items():
            f(tds_row)
        return True

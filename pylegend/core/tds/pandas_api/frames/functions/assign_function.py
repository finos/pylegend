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
    PyLegendDateTime
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
from pylegend.core.tds.pandas_api.frames.functions.shift_function import ShiftFunction
from pylegend.core.tds.pandas_api.frames.helpers.series_helper import has_window_function
from pylegend.core.tds.pandas_api.frames.pandas_api_applied_function_tds_frame import PandasApiAppliedFunction
from pylegend.core.tds.pandas_api.frames.pandas_api_base_tds_frame import PandasApiBaseTdsFrame
from pylegend.core.tds.sql_query_helpers import copy_query, create_sub_query
from pylegend.core.tds.tds_column import TdsColumn, PrimitiveTdsColumn
from pylegend.core.tds.tds_frame import FrameToSqlConfig, FrameToPureConfig


class AssignFunction(PandasApiAppliedFunction):
    __base_frame: PandasApiBaseTdsFrame
    __col_definitions: PyLegendDict[
        str,
        PyLegendCallable[[PandasApiTdsRow], PyLegendUnion[int, float, bool, str, date, datetime, PyLegendPrimitive]],
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
                    [PandasApiTdsRow], PyLegendUnion[int, float, bool, str, date, datetime, PyLegendPrimitive]],
            ]
    ) -> None:
        self.__base_frame = base_frame
        self.__col_definitions = col_definitions

    def to_sql(self, config: FrameToSqlConfig) -> QuerySpecification:
        temp_column_name_suffix = "__INTERNAL_PYLEGEND_COLUMN__"
        db_extension = config.sql_to_string_generator().get_db_extension()
        base_query = self.__base_frame.to_sql_query_object(config)
        should_create_sub_query = (len(base_query.groupBy) > 0) or base_query.select.distinct

        new_query = (
            create_sub_query(base_query, config, "root") if should_create_sub_query else
            copy_query(base_query)
        )

        base_cols = {c.get_name() for c in self.__base_frame.columns()}
        tds_row = PandasApiTdsRow.from_tds_frame("c", self.__base_frame)
        for col, func in self.__col_definitions.items():
            res = func(tds_row)
            res_expr = res if isinstance(res, PyLegendPrimitive) else convert_literal_to_literal_expression(res)
            new_col_expr = res_expr.to_sql_expression(
                {"c": base_query},
                config
            )

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
            for col, func in self.__col_definitions.items():
                res = func(tds_row)
                if isinstance(res, (Series, GroupbySeries)):
                    if has_window_function(res):
                        alias = db_extension.quote_identifier(col + temp_column_name_suffix)
                        new_alias = db_extension.quote_identifier(col)
                        for i, si in enumerate(final_query.select.selectItems):
                            if isinstance(si, SingleColumn) and si.alias == alias:
                                final_query.select.selectItems[i] = SingleColumn(alias=new_alias, expression=si.expression)
            return final_query

        return new_query

    def to_pure(self, config: FrameToPureConfig) -> str:
        zero_column_name = "__INTERNAL_PYLEGEND_COLUMN__"
        temp_column_name_suffix = "__INTERNAL_PYLEGEND_COLUMN__"
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

                    if isinstance(applied_func, (RankFunction, ShiftFunction)):
                        c, window = applied_func.construct_column_expression_and_window_tuples("r")[0]
                        window_expr = window.to_pure_expression(config)
                        function_expr = c[1].to_pure_expression(config)
                        target_col_name = c[0] + temp_column_name_suffix
                        extend = f"->extend({window_expr}, ~{target_col_name}:{generate_pure_lambda('p,w,r', function_expr)})"
                        if isinstance(applied_func, ShiftFunction) and not isinstance(expr, GroupbySeries):
                            extend = f"->extend(~{zero_column_name}:{{r | 0}})" + config.separator(1) + extend
                        extend_exprs.append(extend)
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

    def validate(self) -> bool:
        tds_row = PandasApiTdsRow.from_tds_frame("frame", self.__base_frame)
        for col, f in self.__col_definitions.items():
            f(tds_row)
        return True

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

from abc import ABCMeta
from pylegend.core.language import (
    PyLegendPrimitive,
    PyLegendBoolean,
    PyLegendString,
    PyLegendNumber,
    PyLegendInteger,
    PyLegendFloat,
    PyLegendDecimal,
    PyLegendDate,
    PyLegendDateTime,
    PyLegendStrictDate,
    PyLegendColumnExpression,
    convert_literal_to_literal_expression,
)
from pylegend._typing import (
    PyLegendSequence,
    PyLegendOptional,
    PyLegendUnion,
)
from pylegend.core.language.shared.helpers import escape_column_name
from pylegend.core.language.shared.pylegend_custom_expressions import (
    PyLegendSortDirection as LegendQLApiSortDirection,
    PyLegendDurationUnit as LegendQLApiDurationUnit,
    PyLegendFrameBoundType as LegendQLApiWindowFrameBoundType,
    PyLegendWindowFrameMode as LegendQLApiWindowFrameMode,
    PyLegendWindowFrame as LegendQLApiWindowFrame,
    PyLegendWindow as LegendQLApiWindow,
    PyLegendPartialFrame,
    PyLegendWindowReference as LegendQLApiWindowReference,
    PyLegendRowNumberExpression as LegendQLApiRowNumberExpression,
    PyLegendRankExpression as LegendQLApiRankExpression,
    PyLegendDenseRankExpression as LegendQLApiDenseRankExpression,
    PyLegendPercentRankExpression as LegendQLApiPercentRankExpression,
    PyLegendCumeDistExpression as LegendQLApiCumeDistExpression,
    PyLegendNtileExpression as LegendQLApiNtileExpression,
)
from pylegend.core.sql.metamodel import (
    QuerySpecification,
    FrameBound, SortItemOrdering, SortItemNullOrdering, SortItem, SingleColumn, Expression,
)
from pylegend.core.tds.tds_frame import FrameToSqlConfig, FrameToPureConfig
from typing import TYPE_CHECKING

__all__: PyLegendSequence[str] = [
    "LegendQLApiPrimitive",
    "LegendQLApiBoolean",
    "LegendQLApiString",
    "LegendQLApiNumber",
    "LegendQLApiInteger",
    "LegendQLApiFloat",
    "LegendQLApiDecimal",
    "LegendQLApiDate",
    "LegendQLApiDateTime",
    "LegendQLApiStrictDate",
    "LegendQLApiSortInfo",
    "LegendQLApiSortDirection",
    "LegendQLApiWindow",
    "LegendQLApiPartialFrame",
    "LegendQLApiWindowReference",
    "LegendQLApiWindowFrameBound",
    "LegendQLApiWindowFrameMode",
    "LegendQLApiWindowFrame",
    "LegendQLApiDurationUnit",
    "LegendQLApiWindowFrameBoundType"
]


class LegendQLApiPrimitive(PyLegendPrimitive, metaclass=ABCMeta):
    def ascending(self) -> "LegendQLApiSortInfo":
        val = self.value()
        if isinstance(val, PyLegendColumnExpression):
            return LegendQLApiSortInfo(column_expr=val, direction=LegendQLApiSortDirection.ASC)
        else:
            raise RuntimeError("'ascending' function can only be called on column expressions. "
                               "E.g. - r.col1.ascending() / r['col1'].ascending()\n."
                               f"Found expression type - {type(val)}")

    def descending(self) -> "LegendQLApiSortInfo":
        val = self.value()
        if isinstance(val, PyLegendColumnExpression):
            return LegendQLApiSortInfo(column_expr=val, direction=LegendQLApiSortDirection.DESC)
        else:
            raise RuntimeError("'descending' function can only be called on column expressions. "
                               "E.g. - r.col1.descending() / r['col1'].descending()\n."
                               f"Found expression type - {type(val)}")


class LegendQLApiBoolean(LegendQLApiPrimitive, PyLegendBoolean):
    def __init__(self, expr: PyLegendBoolean):
        PyLegendBoolean.__init__(self, expr.value())


class LegendQLApiString(LegendQLApiPrimitive, PyLegendString):
    def __init__(self, expr: PyLegendString):
        PyLegendString.__init__(self, expr.value())


class LegendQLApiNumber(LegendQLApiPrimitive, PyLegendNumber):
    def __init__(self, expr: PyLegendNumber):
        PyLegendNumber.__init__(self, expr.value())


class LegendQLApiInteger(LegendQLApiPrimitive, PyLegendInteger):
    def __init__(self, expr: PyLegendInteger):
        PyLegendInteger.__init__(self, expr.value())


class LegendQLApiFloat(LegendQLApiPrimitive, PyLegendFloat):
    def __init__(self, expr: PyLegendFloat):
        PyLegendFloat.__init__(self, expr.value())


class LegendQLApiDecimal(LegendQLApiPrimitive, PyLegendDecimal):
    def __init__(self, expr: PyLegendDecimal):
        PyLegendDecimal.__init__(self, expr.value())


class LegendQLApiDate(LegendQLApiPrimitive, PyLegendDate):
    def __init__(self, expr: PyLegendDate):
        PyLegendDate.__init__(self, expr.value())


class LegendQLApiDateTime(LegendQLApiPrimitive, PyLegendDateTime):
    def __init__(self, expr: PyLegendDateTime):
        PyLegendDateTime.__init__(self, expr.value())


class LegendQLApiStrictDate(LegendQLApiPrimitive, PyLegendStrictDate):
    def __init__(self, expr: PyLegendStrictDate):
        PyLegendStrictDate.__init__(self, expr.value())


class LegendQLApiSortInfo:
    __column: str
    __direction: LegendQLApiSortDirection

    def __init__(self, column_expr: PyLegendColumnExpression, direction: LegendQLApiSortDirection) -> None:
        self.__column = column_expr.get_column()
        self.__direction = direction

    def to_sql_node(
            self,
            query: QuerySpecification,
            config: FrameToSqlConfig
    ) -> SortItem:
        return SortItem(
            sortKey=self.__find_column_expression(query, config),
            ordering=(SortItemOrdering.ASCENDING if self.__direction == LegendQLApiSortDirection.ASC
                      else SortItemOrdering.DESCENDING),
            nullOrdering=SortItemNullOrdering.UNDEFINED
        )

    def __find_column_expression(self, query: QuerySpecification, config: FrameToSqlConfig) -> Expression:
        db_extension = config.sql_to_string_generator().get_db_extension()
        filtered = [
            s for s in query.select.selectItems
            if (isinstance(s, SingleColumn) and
                s.alias == db_extension.quote_identifier(self.__column))
        ]
        if len(filtered) == 0:
            raise RuntimeError("Cannot find column: " + self.__column)  # pragma: no cover
        return filtered[0].expression

    def to_pure_expression(self, config: FrameToPureConfig) -> str:
        func = 'ascending' if self.__direction == LegendQLApiSortDirection.ASC else 'descending'
        return f"{func}(~{escape_column_name(self.__column)})"


class LegendQLApiWindowFrameBound:
    __bound_type: LegendQLApiWindowFrameBoundType
    __row_offset: PyLegendOptional[PyLegendUnion[int, float]]
    __duration_unit: PyLegendOptional[LegendQLApiDurationUnit]

    def __init__(
            self,
            bound_type: LegendQLApiWindowFrameBoundType,
            row_offset: PyLegendOptional[PyLegendUnion[int, float]] = None,
            duration_unit: PyLegendOptional[LegendQLApiDurationUnit] = None,
    ) -> None:
        if bound_type in (
                LegendQLApiWindowFrameBoundType.PRECEDING,
                LegendQLApiWindowFrameBoundType.FOLLOWING,
        ) and row_offset is None:
            raise ValueError(f"row_offset must be provided for bound_type {bound_type.name}")

        if bound_type not in (
                LegendQLApiWindowFrameBoundType.PRECEDING,
                LegendQLApiWindowFrameBoundType.FOLLOWING,
        ) and row_offset is not None:
            raise ValueError(f"row_offset is not allowed for bound_type {bound_type.name}")

        self.__bound_type = bound_type
        self.__row_offset = row_offset
        self.__duration_unit = duration_unit

    def to_pure_expression(self, config: FrameToPureConfig) -> str:
        if (self.__bound_type == LegendQLApiWindowFrameBoundType.UNBOUNDED_FOLLOWING
                or self.__bound_type == LegendQLApiWindowFrameBoundType.UNBOUNDED_PRECEDING):
            return "unbounded()"

        elif self.__bound_type == LegendQLApiWindowFrameBoundType.CURRENT_ROW:
            expr = "0"

        else:
            expr = convert_literal_to_literal_expression(self.__row_offset).to_pure_expression(
                config) if self.__row_offset is not None else ""

        if self.__duration_unit is not None:
            expr += f", DurationUnit.{self.__duration_unit.to_pure_expression(config)}"

        return expr

    def to_sql_node(
            self,
            query: QuerySpecification,
            config: FrameToSqlConfig,
    ) -> FrameBound:
        value = (convert_literal_to_literal_expression(abs(self.__row_offset))
                 .to_sql_expression({"w": query}, config)) \
            if self.__row_offset is not None else None

        frame_bound_type = self.__bound_type.to_sql_node(query, config)

        duration_unit = self.__duration_unit.to_sql_node(
            query,
            config) if self.__duration_unit is not None else None

        return FrameBound(frame_bound_type, value, duration_unit)


class LegendQLApiPartialFrame(PyLegendPartialFrame):
    if TYPE_CHECKING:
        from pylegend.core.tds.legendql_api.frames.legendql_api_base_tds_frame import LegendQLApiBaseTdsFrame
        from pylegend.core.language.legendql_api.legendql_api_tds_row import LegendQLApiTdsRow

    __base_frame: "LegendQLApiBaseTdsFrame"

    def __init__(self, base_frame: "LegendQLApiBaseTdsFrame", var_name: str) -> None:
        super().__init__(var_name)
        self.__base_frame = base_frame

    def row_number(
            self,
            row: "LegendQLApiTdsRow"
    ) -> PyLegendInteger:
        return PyLegendInteger(LegendQLApiRowNumberExpression(self, row))

    def rank(
            self,
            window: "LegendQLApiWindowReference",
            row: "LegendQLApiTdsRow"
    ) -> PyLegendInteger:
        return PyLegendInteger(LegendQLApiRankExpression(self, window, row))

    def dense_rank(
            self,
            window: "LegendQLApiWindowReference",
            row: "LegendQLApiTdsRow"
    ) -> PyLegendInteger:
        return PyLegendInteger(LegendQLApiDenseRankExpression(self, window, row))

    def percent_rank(
            self,
            window: "LegendQLApiWindowReference",
            row: "LegendQLApiTdsRow"
    ) -> PyLegendFloat:
        return PyLegendFloat(LegendQLApiPercentRankExpression(self, window, row))

    def cume_dist(
            self,
            window: "LegendQLApiWindowReference",
            row: "LegendQLApiTdsRow"
    ) -> PyLegendFloat:
        return PyLegendFloat(LegendQLApiCumeDistExpression(self, window, row))

    def ntile(
            self,
            row: "LegendQLApiTdsRow",
            num_buckets: int
    ) -> PyLegendInteger:
        return PyLegendInteger(LegendQLApiNtileExpression(self, row, num_buckets))

    def lead(
            self,
            row: "LegendQLApiTdsRow"
    ) -> "LegendQLApiTdsRow":
        from pylegend.core.language.legendql_api.legendql_api_tds_row import LegendQLApiLeadRow
        return LegendQLApiLeadRow(self, row)

    def lag(
            self,
            row: "LegendQLApiTdsRow"
    ) -> "LegendQLApiTdsRow":
        from pylegend.core.language.legendql_api.legendql_api_tds_row import LegendQLApiLagRow
        return LegendQLApiLagRow(self, row)

    def first(
            self,
            window: "LegendQLApiWindowReference",
            row: "LegendQLApiTdsRow"
    ) -> "LegendQLApiTdsRow":
        from pylegend.core.language.legendql_api.legendql_api_tds_row import LegendQLApiFirstRow
        return LegendQLApiFirstRow(self, window, row)

    def last(
            self,
            window: "LegendQLApiWindowReference",
            row: "LegendQLApiTdsRow"
    ) -> "LegendQLApiTdsRow":
        from pylegend.core.language.legendql_api.legendql_api_tds_row import LegendQLApiLastRow
        return LegendQLApiLastRow(self, window, row)

    def nth(
            self,
            window: "LegendQLApiWindowReference",
            row: "LegendQLApiTdsRow",
            offset: int
    ) -> "LegendQLApiTdsRow":
        from pylegend.core.language.legendql_api.legendql_api_tds_row import LegendQLApiNthRow
        return LegendQLApiNthRow(self, window, row, offset)

    def get_base_frame(self) -> "LegendQLApiBaseTdsFrame":
        return self.__base_frame

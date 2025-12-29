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
from enum import Enum
from pylegend.core.language import (
    PyLegendPrimitive,
    PyLegendBoolean,
    PyLegendString,
    PyLegendNumber,
    PyLegendInteger,
    PyLegendFloat,
    PyLegendDate,
    PyLegendDateTime,
    PyLegendStrictDate,
    PyLegendColumnExpression,
    PyLegendExpressionIntegerReturn,
    PyLegendExpressionFloatReturn,
    convert_literal_to_literal_expression,
)
from pylegend._typing import (
    PyLegendSequence,
    PyLegendOptional,
    PyLegendList,
    PyLegendDict,
    PyLegendUnion,
)
from pylegend.core.language.shared.helpers import escape_column_name
from pylegend.core.sql.metamodel import (
    QuerySpecification,
    Expression,
    SingleColumn,
    SortItem,
    SortItemOrdering,
    SortItemNullOrdering,
    Window,
    FunctionCall,
    QualifiedName,
    IntegerLiteral,
    WindowFrame,
    WindowFrameMode,
    FrameBound,
    FrameBoundType,
    DurationUnit
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
    "LegendQLApiDate",
    "LegendQLApiDateTime",
    "LegendQLApiStrictDate",
    "LegendQLApiSortInfo",
    "LegendQLApiSortDirection",
    "LegendQLApiWindow",
    "LegendQLApiPartialFrame",
    "LegendQLApiWindowReference",
    "LegendQLApiFrameBound",
    "LegendQLApiWindowFrameMode",
    "LegendQLApiWindowFrame",
    "LegendQLApiDurationInput",
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


class LegendQLApiDate(LegendQLApiPrimitive, PyLegendDate):
    def __init__(self, expr: PyLegendDate):
        PyLegendDate.__init__(self, expr.value())


class LegendQLApiDateTime(LegendQLApiPrimitive, PyLegendDateTime):
    def __init__(self, expr: PyLegendDateTime):
        PyLegendDateTime.__init__(self, expr.value())


class LegendQLApiStrictDate(LegendQLApiPrimitive, PyLegendStrictDate):
    def __init__(self, expr: PyLegendStrictDate):
        PyLegendStrictDate.__init__(self, expr.value())


class LegendQLApiSortDirection(Enum):
    ASC = 1,
    DESC = 2


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


class LegendQLApiWindowFrameMode(Enum):
    ROWS = 1,
    RANGE = 2

    def to_pure_expression(self, config: FrameToPureConfig) -> str:
        return "rows" if self == LegendQLApiWindowFrameMode.ROWS else "_range"

    def to_sql_node(
            self,
            query: QuerySpecification,
            config: FrameToSqlConfig
    ) -> WindowFrameMode:
        return WindowFrameMode.ROWS if self == LegendQLApiWindowFrameMode.ROWS else WindowFrameMode.RANGE


class LegendQLApiWindowFrameBoundType(Enum):
    UNBOUNDED = 1
    PRECEDING = 2
    CURRENT_ROW = 3
    FOLLOWING = 4

    def to_sql_node(
            self,
            query: QuerySpecification,
            config: FrameToSqlConfig,
            is_start_bound: bool
    ) -> FrameBoundType:
        if self is LegendQLApiWindowFrameBoundType.UNBOUNDED:
            return (
                FrameBoundType.UNBOUNDED_PRECEDING
                if is_start_bound
                else FrameBoundType.UNBOUNDED_FOLLOWING
            )

        mapping = {
            LegendQLApiWindowFrameBoundType.PRECEDING: FrameBoundType.PRECEDING,
            LegendQLApiWindowFrameBoundType.CURRENT_ROW: FrameBoundType.CURRENT_ROW,
            LegendQLApiWindowFrameBoundType.FOLLOWING: FrameBoundType.FOLLOWING,
        }

        try:
            return mapping[self]
        except KeyError:
            raise ValueError(f"Unsupported frame bound type: {self}")


class LegendQLApiDurationUnit(Enum):
    YEARS = 1
    MONTHS = 2
    WEEKS = 3
    DAYS = 4
    HOURS = 5
    MINUTES = 6
    SECONDS = 7
    MILLISECONDS = 8
    MICROSECONDS = 9
    NANOSECONDS = 10

    def to_pure_expression(self, config: FrameToPureConfig) -> str:
        return self.name

    def to_sql_node(
            self,
            query: QuerySpecification,
            config: FrameToSqlConfig
    ) -> DurationUnit:
        mapping = {
            LegendQLApiDurationUnit.YEARS: DurationUnit.YEAR,
            LegendQLApiDurationUnit.MONTHS: DurationUnit.MONTH,
            LegendQLApiDurationUnit.WEEKS: DurationUnit.WEEK,
            LegendQLApiDurationUnit.DAYS: DurationUnit.DAY,
            LegendQLApiDurationUnit.HOURS: DurationUnit.HOUR,
            LegendQLApiDurationUnit.MINUTES: DurationUnit.MINUTE,
            LegendQLApiDurationUnit.SECONDS: DurationUnit.SECOND,
            LegendQLApiDurationUnit.MILLISECONDS: DurationUnit.MILLISECOND,
            LegendQLApiDurationUnit.MICROSECONDS: DurationUnit.MICROSECOND,
            LegendQLApiDurationUnit.NANOSECONDS: DurationUnit.NANOSECOND,
        }

        try:
            return mapping[self]
        except KeyError:
            raise ValueError(f"Unsupported duration unit: {self}")

    @classmethod
    def from_string(cls, value: str) -> "LegendQLApiDurationUnit":
        try:
            return cls[value.upper()]
        except KeyError:
            raise ValueError(
                f"Invalid duration unit '{value}'. "
                f"Supported values: {[u.name.lower() for u in cls]}"
            )


class LegendQLApiDurationInput:
    __offset: int
    __unit: str

    def __init__(self, offset: int, unit: str) -> None:
        self.__offset = offset
        self.__unit = unit

    def get_offset(self) -> int:
        return self.__offset

    def get_unit(self) -> str:
        return self.__unit


class LegendQLApiFrameBound:
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
        if self.__bound_type == LegendQLApiWindowFrameBoundType.UNBOUNDED:
            return "unbounded()"

        elif self.__bound_type == LegendQLApiWindowFrameBoundType.CURRENT_ROW:
            expr = "0"

        else:
            expr = convert_literal_to_literal_expression(
                self.__row_offset
            ).to_pure_expression(config) if self.__row_offset is not None else ""

        if self.__duration_unit is not None:
            expr += f", DurationUnit.{self.__duration_unit.to_pure_expression(config)}"

        return expr

    def to_sql_node(
            self,
            query: QuerySpecification,
            config: FrameToSqlConfig,
            is_start_bound: bool,
    ) -> FrameBound:
        value = (convert_literal_to_literal_expression(abs(self.__row_offset))
                 .to_sql_expression({"w": query}, config)) \
            if self.__row_offset is not None else None
        frame_bound_type = self.__bound_type.to_sql_node(query, config, is_start_bound)
        duration_unit = self.__duration_unit.to_sql_node(
            query,
            config) if self.__duration_unit is not None else None
        return FrameBound(frame_bound_type, value, duration_unit)


class LegendQLApiWindowFrame:
    __mode: LegendQLApiWindowFrameMode
    __start_bound: LegendQLApiFrameBound
    __end_bound: LegendQLApiFrameBound

    def __init__(
            self,
            mode: LegendQLApiWindowFrameMode,
            start_bound: LegendQLApiFrameBound,
            end_bound: LegendQLApiFrameBound,
    ) -> None:
        self.__mode = mode
        self.__start_bound = start_bound
        self.__end_bound = end_bound

    def to_pure_expression(self, config: FrameToPureConfig) -> str:
        mode_str = self.__mode.to_pure_expression(config)
        start_expr = self.__start_bound.to_pure_expression(config)
        end_expr = self.__end_bound.to_pure_expression(config)

        return f"{mode_str}({start_expr}, {end_expr})"

    def to_sql_node(
            self,
            query: QuerySpecification,
            config: FrameToSqlConfig
    ) -> WindowFrame:
        return WindowFrame(
            mode=self.__mode.to_sql_node(query, config),
            start=self.__start_bound.to_sql_node(query, config, True),
            end=self.__end_bound.to_sql_node(query, config, False) if self.__end_bound is not None else None,
        )


class LegendQLApiWindow:
    __partition_by: PyLegendOptional[PyLegendList[str]]
    __order_by: PyLegendOptional[PyLegendList[LegendQLApiSortInfo]]
    __frame: PyLegendOptional[LegendQLApiWindowFrame]

    def __init__(
            self,
            partition_by: PyLegendOptional[PyLegendList[str]] = None,
            order_by: PyLegendOptional[PyLegendList[LegendQLApiSortInfo]] = None,
            frame: PyLegendOptional[LegendQLApiWindowFrame] = None
    ) -> None:
        self.__partition_by = partition_by
        self.__order_by = order_by
        self.__frame = frame

    def get_partition_by(self) -> PyLegendOptional[PyLegendList[str]]:
        return self.__partition_by

    def get_order_by(self) -> PyLegendOptional[PyLegendList[LegendQLApiSortInfo]]:
        return self.__order_by

    def get_frame(self) -> PyLegendOptional[LegendQLApiWindowFrame]:
        return self.__frame

    def to_sql_node(
            self,
            query: QuerySpecification,
            config: FrameToSqlConfig
    ) -> Window:
        return Window(
            windowRef=None,
            partitions=(
                [] if self.__partition_by is None else
                [LegendQLApiWindow.__find_column_expression(query, col, config) for col in self.__partition_by]
            ),
            orderBy=(
                [] if self.__order_by is None else
                [sort_info.to_sql_node(query, config) for sort_info in self.__order_by]
            ),
            windowFrame=(
                None if self.__frame is None else
                self.__frame.to_sql_node(query, config)
            ),
        )

    @staticmethod
    def __find_column_expression(query: QuerySpecification, col: str, config: FrameToSqlConfig) -> Expression:
        db_extension = config.sql_to_string_generator().get_db_extension()
        filtered = [
            s for s in query.select.selectItems
            if (isinstance(s, SingleColumn) and
                s.alias == db_extension.quote_identifier(col))
        ]
        if len(filtered) == 0:
            raise RuntimeError("Cannot find column: " + col)  # pragma: no cover
        return filtered[0].expression

    def to_pure_expression(self, config: FrameToPureConfig) -> str:
        partitions_str = (
            "[]" if self.__partition_by is None or len(self.__partition_by) == 0
            else "~[" + (', '.join(map(escape_column_name, self.__partition_by))) + "]"
        )
        sorts_str = (
            "[]" if self.__order_by is None or len(self.__order_by) == 0
            else "[" + (', '.join([s.to_pure_expression(config) for s in self.__order_by])) + "]"
        )

        frame_str = f", {self.__frame.to_pure_expression(config)}" if self.__frame else ""

        return f"over({partitions_str}, {sorts_str}{frame_str})"


class LegendQLApiPartialFrame:
    if TYPE_CHECKING:
        from pylegend.core.tds.legendql_api.frames.legendql_api_base_tds_frame import LegendQLApiBaseTdsFrame
        from pylegend.core.language.legendql_api.legendql_api_tds_row import LegendQLApiTdsRow

    __base_frame: "LegendQLApiBaseTdsFrame"
    __var_name: str

    def __init__(self, base_frame: "LegendQLApiBaseTdsFrame", var_name: str) -> None:
        self.__base_frame = base_frame
        self.__var_name = var_name

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

    def to_pure_expression(self, config: FrameToPureConfig) -> str:
        return f"${self.__var_name}"

    def get_base_frame(self) -> "LegendQLApiBaseTdsFrame":
        return self.__base_frame


class LegendQLApiWindowReference:
    __window: LegendQLApiWindow
    __var_name: str

    def __init__(self, window: LegendQLApiWindow, var_name: str) -> None:
        self.__window = window
        self.__var_name = var_name

    def to_pure_expression(self, config: FrameToPureConfig) -> str:
        return f"${self.__var_name}"


class LegendQLApiRowNumberExpression(PyLegendExpressionIntegerReturn):
    if TYPE_CHECKING:
        from pylegend.core.language.legendql_api.legendql_api_tds_row import LegendQLApiTdsRow

    __partial_frame: LegendQLApiPartialFrame
    __row: "LegendQLApiTdsRow"

    def __init__(
            self,
            partial_frame: LegendQLApiPartialFrame,
            row: "LegendQLApiTdsRow"
    ) -> None:
        self.__partial_frame = partial_frame
        self.__row = row

    def to_sql_expression(
            self,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        return FunctionCall(
            name=QualifiedName(parts=["row_number"]), distinct=False, arguments=[], filter_=None, window=None
        )

    def to_pure_expression(self, config: FrameToPureConfig) -> str:
        return f"{self.__partial_frame.to_pure_expression(config)}->rowNumber({self.__row.to_pure_expression(config)})"


class LegendQLApiRankExpression(PyLegendExpressionIntegerReturn):
    if TYPE_CHECKING:
        from pylegend.core.language.legendql_api.legendql_api_tds_row import LegendQLApiTdsRow

    __partial_frame: LegendQLApiPartialFrame
    __window_ref: "LegendQLApiWindowReference"
    __row: "LegendQLApiTdsRow"

    def __init__(
            self,
            partial_frame: LegendQLApiPartialFrame,
            window_ref: "LegendQLApiWindowReference",
            row: "LegendQLApiTdsRow"
    ) -> None:
        self.__partial_frame = partial_frame
        self.__window_ref = window_ref
        self.__row = row

    def to_sql_expression(
            self,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        return FunctionCall(
            name=QualifiedName(parts=["rank"]), distinct=False, arguments=[], filter_=None, window=None
        )

    def to_pure_expression(self, config: FrameToPureConfig) -> str:
        return (f"{self.__partial_frame.to_pure_expression(config)}->rank("
                f"{self.__window_ref.to_pure_expression(config)}, {self.__row.to_pure_expression(config)})")


class LegendQLApiDenseRankExpression(PyLegendExpressionIntegerReturn):
    if TYPE_CHECKING:
        from pylegend.core.language.legendql_api.legendql_api_tds_row import LegendQLApiTdsRow

    __partial_frame: LegendQLApiPartialFrame
    __window_ref: "LegendQLApiWindowReference"
    __row: "LegendQLApiTdsRow"

    def __init__(
            self,
            partial_frame: LegendQLApiPartialFrame,
            window_ref: "LegendQLApiWindowReference",
            row: "LegendQLApiTdsRow"
    ) -> None:
        self.__partial_frame = partial_frame
        self.__window_ref = window_ref
        self.__row = row

    def to_sql_expression(
            self,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        return FunctionCall(
            name=QualifiedName(parts=["dense_rank"]), distinct=False, arguments=[], filter_=None, window=None
        )

    def to_pure_expression(self, config: FrameToPureConfig) -> str:
        return (f"{self.__partial_frame.to_pure_expression(config)}->denseRank("
                f"{self.__window_ref.to_pure_expression(config)}, {self.__row.to_pure_expression(config)})")


class LegendQLApiPercentRankExpression(PyLegendExpressionFloatReturn):
    if TYPE_CHECKING:
        from pylegend.core.language.legendql_api.legendql_api_tds_row import LegendQLApiTdsRow

    __partial_frame: LegendQLApiPartialFrame
    __window_ref: "LegendQLApiWindowReference"
    __row: "LegendQLApiTdsRow"

    def __init__(
            self,
            partial_frame: LegendQLApiPartialFrame,
            window_ref: "LegendQLApiWindowReference",
            row: "LegendQLApiTdsRow"
    ) -> None:
        self.__partial_frame = partial_frame
        self.__window_ref = window_ref
        self.__row = row

    def to_sql_expression(
            self,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        return FunctionCall(
            name=QualifiedName(parts=["percent_rank"]), distinct=False, arguments=[], filter_=None, window=None
        )

    def to_pure_expression(self, config: FrameToPureConfig) -> str:
        return (f"{self.__partial_frame.to_pure_expression(config)}->percentRank("
                f"{self.__window_ref.to_pure_expression(config)}, {self.__row.to_pure_expression(config)})")


class LegendQLApiCumeDistExpression(PyLegendExpressionFloatReturn):
    if TYPE_CHECKING:
        from pylegend.core.language.legendql_api.legendql_api_tds_row import LegendQLApiTdsRow

    __partial_frame: LegendQLApiPartialFrame
    __window_ref: "LegendQLApiWindowReference"
    __row: "LegendQLApiTdsRow"

    def __init__(
            self,
            partial_frame: LegendQLApiPartialFrame,
            window_ref: "LegendQLApiWindowReference",
            row: "LegendQLApiTdsRow"
    ) -> None:
        self.__partial_frame = partial_frame
        self.__window_ref = window_ref
        self.__row = row

    def to_sql_expression(
            self,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        return FunctionCall(
            name=QualifiedName(parts=["cume_dist"]), distinct=False, arguments=[], filter_=None, window=None
        )

    def to_pure_expression(self, config: FrameToPureConfig) -> str:
        return (f"{self.__partial_frame.to_pure_expression(config)}->cumulativeDistribution("
                f"{self.__window_ref.to_pure_expression(config)}, {self.__row.to_pure_expression(config)})")


class LegendQLApiNtileExpression(PyLegendExpressionIntegerReturn):
    if TYPE_CHECKING:
        from pylegend.core.language.legendql_api.legendql_api_tds_row import LegendQLApiTdsRow

    __partial_frame: LegendQLApiPartialFrame
    __row: "LegendQLApiTdsRow"
    __num_buckets: int

    def __init__(
            self,
            partial_frame: LegendQLApiPartialFrame,
            row: "LegendQLApiTdsRow",
            num_buckets: int
    ) -> None:
        self.__partial_frame = partial_frame
        self.__row = row
        self.__num_buckets = num_buckets

    def to_sql_expression(
            self,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        return FunctionCall(
            name=QualifiedName(parts=["ntile"]),
            distinct=False,
            arguments=[IntegerLiteral(self.__num_buckets)],
            filter_=None,
            window=None
        )

    def to_pure_expression(self, config: FrameToPureConfig) -> str:
        return (f"{self.__partial_frame.to_pure_expression(config)}->ntile({self.__row.to_pure_expression(config)}, "
                f"{self.__num_buckets})")

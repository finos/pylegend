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

from decimal import Decimal as PythonDecimal
from enum import Enum
from pylegend._typing import (
    PyLegendSequence,
    PyLegendOptional,
    PyLegendList,
    PyLegendDict,
    PyLegendUnion,
)
from pylegend.core.language.shared.expression import (
    PyLegendExpressionFloatReturn,
    PyLegendExpressionIntegerReturn,
)
from pylegend.core.language.shared.helpers import escape_column_name
from pylegend.core.language.shared.literal_expressions import convert_literal_to_literal_expression
from pylegend.core.sql.metamodel import (
    Expression,
    FrameBound,
    FrameBoundType,
    FunctionCall,
    QualifiedName,
    QuerySpecification,
    SingleColumn,
    SortItem,
    SortItemNullOrdering,
    SortItemOrdering,
    StringLiteral,
    Window,
    WindowFrame,
    WindowFrameMode,
    IntegerLiteral,
)
from pylegend.core.tds.tds_frame import (
    FrameToPureConfig,
    FrameToSqlConfig,
)

__all__: PyLegendSequence[str] = [
    "PyLegendSortDirection",
    "PyLegendSortInfo",
    "PyLegendDurationUnit",
    "PyLegendFrameBoundType",
    "PyLegendFrameBound",
    "PyLegendWindowFrameMode",
    "PyLegendWindowFrame",
    "PyLegendWindow",
    "PyLegendPartialFrame",
    "PyLegendWindowReference",
    "PyLegendRowNumberExpression",
    "PyLegendRankExpression",
    "PyLegendDenseRankExpression",
    "PyLegendPercentRankExpression",
    "PyLegendCumeDistExpression",
    "PyLegendNtileExpression",
]


# ---------------------------------------------------------------------------
# Sort direction & sort info
# ---------------------------------------------------------------------------

class PyLegendSortDirection(Enum):
    ASC = 1
    DESC = 2


class PyLegendSortInfo:
    __column: str
    __direction: PyLegendSortDirection
    __null_ordering: SortItemNullOrdering

    def __init__(
            self,
            column_name: str,
            direction: PyLegendSortDirection,
            null_ordering: SortItemNullOrdering = SortItemNullOrdering.UNDEFINED
    ) -> None:
        self.__column = column_name
        self.__direction = direction
        self.__null_ordering = null_ordering

    def to_sql_node(
            self,
            query: QuerySpecification,
            config: FrameToSqlConfig
    ) -> SortItem:
        return SortItem(
            sortKey=self.__find_column_expression(query, config),
            ordering=(SortItemOrdering.ASCENDING if self.__direction == PyLegendSortDirection.ASC
                      else SortItemOrdering.DESCENDING),
            nullOrdering=self.__null_ordering
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
        func = 'ascending' if self.__direction == PyLegendSortDirection.ASC else 'descending'
        return f"{func}(~{escape_column_name(self.__column)})"


# ---------------------------------------------------------------------------
# Duration unit
# ---------------------------------------------------------------------------

class PyLegendDurationUnit(Enum):
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
            config: FrameToSqlConfig,
    ) -> StringLiteral:
        mapping = {
            PyLegendDurationUnit.YEARS: "YEAR",
            PyLegendDurationUnit.MONTHS: "MONTH",
            PyLegendDurationUnit.WEEKS: "WEEK",
            PyLegendDurationUnit.DAYS: "DAY",
            PyLegendDurationUnit.HOURS: "HOUR",
            PyLegendDurationUnit.MINUTES: "MINUTE",
            PyLegendDurationUnit.SECONDS: "SECOND",
            PyLegendDurationUnit.MILLISECONDS: "MILLISECOND",
            PyLegendDurationUnit.MICROSECONDS: "MICROSECOND",
            PyLegendDurationUnit.NANOSECONDS: "NANOSECOND",
        }
        return StringLiteral(mapping[self], quoted=False)

    @classmethod
    def from_string(cls, value: str) -> "PyLegendDurationUnit":
        try:
            return cls[value.upper()]
        except KeyError:
            raise ValueError(
                f"Invalid duration unit '{value}'. "
                f"Supported values: {[u.name.lower() for u in cls]}"
            )


# ---------------------------------------------------------------------------
# Frame bound type & frame bound
# ---------------------------------------------------------------------------

class PyLegendFrameBoundType(Enum):
    UNBOUNDED_PRECEDING = 1
    PRECEDING = 2
    CURRENT_ROW = 3
    FOLLOWING = 4
    UNBOUNDED_FOLLOWING = 5

    def to_sql_node(self, query: QuerySpecification, config: FrameToSqlConfig) -> FrameBoundType:
        _map = {
            PyLegendFrameBoundType.UNBOUNDED_PRECEDING: FrameBoundType.UNBOUNDED_PRECEDING,
            PyLegendFrameBoundType.PRECEDING: FrameBoundType.PRECEDING,
            PyLegendFrameBoundType.CURRENT_ROW: FrameBoundType.CURRENT_ROW,
            PyLegendFrameBoundType.FOLLOWING: FrameBoundType.FOLLOWING,
            PyLegendFrameBoundType.UNBOUNDED_FOLLOWING: FrameBoundType.UNBOUNDED_FOLLOWING,
        }
        return _map[self]

    def to_pure_expression(self) -> str:
        _map = {
            PyLegendFrameBoundType.UNBOUNDED_PRECEDING: "unbounded()",
            PyLegendFrameBoundType.PRECEDING: "0",
            PyLegendFrameBoundType.CURRENT_ROW: "0",
            PyLegendFrameBoundType.FOLLOWING: "0",
            PyLegendFrameBoundType.UNBOUNDED_FOLLOWING: "unbounded()",
        }
        return _map[self]


class PyLegendFrameBound:
    type_: PyLegendFrameBoundType
    value: PyLegendOptional[PyLegendUnion[int, float, PythonDecimal]]
    duration_unit: PyLegendOptional[PyLegendDurationUnit]

    def __init__(
        self,
        type_: PyLegendFrameBoundType,
        value: PyLegendOptional[PyLegendUnion[int, float, PythonDecimal]] = None,
        duration_unit: PyLegendOptional[PyLegendDurationUnit] = None,
    ) -> None:
        self.type_ = type_
        self.value = value
        self.duration_unit = duration_unit

    def to_sql_node(
        self,
        query: QuerySpecification,
        config: FrameToSqlConfig,
    ) -> FrameBound:
        value_expression: PyLegendOptional[Expression] = None
        if self.value is not None:
            abs_val: PyLegendUnion[int, float, PythonDecimal] = abs(self.value)  # type: ignore
            value_expression = (
                convert_literal_to_literal_expression(abs_val)
                .to_sql_expression({"w": query}, config)
            )
        duration_unit_node = (
            self.duration_unit.to_sql_node(query, config)
            if self.duration_unit is not None else None
        )
        return FrameBound(
            type_=self.type_.to_sql_node(query, config),
            value=value_expression,
            duration_unit=duration_unit_node,
        )

    def to_pure_expression(self, config: FrameToPureConfig) -> str:
        if self.type_ in (PyLegendFrameBoundType.UNBOUNDED_PRECEDING,
                          PyLegendFrameBoundType.UNBOUNDED_FOLLOWING):
            return "unbounded()"

        if self.type_ == PyLegendFrameBoundType.CURRENT_ROW:
            expr = "0"
        elif self._should_use_value():
            value_expr = convert_literal_to_literal_expression(self.value).to_pure_expression(config)
            if self.type_ == PyLegendFrameBoundType.PRECEDING:
                expr = f"minus({value_expr})"
            else:
                expr = value_expr
        else:
            expr = self.type_.to_pure_expression()

        if self.duration_unit is not None:
            expr += f", DurationUnit.{self.duration_unit.to_pure_expression(config)}"

        return expr

    def _should_use_value(self) -> bool:
        if self.type_ in (PyLegendFrameBoundType.UNBOUNDED_PRECEDING, PyLegendFrameBoundType.UNBOUNDED_FOLLOWING):
            return False
        return self.value is not None


# ---------------------------------------------------------------------------
# Window frame mode & window frame
# ---------------------------------------------------------------------------

class PyLegendWindowFrameMode(Enum):
    RANGE = 1
    ROWS = 2

    def to_sql_node(self) -> WindowFrameMode:
        _map = {
            PyLegendWindowFrameMode.RANGE: WindowFrameMode.RANGE,
            PyLegendWindowFrameMode.ROWS: WindowFrameMode.ROWS,
        }
        return _map[self]

    def to_pure_expression(self) -> str:
        _map = {
            PyLegendWindowFrameMode.RANGE: "_range",
            PyLegendWindowFrameMode.ROWS: "rows",
        }
        return _map[self]


class PyLegendWindowFrame:
    mode: PyLegendWindowFrameMode
    start: PyLegendFrameBound
    end: PyLegendOptional[PyLegendFrameBound]

    def __init__(
        self,
        mode: PyLegendWindowFrameMode,
        start: PyLegendFrameBound,
        end: PyLegendOptional[PyLegendFrameBound] = None,
    ) -> None:
        self.mode = mode
        self.start = start
        self.end = end

    def to_sql_node(
        self,
        query: QuerySpecification,
        config: FrameToSqlConfig,
    ) -> WindowFrame:
        return WindowFrame(
            mode=self.mode.to_sql_node(),
            start=self.start.to_sql_node(query, config),
            end=(
                None if self.end is None
                else self.end.to_sql_node(query, config)
            ),
        )

    def to_pure_expression(self, config: FrameToPureConfig) -> str:
        mode_expr = self.mode.to_pure_expression()
        start_expr = self.start.to_pure_expression(config)
        end_expr = (
            "" if self.end is None
            else ", " + self.end.to_pure_expression(config)
        )
        return f"{mode_expr}({start_expr}{end_expr})"


# ---------------------------------------------------------------------------
# Window
# ---------------------------------------------------------------------------

class PyLegendWindow:
    __partition_by: PyLegendOptional[PyLegendList[str]]
    __order_by: PyLegendOptional[PyLegendList[PyLegendSortInfo]]
    __frame: PyLegendOptional[PyLegendWindowFrame]

    def __init__(
            self,
            partition_by: PyLegendOptional[PyLegendList[str]] = None,
            order_by: PyLegendOptional[PyLegendList[PyLegendSortInfo]] = None,
            frame: PyLegendOptional[PyLegendWindowFrame] = None
    ) -> None:
        self.__partition_by = partition_by
        self.__order_by = order_by
        self.__frame = frame

    def to_sql_node(
            self,
            query: QuerySpecification,
            config: FrameToSqlConfig
    ) -> Window:
        return Window(
            windowRef=None,
            partitions=(
                [] if self.__partition_by is None else
                [PyLegendWindow.__find_column_expression(query, col, config) for col in self.__partition_by]
            ),
            orderBy=(
                [] if self.__order_by is None else
                [sort_info.to_sql_node(query, config) for sort_info in self.__order_by]
            ),
            windowFrame=(
                None if self.__frame is None
                else self.__frame.to_sql_node(query, config)
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
            "" if self.__partition_by is None or len(self.__partition_by) == 0
            else "~[" + (', '.join(map(escape_column_name, self.__partition_by))) + "], "
        )
        sorts_str = (
            "[]" if self.__order_by is None or len(self.__order_by) == 0
            else "[" + (', '.join([s.to_pure_expression(config) for s in self.__order_by])) + "]"
        )
        frame_str = (
            "" if self.__frame is None
            else ", " + self.__frame.to_pure_expression(config)
        )
        return f"over({partitions_str}{sorts_str}{frame_str})"


# ---------------------------------------------------------------------------
# Partial frame & window reference
# ---------------------------------------------------------------------------

class PyLegendPartialFrame:
    __var_name: str

    def __init__(self, var_name: str) -> None:
        self.__var_name = var_name

    def to_pure_expression(self, config: FrameToPureConfig) -> str:
        return f"${self.__var_name}"


class PyLegendWindowReference:
    __window: PyLegendWindow
    __var_name: str

    def __init__(self, window: PyLegendWindow, var_name: str) -> None:
        self.__window = window
        self.__var_name = var_name

    def to_pure_expression(self, config: FrameToPureConfig) -> str:
        return f"${self.__var_name}"


# ---------------------------------------------------------------------------
# Window function expressions
#
# These accept any object that has a ``to_pure_expression(config)`` method
# (duck-typed) for the ``partial_frame``, ``window_ref`` and ``row``
# parameters, making them reusable across both the Pandas and LegendQL APIs.
# ---------------------------------------------------------------------------

class PyLegendRowNumberExpression(PyLegendExpressionIntegerReturn):
    __partial_frame: PyLegendPartialFrame
    __row: object  # any object with to_pure_expression(config) -> str

    def __init__(
            self,
            partial_frame: PyLegendPartialFrame,
            row: object,
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
        return (f"{self.__partial_frame.to_pure_expression(config)}"
                f"->rowNumber({self.__row.to_pure_expression(config)})")  # type: ignore


class PyLegendRankExpression(PyLegendExpressionIntegerReturn):
    __partial_frame: PyLegendPartialFrame
    __window_ref: "PyLegendWindowReference"
    __row: object

    def __init__(
            self,
            partial_frame: PyLegendPartialFrame,
            window_ref: "PyLegendWindowReference",
            row: object,
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
                f"{self.__window_ref.to_pure_expression(config)}, "
                f"{self.__row.to_pure_expression(config)})")  # type: ignore


class PyLegendDenseRankExpression(PyLegendExpressionIntegerReturn):
    __partial_frame: PyLegendPartialFrame
    __window_ref: "PyLegendWindowReference"
    __row: object

    def __init__(
            self,
            partial_frame: PyLegendPartialFrame,
            window_ref: "PyLegendWindowReference",
            row: object,
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
                f"{self.__window_ref.to_pure_expression(config)}, "
                f"{self.__row.to_pure_expression(config)})")  # type: ignore


class PyLegendPercentRankExpression(PyLegendExpressionFloatReturn):
    __partial_frame: PyLegendPartialFrame
    __window_ref: "PyLegendWindowReference"
    __row: object

    def __init__(
            self,
            partial_frame: PyLegendPartialFrame,
            window_ref: "PyLegendWindowReference",
            row: object,
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
                f"{self.__window_ref.to_pure_expression(config)}, "
                f"{self.__row.to_pure_expression(config)})")  # type: ignore


class PyLegendCumeDistExpression(PyLegendExpressionFloatReturn):
    __partial_frame: PyLegendPartialFrame
    __window_ref: "PyLegendWindowReference"
    __row: object

    def __init__(
            self,
            partial_frame: PyLegendPartialFrame,
            window_ref: "PyLegendWindowReference",
            row: object,
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
                f"{self.__window_ref.to_pure_expression(config)}, "
                f"{self.__row.to_pure_expression(config)})")  # type: ignore


class PyLegendNtileExpression(PyLegendExpressionIntegerReturn):
    __partial_frame: PyLegendPartialFrame
    __row: object
    __num_buckets: int

    def __init__(
            self,
            partial_frame: PyLegendPartialFrame,
            row: object,
            num_buckets: int,
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
        return (f"{self.__partial_frame.to_pure_expression(config)}"
                f"->ntile({self.__row.to_pure_expression(config)}, "  # type: ignore
                f"{self.__num_buckets})")

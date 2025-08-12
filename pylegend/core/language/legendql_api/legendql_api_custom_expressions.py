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
)
from pylegend._typing import (
    PyLegendSequence,
    PyLegendOptional,
    PyLegendList,
    PyLegendDict,
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
    QualifiedName, IntegerLiteral
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


class LegendQLApiWindowFrame(metaclass=ABCMeta):
    pass


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
            windowFrame=None
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
        return f"over({partitions_str}, {sorts_str})"


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

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
)
from pylegend._typing import (
    PyLegendSequence,
    PyLegendOptional,
    PyLegendList,
    PyLegendDict,
    TYPE_CHECKING,
)
from pylegend.core.language.shared.expression import (
    PyLegendExpressionFloatReturn,
    PyLegendExpressionIntegerReturn,
)
from pylegend.core.language.shared.helpers import escape_column_name
from pylegend.core.sql.metamodel import (
    Expression,
    FunctionCall,
    QualifiedName,
    QuerySpecification,
    SingleColumn,
    SortItem,
    SortItemNullOrdering,
    SortItemOrdering,
    Window
)
from pylegend.core.tds.tds_frame import (
    FrameToPureConfig,
    FrameToSqlConfig,
)

__all__: PyLegendSequence[str] = [
    "PandasApiPrimitive",
    "PandasApiBoolean",
    "PandasApiString",
    "PandasApiNumber",
    "PandasApiInteger",
    "PandasApiFloat",
    "PandasApiDate",
    "PandasApiDateTime",
    "PandasApiStrictDate",
    "PandasApiSortInfo",
    "PandasApiSortDirection",
    "PandasApiWindow",
    "PandasApiWindowReference",
    "PandasApiWindowFrame",
    "PandasApiRankExpression",
    "PandasApiDenseRankExpression",
    "PandasApiRowNumberExpression",
    "PandasApiPartialFrame",
    "PandasApiPercentRankExpression",
]


class PandasApiPrimitive(PyLegendPrimitive, metaclass=ABCMeta):
    pass


class PandasApiBoolean(PandasApiPrimitive, PyLegendBoolean):
    def __init__(self, expr: PyLegendBoolean):
        PyLegendBoolean.__init__(self, expr.value())


class PandasApiString(PandasApiPrimitive, PyLegendString):
    def __init__(self, expr: PyLegendString):
        PyLegendString.__init__(self, expr.value())


class PandasApiNumber(PandasApiPrimitive, PyLegendNumber):
    def __init__(self, expr: PyLegendNumber):
        PyLegendNumber.__init__(self, expr.value())


class PandasApiInteger(PandasApiPrimitive, PyLegendInteger):
    def __init__(self, expr: PyLegendInteger):
        PyLegendInteger.__init__(self, expr.value())


class PandasApiFloat(PandasApiPrimitive, PyLegendFloat):
    def __init__(self, expr: PyLegendFloat):
        PyLegendFloat.__init__(self, expr.value())


class PandasApiDate(PandasApiPrimitive, PyLegendDate):
    def __init__(self, expr: PyLegendDate):
        PyLegendDate.__init__(self, expr.value())


class PandasApiDateTime(PandasApiPrimitive, PyLegendDateTime):
    def __init__(self, expr: PyLegendDateTime):
        PyLegendDateTime.__init__(self, expr.value())


class PandasApiStrictDate(PandasApiPrimitive, PyLegendStrictDate):
    def __init__(self, expr: PyLegendStrictDate):
        PyLegendStrictDate.__init__(self, expr.value())


class PandasApiSortDirection(Enum):
    ASC = 1,
    DESC = 2


class PandasApiSortInfo:
    __column: str
    __direction: PandasApiSortDirection
    __null_ordering: SortItemNullOrdering

    def __init__(
            self,
            column_name: str,
            direction: PandasApiSortDirection,
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
            ordering=(SortItemOrdering.ASCENDING if self.__direction == PandasApiSortDirection.ASC
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
        func = 'ascending' if self.__direction == PandasApiSortDirection.ASC else 'descending'
        return f"{func}(~{escape_column_name(self.__column)})"


class PandasApiWindowFrame(metaclass=ABCMeta):
    pass


class PandasApiWindow:
    __partition_by: PyLegendOptional[PyLegendList[str]]
    __order_by: PyLegendOptional[PyLegendList[PandasApiSortInfo]]
    __frame: PyLegendOptional[PandasApiWindowFrame]

    def __init__(
            self,
            partition_by: PyLegendOptional[PyLegendList[str]] = None,
            order_by: PyLegendOptional[PyLegendList[PandasApiSortInfo]] = None,
            frame: PyLegendOptional[PandasApiWindowFrame] = None
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
                [PandasApiWindow.__find_column_expression(query, col, config) for col in self.__partition_by]
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
            "" if self.__partition_by is None or len(self.__partition_by) == 0
            else "~[" + (', '.join(map(escape_column_name, self.__partition_by))) + "], "
        )
        sorts_str = (
            "[]" if self.__order_by is None or len(self.__order_by) == 0
            else "[" + (', '.join([s.to_pure_expression(config) for s in self.__order_by])) + "]"
        )
        return f"over({partitions_str}{sorts_str})"


class PandasApiPartialFrame:
    if TYPE_CHECKING:
        from pylegend.core.tds.pandas_api.frames.pandas_api_base_tds_frame import PandasApiBaseTdsFrame
        from pylegend.core.language.pandas_api.pandas_api_tds_row import PandasApiTdsRow

    __base_frame: "PandasApiBaseTdsFrame"
    __var_name: str

    def __init__(self, base_frame: "PandasApiBaseTdsFrame", var_name: str) -> None:
        self.__base_frame = base_frame
        self.__var_name = var_name

    def row_number(
            self,
            row: "PandasApiTdsRow"
    ) -> PyLegendInteger:
        return PyLegendInteger(PandasApiRowNumberExpression(self, row))

    def rank(
            self,
            window: "PandasApiWindowReference",
            row: "PandasApiTdsRow"
    ) -> PyLegendInteger:
        return PyLegendInteger(PandasApiRankExpression(self, window, row))

    def dense_rank(
            self,
            window: "PandasApiWindowReference",
            row: "PandasApiTdsRow"
    ) -> PyLegendInteger:
        return PyLegendInteger(PandasApiDenseRankExpression(self, window, row))

    def percent_rank(
            self,
            window: "PandasApiWindowReference",
            row: "PandasApiTdsRow"
    ) -> PyLegendFloat:
        return PyLegendFloat(PandasApiPercentRankExpression(self, window, row))

    def to_pure_expression(self, config: FrameToPureConfig) -> str:
        return f"${self.__var_name}"


class PandasApiWindowReference:
    __window: PandasApiWindow
    __var_name: str

    def __init__(self, window: PandasApiWindow, var_name: str) -> None:
        self.__window = window
        self.__var_name = var_name

    def to_pure_expression(self, config: FrameToPureConfig) -> str:
        return f"${self.__var_name}"


class PandasApiRowNumberExpression(PyLegendExpressionIntegerReturn):
    if TYPE_CHECKING:
        from pylegend.core.language.pandas_api.pandas_api_tds_row import PandasApiTdsRow

    __partial_frame: PandasApiPartialFrame
    __row: "PandasApiTdsRow"

    def __init__(
            self,
            partial_frame: PandasApiPartialFrame,
            row: "PandasApiTdsRow"
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


class PandasApiRankExpression(PyLegendExpressionIntegerReturn):
    if TYPE_CHECKING:
        from pylegend.core.language.pandas_api.pandas_api_tds_row import PandasApiTdsRow

    __partial_frame: PandasApiPartialFrame
    __window_ref: "PandasApiWindowReference"
    __row: "PandasApiTdsRow"

    def __init__(
            self,
            partial_frame: PandasApiPartialFrame,
            window_ref: "PandasApiWindowReference",
            row: "PandasApiTdsRow"
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


class PandasApiDenseRankExpression(PyLegendExpressionIntegerReturn):
    if TYPE_CHECKING:
        from pylegend.core.language.pandas_api.pandas_api_tds_row import PandasApiTdsRow

    __partial_frame: PandasApiPartialFrame
    __window_ref: "PandasApiWindowReference"
    __row: "PandasApiTdsRow"

    def __init__(
            self,
            partial_frame: PandasApiPartialFrame,
            window_ref: "PandasApiWindowReference",
            row: "PandasApiTdsRow"
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


class PandasApiPercentRankExpression(PyLegendExpressionFloatReturn):
    if TYPE_CHECKING:
        from pylegend.core.language.pandas_api.pandas_api_tds_row import PandasApiTdsRow

    __partial_frame: PandasApiPartialFrame
    __window_ref: "PandasApiWindowReference"
    __row: "PandasApiTdsRow"

    def __init__(
            self,
            partial_frame: PandasApiPartialFrame,
            window_ref: "PandasApiWindowReference",
            row: "PandasApiTdsRow"
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

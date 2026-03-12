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

from pylegend._typing import (
    PyLegendSequence,
    PyLegendCallable,
    PyLegendOptional,
    PyLegendList,
    PyLegendDict,
)
from pylegend.core.language.shared.expression import PyLegendExpressionIntegerReturn
from pylegend.core.language.shared.primitives import (
    PyLegendInteger,
)
from pylegend.core.language.shared.primitives.primitive import PyLegendPrimitiveOrPythonPrimitive
from pylegend.core.language.shared.primitive_collection import PyLegendPrimitiveCollection
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
)
from pylegend.core.tds.tds_frame import FrameToSqlConfig, FrameToPureConfig
from typing import TYPE_CHECKING

__all__: PyLegendSequence[str] = [
    "LegacyApiOLAPGroupByOperation",
    "LegacyApiOLAPAggregation",
    "LegacyApiOLAPRank",
    "olap_agg",
    "olap_rank",
    "LegacyApiSortInfo",
    "LegacyApiWindow",
    "LegacyApiPartialFrame",
    "LegacyApiRankExpression",
    "LegacyApiDenseRankExpression",
]


class LegacyApiOLAPGroupByOperation:
    def __init__(self, _type: str, name: PyLegendOptional[str]) -> None:
        self._type = _type

        if name and not isinstance(name, str):
            raise TypeError('"name" should be a string')
        self.name = name


class LegacyApiOLAPAggregation(LegacyApiOLAPGroupByOperation):
    def __init__(
            self,
            column_name: str,
            function: PyLegendCallable[[PyLegendPrimitiveCollection], PyLegendPrimitiveOrPythonPrimitive]
    ) -> None:
        self.column_name = column_name

        if not callable(function):
            raise TypeError('Function should be a lambda which takes in a mapped list as a parameter')
        param_check_fail = function.__code__.co_argcount != 1
        if param_check_fail:
            raise TypeError('Function should be a lambda which takes in a mapped list as a parameter')
        self.function = function

        super().__init__(_type='tdsOlapAggregation', name=None)


class LegacyApiOLAPRank(LegacyApiOLAPGroupByOperation):
    def __init__(self, rank: PyLegendCallable[["LegacyApiPartialFrame"], PyLegendPrimitiveOrPythonPrimitive]) -> None:
        if not callable(rank):
            raise TypeError('Rank function should be a lambda which takes in a mapped list as a parameter')
        param_check_fail = rank.__code__.co_argcount != 1
        if param_check_fail:
            raise TypeError('Rank function should be a lambda which takes in a mapped list as a parameter')
        self.rank = rank

        super().__init__(_type='tdsOlapRank', name=None)


def olap_agg(
        column_name: str,
        function: PyLegendCallable[[PyLegendPrimitiveCollection], PyLegendPrimitiveOrPythonPrimitive]
) -> LegacyApiOLAPAggregation:
    return LegacyApiOLAPAggregation(column_name=column_name, function=function)


def olap_rank(rank: PyLegendCallable[["LegacyApiPartialFrame"], PyLegendPrimitiveOrPythonPrimitive]) -> LegacyApiOLAPRank:
    return LegacyApiOLAPRank(rank=rank)


class LegacyApiSortInfo:
    __column: str
    __direction: str

    def __init__(self, column: str, direction: str = "ASC") -> None:
        if direction.upper() not in ("ASC", "DESC"):
            raise ValueError(
                f"Sort direction must be 'ASC' or 'DESC' (case insensitive). Got: '{direction}'"
            )
        self.__column = column
        self.__direction = direction.upper()

    def get_column(self) -> str:
        return self.__column

    def get_direction(self) -> str:
        return self.__direction

    def to_sql_sort_item(
            self,
            query: QuerySpecification,
            config: FrameToSqlConfig
    ) -> SortItem:
        return SortItem(
            sortKey=self.__find_column_expression(query, config),
            ordering=(SortItemOrdering.ASCENDING if self.__direction == "ASC"
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
        func = 'ascending' if self.__direction == 'ASC' else 'descending'
        return f"{func}(~{escape_column_name(self.__column)})"


class LegacyApiWindow:
    __partition_by: PyLegendOptional[PyLegendList[str]]
    __order_by: PyLegendOptional[PyLegendList[LegacyApiSortInfo]]

    def __init__(
            self,
            partition_by: PyLegendOptional[PyLegendList[str]] = None,
            order_by: PyLegendOptional[PyLegendList[LegacyApiSortInfo]] = None,
    ) -> None:
        self.__partition_by = partition_by
        self.__order_by = order_by

    def get_partition_by(self) -> PyLegendOptional[PyLegendList[str]]:
        return self.__partition_by

    def get_order_by(self) -> PyLegendOptional[PyLegendList[LegacyApiSortInfo]]:
        return self.__order_by

    def to_sql_node(
            self,
            query: QuerySpecification,
            config: FrameToSqlConfig
    ) -> Window:
        return Window(
            windowRef=None,
            partitions=(
                [] if self.__partition_by is None else
                [LegacyApiWindow.__find_column_expression(query, col, config)
                 for col in self.__partition_by]
            ),
            orderBy=(
                [] if self.__order_by is None else
                [sort_info.to_sql_sort_item(query, config) for sort_info in self.__order_by]
            ),
            windowFrame=None,
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


class LegacyApiPartialFrame:
    if TYPE_CHECKING:
        from pylegend.core.tds.legacy_api.frames.legacy_api_base_tds_frame import LegacyApiBaseTdsFrame
        from pylegend.core.language.legacy_api.legacy_api_tds_row import LegacyApiTdsRow

    __base_frame: "LegacyApiBaseTdsFrame"
    __var_name: str

    def __init__(self, base_frame: "LegacyApiBaseTdsFrame", var_name: str) -> None:
        self.__base_frame = base_frame
        self.__var_name = var_name

    def rank(self) -> PyLegendInteger:
        return PyLegendInteger(LegacyApiRankExpression(self))

    def dense_rank(self) -> PyLegendInteger:
        return PyLegendInteger(LegacyApiDenseRankExpression(self))

    def to_pure_expression(self, config: FrameToPureConfig) -> str:
        return f"${self.__var_name}"

    def get_base_frame(self) -> "LegacyApiBaseTdsFrame":
        return self.__base_frame


class LegacyApiRankExpression(PyLegendExpressionIntegerReturn):
    __partial_frame: LegacyApiPartialFrame

    def __init__(self, partial_frame: LegacyApiPartialFrame) -> None:
        self.__partial_frame = partial_frame

    def to_sql_expression(
            self,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        return FunctionCall(
            name=QualifiedName(parts=["rank"]), distinct=False, arguments=[], filter_=None, window=None
        )

    def to_pure_expression(self, config: FrameToPureConfig) -> str:
        return f"{self.__partial_frame.to_pure_expression(config)}->rank()"


class LegacyApiDenseRankExpression(PyLegendExpressionIntegerReturn):
    __partial_frame: LegacyApiPartialFrame

    def __init__(self, partial_frame: LegacyApiPartialFrame) -> None:
        self.__partial_frame = partial_frame

    def to_sql_expression(
            self,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        return FunctionCall(
            name=QualifiedName(parts=["dense_rank"]), distinct=False, arguments=[], filter_=None, window=None
        )

    def to_pure_expression(self, config: FrameToPureConfig) -> str:
        return f"{self.__partial_frame.to_pure_expression(config)}->denseRank()"

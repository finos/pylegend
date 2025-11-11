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
    PyLegendOptional,
    PyLegendCallable,
    PyLegendList,
    PyLegendSequence,
)
from pylegend.core.language.shared.tds_row import AbstractTdsRow
from pylegend.core.sql.metamodel import (
    QuerySpecification,
    SortItemOrdering,
    SortItem,
    SortItemNullOrdering, SingleColumn, Expression,
)
from pylegend.core.tds.pandas_api.frames.pandas_api_applied_function_tds_frame import (
    PandasApiAppliedFunction,
)
from pylegend.core.tds.pandas_api.frames.pandas_api_base_tds_frame import (
    PandasApiBaseTdsFrame,
)
from pylegend.core.tds.sql_query_helpers import create_sub_query, copy_query
from pylegend.core.tds.tds_frame import FrameToSqlConfig, FrameToPureConfig
from pylegend.core.tds.tds_column import TdsColumn
from pylegend.core.language.shared.helpers import escape_column_name


class SortValuesFunction(PandasApiAppliedFunction):
    __base_frame: PandasApiBaseTdsFrame
    __by: PyLegendList[str]
    __axis: PyLegendUnion[str, int]
    __ascending: PyLegendList[bool]
    __inplace: bool
    __kind: PyLegendOptional[str]
    __na_position: str
    __ignore_index: bool
    key: PyLegendOptional[PyLegendCallable[[AbstractTdsRow], AbstractTdsRow]] = None

    @classmethod
    def name(cls) -> str:
        return "sort_values"  # pragma: no cover

    def __init__(
            self,
            base_frame: PandasApiBaseTdsFrame,
            by: PyLegendUnion[str, PyLegendList[str]],
            axis: PyLegendUnion[str, int],
            ascending: PyLegendUnion[bool, PyLegendList[bool]],
            inplace: bool,
            kind: PyLegendOptional[str],
            na_position: str,
            ignore_index: bool,
            key: PyLegendOptional[PyLegendCallable[[AbstractTdsRow], AbstractTdsRow]] = None
    ) -> None:
        self.__base_frame = base_frame
        self.__by_input = by
        self.__axis = axis
        self.__ascending_input = ascending
        self.__inplace = inplace
        self.__kind = kind
        self.__na_position = na_position
        self.__ignore_index = ignore_index
        self.__key = key

    def to_sql(self, config: FrameToSqlConfig) -> QuerySpecification:
        base_query: QuerySpecification = self.__base_frame.to_sql_query_object(config)
        should_create_sub_query = (base_query.offset is not None) or (base_query.limit is not None)
        new_query = (
            create_sub_query(base_query, config, "root") if should_create_sub_query
            else copy_query(base_query)
        )
        new_query.orderBy = [
            SortItem(
                sortKey=self.get_expression_from_column_name(new_query, column_name, config),
                ordering=(
                    SortItemOrdering.ASCENDING if ascending
                    else SortItemOrdering.DESCENDING
                ),
                nullOrdering=SortItemNullOrdering.UNDEFINED,
            )
            for column_name, ascending in zip(self.__by, self.__ascending)
        ]
        return new_query

    def get_expression_from_column_name(self, query: QuerySpecification, column_name: str,
                                        config: FrameToSqlConfig) -> Expression:
        db_extension = config.sql_to_string_generator().get_db_extension()
        filtered = [
            s for s in query.select.selectItems
            if (isinstance(s, SingleColumn) and
                s.alias == db_extension.quote_identifier(column_name))
        ]
        if len(filtered) == 0:
            raise RuntimeError("Cannot find column: " + column_name)  # pragma: no cover
        return filtered[0].expression

    def to_pure(self, config: FrameToPureConfig) -> str:
        escaped_columns = []
        for col_name in self.__by:
            escaped_columns.append(escape_column_name(col_name))
        sort_items = [
            f"~{column_name}->ascending()" if ascending else f"~{column_name}->descending()"
            for column_name, ascending in zip(escaped_columns, self.__ascending)
        ]
        return (
                f"{self.__base_frame.to_pure(config)}{config.separator(1)}"
                + f"->sort([{', '.join(sort_items)}])"
        )

    def base_frame(self) -> PandasApiBaseTdsFrame:
        return self.__base_frame

    def tds_frame_parameters(self) -> PyLegendList["PandasApiBaseTdsFrame"]:
        return []

    def calculate_columns(self) -> PyLegendSequence["TdsColumn"]:
        return [c.copy() for c in self.__base_frame.columns()]

    def validate(self) -> bool:
        if self.__axis not in [0, "index"]:
            raise ValueError(
                "Axis parameter of sort_values function must be 0 or 'index'"
            )

        if self.__inplace is not False:
            raise ValueError("Inplace parameter of sort_values function must be False")

        if self.__kind is not None:
            raise NotImplementedError(
                "Kind parameter of sort_values function is not supported"
            )

        if self.__ignore_index is not True:
            raise ValueError(
                "Ignore_index parameter of sort_values function must be True"
            )

        if self.__key is not None:
            raise NotImplementedError(
                "Key parameter of sort_values function is not supported"
            )

        base_frame_columns = [
            column.get_name() for column in self.__base_frame.columns()
        ]

        self.__by = self._build_by_list()
        self.__ascending = self._build_ascending_list()

        if len(self.__by) != len(self.__ascending):
            raise ValueError(
                "The number of columns in 'by' must equal the number of values in 'ascending' for sort_values function."
            )

        for column in self.__by:
            if column not in base_frame_columns:
                raise ValueError(
                    f"Column - '{column}' in sort_values columns list doesn't exist in the current frame. "
                    f"Current frame columns: {base_frame_columns}"
                )

        return True

    def _build_by_list(self) -> PyLegendList[str]:
        if isinstance(self.__by_input, str):
            return [self.__by_input]
        else:
            return self.__by_input

    def _build_ascending_list(self) -> PyLegendList[bool]:
        if self.__ascending_input is True:
            return [True for _ in self.__by]
        elif self.__ascending_input is False:
            return [False for _ in self.__by]
        else:
            return self.__ascending_input

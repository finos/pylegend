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

import re

from pylegend._typing import (
    PyLegendUnion,
    PyLegendOptional,
    PyLegendSequence,
    PyLegendList,
    PyLegendTuple
)
from pylegend.core.language import (
    PyLegendInteger,
)
from pylegend.core.language.shared.helpers import escape_column_name
from pylegend.core.sql.metamodel import (
    QuerySpecification,
    SingleColumn,
    SelectItem
)
from pylegend.core.tds.pandas_api.frames.pandas_api_applied_function_tds_frame import (
    PandasApiAppliedFunction,
)
from pylegend.core.tds.pandas_api.frames.pandas_api_base_tds_frame import (
    PandasApiBaseTdsFrame,
)
from pylegend.core.tds.sql_query_helpers import copy_query, create_sub_query
from pylegend.core.tds.tds_column import TdsColumn
from pylegend.core.tds.tds_frame import FrameToSqlConfig, FrameToPureConfig

__all__: PyLegendSequence[str] = ["PandasApiFilterFunction"]


class PandasApiFilterFunction(PandasApiAppliedFunction):
    __base_frame: PandasApiBaseTdsFrame
    __items: PyLegendOptional[PyLegendList[str]]
    __like: PyLegendOptional[str]
    __regex: PyLegendOptional[str]
    __axis: PyLegendOptional[PyLegendUnion[str, int, PyLegendInteger]]

    @classmethod
    def name(cls) -> str:
        return "filter"  # pragma: no cover

    def __init__(
            self,
            base_frame: PandasApiBaseTdsFrame,
            items: PyLegendOptional[PyLegendList[str]],
            like: PyLegendOptional[str],
            regex: PyLegendOptional[str],
            axis: PyLegendOptional[PyLegendUnion[str, int, PyLegendInteger]],
    ) -> None:
        self.__base_frame = base_frame
        self.__items = items
        self.__like = like
        self.__regex = regex
        self.__axis = 1 if axis is None else axis

    def __get_desired_columns(
            self, col_names: PyLegendSequence[str]
    ) -> PyLegendSequence[str]:
        if self.__items is not None:
            return self.__items
        elif self.__like is not None:
            return [col for col in col_names if self.__like in col]
        elif self.__regex is not None:
            regex_pattern = re.compile(self.__regex)
            return [col for col in col_names if regex_pattern.search(col)]

        return []  # pragma: no cover

    def to_sql(self, config: FrameToSqlConfig) -> QuerySpecification:
        base_query = self.__base_frame.to_sql_query_object(config)
        db_extension = config.sql_to_string_generator().get_db_extension()
        columns_to_retain = [db_extension.quote_identifier(x) for x in
                             self.__get_desired_columns([c.get_name() for c in self.__base_frame.columns()])]

        sub_query_required = (
                len(base_query.groupBy) > 0 or
                len(base_query.orderBy) > 0 or
                base_query.having is not None or
                base_query.select.distinct
        )

        if sub_query_required:
            new_query = create_sub_query(base_query, config, "root", columns_to_retain=columns_to_retain)
            return new_query
        else:
            new_cols_with_index: PyLegendList[PyLegendTuple[int, SelectItem]] = []
            for col in base_query.select.selectItems:
                if not isinstance(col, SingleColumn):
                    raise ValueError("Select operation not supported for queries with columns other than SingleColumn")
                if col.alias is None:
                    raise ValueError("Select operation not supported for queries with SingleColumns with missing alias")
                if col.alias in columns_to_retain:
                    new_cols_with_index.append((columns_to_retain.index(col.alias), col))

            new_select_items = [y[1] for y in sorted(new_cols_with_index, key=lambda x: x[0])]
            new_query = copy_query(base_query)
            new_query.select.selectItems = new_select_items
            return new_query

    def to_pure(self, config: FrameToPureConfig) -> str:
        col_names = [c.get_name() for c in self.__base_frame.columns()]
        desired_columns = self.__get_desired_columns(col_names)
        escaped_columns = [escape_column_name(col_name) for col_name in desired_columns]
        return (
            f"{self.__base_frame.to_pure(config)}{config.separator(1)}"
            f"->select(~[{', '.join(escaped_columns)}])"
        )

    def base_frame(self) -> PandasApiBaseTdsFrame:
        return self.__base_frame

    def tds_frame_parameters(self) -> PyLegendList["PandasApiBaseTdsFrame"]:
        return []

    def calculate_columns(self) -> PyLegendSequence["TdsColumn"]:
        base_cols = [c.copy() for c in self.__base_frame.columns()]
        desired_col_names = self.__get_desired_columns([c.get_name() for c in base_cols])
        return [
            base_col.copy()
            for base_col in base_cols
            if base_col.get_name() in desired_col_names
        ]

    def validate(self) -> bool:
        mutual_exclusion = sum(
            [
                self.__items is not None,
                self.__like is not None,
                self.__regex is not None,
            ]
        )
        if mutual_exclusion > 1:
            raise TypeError(
                "Keyword arguments `items`, `like`, or `regex` are mutually exclusive"
            )
        if mutual_exclusion == 0:
            raise TypeError("Must pass either `items`, `like`, or `regex`")

        base_cols = [c.get_name() for c in self.__base_frame.columns()]
        if self.__items is not None:
            if not isinstance(self.__items, (list, PyLegendList)):
                raise TypeError(
                    f"Index(...) must be called with a collection, got '{self.__items}'"
                )
            invalid_cols = [item for item in self.__items if item not in base_cols]
            if invalid_cols:
                raise ValueError(
                    f"Columns {invalid_cols} in `filter` items list do not exist. Available: {base_cols}"
                )

        if self.__like is not None:
            if not isinstance(self.__like, str):
                raise TypeError(f"'like' must be a string, got {type(self.__like)}")
            if not any(self.__like in col for col in base_cols):
                raise ValueError(
                    f"No columns match the pattern '{self.__like}'. Available: {base_cols}"
                )

        if self.__regex is not None:
            if not isinstance(self.__regex, str):
                raise TypeError(f"'regex' must be a string, got {type(self.__regex)}")
            try:
                regex_pattern = re.compile(self.__regex)
            except re.error as e:
                raise ValueError(f"Invalid regex pattern '{self.__regex}': {e}")
            if not any(regex_pattern.search(col) for col in base_cols):
                raise ValueError(
                    f"No columns match the regex '{self.__regex}'. Available: {base_cols}"
                )

        if not isinstance(
                self.__axis, (str, int, PyLegendInteger)
        ) or self.__axis not in [1, "columns"]:
            raise ValueError(
                f"Unsupported axis value: {self.__axis}. Expected 1 or 'columns'"
            )

        return True

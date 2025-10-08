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
from datetime import date, datetime

from pylegend.core.language.shared.literal_expressions import convert_literal_to_literal_expression

from pylegend.core.language.shared.primitive_collection import create_primitive_collection

from pylegend._typing import (
    PyLegendList,
    PyLegendSequence,
    PyLegendDict,
    PyLegendCallable,
    PyLegendUnion,
)

from pylegend.core.tds.pandas_api.frames.pandas_api_applied_function_tds_frame import PandasApiAppliedFunction
from pylegend.core.tds.sql_query_helpers import copy_query, create_sub_query
from pylegend.core.sql.metamodel import (
    QuerySpecification,
    SingleColumn, Select, QualifiedNameReference, QualifiedName,
)
from pylegend.core.tds.pandas_api.frames.pandas_api_base_tds_frame import PandasApiBaseTdsFrame
from pylegend.core.tds.tds_column import TdsColumn, PrimitiveTdsColumn
from pylegend.core.tds.tds_frame import FrameToSqlConfig, FrameToPureConfig
from pylegend.core.language import (
    LegacyApiTdsRow,
    PyLegendPrimitive,
    PyLegendInteger,
    PyLegendFloat,
    PyLegendNumber,
    PyLegendBoolean,
    PyLegendString,
)
from pylegend._typing import *

__all__: PyLegendSequence[str] = [
    "PandasApiFilterFunction"
]


class PandasApiFilterFunction(PandasApiAppliedFunction):
    __base_frame: PandasApiBaseTdsFrame
    __items: PyLegendUnion[list, PyLegendList]
    __like: str
    __regex: str
    __axis: PyLegendUnion[str, int, PyLegendInteger]

    @classmethod
    def name(cls) -> str:
        return "filter"

    def __init__(
            self,
            base_frame: PandasApiBaseTdsFrame,
            items: PyLegendOptional[PyLegendUnion[list, PyLegendList]],
            like: PyLegendOptional[str],
            regex: PyLegendOptional[str],
            axis: PyLegendUnion[str, int, PyLegendInteger]
    ) -> None:
        self.__base_frame = base_frame
        self.__items = items
        self.__like = like
        self.__regex = regex
        self.__axis = 1 if axis is None else axis

    def __get_desired_columns(self, col_names: PyLegendSequence[str]) -> PyLegendSequence[str]:
        if self.__items is not None:
            return self.__items
        elif self.__like is not None:
            return [col for col in col_names if self.__like in col]
        elif self.__regex is not None:
            regex_pattern = re.compile(self.__regex)
            return [col for col in col_names if regex_pattern.search(col)]

        return []

    def to_sql(self, config: FrameToSqlConfig) -> QuerySpecification:
        base_query = self.__base_frame.to_sql_query_object(config)
        should_create_sub_query = base_query.select.distinct or (len(base_query.orderBy) > 0) or (
                len(base_query.groupBy) > 0) or (base_query.where is not None)
        base_query = create_sub_query(base_query, config, "root") if should_create_sub_query \
            else copy_query(base_query)

        col_names = [c.get_name() for c in self.__base_frame.columns()]
        desired_columns = [
            SingleColumn(
                alias=config.sql_to_string_generator().get_db_extension().quote_identifier(col),
                expression=QualifiedNameReference(QualifiedName(['"root"', col]))
            )
            for col in self.__get_desired_columns(col_names)
        ]

        base_query.select = Select(distinct=False, selectItems=desired_columns)
        return base_query

    def to_pure(self, config: FrameToPureConfig) -> str:
        col_names = [c.get_name() for c in self.__base_frame.columns()]
        desired_columns = self.__get_desired_columns(col_names)

        selected_columns = [
            col if col.isidentifier() else f"'{col.replace('\'', '\\\'')}'"
            for col in desired_columns
        ]
        return f"{self.__base_frame.to_pure(config)}{config.separator(1)}->select(~[{', '.join(selected_columns)}])"

    def base_frame(self) -> PandasApiBaseTdsFrame:
        return self.__base_frame

    def tds_frame_parameters(self) -> PyLegendList["PandasApiBaseTdsFrame"]:
        return []

    def calculate_columns(self) -> PyLegendSequence["TdsColumn"]:
        base_cols = [c.copy() for c in self.__base_frame.columns()]
        if self.__items is not None:
            return [base_col.copy() for c in self.__items for base_col in base_cols if c == base_col.get_name()]
        elif self.__like is not None:
            return [col.copy() for col in base_cols if self.__like in col.get_name()]
        elif self.__regex is not None:
            return [col.copy() for col in base_cols if re.search(self.__regex, col.get_name())]
        return []

    def validate(self) -> bool:
        mutual_exclusion = sum([self.__items is not None, self.__like is not None, self.__regex is not None])
        if mutual_exclusion > 1:
            raise TypeError("Keyword arguments `items`, `like`, or `regex` are mutually exclusive")
        if mutual_exclusion == 0:
            raise TypeError("Must pass either `items`, `like`, or `regex`")

        base_cols = [c.get_name() for c in self.__base_frame.columns()]
        if self.__items is not None:
            if not isinstance(self.__items, PyLegendUnion[list, PyLegendList]):
                raise TypeError(f"Index(...) must be called with a collection, got '{self.__items}'")
            invalid_cols = [item for item in self.__items if item not in base_cols]
            if invalid_cols:
                raise ValueError(f"Columns {invalid_cols} in `filter` items list do not exist. Available: {base_cols}")

        if self.__like is not None:
            if not isinstance(self.__like, str):
                raise TypeError(f"'like' must be a string, got {type(self.__like)}")
            if not any(self.__like in col for col in base_cols):
                raise ValueError(f"No columns match the pattern '{self.__like}'. Available: {base_cols}")

        if self.__regex is not None:
            if not isinstance(self.__regex, str):
                raise TypeError(f"'regex' must be a string, got {type(self.__regex)}")
            try:
                regex_pattern = re.compile(self.__regex)
            except re.error as e:
                raise ValueError(f"Invalid regex pattern '{self.__regex}': {e}")
            if not any(regex_pattern.search(col) for col in base_cols):
                raise ValueError(f"No columns match the regex '{self.__regex}'. Available: {base_cols}")

        if not isinstance(self.__axis, PyLegendUnion[str, int, PyLegendInteger]) or \
                self.__axis not in [1, "columns"]:
            raise ValueError(f"Unsupported axis value: {self.__axis}. Expected 1 or 'columns'")

        return True

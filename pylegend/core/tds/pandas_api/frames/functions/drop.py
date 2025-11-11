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
from typing import Any

from pylegend.core.language.shared.helpers import escape_column_name
from pylegend.core.language.shared.primitives.boolean import PyLegendBoolean

from pylegend.core.language.shared.primitives.integer import PyLegendInteger

from pylegend._typing import (
    PyLegendList,
    PyLegendTuple,
    PyLegendSequence,
    PyLegendUnion,
    PyLegendOptional
)
from pylegend.core.sql.metamodel import (
    QuerySpecification,
    SingleColumn,
    SelectItem
)
from pylegend.core.tds.pandas_api.frames.pandas_api_applied_function_tds_frame import PandasApiAppliedFunction
from pylegend.core.tds.pandas_api.frames.pandas_api_base_tds_frame import PandasApiBaseTdsFrame
from pylegend.core.tds.sql_query_helpers import copy_query
from pylegend.core.tds.tds_frame import FrameToPureConfig, FrameToSqlConfig
from pylegend.core.tds.tds_column import TdsColumn

__all__: PyLegendSequence[str] = [
    "PandasApiDropFunction"
]


class PandasApiDropFunction(PandasApiAppliedFunction):
    __base_frame: PandasApiBaseTdsFrame
    __labels: PyLegendOptional[PyLegendUnion[Any, PyLegendList[Any], PyLegendTuple[Any]]]
    __axis: PyLegendUnion[str, int, PyLegendInteger]
    __index: PyLegendOptional[PyLegendUnion[Any, PyLegendList[Any], PyLegendTuple[Any]]]
    __columns: PyLegendOptional[PyLegendUnion[Any, PyLegendList[Any], PyLegendTuple[Any]]]
    __level: PyLegendOptional[PyLegendUnion[int, PyLegendInteger, str]]
    __inplace: PyLegendUnion[bool, PyLegendBoolean]
    __error: str

    @classmethod
    def name(cls) -> str:
        return "drop"

    def __init__(
            self,
            base_frame: PandasApiBaseTdsFrame,
            labels: PyLegendOptional[PyLegendUnion[Any, PyLegendList[Any], PyLegendTuple[Any]]],
            axis: PyLegendUnion[str, int, PyLegendInteger],
            index: PyLegendOptional[PyLegendUnion[Any, PyLegendList[Any], PyLegendTuple[Any]]],
            columns: PyLegendOptional[PyLegendUnion[Any, PyLegendList[Any], PyLegendTuple[Any]]],
            level: PyLegendOptional[PyLegendUnion[int, PyLegendInteger, str]],
            inplace: PyLegendUnion[bool, PyLegendBoolean],
            error: str
    ) -> None:
        self.__base_frame = base_frame
        self.__labels = labels
        self.__axis = axis
        self.__index = index
        self.__columns = columns
        self.__level = level
        self.__inplace = inplace
        self.__error = error

    def to_sql(self, config: FrameToSqlConfig) -> QuerySpecification:
        base_query = self.__base_frame.to_sql_query_object(config)
        db_extension = config.sql_to_string_generator().get_db_extension()
        base_cols = [c.get_name() for c in self.__base_frame.columns()]

        if self.__error == "raise":
            not_found = [col for col in self.__columns if col not in base_cols]
            if not_found:
                raise KeyError(f"{not_found} not found in axis")

        columns_to_retain = [
            db_extension.quote_identifier(col)
            for col in base_cols if col not in self.__columns
        ]
        new_cols_with_index: PyLegendList[PyLegendTuple[int, 'SelectItem']] = []
        for col in base_query.select.selectItems:
            if not isinstance(col, SingleColumn):
                raise ValueError("Drop operation not supported for queries "
                                 "with columns other than SingleColumn")  # pragma: no cover
            if col.alias is None:
                raise ValueError("Drop operation not supported for queries "
                                 "with SingleColumns with missing alias")  # pragma: no cover
            if col.alias in columns_to_retain:
                new_cols_with_index.append((columns_to_retain.index(col.alias), col))

        new_select_items = [y[1] for y in sorted(new_cols_with_index, key=lambda x: x[0])]
        new_query = copy_query(base_query)
        new_query.select.selectItems = new_select_items
        return new_query

    def to_pure(self, config: FrameToPureConfig) -> str:
        base_cols = [c.get_name() for c in self.__base_frame.columns()]
        if self.__error == "raise":
            not_found = [col for col in self.__columns if col not in base_cols]
            if not_found:
                raise KeyError(f"{not_found} not found in axis")

        new_cols = []
        for col_name in self.__base_frame.columns():
            col_name = col_name.get_name()
            if col_name not in self.__columns:
                new_cols.append(escape_column_name(col_name))

        return (f"{self.__base_frame.to_pure(config)}{config.separator(1)}" +
                f"->select(~[{', '.join(new_cols)}])")

    def base_frame(self) -> PandasApiBaseTdsFrame:
        return self.__base_frame

    def tds_frame_parameters(self) -> PyLegendList["PandasApiBaseTdsFrame"]:
        return []

    def calculate_columns(self) -> PyLegendSequence["TdsColumn"]:
        base_cols = [c.copy() for c in self.__base_frame.columns()]
        if self.__columns is not None:
            new_cols = []
            for col in base_cols:
                if col.get_name() not in self.__columns:
                    new_cols.append(col.copy())
            return new_cols
        return base_cols

    def validate(self) -> bool:
        valid_paramters: int = 0
        if self.__axis is not None:
            if isinstance(self.__axis, PyLegendUnion[str, int, PyLegendInteger]):
                if self.__axis != 1 and self.__axis != "columns":
                    if self.__axis == 0 or self.__axis == "index":
                        raise NotImplementedError(f"Axis {self.__axis} is not supported for 'drop' function in PandasApi")
                    else:
                        raise ValueError(f"No axis named {self.__axis} for object type Tds DataFrame")
            else:
                raise TypeError(f"No axis named {self.__axis} for object type Tds DataFrame")
        if self.__level is not None:
            raise NotImplementedError("'level' parameter is not supported for 'drop' function in PandasApi")

        if self.__index is not None:
            raise NotImplementedError("'index' parameter is not supported for 'drop' function in PandasApi")

        if self.__labels is not None:
            valid_paramters += 1

            if self.__columns is None:
                self.__columns = self.__labels
            else:
                raise ValueError("Cannot specify both 'labels' and 'columns'")

        if self.__columns is not None:
            def _normalize_columns(columns):
                if columns is None:
                    return []
                if isinstance(columns, (str, int, float)):
                    return [columns]
                if isinstance(columns, dict):
                    return list(columns.keys())
                if isinstance(columns, (set, tuple, list)):
                    return list(columns)
                try:
                    import numpy as np
                    if isinstance(columns, np.ndarray):
                        return list(columns)
                except ImportError:
                    pass
                raise TypeError(f"Unsupported type for columns: {type(columns)}")
            valid_paramters += 1
            self.__columns = _normalize_columns(self.__columns)

        if isinstance(self.__inplace, PyLegendUnion[bool, PyLegendBoolean]):
            if self.__inplace is False:
                raise NotImplementedError(f"Only inplace=True is supported. Got inplace={self.__inplace!r}")
        else:
            raise TypeError(f"Inplace must be True. Got inplace={self.__inplace!r}")

        if valid_paramters == 0:
            raise ValueError("Need to specify at least one of 'labels' or 'columns'")

        return True

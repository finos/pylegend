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
    PyLegendList,
    PyLegendSet,
    PyLegendSequence,
    PyLegendUnion,
    PyLegendOptional
)
from pylegend.core.language.shared.primitives.boolean import PyLegendBoolean
from pylegend.core.language.shared.primitives.integer import PyLegendInteger
from pylegend.core.sql.metamodel import (
    QuerySpecification
)
from pylegend.core.tds.pandas_api.frames.functions.filter import PandasApiFilterFunction
from pylegend.core.tds.pandas_api.frames.pandas_api_applied_function_tds_frame import PandasApiAppliedFunction
from pylegend.core.tds.pandas_api.frames.pandas_api_base_tds_frame import PandasApiBaseTdsFrame
from pylegend.core.tds.tds_column import TdsColumn
from pylegend.core.tds.tds_frame import FrameToPureConfig, FrameToSqlConfig

__all__: PyLegendSequence[str] = [
    "PandasApiDropFunction"
]


class PandasApiDropFunction(PandasApiAppliedFunction):
    __base_frame: PandasApiBaseTdsFrame
    __labels: PyLegendOptional[PyLegendUnion[str, PyLegendSequence[str], PyLegendSet[str]]]
    __axis: PyLegendUnion[str, int, PyLegendInteger]
    __index: PyLegendOptional[PyLegendUnion[str, PyLegendSequence[str], PyLegendSet[str]]]
    __columns: PyLegendOptional[PyLegendUnion[str, PyLegendSequence[str], PyLegendSet[str]]]
    __level: PyLegendOptional[PyLegendUnion[int, PyLegendInteger, str]]
    __inplace: PyLegendUnion[bool, PyLegendBoolean]
    __errors: str

    @classmethod
    def name(cls) -> str:
        return "drop"  # pragma: no cover

    def __init__(
            self,
            base_frame: PandasApiBaseTdsFrame,
            labels: PyLegendOptional[PyLegendUnion[str, PyLegendSequence[str], PyLegendSet[str]]],
            axis: PyLegendUnion[str, int, PyLegendInteger],
            index: PyLegendOptional[PyLegendUnion[str, PyLegendSequence[str], PyLegendSet[str]]],
            columns: PyLegendOptional[PyLegendUnion[str, PyLegendSequence[str], PyLegendSet[str]]],
            level: PyLegendOptional[PyLegendUnion[int, PyLegendInteger, str]],
            inplace: PyLegendUnion[bool, PyLegendBoolean],
            errors: str
    ) -> None:
        self.__base_frame = base_frame
        self.__labels = labels
        self.__axis = axis
        self.__index = index
        self.__columns = columns
        self.__level = level
        self.__inplace = inplace
        self.__errors = errors

    def to_sql(self, config: FrameToSqlConfig) -> QuerySpecification:
        base_cols = [c.get_name() for c in self.__base_frame.columns()]

        if self.__errors == "raise":
            not_found = [col for col in self.__columns if col not in base_cols]  # type: ignore
            if not_found:
                raise KeyError(f"{not_found} not found in axis")

        columns_to_retain = [col for col in base_cols if col not in self.__columns]  # type: ignore
        filter_func = PandasApiFilterFunction(
            base_frame=self.__base_frame,
            items=columns_to_retain,
            like=None,
            regex=None,
            axis=1
        )
        return filter_func.to_sql(config)

    def to_pure(self, config: FrameToPureConfig) -> str:
        base_cols = [c.get_name() for c in self.__base_frame.columns()]
        if self.__errors == "raise":
            not_found = [col for col in self.__columns if col not in base_cols]  # type: ignore
            if not_found:
                raise KeyError(f"{not_found} not found in axis")

        columns_to_retain = [col for col in base_cols if col not in self.__columns]  # type: ignore
        filter_func = PandasApiFilterFunction(
            base_frame=self.__base_frame,
            items=columns_to_retain,
            like=None,
            regex=None,
            axis=1
        )
        return filter_func.to_pure(config)

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
        return base_cols  # pragma: no cover

    def validate(self) -> bool:
        valid_paramters: int = 0
        if self.__axis is not None:
            if isinstance(self.__axis, (str, int, PyLegendInteger)):
                if self.__axis != 1 and self.__axis != "columns":
                    if self.__axis == 0 or self.__axis == "index":
                        raise NotImplementedError(
                            f"Axis {self.__axis} is not supported for 'drop' function in PandasApi")
                    else:
                        raise ValueError(f"No axis named {self.__axis} for object type Tds DataFrame")
            else:
                raise TypeError(f"No axis named {self.__axis} for object type Tds DataFrame")  # pragma: no cover
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
            def _normalize_columns(columns):  # type: ignore
                if columns is None:
                    return []  # pragma: no cover
                if isinstance(columns, str):
                    return [columns]
                if isinstance(columns, (PyLegendSequence, PyLegendSet)):
                    return list(columns)
                raise TypeError(f"Unsupported type for columns: {type(columns)}")

            valid_paramters += 1
            self.__columns = _normalize_columns(self.__columns)  # type: ignore

        if isinstance(self.__inplace, (bool, PyLegendBoolean)):
            if self.__inplace is False:
                raise NotImplementedError(f"Only inplace=True is supported. Got inplace={self.__inplace!r}")
        else:
            raise TypeError(f"Inplace must be True. Got inplace={self.__inplace!r}")  # pragma: no cover

        if valid_paramters == 0:
            raise ValueError("Need to specify at least one of 'labels' or 'columns'")

        return True

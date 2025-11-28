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
    PyLegendOptional,
    PyLegendUnion,
    PyLegendList,
    TYPE_CHECKING,
)
from pylegend.core.language.pandas_api.pandas_api_aggregate_specification import PyLegendAggInput
from pylegend.core.language.shared.primitives.primitive import PyLegendPrimitiveOrPythonPrimitive
from pylegend.core.tds.pandas_api.frames.pandas_api_base_tds_frame import PandasApiBaseTdsFrame

if TYPE_CHECKING:
    from pylegend.core.tds.pandas_api.frames.pandas_api_tds_frame import PandasApiTdsFrame


class PandasApiGroupbyTdsFrame:
    __base_frame: PandasApiBaseTdsFrame
    __by: PyLegendUnion[str, PyLegendList[str]]
    __level: PyLegendOptional[PyLegendUnion[str, int, PyLegendList[str]]]
    __as_index: bool
    __sort: bool
    __group_keys: bool
    __observed: bool
    __dropna: bool

    __grouping_column_name_list: PyLegendList[str]


    @classmethod
    def name(cls) -> str:
        return "groupby"
    
    def __init__(
            self,
            base_frame: PandasApiBaseTdsFrame,
            by: PyLegendUnion[str, PyLegendList[str]],
            level: PyLegendOptional[PyLegendUnion[str, int, PyLegendList[str]]] = None,
            as_index: bool = False,
            sort: bool = True,
            group_keys: bool = False,
            observed: bool = False,
            dropna: bool = False,
    ) -> None:
        self.__base_frame = base_frame
        self.__by = by
        self.__level = level
        self.__as_index = as_index
        self.__sort = sort
        self.__group_keys = group_keys
        self.__observed = observed
        self.__dropna = dropna

        self.__validate()

    
    def __validate(self) -> None:

        if self.__level is not None:
            raise NotImplementedError(
                f"The 'level' parameter of the groupby function is not supported yet. "
                f"Please specify groupby column names using the 'by' parameter."
            )
        
        if self.__as_index is not False:
            raise NotImplementedError(
                f"The 'as_index' parameter of the groupby function must be False, "
                f"but got: {self.__as_index} (type: {type(self.__as_index).__name__})"
            )
        
        if self.__group_keys is not False:
            raise NotImplementedError(
                f"The 'group_keys' parameter of the groupby function must be False, "
                f"but got: {self.__group_keys} (type: {type(self.__group_keys).__name__})"
            )
        
        if self.__observed is not False:
            raise NotImplementedError(
                f"The 'observed' parameter of the groupby function must be False, "
                f"but got: {self.__observed} (type: {type(self.__observed).__name__})"
            )
        
        if self.__dropna is not False:
            raise NotImplementedError(
                f"The 'dropna' parameter of the groupby function must be False, "
                f"but got: {self.__dropna} (type: {type(self.__dropna).__name__})"
            )

        input_cols: PyLegendList[str] = []
        if isinstance(self.__by, str):
            input_cols = [self.__by]
        elif isinstance(self.__by, PyLegendList) and all(isinstance(col, str) for col in self.__by):
            input_cols = self.__by
        else:
            raise TypeError(
                f"The 'by' parameter in groupby function must be a string or a list of strings."
                f"but got: {self.__by} (type: {type(self.__by).__name__})"
            )
    
        available_columns = {c.get_name() for c in self.__base_frame.columns()}

        for col in input_cols:
            if col not in available_columns:
                raise KeyError(
                    f"Column - '{col}' in groupby function's provided columns list doesn't exist in the current frame. "
                    f"Current frame columns: {available_columns}"
                )

        self.__grouping_column_name_list: PyLegendList[str] = input_cols

    def __getitem__(self, item: PyLegendUnion[str, PyLegendList[str]]) -> "PandasApiGroupbyTdsFrame":
        columns_to_select: PyLegendList[str] = []
        if isinstance(item, str):
            columns_to_select = [item]
        elif isinstance(item, list):
            columns_to_select = item
        else:
            raise TypeError(
                f"Column selection after groupby function must be a string or a list of strings, "
                f"but got: {item} (type: {type(item).__name__})"
            )

        available_columns = {c.get_name() for c in self.__base_frame.columns()}
        for col in columns_to_select:
            if col not in available_columns:
                raise KeyError(
                    f"Column - '{col}' in groupby function's aggregation list doesn't exist in the current frame. "
                    f"Current frame columns: {available_columns}"
                )

        self.__selected_columns = columns_to_select.copy()
        return self
    
    def base_frame(self) -> PandasApiBaseTdsFrame:
        return self.__base_frame
    
    def grouping_column_name_list(self) -> PyLegendList[str]:
        return self.__grouping_column_name_list.copy()
    
    def selected_columns(self) -> PyLegendList[str]:
        return self.__selected_columns.copy()

    def aggregate(
        self,
        func: PyLegendAggInput,
        axis: PyLegendUnion[int, str] = 0,
        *args: PyLegendPrimitiveOrPythonPrimitive,
        **kwargs: PyLegendPrimitiveOrPythonPrimitive
    ) -> "PandasApiTdsFrame":
        from pylegend.core.tds.pandas_api.frames.pandas_api_applied_function_tds_frame import (
            PandasApiAppliedFunctionTdsFrame
        )
        from pylegend.core.tds.pandas_api.frames.functions.aggregate_function import AggregateFunction
        return PandasApiAppliedFunctionTdsFrame(AggregateFunction(
            self,
            func,
            axis,
            *args,
            **kwargs
        ))
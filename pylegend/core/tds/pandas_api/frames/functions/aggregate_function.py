# Copyright 2025 Goldman Sachs
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import numpy as np
import collections.abc
from pylegend._typing import (
    PyLegendSequence,
    PyLegendUnion,
    PyLegendList,
    PyLegendMapping,
)
from pylegend.core.language.pandas_api.pandas_api_aggregate_specification import (
    PyLegendAggList,
    PyLegendAggInput
)
from pylegend.core.language.shared.primitives.primitive import PyLegendPrimitive
from pylegend.core.sql.metamodel import QuerySpecification
from pylegend.core.tds.pandas_api.frames.pandas_api_applied_function_tds_frame import PandasApiAppliedFunction
from pylegend.core.tds.pandas_api.frames.pandas_api_base_tds_frame import PandasApiBaseTdsFrame
from pylegend.core.tds.tds_column import TdsColumn
from pylegend.core.tds.tds_frame import FrameToPureConfig, FrameToSqlConfig


class AggregateFunction(PandasApiAppliedFunction):
    __base_frame: PandasApiBaseTdsFrame
    __func: dict[str, PyLegendAggList]
    __axis: PyLegendUnion[int, str]
    __args: PyLegendSequence[PyLegendPrimitive]
    __kwargs: PyLegendMapping[str, PyLegendPrimitive]

    @classmethod
    def name(cls) -> str:
        return "aggregate_function"  # pragma: no cover
    
    def __init__(
            self,
            base_frame: PandasApiBaseTdsFrame,
            func: PyLegendAggInput,
            axis: PyLegendUnion[int, str],
            *args: PyLegendPrimitive,
            **kwargs: PyLegendPrimitive
    ) -> None:
        self.__base_frame = base_frame
        self.__func_input = func
        self.__axis = axis
        self.__args = args
        self.__kwargs = kwargs

    def to_sql(self, config: FrameToSqlConfig) -> QuerySpecification:
        pass

    def to_pure(self, config: FrameToPureConfig) -> str:
        pass

    def base_frame(self) -> PandasApiBaseTdsFrame:
        return self.__base_frame

    def tds_frame_parameters(self) -> PyLegendList["PandasApiBaseTdsFrame"]:
        return []

    def calculate_columns(self) -> PyLegendSequence["TdsColumn"]:
        return [c.copy() for c in self.__base_frame.columns()]

    def validate(self) -> bool:
        if self.__axis not in [0, "index"]:
            raise NotImplementedError(
                f"The 'axis' parameter of the aggregate function must be 0 or 'index', but got: {self.__axis}"
            )

        self.__func = self.normalize_input_func_to_standard_func(self.__func_input)

        return True

    def normalize_input_func_to_standard_func(
            self,
            func_input: PyLegendAggInput
    ) -> dict[str, PyLegendAggList]:

        column_names = {col.get_name() for col in self.calculate_columns()}

        if isinstance(func_input, collections.abc.Mapping):
            normalized: dict[str, PyLegendAggList] = {}

            for key, value in func_input.items():
                if not isinstance(key, str):
                    raise TypeError(
                        f"Invalid `func` argument for the aggregate function.\n"
                        f"When a dictionary is provided, all keys must be strings.\n"
                        f"But got key: {key!r} (type: {type(key).__name__})\n"
                    )
                if key not in column_names:
                    raise ValueError(
                        f"Invalid `func` argument for the aggregate function.\n"
                        f"When a dictionary is provided, all keys must be column names.\n"
                        f"Available columns are: {sorted(column_names)}\n"
                        f"But got key: {key!r} (type: {type(key).__name__})\n"
                    )

                if isinstance(value, collections.abc.Sequence) and not isinstance(value, str):
                    for i, f in enumerate(value):
                        if not (callable(f) or isinstance(f, str) or isinstance(f, np.ufunc)):
                            raise TypeError(
                                f"Invalid `func` argument for the aggregate function.\n"
                                f"When a dictionary is provided with a key whose value is a list, "
                                f"all elements of the list must be of type callable, str, or np.ufunc.\n"
                                f"But got element at index {i} (0-indexed) of key "
                                f"{key!r}: {f!r} (type: {type(f).__name__})\n"
                            )
                    normalized[key] = value
                else:
                    if not (callable(value) or isinstance(value, str) or isinstance(f, np.ufunc)):
                        raise TypeError(
                            f"Invalid `func` argument for the aggregate function.\n"
                            f"When a dictionary is provided, a key can have its value as a "
                            f"callable, str, np.ufunc, or a list of those.\n"
                            f"But got value of the key {key!r}: {value!r} (type: {type(value).__name__})\n"
                        )
                    normalized[key] = PyLegendAggList([value])

            return normalized

        elif isinstance(func_input, collections.abc.Sequence) and not isinstance(func_input, str):
            for i, f in enumerate(func_input):
                if not (callable(f) or isinstance(f, str) or isinstance(f, np.ufunc)):
                    raise TypeError(
                        f"Invalid `func` argument for the aggregate function.\n"
                        f"When a list is provided, all elements must be of type callable, str, or np.ufunc.\n"
                        f"But got element at index {i} (0-indexed): {f!r} (type: {type(f).__name__})\n"
                    )
            return {col: PyLegendAggList(func_input.copy())
                    for col in column_names}

        elif callable(func_input) or isinstance(func_input, str) or isinstance(func_input, np.ufunc):
            return {col: PyLegendAggList([func_input])
                    for col in column_names}

        else:
            raise TypeError(
                "Invalid `func` argument for aggregate function. "
                "Expected a callable, str, np.ufunc, or a list of those, "
                "or a mapping[str -> those or a list of those]. "
                f"But got: {func_input!r} (type: {type(func_input).__name__})"
            )

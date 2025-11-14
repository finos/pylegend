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


import numpy as np
from pylegend._typing import (
    PyLegendSequence,
    PyLegendUnion,
    PyLegendList,
    PyLegendCallable,
)
from typing import Hashable, Mapping
from pylegend.core.language.shared.primitive_collection import PyLegendPrimitiveCollection
from pylegend.core.language.shared.primitives.primitive import PyLegendPrimitive
from pylegend.core.tds.pandas_api.frames.pandas_api_applied_function_tds_frame import PandasApiAppliedFunction
from pylegend.core.tds.pandas_api.frames.pandas_api_base_tds_frame import PandasApiBaseTdsFrame
from pylegend.core.tds.tds_column import TdsColumn


class AggregateFunction(PandasApiAppliedFunction):
    __base_frame: PandasApiBaseTdsFrame
    __func: Mapping[
        str,
        PyLegendList[
            PyLegendUnion[
                PyLegendCallable[[PyLegendPrimitiveCollection], PyLegendPrimitive],
                str,
                np.ufunc
            ]
        ]
    ]
    __axis: PyLegendUnion[int, str]
    __args: PyLegendSequence[PyLegendPrimitive]
    __kwargs: Mapping[str, PyLegendPrimitive]


    @classmethod
    def name(cls) -> str:
        return "aggregate_function"  # pragma: no cover
    
    def __init__(
            self,
            base_frame: PandasApiBaseTdsFrame,
            func: PyLegendUnion[
                None,
                PyLegendCallable[[PyLegendPrimitiveCollection], PyLegendPrimitive],
                str,
                np.ufunc,
                PyLegendList[
                    PyLegendUnion[
                        PyLegendCallable[[PyLegendPrimitiveCollection], PyLegendPrimitive],
                        str,
                        np.ufunc
                    ]
                ],
                Mapping[
                    Hashable,
                    PyLegendUnion[
                        PyLegendCallable[[PyLegendPrimitiveCollection], PyLegendPrimitive],
                        str,
                        np.ufunc,
                        PyLegendList[
                            PyLegendUnion[
                                PyLegendCallable[[PyLegendPrimitiveCollection], PyLegendPrimitive],
                                str,
                                np.ufunc
                            ]
                        ],
                    ]
                ]
            ],
            axis: PyLegendUnion[int, str],
            *args: PyLegendSequence[PyLegendPrimitive],
            **kwargs: Mapping[str, PyLegendPrimitive]
    ) -> None:
        self.__base_frame = base_frame
        self.__func_input = func
        self.__axis = axis
        self.__args = args
        self.__kwargs = kwargs

    def calculate_columns(self) -> PyLegendSequence["TdsColumn"]:
        return [c.copy() for c in self.__base_frame.columns()]

    def validate(self) -> bool:
        if self.__axis not in [0, "index"]:
            raise NotImplementedError(
                f"The 'axis' parameter of the aggregate function must be 0 or 'index', but got: {self.__axis}"
            )
        
        self.__func = self.map_input_func_to_standard_func(self.__func_input)
        
        return True
    
    def map_input_func_to_standard_func(
            self,
            func_input: PyLegendUnion[
                None,
                PyLegendCallable[[PyLegendPrimitiveCollection], PyLegendPrimitive],
                str,
                np.ufunc,
                PyLegendList[
                    PyLegendUnion[
                        PyLegendCallable[[PyLegendPrimitiveCollection], PyLegendPrimitive],
                        str,
                        np.ufunc
                    ]
                ],
                Mapping[
                    Hashable,
                    PyLegendUnion[
                        PyLegendCallable[[PyLegendPrimitiveCollection], PyLegendPrimitive],
                        str,
                        np.ufunc,
                        PyLegendList[
                            PyLegendUnion[
                                PyLegendCallable[[PyLegendPrimitiveCollection], PyLegendPrimitive],
                                str,
                                np.ufunc
                            ]
                        ],
                    ]
                ]
            ],
    ) -> Mapping[
            str,
            PyLegendList[
                PyLegendUnion[
                    PyLegendCallable[[PyLegendPrimitiveCollection], PyLegendPrimitive],
                    str,
                    np.ufunc
                ]
            ]
        ]:

        if type(func_input) is None:
            raise ValueError("The 'func' parameter of the aggregate function must be a function, or a list of functions, or a mapping of column names to functions. Got: None")
        
        if isinstance(func_input, (PyLegendCallable, str, np.ufunc)):
            return {
                col.get_name(): [func_input]
                for col in self.__base_frame.columns()
            }



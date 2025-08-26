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

from abc import ABCMeta, abstractmethod

from pylegend.core.language import PyLegendBoolean, PyLegendPrimitiveOrPythonPrimitive, PyLegendPrimitiveCollection, \
    PyLegendPrimitive
from pylegend.core.language.legendql_api.legendql_api_custom_expressions import (
    LegendQLApiPrimitive,
    LegendQLApiSortInfo,
    LegendQLApiWindow,
    LegendQLApiPartialFrame,
    LegendQLApiWindowReference,
)
from pylegend.core.language.legendql_api.legendql_api_tds_row import LegendQLApiTdsRow
from pylegend.core.tds.tds_frame import (
    PyLegendTdsFrame
)
from pylegend._typing import (
    PyLegendSequence,
    PyLegendCallable,
    PyLegendUnion,
    PyLegendList,
    PyLegendOptional,
    PyLegendTuple,
)

__all__: PyLegendSequence[str] = [
    "LegendQLApiTdsFrame"
]


class LegendQLApiTdsFrame(PyLegendTdsFrame, metaclass=ABCMeta):

    @abstractmethod
    def head(self, count: int = 5) -> "LegendQLApiTdsFrame":
        pass  # pragma: no cover

    @abstractmethod
    def limit(self, count: int = 5) -> "LegendQLApiTdsFrame":
        pass  # pragma: no cover

    @abstractmethod
    def drop(self, count: int = 5) -> "LegendQLApiTdsFrame":
        pass  # pragma: no cover

    @abstractmethod
    def slice(self, start_row: int, end_row_exclusive: int) -> "LegendQLApiTdsFrame":
        pass  # pragma: no cover

    @abstractmethod
    def distinct(self) -> "LegendQLApiTdsFrame":
        pass  # pragma: no cover

    @abstractmethod
    def select(
            self,
            columns: PyLegendUnion[
                str,
                PyLegendList[str],
                PyLegendCallable[
                    [LegendQLApiTdsRow],
                    PyLegendUnion[LegendQLApiPrimitive, PyLegendList[LegendQLApiPrimitive]]
                ]
            ]
    ) -> "LegendQLApiTdsFrame":
        pass  # pragma: no cover

    @abstractmethod
    def sort(
            self,
            sort_infos: PyLegendUnion[
                str,
                PyLegendList[str],
                PyLegendCallable[
                    [LegendQLApiTdsRow],
                    PyLegendUnion[
                        LegendQLApiPrimitive,
                        LegendQLApiSortInfo,
                        PyLegendList[PyLegendUnion[LegendQLApiPrimitive, LegendQLApiSortInfo]],
                    ]
                ]
            ]
    ) -> "LegendQLApiTdsFrame":
        pass  # pragma: no cover

    @abstractmethod
    def concatenate(self, other: "LegendQLApiTdsFrame") -> "LegendQLApiTdsFrame":
        pass  # pragma: no cover

    @abstractmethod
    def filter(
            self,
            filter_function: PyLegendCallable[[LegendQLApiTdsRow], PyLegendUnion[bool, PyLegendBoolean]]
    ) -> "LegendQLApiTdsFrame":
        pass  # pragma: no cover

    @abstractmethod
    def rename(
            self,
            column_renames: PyLegendUnion[
                PyLegendTuple[str, str],
                PyLegendList[PyLegendTuple[str, str]],
                PyLegendCallable[
                    [LegendQLApiTdsRow],
                    PyLegendUnion[
                        PyLegendTuple[LegendQLApiPrimitive, str],
                        PyLegendList[PyLegendTuple[LegendQLApiPrimitive, str]]
                    ]
                ]
            ]
    ) -> "LegendQLApiTdsFrame":
        pass  # pragma: no cover

    @abstractmethod
    def extend(
            self,
            extend_columns: PyLegendUnion[
                PyLegendTuple[
                    str,
                    PyLegendCallable[[LegendQLApiTdsRow], PyLegendPrimitiveOrPythonPrimitive]
                ],
                PyLegendTuple[
                    str,
                    PyLegendCallable[[LegendQLApiTdsRow], PyLegendPrimitiveOrPythonPrimitive],
                    PyLegendCallable[[PyLegendPrimitiveCollection], PyLegendPrimitive]
                ],
                PyLegendList[
                    PyLegendUnion[
                        PyLegendTuple[
                            str,
                            PyLegendCallable[[LegendQLApiTdsRow], PyLegendPrimitiveOrPythonPrimitive]
                        ],
                        PyLegendTuple[
                            str,
                            PyLegendCallable[[LegendQLApiTdsRow], PyLegendPrimitiveOrPythonPrimitive],
                            PyLegendCallable[[PyLegendPrimitiveCollection], PyLegendPrimitive]
                        ]
                    ]
                ]
            ]
    ) -> "LegendQLApiTdsFrame":
        pass  # pragma: no cover

    @abstractmethod
    def join(
            self,
            other: "LegendQLApiTdsFrame",
            join_condition: PyLegendCallable[
                [LegendQLApiTdsRow, LegendQLApiTdsRow], PyLegendUnion[bool, PyLegendBoolean]
            ],
            join_type: str = 'LEFT_OUTER'
    ) -> "LegendQLApiTdsFrame":
        pass  # pragma: no cover

    @abstractmethod
    def inner_join(
            self,
            other: "LegendQLApiTdsFrame",
            join_condition: PyLegendCallable[
                [LegendQLApiTdsRow, LegendQLApiTdsRow], PyLegendUnion[bool, PyLegendBoolean]
            ]
    ) -> "LegendQLApiTdsFrame":
        pass  # pragma: no cover

    @abstractmethod
    def left_join(
            self,
            other: "LegendQLApiTdsFrame",
            join_condition: PyLegendCallable[
                [LegendQLApiTdsRow, LegendQLApiTdsRow], PyLegendUnion[bool, PyLegendBoolean]
            ]
    ) -> "LegendQLApiTdsFrame":
        pass  # pragma: no cover

    @abstractmethod
    def right_join(
            self,
            other: "LegendQLApiTdsFrame",
            join_condition: PyLegendCallable[
                [LegendQLApiTdsRow, LegendQLApiTdsRow], PyLegendUnion[bool, PyLegendBoolean]
            ]
    ) -> "LegendQLApiTdsFrame":
        pass  # pragma: no cover

    @abstractmethod
    def as_of_join(
            self,
            other: "LegendQLApiTdsFrame",
            match_function: PyLegendCallable[
                [LegendQLApiTdsRow, LegendQLApiTdsRow], PyLegendUnion[bool, PyLegendBoolean]
            ],
            join_condition: PyLegendOptional[
                PyLegendCallable[[LegendQLApiTdsRow, LegendQLApiTdsRow], PyLegendUnion[bool, PyLegendBoolean]]
            ] = None
    ) -> "LegendQLApiTdsFrame":
        pass  # pragma: no cover

    @abstractmethod
    def group_by(
            self,
            grouping_columns: PyLegendUnion[
                str,
                PyLegendList[str],
                PyLegendCallable[
                    [LegendQLApiTdsRow],
                    PyLegendUnion[LegendQLApiPrimitive, PyLegendList[LegendQLApiPrimitive]]
                ]
            ],
            aggregate_specifications: PyLegendUnion[
                PyLegendTuple[
                    str,
                    PyLegendCallable[[LegendQLApiTdsRow], PyLegendPrimitiveOrPythonPrimitive],
                    PyLegendCallable[[PyLegendPrimitiveCollection], PyLegendPrimitive]
                ],
                PyLegendList[
                    PyLegendTuple[
                        str,
                        PyLegendCallable[[LegendQLApiTdsRow], PyLegendPrimitiveOrPythonPrimitive],
                        PyLegendCallable[[PyLegendPrimitiveCollection], PyLegendPrimitive]
                    ]
                ]
            ]
    ) -> "LegendQLApiTdsFrame":
        pass  # pragma: no cover

    @abstractmethod
    def window(
            self,
            partition_by: PyLegendOptional[
                PyLegendUnion[
                    str,
                    PyLegendList[str],
                    PyLegendCallable[
                        [LegendQLApiTdsRow],
                        PyLegendUnion[LegendQLApiPrimitive, PyLegendList[LegendQLApiPrimitive]]
                    ]
                ]
            ] = None,
            order_by: PyLegendOptional[
                PyLegendUnion[
                    str,
                    PyLegendList[str],
                    PyLegendCallable[
                        [LegendQLApiTdsRow],
                        PyLegendUnion[
                            LegendQLApiPrimitive,
                            LegendQLApiSortInfo,
                            PyLegendList[PyLegendUnion[LegendQLApiPrimitive, LegendQLApiSortInfo]],
                        ]
                    ]
                ]
            ] = None
    ) -> LegendQLApiWindow:
        pass

    @abstractmethod
    def window_extend(
            self,
            window: LegendQLApiWindow,
            extend_columns: PyLegendUnion[
                PyLegendTuple[
                    str,
                    PyLegendCallable[
                        [LegendQLApiPartialFrame, LegendQLApiWindowReference, LegendQLApiTdsRow],
                        PyLegendPrimitiveOrPythonPrimitive
                    ]
                ],
                PyLegendTuple[
                    str,
                    PyLegendCallable[
                        [LegendQLApiPartialFrame, LegendQLApiWindowReference, LegendQLApiTdsRow],
                        PyLegendPrimitiveOrPythonPrimitive
                    ],
                    PyLegendCallable[[PyLegendPrimitiveCollection], PyLegendPrimitive]
                ],
                PyLegendList[
                    PyLegendUnion[
                        PyLegendTuple[
                            str,
                            PyLegendCallable[
                                [LegendQLApiPartialFrame, LegendQLApiWindowReference, LegendQLApiTdsRow],
                                PyLegendPrimitiveOrPythonPrimitive
                            ]
                        ],
                        PyLegendTuple[
                            str,
                            PyLegendCallable[
                                [LegendQLApiPartialFrame, LegendQLApiWindowReference, LegendQLApiTdsRow],
                                PyLegendPrimitiveOrPythonPrimitive
                            ],
                            PyLegendCallable[[PyLegendPrimitiveCollection], PyLegendPrimitive]
                        ]
                    ]
                ]
            ]
    ) -> "LegendQLApiTdsFrame":
        pass

    @abstractmethod
    def project(
            self,
            project_columns: PyLegendUnion[
                PyLegendTuple[
                    str,
                    PyLegendCallable[[LegendQLApiTdsRow], PyLegendPrimitiveOrPythonPrimitive]
                ],
                PyLegendList[
                    PyLegendTuple[
                        str,
                        PyLegendCallable[[LegendQLApiTdsRow], PyLegendPrimitiveOrPythonPrimitive]
                    ]
                ]
            ]
    ) -> "LegendQLApiTdsFrame":
        pass  # pragma: no cover

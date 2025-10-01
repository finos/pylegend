# Copyright 2023 Goldman Sachs
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
from pylegend._typing import (
    PyLegendSequence,
    PyLegendList,
)
from pylegend.core.tds.abstract.frames.applied_function_tds_frame import AppliedFunction, AppliedFunctionTdsFrame
from pylegend.core.sql.metamodel import QuerySpecification
from pylegend.core.tds.tds_column import TdsColumn
from pylegend.core.tds.tds_frame import FrameToSqlConfig
from pylegend.core.tds.tds_frame import FrameToPureConfig
from pylegend.core.tds.pandas_api.frames.pandas_api_base_tds_frame import PandasApiBaseTdsFrame


__all__: PyLegendSequence[str] = [
    "PandasApiAppliedFunctionTdsFrame",
    "PandasApiAppliedFunction",
]


class PandasApiAppliedFunction(AppliedFunction, metaclass=ABCMeta):
    @abstractmethod
    def tds_frame_parameters(self) -> PyLegendSequence["PandasApiBaseTdsFrame"]:
        pass  # pragma: no cover


class PandasApiAppliedFunctionTdsFrame(PandasApiBaseTdsFrame):
    __applied_function: PandasApiAppliedFunction

    def __init__(self, applied_function: PandasApiAppliedFunction):
        applied_function.validate()
        super().__init__(columns=applied_function.calculate_columns())
        self.__applied_function = applied_function

    def to_sql_query_object(self, config: FrameToSqlConfig) -> QuerySpecification:
        return self.__applied_function.to_sql(config)

    def to_pure(self, config: FrameToPureConfig) -> str:
        return self.__applied_function.to_pure(config)

    def get_all_tds_frames(self) -> PyLegendList["PandasApiBaseTdsFrame"]:
        return [
            y
            for x in [self.__applied_function.base_frame()] + self.__applied_function.tds_frame_parameters()
            for y in x.get_all_tds_frames()
        ] + [self]

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
from pylegend.core.sql.metamodel import QuerySpecification
from pylegend.core.tds.tds_column import TdsColumn
from pylegend.core.tds.tds_frame import FrameToSqlConfig
from pylegend.core.tds.legend_api.frames.legend_api_base_tds_frame import LegendApiBaseTdsFrame


__all__: PyLegendSequence[str] = [
    "LegendApiAppliedFunctionTdsFrame",
    "LegendApiAppliedFunction",
]


class LegendApiAppliedFunction(metaclass=ABCMeta):
    @classmethod
    @abstractmethod
    def name(cls) -> str:
        pass  # pragma: no cover

    @abstractmethod
    def to_sql(self, config: FrameToSqlConfig) -> QuerySpecification:
        pass  # pragma: no cover

    @abstractmethod
    def base_frame(self) -> LegendApiBaseTdsFrame:
        pass  # pragma: no cover

    @abstractmethod
    def tds_frame_parameters(self) -> PyLegendList["LegendApiBaseTdsFrame"]:
        pass  # pragma: no cover

    @abstractmethod
    def calculate_columns(self) -> PyLegendSequence["TdsColumn"]:
        pass  # pragma: no cover

    @abstractmethod
    def validate(self) -> bool:
        pass  # pragma: no cover


class LegendApiAppliedFunctionTdsFrame(LegendApiBaseTdsFrame):
    __applied_function: LegendApiAppliedFunction

    def __init__(self, applied_function: LegendApiAppliedFunction):
        applied_function.validate()
        super().__init__(columns=applied_function.calculate_columns())
        self.__applied_function = applied_function

    def to_sql_query_object(self, config: FrameToSqlConfig) -> QuerySpecification:
        return self.__applied_function.to_sql(config)

    def get_all_tds_frames(self) -> PyLegendList["LegendApiBaseTdsFrame"]:
        return [
            y
            for x in [self.__applied_function.base_frame()] + self.__applied_function.tds_frame_parameters()
            for y in x.get_all_tds_frames()
        ] + [self]
